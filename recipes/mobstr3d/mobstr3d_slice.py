"""Single-slice mobstr3D DENSE preprocessing: segmentation, contours and phase unwrapping.

mobstr3D's DICOM preprocessing (mobstr3D.preprocessing.input_DICOM) indexes a whole
multi-slice DENSE exam before it segments and unwraps. Segmentation, contour extraction,
seed selection and the spatiotemporal phase unwrap only ever use one slice's cine, so
this module runs them on a single slice (magnitude plus x/y/z encoded phase, all frames)
as soon as that slice is acquired. Slice correction and the pseudo-cardiac transform
need the whole stack and stay in mobstr3D.

The upstream functions are called unchanged where they work on arrays (seed selection,
3D/2D flood-fill unwrapping, contour extraction). File-based steps are replaced by their
array equivalents: nnU-Net receives each magnitude frame exactly as it would read the
NIfTI mobstr3D writes (SimpleITK axis order, unit spacing), and displacements use the
same scaling and RCswap/RCSflip handling as mobstr3D's extract_displacements.

Command line: mobstr3d-slice --dicom-dir DICOMS --output-dir OUT processes every slice in
a folder of DENSE DICOMs independently and writes one folder per slice.
"""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from importlib.metadata import version as package_version
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")
# nnU-Net warns when these are unset; inference never reads them
for _name in ("nnUNet_raw", "nnUNet_preprocessed", "nnUNet_results"):
    os.environ.setdefault(_name, ".")

import cv2  # noqa: E402
import numpy as np  # noqa: E402

from mobstr3D.preprocessing.DICOM.phaseunwrapping.unwrap import get_seed_point, unwrap_phase_3d_floodfill  # noqa: E402
from mobstr3D.preprocessing.DICOM.segmentation import process_segmentation  # noqa: E402

MODEL_DIR = os.environ.get(
    "MOBSTR3D_MODEL",
    "/opt/mobstr3D/src/mobstr3D/preprocessing/DICOM/segmentation/modelESv2/nnUNetTrainer__nnUNetPlans__2d",
)
ENCODINGS = ("MAG", "XPHA", "YPHA", "ZPHA")
PHASE_KEYS = {"XPHA": "x", "YPHA": "y", "ZPHA": "z"}
PHASE_QUALITY_METHODS = ("cropped", "masked")
CONTOUR_METHODS = ("marching_squares", "cv2_find_contours")
DEBUG_FLAGS = (
    "debug_dense_series", "debug_segmentation", "debug_segmentation_all", "debug_contours",
    "debug_masks_from_contours", "debug_seedframe", "debug_seedselection", "debug_phasequality",
    "debug_phaseunwrapping_3D", "debug_phaseunwrapping_2D", "debug_phaseunwrapping",
    "debug_displacements", "debug_displacements_mov",
)


class UpstreamError(RuntimeError):
    """A mobstr3D function logged an error and called sys.exit."""


class _Logger:
    """The loguru-style interface mobstr3D expects, forwarding to logging."""

    def __init__(self, log):
        self._log = log
        self.last_error = None

    def info(self, message):
        self._log.info(message)

    success = info

    def warning(self, message):
        self._log.warning(message)

    def error(self, message):
        self.last_error = message
        self._log.error(message)


def _upstream(logger, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except SystemExit:
        raise UpstreamError(logger.last_error or f"mobstr3D {function.__name__} failed") from None


@dataclass(frozen=True)
class Settings:
    frame_of_seed: int = 5
    end_frame: int = -1  # -1: last acquired frame
    phase_quality_method: str = "cropped"
    label_to_contour_method: str = "marching_squares"

    def validate(self):
        if type(self.frame_of_seed) is not int or self.frame_of_seed < 0:
            raise ValueError("frame_of_seed must be a non-negative integer")
        if type(self.end_frame) is not int or (self.end_frame != -1 and self.end_frame < self.frame_of_seed):
            raise ValueError("end_frame must be -1 (last frame) or at least frame_of_seed")
        if self.phase_quality_method not in PHASE_QUALITY_METHODS:
            raise ValueError(f"phase_quality_method must be one of {PHASE_QUALITY_METHODS}")
        if self.label_to_contour_method not in CONTOUR_METHODS:
            raise ValueError(f"label_to_contour_method must be one of {CONTOUR_METHODS}")
        return self

    def upstream_config(self, end_frame):
        """The subset of a mobstr3D config.toml that the single-slice steps read."""
        return {
            "preprocessing_unwrapping": {
                "mask_flag": True,
                "seed_point_selection": "auto",
                "phase_quality_method": self.phase_quality_method,
                "frame_of_seed": self.frame_of_seed,
                "define_end_frame": True,
                "end_frame": end_frame,
            },
            "preprocessing_segmentation": {"label_to_contour_method": self.label_to_contour_method},
            "model_parameters": {"num_of_elements": [8, 1, 1]},
            "debug_flags": {name: False for name in DEBUG_FLAGS},
        }


@dataclass(frozen=True)
class DenseParameters:
    """DENSE encoding parameters from the phase images' ImageComments."""
    encoding_frequency: float  # cycles/mm
    scale: float
    rc_swap: int
    rcs_flip: tuple
    frames: int

    @classmethod
    def from_comment(cls, comment):
        fields = {}
        for name, pattern in (
            ("scale", r"Scale:([-+0-9.eE]+)"),
            ("encoding_frequency", r"EncFreq:([-+0-9.eE]+)"),
            ("rc_swap", r"RCswap:(\d+)"),
            ("rcs_flip", r"RCSflip:(\d+)/(\d+)/(\d+)"),
            ("frames", r"Phs:\d+/(\d+)"),
        ):
            match = re.search(pattern, comment or "")
            if match is None:
                raise ValueError(f"DENSE phase ImageComments has no {pattern.split(':')[0]} field: {comment!r}")
            fields[name] = match.groups()
        parameters = cls(
            encoding_frequency=float(fields["encoding_frequency"][0]),
            scale=float(fields["scale"][0]),
            rc_swap=int(fields["rc_swap"][0]),
            rcs_flip=tuple(int(value) for value in fields["rcs_flip"]),
            frames=int(fields["frames"][0]),
        )
        if parameters.rc_swap not in (0, 1):
            raise ValueError(f"Unsupported DENSE RCswap {parameters.rc_swap}")
        if not parameters.encoding_frequency or not parameters.scale:
            raise ValueError("DENSE EncFreq and Scale must be non-zero")
        return parameters


def encoding_from_comment(comment):
    """MAG/XPHA/YPHA/ZPHA from a DENSE ImageComments string, as mobstr3D's index_DENSE_series."""
    comment = (comment or "").upper()
    if "DENSE" not in comment:
        return None
    if "MAG" in comment:
        return "MAG"
    for axis in "XYZ":
        if f"{axis}-ENC PHA" in comment:
            return f"{axis}PHA"
    return None


def frame_from_comment(comment):
    match = re.search(r"Phs:(\d+)/(\d+)", comment or "")
    return (int(match.group(1)), int(match.group(2))) if match else None


@dataclass
class DenseSlice:
    """One DENSE slice: stored pixel values per encoding, indexed by cardiac frame."""
    images: dict  # encoding -> {frame: (rows, cols) array of stored values}
    pixel_spacing: tuple  # DICOM PixelSpacing [row, col] in mm
    slice_location: float  # ImagePositionPatient projected onto the row x column normal, mm
    inverted: bool  # SliceLocation is the negative of slice_location (mobstr3D flag_slice_location)
    parameters: DenseParameters
    label: str = "Slice_0"

    def frame_range(self, settings):
        available = sorted(set.intersection(*(set(self.images.get(enc, {})) for enc in ENCODINGS)))
        if not available:
            raise ValueError(f"{self.label}: no frame has all of {ENCODINGS}")
        end = available[-1] if settings.end_frame == -1 else settings.end_frame
        frames = list(range(settings.frame_of_seed, end + 1))
        missing = [frame for frame in frames if frame not in available]
        if missing:
            raise ValueError(f"{self.label}: frames {missing} are missing one or more of {ENCODINGS}")
        return frames

    def stack(self, encoding, frames):
        return np.stack([np.asarray(self.images[encoding][frame], dtype=np.float64) for frame in frames])


def slice_location(position, row_dir, col_dir):
    """mobstr3D get_slice_location: position projected onto the normal of the image plane."""
    return float(np.dot(np.asarray(position, float), np.cross(np.asarray(row_dir, float), np.asarray(col_dir, float))))


def slice_inverted(computed, header_slice_location):
    """mobstr3D flag_slice_location, comparing against the scanner's SliceLocation."""
    if np.isclose(computed, header_slice_location, atol=1e-3):
        return False
    if np.isclose(computed, -header_slice_location, atol=1e-3):
        return True
    raise ValueError(f"Calculated slice location {computed} is neither equal nor inverted to SliceLocation {header_slice_location}")


class Segmenter:
    """modelESv2 nnU-Net (fold 0, checkpoint_final), configured as mobstr3D init_nnUNetv2."""

    def __init__(self, model_dir=MODEL_DIR, device=None):
        import torch
        from nnunetv2.inference.predict_from_raw_data import nnUNetPredictor

        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = device
        self.predictor = nnUNetPredictor(
            tile_step_size=0.5, use_gaussian=True, use_mirroring=True, perform_everything_on_device=True,
            device=torch.device(device), verbose=False, verbose_preprocessing=False, allow_tqdm=False,
        )
        self.predictor.initialize_from_trained_model_folder(model_dir, use_folds=(0,), checkpoint_name="checkpoint_final.pth")

    def __call__(self, magnitude):
        """(frames, rows, cols) magnitude -> (frames, rows, cols) labels: 0 background, 1 myocardium, 2 blood pool.

        mobstr3D saves each frame's pixel_array as a 2D NIfTI with an identity affine; nnU-Net's
        SimpleITK reader returns it transposed with spacing (999, 1, 1). Frames are predicted one at
        a time because z-score normalisation is per image.
        """
        labels = []
        for plane in magnitude:
            image = np.ascontiguousarray(plane.T, dtype=np.float32)[None, None]
            seg = self.predictor.predict_single_npy_array(image, {"spacing": [999.0, 1.0, 1.0]})
            labels.append(np.asarray(seg)[0].T)
        return np.stack(labels).astype(np.uint8)


def _largest_component(mask):
    count, labels = cv2.connectedComponents(mask, connectivity=8)
    if count - 1 > 1:
        unique, counts = np.unique(labels[labels != 0], return_counts=True)
        return labels == unique[np.argmax(counts)]
    return labels == 1


def contours_from_labels(labels, method, logger):
    """Endocardial and epicardial contours (pixel x=column, y=row) from one frame's labels.

    Array version of mobstr3D create_contours_from_labels: keep the largest myocardium and
    blood pool regions, then extract both boundaries. Returns the image coordinate system
    points that mobstr3D writes to contours_ics.json.
    """
    labels = labels.astype(np.uint8)
    cleaned = np.zeros_like(labels)
    cleaned[_largest_component((labels == 2).astype(np.uint8))] = 2
    cleaned[_largest_component((labels == 1).astype(np.uint8))] = 1
    background = (cleaned == 0).astype(np.uint8) * 255
    myocardium = (cleaned == 1).astype(np.uint8) * 255
    bloodpool = (cleaned == 2).astype(np.uint8) * 255
    if method == "marching_squares":
        endo = process_segmentation.marching_squares_endo(cv2.bitwise_or(background, myocardium), logger)
        epi = process_segmentation.marching_squares_epi(cv2.bitwise_or(myocardium, bloodpool), logger)
    else:
        endo = process_segmentation.cv2_find_contours_endo(bloodpool, logger)
        epi = process_segmentation.cv2_find_contours_epi(cv2.bitwise_not(cv2.bitwise_or(myocardium, bloodpool)), logger)
    return np.asarray(endo, dtype=np.float64), np.asarray(epi, dtype=np.float64)


def displacements_from_phase(uw, dense):
    """mobstr3D extract_displacements for one frame: unwrapped phase (rad) -> mm, image coordinates.

    uw: {"x", "y", "z"} -> (rows, cols) unwrapped phase, NaN outside the unwrapped region.
    Returns (locations, displacements, valid): (N, 3) arrays in mm for the N pixels unwrapped in all
    three directions (row-major order), and the (rows, cols) mask of those pixels.
    """
    p = dense.parameters
    ps0, ps1 = dense.pixel_spacing
    cycles = 2 * np.pi * p.encoding_frequency * p.scale
    xfac, yfac, zfac = 1 / (cycles * ps0), 1 / (cycles * ps1), 1 / (cycles * ps0)
    first, second = (uw["x"], uw["y"]) if p.rc_swap == 0 else (uw["y"], uw["x"])
    rflip, cflip, sflip = (-1.0 if flip else 1.0 for flip in p.rcs_flip)
    dx = rflip * first * xfac * ps0
    dy = cflip * second * yfac * ps1
    dz = sflip * uw["z"] * zfac * ps0
    if dense.inverted:
        dz = -dz
    valid = ~np.isnan(uw["x"]) & ~np.isnan(uw["y"]) & ~np.isnan(uw["z"])
    rows, cols = np.nonzero(valid)
    locations = np.column_stack(((cols + 0.5) * ps0, (rows + 0.5) * ps1, np.full(rows.shape, dense.slice_location)))
    displacements = np.column_stack((dx[valid], dy[valid], dz[valid]))
    return locations, displacements, valid


@dataclass
class SliceResult:
    frames: list
    labels: np.ndarray  # (frames, rows, cols) uint8 nnU-Net labels
    displacement: np.ndarray  # (frames, rows, cols, 3) mm, NaN where not unwrapped
    locations: dict  # "Frame_n" -> (N, 3) mm, mobstr3D image coordinate system
    displacements: dict  # "Frame_n" -> (N, 3) mm
    contours: dict  # {"endo"|"epi": {"Frame_n": (M, 2) pixel x/y or None}}
    seed_points: dict  # "Frame_n" -> {"x"|"y"|"z": [(row, col), ...]}
    imaging_parameters: dict


def process_slice(dense, settings, segmenter, log=logging.getLogger("mobstr3d")):
    """Segment, contour and unwrap one DENSE slice. Returns a SliceResult."""
    settings.validate()
    logger = _Logger(log)
    frames = dense.frame_range(settings)
    config = settings.upstream_config(frames[-1])
    imaging_parameters = {
        "PixelSpacing": np.asarray(dense.pixel_spacing, dtype=float),
        "EncodingFrequency": dense.parameters.encoding_frequency,
        "Scale": dense.parameters.scale,
        "Frames": dense.parameters.frames,
        "DENSE": {"RCswap": str(dense.parameters.rc_swap), "RCSflip": "/".join(map(str, dense.parameters.rcs_flip))},
    }
    log.info("%s: segmenting frames %d-%d", dense.label, frames[0], frames[-1])
    labels = segmenter(dense.stack("MAG", frames))

    contours = {"endo": {}, "epi": {}}
    for index, frame in enumerate(frames):
        key = f"Frame_{frame}"
        try:
            endo, epi = _upstream(logger, contours_from_labels, labels[index], settings.label_to_contour_method, logger)
        except (UpstreamError, ValueError, ZeroDivisionError, IndexError) as error:
            log.warning("%s %s: no contours (%s)", dense.label, key, error)
            endo = epi = None
        contours["endo"][key], contours["epi"][key] = endo, epi

    # Wrapped phase in radians, as mobstr3D: stored values 0..4095 -> [-pi, pi]
    wrapped = {}
    for encoding, key in PHASE_KEYS.items():
        stored = dense.stack(encoding, frames)
        if stored.min() < 0 or stored.max() > 4095:
            raise ValueError(f"{dense.label}: {encoding} values must be stored phase 0..4095")
        wrapped[key] = np.moveaxis(stored / 4095.0 * (2 * np.pi) - np.pi, 0, -1)  # (rows, cols, frames)
    mask_3d = np.moveaxis(labels, 0, -1).astype(np.float64)

    seed_frame = f"Frame_{settings.frame_of_seed}"
    seed = _upstream(logger, get_seed_point, wrapped["x"][:, :, 0], wrapped["y"][:, :, 0], wrapped["z"][:, :, 0],
                     labels[0], dense.label, settings.frame_of_seed, imaging_parameters, None, config, logger)
    seed_points = {seed_frame: seed}
    args = {"mask": None, "mask_3D": mask_3d, "pixel_size": imaging_parameters["PixelSpacing"],
            "connectivity": 4, "searchradius": 2, "slice_key": dense.label}
    unwrapped = {}
    for key in ("x", "y", "z"):
        seed_points, _, unwrapped[key] = _upstream(
            logger, unwrap_phase_3d_floodfill, wrapped[key], args, phase_key=key, config=config,
            mylogger=logger, seed_points_3D=seed_points)

    displacement = np.full(labels.shape + (3,), np.nan)
    locations, displacements = {}, {}
    for index, frame in enumerate(frames):
        uw = {key: unwrapped[key][:, :, index] for key in ("x", "y", "z")}
        locs, disps, valid = displacements_from_phase(uw, dense)
        locations[f"Frame_{frame}"], displacements[f"Frame_{frame}"] = locs, disps
        displacement[index][valid] = disps
    log.info("%s: unwrapped %d frames, %d myocardial pixels at the seed frame", dense.label, len(frames),
             len(locations[seed_frame]))
    return SliceResult(frames, labels, displacement, locations, displacements, contours, seed_points, imaging_parameters)


def jsonable(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def load_dicom_slices(folder, log=logging.getLogger("mobstr3d")):
    """Group a folder of DENSE DICOMs into DenseSlices, ordered base to apex as mobstr3D's Slice_n."""
    import pydicom

    by_location = {}
    for path in sorted(Path(folder).rglob("*")):
        if not path.is_file() or path.suffix.lower() not in (".dcm", ".ima"):
            continue
        ds = pydicom.dcmread(path)
        comment = str(getattr(ds, "ImageComments", ""))
        encoding = encoding_from_comment(comment)
        if encoding is None:
            log.info("Skipping %s: not a DENSE magnitude or phase image", path.name)
            continue
        iop = [float(v) for v in ds.ImageOrientationPatient]
        location = slice_location(ds.ImagePositionPatient, iop[:3], iop[3:])
        entry = by_location.setdefault(round(location, 3), {"images": {}, "datasets": []})
        frame = int(ds.InstanceNumber) - 1
        entry["images"].setdefault(encoding, {})[frame] = ds.pixel_array
        entry["datasets"].append((encoding, ds, location))
    slices = []
    for index, key in enumerate(sorted(by_location, reverse=True)):
        entry = by_location[key]
        phase = next((ds for enc, ds, _ in entry["datasets"] if enc != "MAG"), None)
        if phase is None:
            raise ValueError(f"Slice at {key} mm has no DENSE phase images")
        mag = next(ds for enc, ds, _ in entry["datasets"] if enc == "MAG")
        computed = next(loc for enc, _, loc in entry["datasets"] if enc == "MAG")
        slices.append(DenseSlice(
            images=entry["images"],
            pixel_spacing=tuple(float(v) for v in phase.PixelSpacing),
            slice_location=computed,
            inverted=slice_inverted(computed, float(mag.SliceLocation)),
            parameters=DenseParameters.from_comment(str(phase.ImageComments)),
            label=f"Slice_{index}",
        ))
    return slices


def write_result(result, dense, output_dir):
    """Write one slice's outputs in mobstr3D's own file formats."""
    import nibabel as nib

    output_dir.mkdir(parents=True, exist_ok=True)
    nib.save(nib.Nifti1Image(np.moveaxis(result.labels, 0, -1), np.eye(4)), output_dir / "segmentation.nii.gz")
    files = {
        "displacements_ics.json": {"locations": result.locations, "displacements": result.displacements},
        "contours_ics.json": result.contours,
        "seed_points.json": result.seed_points,
        "imaging_parameters.json": {**result.imaging_parameters, "SliceLocation": dense.slice_location,
                                    "SliceLocationInverted": dense.inverted, "FramesProcessed": result.frames},
    }
    for name, content in files.items():
        (output_dir / name).write_text(json.dumps(jsonable(content), indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(description="mobstr3D single-slice DENSE segmentation and phase unwrapping")
    parser.add_argument("--version", action="version", version=f"mobstr3D {package_version('mobstr3D')}")
    parser.add_argument("--dicom-dir", type=Path, required=True, help="folder of DENSE magnitude and x/y/z phase DICOMs")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--frame-of-seed", type=int, default=Settings.frame_of_seed)
    parser.add_argument("--end-frame", type=int, default=Settings.end_frame, help="last frame to process; -1 for the last acquired frame")
    parser.add_argument("--phase-quality", choices=PHASE_QUALITY_METHODS, default=Settings.phase_quality_method)
    parser.add_argument("--contour-method", choices=CONTOUR_METHODS, default=Settings.label_to_contour_method)
    parser.add_argument("--slice", action="append", default=None, help="only process this Slice_n (repeatable)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = Settings(args.frame_of_seed, args.end_frame, args.phase_quality, args.contour_method).validate()
    slices = load_dicom_slices(args.dicom_dir)
    if args.slice:
        slices = [dense for dense in slices if dense.label in args.slice]
    if not slices:
        parser.error("no DENSE slices found")
    segmenter = Segmenter()
    for dense in slices:
        result = process_slice(dense, settings, segmenter)
        write_result(result, dense, args.output_dir / dense.label)
        print(f"{dense.label}: {len(result.frames)} frames unwrapped -> {args.output_dir / dense.label}")


if __name__ == "__main__":
    sys.exit(main())
