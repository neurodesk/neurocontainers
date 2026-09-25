"""Capture-first FIRE experiments. No assumptions about undocumented ICE taps."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import logging
import os
import platform
import traceback
from copy import deepcopy
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import ismrmrd
import numpy as np


def plain(value: Any) -> Any:
    """Include every fixed header field, including vendor/user fields."""
    if isinstance(value, ctypes.Array):
        return [plain(x) for x in value]
    if isinstance(value, ctypes.Structure):
        return {name: plain(getattr(value, name)) for name, *_ in value._fields_}
    return value


def parameters(config: Any) -> dict[str, Any]:
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return {}
    value = config.get("parameters") if isinstance(config, dict) else None
    return value if isinstance(value, dict) else {}


def enabled(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def calibration(acq: ismrmrd.Acquisition) -> bool:
    return any(
        acq.is_flag_set(getattr(ismrmrd, flag))
        for flag in (
            "ACQ_IS_PARALLEL_CALIBRATION",
            "ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING",
        )
    )


class Capture:
    def __init__(self, root: str | Path, app: str, config: Any, metadata: Any) -> None:
        self.path = Path(root) / (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "-"
            + app
            + "-"
            + uuid4().hex
        )
        self.path.mkdir(parents=True, mode=0o700)
        self.summary = {
            "application": app,
            "status": "recording",
            "counts": {},
            "flags": {},
            "measurement_uids": [],
            "warnings": [],
            "input_domain": parameters(config).get("inputdomain", "unknown"),
            "close_message_received": False,
            "runtime": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "recipe_version": os.environ.get("FIRE_POC_VERSION", "source"),
                "source_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "packages": {
                    p: version(p) for p in ("numpy", "ismrmrd", "h5py", "pydicom")
                },
            },
            "acquisition_layouts": {},
        }
        (self.path / "config.json").write_text(
            json.dumps(config, indent=2, default=str)
        )
        xml = (
            metadata
            if isinstance(metadata, (str, bytes))
            else ismrmrd.xsd.ToXML(metadata)
        )
        if isinstance(xml, str):
            xml = xml.encode()
        (self.path / "metadata.xml").write_bytes(xml)
        self.dataset = ismrmrd.Dataset(str(self.path / "input.h5"))
        self.dataset.write_xml_header(xml)
        self.events = (self.path / "events.jsonl").open("w")
        self.save_summary()

    def save_summary(self) -> None:
        temp = self.path / "summary.tmp"
        temp.write_text(json.dumps(self.summary, indent=2))
        temp.replace(self.path / "summary.json")

    def record(self, item: Any) -> None:
        kind = type(item).__name__
        counts = self.summary["counts"]
        index = counts.get(kind, 0)
        counts[kind] = index + 1
        event = {
            "type": kind,
            "index": index,
            "received_utc": datetime.now(timezone.utc).isoformat(),
        }
        if isinstance(item, ismrmrd.Acquisition):
            self.dataset.append_acquisition(item)
            layout = json.dumps(
                {
                    "encoding": int(item.encoding_space_ref),
                    "samples": int(item.number_of_samples),
                    "channels": int(item.active_channels),
                    "trajectory_dimensions": int(item.trajectory_dimensions),
                    "discard_pre": int(item.discard_pre),
                    "discard_post": int(item.discard_post),
                    "center_sample": int(item.center_sample),
                },
                sort_keys=True,
            )
            layouts = self.summary["acquisition_layouts"]
            layouts[layout] = layouts.get(layout, 0) + 1
            flags = [
                name
                for name in dir(ismrmrd)
                if name.startswith("ACQ_") and item.is_flag_set(getattr(ismrmrd, name))
            ]
            event["flag_names"] = flags
            for flag in flags:
                self.summary["flags"][flag] = self.summary["flags"].get(flag, 0) + 1
            uid = int(item.measurement_uid)
            if uid not in self.summary["measurement_uids"]:
                self.summary["measurement_uids"].append(uid)
        elif isinstance(item, ismrmrd.Image):
            # Separate series and shape to retain heterogeneous image streams.
            key = f"images_{item.image_series_index}_" + "_".join(
                map(str, item.data.shape)
            )
            key += f"_{item.data_type}"
            self.dataset.append_image(key, item)
            event["dataset"] = key
            event["attributes"] = item.attribute_string
        elif isinstance(item, ismrmrd.Waveform):
            self.dataset.append_waveform(item)
        else:
            event["value"] = str(item)
        if hasattr(item, "getHead"):
            event["header"] = plain(item.getHead())
        if hasattr(item, "data"):
            event["shape"] = list(item.data.shape)
            event["dtype"] = str(item.data.dtype)
            event["finite"] = bool(np.isfinite(item.data).all())
            event["abs_max"] = (
                float(np.max(np.abs(item.data.astype(np.complex128))))
                if item.data.size and event["finite"]
                else None
            )
        self.events.write(json.dumps(event) + "\n")
        self.events.flush()
        self.dataset._file.flush()
        if sum(counts.values()) % 100 == 0:
            self.save_summary()

    def close(self, status: str, error: str | None = None) -> None:
        self.summary["status"] = status
        if error:
            self.summary["error"] = error
        if not self.summary["counts"].get("Image"):
            self.summary["warnings"].append(
                "No original images received. Native ICE images require a "
                "scanner-side parallel branch."
            )
        if not any("CALIBRATION" in flag for flag in self.summary["flags"]):
            self.summary["warnings"].append(
                "No calibration flags received; inspect the ICE export configuration."
            )
        self.events.close()
        self.dataset.close()
        self.save_summary()


def ifft_centered(data: np.ndarray, axes: tuple[int, ...]) -> np.ndarray:
    return np.fft.fftshift(
        np.fft.ifftn(np.fft.ifftshift(data, axes=axes), axes=axes, norm="ortho"),
        axes=axes,
    )


def fft_centered(data: np.ndarray, axes: tuple[int, ...]) -> np.ndarray:
    return np.fft.fftshift(
        np.fft.fftn(np.fft.ifftshift(data, axes=axes), axes=axes, norm="ortho"),
        axes=axes,
    )


def readout_oversampling(acq: ismrmrd.Acquisition, width: int) -> int:
    """OpenRecon states encodedSpace at base resolution while the ADC keeps the
    vendor readout oversampling, so the factor comes from the retained samples."""
    retained = (
        int(acq.number_of_samples) - int(acq.discard_pre) - int(acq.discard_post)
    )
    factor, remainder = divmod(retained, width)
    return factor if factor >= 1 and remainder == 0 else 0


def remove_readout_oversampling(
    data: np.ndarray, width: int, domain: str
) -> np.ndarray:
    """Crop the readout in image space; cropping k-space would shrink the FOV."""
    samples = data.shape[-1]
    if samples == width:
        return data
    start = (samples - width) // 2
    if domain == "x-ky":
        return data[..., start : start + width]
    cropped = ifft_centered(data, (-1,))[..., start : start + width]
    return fft_centered(cropped, (-1,)).astype(np.complex64, copy=False)


def rss(data: np.ndarray, domain: str) -> np.ndarray:
    # The current ICE hybrid-space tap needs the negative-exponent PE transform.
    coils = (
        fft_centered(data, (-2,)) if domain == "x-ky"
        else ifft_centered(data, (-2, -1))
    )
    return np.sqrt(np.sum(np.abs(coils) ** 2, axis=0))


def group_key(acq: ismrmrd.Acquisition) -> tuple:
    """Collect candidate frames; ACS segment coverage is resolved separately."""
    return (
        int(acq.measurement_uid),
        int(acq.encoding_space_ref),
        *(
            int(getattr(acq.idx, field))
            for field in (
                "slice",
                "repetition",
                "contrast",
                "phase",
                "set",
                "average",
            )
        ),
        calibration(acq),
    )


def acs_frames(group: list[ismrmrd.Acquisition]) -> list[list[ismrmrd.Acquisition]]:
    """Separate repeated ACS regions without breaking disjoint multi-shot data.

    Only identical, unique PE coverage in every segment establishes separate
    frames. Other layouts retain strict duplicate checks in assemble().
    """
    segments = {}
    for acq in group:
        segments.setdefault(int(acq.idx.segment), []).append(acq)
    coverage = [
        {int(acq.idx.kspace_encode_step_1) for acq in shot}
        for shot in segments.values()
    ]
    if (
        len(segments) > 1
        and all(lines == coverage[0] for lines in coverage)
        and all(len(shot) == len(lines) for shot, lines in zip(segments.values(), coverage))
    ):
        logging.info(
            "ACS repeated segment coverage: slice=%d, segments=%s, PE lines=%d; "
            "reconstructing each segment separately",
            group[0].idx.slice, list(segments), len(coverage[0]),
        )
        return list(segments.values())
    return [group]


def groups_from_file(path: str | Path) -> dict[tuple, list[ismrmrd.Acquisition]]:
    groups = {}
    dataset = ismrmrd.Dataset(str(path), create_if_needed=False)
    try:
        for i in range(dataset.number_of_acquisitions()):
            acq = dataset.read_acquisition(i)
            if any(
                acq.is_flag_set(getattr(ismrmrd, flag))
                for flag in (
                    "ACQ_IS_NOISE_MEASUREMENT",
                    "ACQ_IS_PHASECORR_DATA",
                    "ACQ_IS_NAVIGATION_DATA",
                    "ACQ_IS_DUMMYSCAN_DATA",
                    "ACQ_IS_SURFACECOILCORRECTIONSCAN_DATA",
                )
            ):
                continue
            groups.setdefault(group_key(acq), []).append(acq)
    finally:
        dataset.close()
    return groups


def acquisition_context(acq: ismrmrd.Acquisition) -> str:
    """Header-only diagnostics for ambiguous scanner frame boundaries."""
    counters = {
        name: int(getattr(acq.idx, name))
        for name in (
            "kspace_encode_step_1", "kspace_encode_step_2", "slice",
            "repetition", "contrast", "phase", "set", "average", "segment",
        )
    }
    return (
        f"scan_counter={acq.scan_counter}, measurement_uid={acq.measurement_uid}, "
        f"encoding_space_ref={acq.encoding_space_ref}, flags={int(acq.flags):#x}, "
        f"counters={counters}, user={list(acq.idx.user)}, "
        f"timestamp={acq.acquisition_time_stamp}"
    )


def assemble(group: list[ismrmrd.Acquisition], metadata: Any, domain: str) -> tuple:
    """Strict 2D Cartesian contract; retain ACS location on the encoded grid.

    Any vendor readout oversampling is removed, so the returned grid always
    matches encodedSpace and the header field of view.
    """
    first = group[0]
    enc = metadata.encoding[first.encoding_space_ref]
    matrix = enc.encodedSpace.matrixSize
    if (
        str(
            enc.trajectory.value if hasattr(enc.trajectory, "value") else enc.trajectory
        )
        != "cartesian"
    ):
        raise ValueError("Only Cartesian, already regridded input is supported")
    if matrix.z != 1 or any(a.idx.kspace_encode_step_2 for a in group):
        raise ValueError("Only 2D single-slab data is supported")
    limit = enc.encodingLimits.kspace_encoding_step_1
    if limit is None:
        raise ValueError("An explicit PE encoding limit and center are required")
    ny, nx = int(matrix.y), int(matrix.x)
    # FIRE can remove RO oversampling but retain the original encodedSpace
    # and center_sample. Accept only a complete recon-width readout whose
    # matrix/FOV ratios describe the same voxel spacing. Do not infer domain.
    retained = first.number_of_samples - first.discard_pre - first.discard_post
    recon = enc.reconSpace
    corrected_readout = (
        0 < int(recon.matrixSize.x) == retained < nx
        and nx % retained == 0
        and np.isclose(
            enc.encodedSpace.fieldOfView_mm.x / nx,
            recon.fieldOfView_mm.x / retained,
        )
        and recon.matrixSize.y == matrix.y
        and recon.matrixSize.z == matrix.z
        and np.allclose(
            [recon.fieldOfView_mm.y, recon.fieldOfView_mm.z],
            [enc.encodedSpace.fieldOfView_mm.y, enc.encodedSpace.fieldOfView_mm.z],
        )
    )
    original_nx = nx
    if corrected_readout:
        enc = deepcopy(enc)
        enc.encodedSpace.matrixSize.x = int(retained)
        enc.encodedSpace.fieldOfView_mm.x = recon.fieldOfView_mm.x
        matrix = enc.encodedSpace.matrixSize
        nx = int(retained)
        logging.warning(
            "ACS readout header correction: encoded width %d -> %d, FOV %g mm; "
            "using reconSpace RO geometry, preserving inputdomain=%s",
            original_nx, nx, enc.encodedSpace.fieldOfView_mm.x, domain,
        )
    factor = readout_oversampling(first, nx)
    width = nx * factor
    data = np.zeros((first.active_channels, ny, width), dtype=np.complex64)
    seen = {}
    for acq in group:
        if acq.trajectory_dimensions or acq.is_flag_set(ismrmrd.ACQ_IS_REVERSE):
            raise ValueError(
                "Normalize readout polarity and regrid before reconstruction"
            )
        if acq.active_channels != first.active_channels or list(
            acq.channel_mask
        ) != list(first.channel_mask):
            raise ValueError("Coil layout changed within a frame")
        for field in ("position", "read_dir", "phase_dir", "slice_dir"):
            if not np.allclose(getattr(acq, field), getattr(first, field)):
                raise ValueError("Geometry changed within a frame")
        if not np.isfinite(acq.data).all():
            raise ValueError("Nonfinite acquisition data")
        ky = int(acq.idx.kspace_encode_step_1) - int(limit.center) + ny // 2
        if not 0 <= ky < ny:
            raw_lines = [int(a.idx.kspace_encode_step_1) for a in group]
            raise ValueError(
                f"Out-of-range PE line: mapped_ky={ky}, grid_y={ny}, "
                f"PE_limits=(minimum={limit.minimum}, maximum={limit.maximum}, "
                f"center={limit.center}), group_line_range="
                f"({min(raw_lines)}, {max(raw_lines)}), group_size={len(group)}; "
                f"acquisition: {acquisition_context(acq)}"
            )
        if ky in seen:
            previous = seen[ky]
            raise ValueError(
                f"repeated PE line: mapped_ky={ky}, grid_y={ny}, "
                f"PE_limits=(minimum={limit.minimum}, maximum={limit.maximum}, "
                f"center={limit.center}), group_size={len(group)}, "
                f"unique_lines_before_failure={len(seen)}; "
                f"previous: {acquisition_context(previous)}; "
                f"current: {acquisition_context(acq)}; "
                "inspect frame boundaries before combining acquisitions"
            )
        start, stop = (
            int(acq.discard_pre),
            int(acq.number_of_samples - acq.discard_post),
        )
        line = acq.data[:, start:stop]
        if line.shape[1] != width:
            raise ValueError(
                "Readout width after discards must be a constant integer multiple "
                "of the encoded matrix; no implicit cropping. "
                f"inputdomain={domain}, encoding_space_ref={acq.encoding_space_ref}, "
                f"scan_counter={acq.scan_counter}, slice={acq.idx.slice}, "
                f"repetition={acq.idx.repetition}, "
                f"kspace_encode_step_1={acq.idx.kspace_encode_step_1}, "
                f"flags={int(acq.flags):#x}, data_shape={acq.data.shape}, "
                f"number_of_samples={acq.number_of_samples}, "
                f"discard_pre={acq.discard_pre}, discard_post={acq.discard_post}, "
                f"retained_samples={line.shape[1]}, center_sample={acq.center_sample}, "
                f"sample_time_us={acq.sample_time_us}, "
                f"readout_oversampling={factor}, expected_samples={width}, "
                f"encoded_matrix=({matrix.x}, {matrix.y}, {matrix.z}), "
                f"encoded_fov_mm=({enc.encodedSpace.fieldOfView_mm.x}, "
                f"{enc.encodedSpace.fieldOfView_mm.y}, {enc.encodedSpace.fieldOfView_mm.z}), "
                f"recon_matrix=({enc.reconSpace.matrixSize.x}, "
                f"{enc.reconSpace.matrixSize.y}, {enc.reconSpace.matrixSize.z}), "
                f"recon_fov_mm=({enc.reconSpace.fieldOfView_mm.x}, "
                f"{enc.reconSpace.fieldOfView_mm.y}, {enc.reconSpace.fieldOfView_mm.z})"
            )
        center = int(acq.center_sample) - start
        if corrected_readout and center in (original_nx // 2 - 1, original_nx // 2):
            # Legacy center from the uncropped readout (zero- or one-based
            # midpoint convention). This exception is limited to that layout.
            center = nx // 2
        if domain == "kx-ky" and center != width // 2:
            raise ValueError(
                "Asymmetric readout is unsupported; center_sample must match "
                "the grid center"
            )
        data[:, ky] = line
        seen[ky] = acq
    return (
        remove_readout_oversampling(data, nx, domain),
        np.array(sorted(seen)),
        first,
        enc,
    )


def image_from_rss(
    values: np.ndarray,
    acq: ismrmrd.Acquisition,
    enc: Any,
    series: int,
    index: int,
    description: str,
    scanner_display: bool = False,
) -> ismrmrd.Image:
    if not 1 <= index <= 65535:
        raise ValueError("POC output image index exceeds the MRD uint16 range")
    if not np.isfinite(values).all():
        raise ValueError("Nonfinite RSS image")
    if scanner_display:
        maximum = float(values.max())
        pixels = (
            np.rint(np.clip(values / maximum, 0, 1) * 4095).astype(np.int16)
            if maximum > 0 else np.zeros(values.shape, dtype=np.int16)
        )
    else:
        pixels = values.astype(np.float32)
    image = ismrmrd.Image.from_array(pixels, transpose=False)
    for field in (
        "measurement_uid",
        "position",
        "read_dir",
        "phase_dir",
        "slice_dir",
        "patient_table_position",
        "acquisition_time_stamp",
        "physiology_time_stamp",
    ):
        setattr(image, field, getattr(acq, field))
    for field in ("slice", "repetition", "contrast", "phase", "set", "average"):
        setattr(image, field, getattr(acq.idx, field))
    fov = enc.encodedSpace.fieldOfView_mm
    image.field_of_view = (fov.x, fov.y, fov.z)
    image.image_type = ismrmrd.IMTYPE_MAGNITUDE
    image.image_series_index = series
    image.image_index = index
    meta = ismrmrd.Meta()
    meta["DataRole"] = "Image"
    meta["Keep_image_geometry"] = 1
    meta["ImageProcessingHistory"] = ["FIRE", "POC", description]
    meta["SequenceDescriptionAdditional"] = description
    meta["ImageComments"] = (
        "Research proof of concept; encoded grid, readout oversampling removed"
    )
    if scanner_display:
        meta["WindowCenter"] = "2048"
        meta["WindowWidth"] = "4096"
        meta["ImageComments"] += "; per-image display normalization to 0..4095"
    image.attribute_string = meta.serialize()
    return image


def patches(data: np.ndarray, size: int) -> np.ndarray:
    """Rows are valid neighborhoods, columns are coil/PE/RO offsets."""
    c, ny, nx = data.shape
    if size < 1 or size % 2 != 1 or size > min(ny, nx):
        raise ValueError("Kernel must be odd, positive, and fit the calibration region")
    windows = np.lib.stride_tricks.sliding_window_view(
        data, (size, size), axis=(-2, -1)
    )
    return windows.transpose(1, 2, 0, 3, 4).reshape(-1, c * size * size)


def train_kernel(refs: np.ndarray, size: int = 3, ridge: float = 0.001) -> np.ndarray:
    """Conventional slice-GRAPPA from phase-matched single-band references."""
    if not np.isfinite(ridge) or ridge <= 0:
        raise ValueError("ridge must be finite and positive")
    source = patches(refs.sum(axis=0), size).astype(np.complex128)
    radius = size // 2
    target = refs[
        :, :, radius : refs.shape[-2] - radius, radius : refs.shape[-1] - radius
    ].transpose(2, 3, 0, 1)
    target = target.reshape(source.shape[0], -1)
    gram = source.conj().T @ source
    scale = float(np.trace(gram).real / gram.shape[0])
    if scale <= 0:
        raise ValueError("Empty calibration signal")
    weights = np.linalg.solve(
        gram + ridge * scale * np.eye(gram.shape[0]), source.conj().T @ target
    )
    return weights


def apply_kernel(
    data: np.ndarray, weights: np.ndarray, slices: int, size: int
) -> np.ndarray:
    radius = size // 2
    padded = np.pad(data, ((0, 0), (radius, radius), (radius, radius)))
    result = patches(padded, size) @ weights
    return result.reshape(
        data.shape[1], data.shape[2], slices, data.shape[0]
    ).transpose(2, 3, 0, 1)


def reconstruct(
    path: str | Path,
    app: str,
    config: Any,
    metadata: Any,
    artifacts: str | Path | None = None,
) -> Iterator[ismrmrd.Image]:
    params = parameters(config)
    domain = params.get("inputdomain", "unknown")
    if domain not in ("kx-ky", "x-ky") or not enabled(
        params.get("preprocessed", False)
    ):
        raise ValueError(
            "Set inputdomain and confirm preprocessed only after inspecting the ICE tap"
        )
    events = Path(path).with_name("events.jsonl")
    if events.exists():
        with events.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event["type"] == "Image" and event["header"][
                    "image_series_index"
                ] == int(params.get("series", 60000)):
                    raise ValueError(
                        "Derived series collides with an original image series"
                    )
    groups = groups_from_file(path)
    if not groups:
        raise ValueError("No reconstructable acquisitions")
    series = int(params.get("series", 60000))
    if not 1 <= series <= 65535:
        raise ValueError("series must be 1..65535 and distinct from original series")
    if app == "acsrss":
        index = 0
        frames = (
            frame
            for key, group in groups.items() if key[-1]
            for frame in acs_frames(group)
        )
        for group in frames:
            data, lines, acq, enc = assemble(group, metadata, domain)
            if len(lines) < 2 or np.any(np.diff(lines) != 1):
                raise ValueError(
                    "ACS must contain a contiguous PE region with at least two lines"
                )
            index += 1
            yield image_from_rss(
                rss(data, domain), acq, enc, series, index, "ACS-RSS-POC"
            )
        if not index:
            raise ValueError("No calibration acquisitions were flagged")
        return
    mapping = params.get("smsmap", {})
    if isinstance(mapping, str):
        mapping = json.loads(mapping)
    if not mapping:
        raise ValueError(
            "smsmap must explicitly map packet indices to physical single-band "
            "slices and CAIPI shifts"
        )
    if domain != "kx-ky":
        raise ValueError(
            "Slice-GRAPPA POC requires kx-ky input; capture x-ky for adapter "
            "development"
        )
    refs = {}
    for key, group in groups.items():
        if not key[-1]:
            continue
        if any(
            a.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING)
            for a in group
        ):
            raise ValueError(
                "Combined calibration/imaging is ambiguous for single-band training"
            )
        data, lines, acq, enc = assemble(group, metadata, domain)
        refkey = (key[0], key[1], key[4], key[5], key[6], key[2])
        if refkey in refs:
            raise ValueError(
                "Multiple reference frames per slice; select a calibration "
                "epoch explicitly"
            )
        refs[refkey] = data, lines, acq, enc
    size, ridge = int(params.get("kernelsize", 3)), float(params.get("ridge", 0.001))
    kernels = {}
    index = 0
    for key, group in groups.items():
        if key[-1]:
            continue
        data, lines, acq, enc = assemble(group, metadata, domain)
        if len(lines) != data.shape[1]:
            raise ValueError(
                "POC requires full PE sampling of SMS input; in-plane GRAPPA is "
                "not implemented"
            )
        spec = mapping.get(str(key[2]))
        if not isinstance(spec, dict):
            raise ValueError(f"Missing SMS mapping for packet {key[2]}")
        slices, shifts = spec["slices"], spec["shifts"]
        if (
            len(slices) < 2
            or len(slices) != len(shifts)
            or len(set(slices)) != len(slices)
        ):
            raise ValueError(
                "Supply distinct physical slices and one CAIPI FOV fraction per slice"
            )
        if not np.isfinite(shifts).all():
            raise ValueError("CAIPI shifts must be finite")
        prefix = (key[0], key[1], key[4], key[5], key[6])
        selected = [refs[(*prefix, s)] for s in slices]
        ref_lines = selected[0][1]
        for ref, rl, rh, re in selected:
            if ref.shape != data.shape or not np.array_equal(rl, ref_lines):
                raise ValueError("Reference grids do not match SMS input")
            if list(rh.channel_mask) != list(acq.channel_mask):
                raise ValueError("Reference and SMS coil masks differ")
            for field in ("read_dir", "phase_dir", "slice_dir"):
                if not np.allclose(getattr(rh, field), getattr(acq, field)):
                    raise ValueError("Reference and SMS orientations differ")
            if re.encodedSpace != enc.encodedSpace:
                raise ValueError("Reference and SMS encoding geometries differ")
        if np.any(np.diff(ref_lines) != 1):
            raise ValueError("Reference PE lines must be contiguous")
        ramps = np.exp(
            -2j
            * np.pi
            * np.asarray(shifts)[:, None]
            * (np.arange(data.shape[1])[None, :] - data.shape[1] // 2)
        )
        kernelkey = (*prefix, key[2])
        if kernelkey not in kernels:
            phased = np.stack([r[0] for r in selected]) * ramps[:, None, :, None]
            weights = train_kernel(phased[:, :, ref_lines, :], size, ridge)
            kernels[kernelkey] = weights
            if artifacts is not None:
                target = Path(artifacts)
                target.mkdir(parents=True, exist_ok=True)
                np.savez_compressed(
                    target / ("kernel-" + "-".join(map(str, kernelkey)) + ".npz"),
                    weights=weights,
                    slices=slices,
                    caipi_shifts=shifts,
                    reference_lines=ref_lines,
                    kernel_size=size,
                    ridge=ridge,
                    config=json.dumps(config),
                )
        separated = apply_kernel(data, kernels[kernelkey], len(slices), size)
        separated *= ramps.conj()[:, None, :, None]
        if artifacts is not None:
            target = Path(artifacts)
            target.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                target / ("separated-" + "-".join(map(str, key[:-1])) + ".npz"),
                kspace=separated,
                slices=slices,
                positions=np.array([list(r[2].position) for r in selected]),
                input_domain=domain,
                config=json.dumps(config),
            )
        for output, (_, _, reference, _) in zip(separated, selected):
            # Geometry comes from the physical single-band slice, timing from SMS.
            image = image_from_rss(
                rss(output, domain),
                reference,
                enc,
                series,
                index + 1,
                "SLICE-GRAPPA-POC",
            )
            for field in ("repetition", "contrast", "phase", "set", "average"):
                setattr(image, field, getattr(acq.idx, field))
            image.acquisition_time_stamp = acq.acquisition_time_stamp
            image.physiology_time_stamp = acq.physiology_time_stamp
            index += 1
            yield image
    if not index:
        raise ValueError("No SMS imaging frames found")


def save_dicom(
    image: ismrmrd.Image, path: Path, study_uid: str, series_uid: str, frame_uid: str
) -> None:
    """Offline derived MR preview. Scanner DICOM production uses returned MRD images."""
    from pydicom.dataset import FileDataset, FileMetaDataset
    from pydicom.uid import ExplicitVRLittleEndian, MRImageStorage, generate_uid

    meta = FileMetaDataset()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    meta.MediaStorageSOPClassUID = MRImageStorage
    meta.MediaStorageSOPInstanceUID = generate_uid()
    ds = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPClassUID, ds.SOPInstanceUID = (
        meta.MediaStorageSOPClassUID,
        meta.MediaStorageSOPInstanceUID,
    )
    ds.StudyInstanceUID, ds.SeriesInstanceUID = study_uid, series_uid
    ds.FrameOfReferenceUID, ds.PositionReferenceIndicator = frame_uid, ""
    ds.AcquisitionNumber, ds.PatientPosition = "", ""
    ds.Modality, ds.Manufacturer = "MR", "NeuroDesk"
    ds.PatientName, ds.PatientID = "", ""
    ds.PatientBirthDate, ds.PatientSex = "", ""
    ds.StudyDate, ds.StudyTime = "", ""
    ds.AccessionNumber, ds.ReferringPhysicianName, ds.StudyID = "", "", ""
    ds.ImageType = ["DERIVED", "SECONDARY", "OTHER"]
    ds.SeriesDescription = "FIRE POC offline preview"
    ds.SeriesNumber, ds.InstanceNumber = (
        int(image.image_series_index),
        int(image.image_index),
    )
    ds.ScanningSequence, ds.SequenceVariant, ds.ScanOptions = "RM", "NONE", ""
    ds.MRAcquisitionType = "2D"
    ds.RepetitionTime, ds.EchoTime, ds.EchoTrainLength = "", "", ""
    values = image.data[0, 0].astype(np.float64)
    maximum = float(values.max())
    scale = maximum / 65535 if maximum > 0 else 1.0
    ds.RescaleSlope, ds.RescaleIntercept = scale, 0
    ds.Rows, ds.Columns = values.shape
    ds.SamplesPerPixel, ds.PhotometricInterpretation = 1, "MONOCHROME2"
    ds.BitsAllocated, ds.BitsStored, ds.HighBit, ds.PixelRepresentation = 16, 16, 15, 0
    dx, dy = image.field_of_view[0] / ds.Columns, image.field_of_view[1] / ds.Rows
    ds.PixelSpacing, ds.SliceThickness = [dy, dx], float(image.field_of_view[2])
    read, phase = np.asarray(image.read_dir), np.asarray(image.phase_dir)
    ds.ImageOrientationPatient = [*map(float, read), *map(float, phase)]
    ds.ImagePositionPatient = list(
        np.asarray(image.position)
        - read * dx * (ds.Columns - 1) / 2
        - phase * dy * (ds.Rows - 1) / 2
    )
    ds.PixelData = np.rint(values / scale).clip(0, 65535).astype("<u2").tobytes()
    ds.save_as(path, enforce_file_format=True)


def write_outputs(
    images: Iterator[ismrmrd.Image],
    destination: str | Path,
    metadata: Any,
    connection: Any = None,
    dicom: bool = False,
) -> int:
    from pydicom.uid import generate_uid

    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    dataset = ismrmrd.Dataset(str(destination / "output.h5"))
    dataset.write_xml_header(ismrmrd.xsd.ToXML(metadata).encode())
    study, series, frame = generate_uid(), generate_uid(), generate_uid()
    count = 0
    try:
        for image in images:
            dataset.append_image("images", image)
            count += 1
            if dicom:
                save_dicom(
                    image, destination / f"{count:06d}.dcm", study, series, frame
                )
            if connection is not None:
                connection.send_image(image)
    finally:
        dataset.close()
    return count


def process_acs(connection: Any, config: Any, metadata: Any) -> None:
    """Reconstruct ACS in memory and return MRD images to the scanner injector."""
    try:
        params = parameters(config)
        domain = params.get("inputdomain", "x-ky")
        if domain not in ("kx-ky", "x-ky"):
            raise ValueError("inputdomain must be kx-ky or x-ky")
        series = int(params.get("series", 60000))
        if not 1 <= series <= 65535:
            raise ValueError("series must be 1..65535")
        groups = {}
        original_series = set()
        received = 0
        connection.send_logging(
            1,
            f"ACS RSS: inputdomain={domain}; assuming regridded, phase-corrected "
            "Cartesian input. No data files are written.",
        )
        connection.send_logging(
            1, "ACS Fourier convention: " + (
                "centered PE FFT (negative exponent); readout unchanged"
                if domain == "x-ky" else "centered RO+PE IFFT (positive exponent)"
            ),
        )
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                original_series.add(int(item.image_series_index))
                if enabled(params.get("sendoriginal", True)):
                    connection.send_image(item)
            elif isinstance(item, ismrmrd.Acquisition):
                received += 1
                if calibration(item) and not any(
                    item.is_flag_set(getattr(ismrmrd, flag))
                    for flag in (
                        "ACQ_IS_NOISE_MEASUREMENT",
                        "ACQ_IS_PHASECORR_DATA",
                        "ACQ_IS_NAVIGATION_DATA",
                        "ACQ_IS_DUMMYSCAN_DATA",
                        "ACQ_IS_SURFACECOILCORRECTIONSCAN_DATA",
                    )
                ):
                    groups.setdefault(group_key(item), []).append(item)
        if not groups:
            connection.send_logging(
                2,
                f"Received {received} acquisitions, but no ACS-flagged data; "
                "no derived images returned.",
            )
            return
        if series in original_series:
            raise ValueError("Derived series collides with an original image series")
        if isinstance(metadata, (str, bytes)):
            metadata = ismrmrd.xsd.CreateFromDocument(metadata)
        count = 0
        frames = (frame for group in groups.values() for frame in acs_frames(group))
        for group in frames:
            data, lines, acq, enc = assemble(group, metadata, domain)
            if len(lines) < 2 or np.any(np.diff(lines) != 1):
                raise ValueError(
                    "ACS must contain a contiguous PE region with at least two lines"
                )
            count += 1
            values = rss(data, domain)
            input_min = min(float(np.abs(a.data).min()) for a in group)
            input_max = max(float(np.abs(a.data).max()) for a in group)
            image = image_from_rss(
                values, acq, enc, series, count, "ACS-RSS-POC", scanner_display=True
            )
            logging.info(
                "ACS pixel stats: image=%d slice=%d segments=%s input_abs=[%g,%g] "
                "RSS=[%g,%g] RSS_nonzero=%d/%d output=int16[%d,%d]",
                count, acq.idx.slice, sorted({int(a.idx.segment) for a in group}),
                input_min, input_max, float(values.min()), float(values.max()),
                np.count_nonzero(values), values.size,
                int(image.data.min()), int(image.data.max()),
            )
            if not np.any(values):
                connection.send_logging(2, f"ACS image {count}, slice {acq.idx.slice}: zero RSS signal")
            connection.send_image(image)
        connection.send_logging(1, f"Returned {count} ACS RSS images via ISMRMRD")
    except Exception:
        error = traceback.format_exc()
        logging.exception("ACS RSS reconstruction failed")
        connection.send_logging(3, error)
    finally:
        connection.send_close()


def process(connection: Any, config: Any, metadata: Any, app: str) -> None:
    capture = None
    try:
        params = parameters(config)
        root = os.environ.get("FIRE_POC_CAPTURE_ROOT", "/tmp/share/fire-poc")
        capture = Capture(root, app, config, metadata)
        connection.send_logging(1, f"Capture directory: {capture.path}")
        for item in connection:
            if item is None:
                capture.summary["close_message_received"] = True
                break
            capture.record(item)
            if isinstance(item, ismrmrd.Image) and enabled(
                params.get("sendoriginal", True)
            ):
                connection.send_image(item)
        capture.close("captured" if capture.summary["counts"] else "empty")
        has_acquisitions = capture.summary["counts"].get("Acquisition", 0) > 0
        if params.get("mode", "capture") == "reconstruct" and not has_acquisitions:
            warning = "No acquisitions received; reconstruction skipped. Check the earlier adjustment session."
            capture.summary["warnings"].append(warning)
            connection.send_logging(2, warning)
        elif params.get("mode", "capture") == "reconstruct":
            if isinstance(metadata, (str, bytes)):
                metadata = ismrmrd.xsd.CreateFromDocument(metadata)
            output = capture.path / "reconstruction"
            images = reconstruct(
                capture.path / "input.h5", app, config, metadata, artifacts=output
            )
            capture.summary["output_images"] = write_outputs(
                images, output, metadata, connection, True
            )
            capture.summary["status"] = "reconstructed"
        capture.save_summary()
        connection.send_logging(1, json.dumps(capture.summary))
    except Exception:
        error = traceback.format_exc()
        logging.exception("FIRE POC failed")
        if capture is not None:
            if not capture.events.closed:
                capture.close("failed", error)
            else:
                capture.summary.update(status="reconstruction_failed", error=error)
                capture.save_summary()
        connection.send_logging(3, error)
    finally:
        connection.send_close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument(
        "--app", choices=["acsrss", "slicegrappa"], required=True
    )
    parser.add_argument(
        "--config", type=Path, required=True, help="JSON with parameters object"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="New output directory"
    )
    parser.add_argument(
        "--dicom", action="store_true", help="Also write offline derived MR previews"
    )
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output directory must not exist")
    metadata = ismrmrd.xsd.CreateFromDocument(
        (args.capture / "metadata.xml").read_bytes()
    )
    config = json.loads(args.config.read_text())
    images = reconstruct(
        args.capture / "input.h5", args.app, config, metadata, artifacts=args.output
    )
    count = write_outputs(images, args.output, metadata, dicom=args.dicom)
    (args.output / "config.json").write_text(json.dumps(config, indent=2))
    print(f"Wrote {count} images to {args.output}")


if __name__ == "__main__":
    main()
