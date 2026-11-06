"""OpenRecon UNI denoising using the Neurodesk easy-mp2rage algorithm."""
from collections import defaultdict
from dataclasses import dataclass
import json
import logging
import traceback
import ismrmrd
import nibabel as nib
import numpy as np
import constants
import openreconi2iexample as shared
from mp2rage_denoise_core import robust_combination

@dataclass(frozen=True)
class SourceVolume:
    images: tuple
    voxels: np.ndarray
    affine: np.ndarray
    orientation: np.ndarray

    @classmethod
    def from_images(cls, images):
        """Volume reoriented to the closest canonical (RAS) axes, as dcm2niix does for 3D
        acquisitions, so x, y and z are the sagittal, coronal and axial planes MP2RAGE
        slices along."""
        for image in images:
            _validate_image(image)
        items, axis = shared._ordered_source_geometry_items(
            [(image, image.data[0, 0]) for image in images], "MP2RAGE"
        )
        ordered = tuple(image for image, _ in items)
        first = ordered[0].getHead()
        nx, ny, _ = map(int, first.matrix_size)
        read = np.asarray(first.read_dir, dtype=float)
        phase = np.asarray(first.phase_dir, dtype=float)
        spacing = np.asarray(first.field_of_view, dtype=float) / np.asarray(first.matrix_size)
        positions = np.array([image.getHead().position for image in ordered], dtype=float)
        if len(ordered) > 1:
            dz = float(np.median(np.diff(positions @ axis)))
            if not np.isfinite(dz) or dz <= 0:
                raise ValueError("MP2RAGE requires distinct regularly spaced slices")
        else:
            dz = float(spacing[2])
        expected = positions[0] + np.arange(len(ordered))[:, None] * axis * dz
        if not np.allclose(positions, expected, atol=0.01, rtol=0):
            raise ValueError("MP2RAGE slices must form a regular volume without gaps or in-plane drift")
        for image in ordered:
            header = image.getHead()
            for field in ("matrix_size", "field_of_view", "read_dir", "phase_dir", "slice_dir"):
                if not np.allclose(getattr(header, field), getattr(first, field), atol=1e-5, rtol=1e-5):
                    raise ValueError(f"MP2RAGE source slices have inconsistent {field}")
        affine = np.eye(4)
        affine[:3, 0] = read * spacing[0]
        affine[:3, 1] = phase * spacing[1]
        affine[:3, 2] = axis * dz
        affine[:3, 3] = positions[0] - read * spacing[0] * (nx - 1) / 2 - phase * spacing[1] * (ny - 1) / 2
        affine[:2, :] *= -1
        voxels = np.stack([plane.T for _, plane in items], axis=2).astype(np.float32)
        canonical = nib.as_closest_canonical(nib.Nifti1Image(voxels, affine))
        return cls(ordered, np.asarray(canonical.dataobj, dtype=np.float32), canonical.affine, nib.orientations.io_orientation(affine))


def _validate_image(image):
    header = image.getHead()
    data = np.asarray(image.data)
    if header.image_type != ismrmrd.IMTYPE_MAGNITUDE or np.iscomplexobj(data):
        raise ValueError("MP2RAGE accepts reconstructed magnitude images only")
    nx, ny, nz = map(int, header.matrix_size)
    if header.channels != 1 or nz != 1 or data.shape != (1, 1, ny, nx) or min(nx, ny) < 1:
        raise ValueError("MP2RAGE requires single-channel, single-plane images matching matrix_size")
    if not np.isfinite(data).all():
        raise ValueError("MP2RAGE input pixels must be finite")
    fov = np.asarray(header.field_of_view, dtype=float)
    position = np.asarray(header.position, dtype=float)
    axes = np.array([header.read_dir, header.phase_dir, header.slice_dir], dtype=float)
    if not np.isfinite(fov).all() or np.any(fov <= 0) or not np.isfinite(position).all():
        raise ValueError("MP2RAGE requires finite positions and positive field_of_view")
    if not np.isfinite(axes).all() or not np.allclose(axes @ axes.T, np.eye(3), atol=1e-4):
        raise ValueError("MP2RAGE requires orthonormal read, phase and slice directions")
    if not np.allclose(np.cross(axes[0], axes[1]), axes[2], atol=1e-4):
        raise ValueError("MP2RAGE source directions must form a right-handed coordinate system")


def denoise(uni, inv1, inv2, mf=6.0):
    if not np.isfinite(mf) or mf <= 0:
        raise ValueError("Noise factor must be finite and positive")
    if uni.shape != inv1.shape or uni.shape != inv2.shape or uni.ndim != 3:
        raise ValueError("UNI, INV1 and INV2 must be matching 3D volumes")
    if not all(np.isfinite(a).all() for a in (uni, inv1, inv2)):
        raise ValueError("Input pixels must be finite")
    if np.any(inv1 < 0) or np.any(inv2 < 0):
        raise ValueError("INV1 and INV2 must be magnitude images")
    result = robust_combination(uni, inv1, inv2, mf)
    if not np.isfinite(result).all():
        raise ValueError("Denoising produced non-finite pixels; check UNI scaling")
    return result


def process(connection, config, metadata):
    try:
        settings = {} if config in (None, "", "mp2ragedenoise") else (json.loads(config) if isinstance(config, str) else config)
        settings = settings.get("parameters", settings)
        ids = [int(settings.get(role + "series", default)) for role, default in (("uni", 0), ("inv1", 1), ("inv2", 2))]
        if len(set(ids)) != 3:
            raise ValueError("UNI, INV1 and INV2 series indices must be distinct")
        groups = defaultdict(list)
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                raise ValueError("Expected reconstructed MRD images")
            groups[int(item.image_series_index)].append(item)
        if set(groups) != set(ids):
            raise ValueError(f"Expected exactly UNI/INV1/INV2 series {ids}; received {sorted(groups)}")
        volumes = [SourceVolume.from_images(groups[i]) for i in ids]
        uni, inv1, inv2 = volumes
        if any(v.voxels.shape != uni.voxels.shape or not np.allclose(v.affine, uni.affine, atol=0.01, rtol=0) for v in volumes[1:]):
            raise ValueError("UNI, INV1 and INV2 geometry must match")
        result = denoise(uni.voxels, inv1.voxels, inv2.voxels, float(settings.get("noisefactor", 6)))
        orientation = nib.orientations.ornt_transform(np.array([[0, 1], [1, 1], [2, 1]]), uni.orientation)
        native = nib.orientations.apply_orientation(result, orientation)
        series = next(i for i in range(1, 65536) if i not in groups)
        identity = shared._build_output_series_identity(uni.images[0], series, "UNI-DEN", "source")
        outputs = []
        for index, source in enumerate(uni.images):
            pixels = native[:, :, index].T.astype(np.float32)
            output = ismrmrd.Image.from_array(pixels, transpose=False)
            header = source.getHead()
            header.data_type = output.data_type
            output.setHead(header)
            shared._stamp_output_image(output, source, series, index, identity["series_name"], "Image", "UNI-DEN", ["MP2RAGE_DENOISE"], {"Keep_image_geometry": 1}, series_identity=identity)
            outputs.append(output)
        connection.send_image(outputs)
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    for role in ("uni", "inv1", "inv2", "output"):
        parser.add_argument("--" + role, required=True)
    parser.add_argument("--noise-factor", type=float, default=6)
    args = parser.parse_args()
    inputs = [nib.load(getattr(args, role)) for role in ("uni", "inv1", "inv2")]
    if any(im.shape != inputs[0].shape or not np.allclose(im.affine, inputs[0].affine, atol=0.01, rtol=0) for im in inputs[1:]):
        parser.error("Input geometries must match")
    result = denoise(*(im.get_fdata() for im in inputs), args.noise_factor)
    header = inputs[0].header.copy()
    header.set_data_dtype(np.float32)
    nib.save(nib.Nifti1Image(result.astype(np.float32), inputs[0].affine, header), args.output)


if __name__ == "__main__":
    main()
