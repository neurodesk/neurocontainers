import copy
from dataclasses import dataclass

import ismrmrd
import numpy as np
import openreconi2iexample as helpers


def _vector(header, name: str) -> np.ndarray:
    value = np.asarray(getattr(header, name), dtype=float)
    if value.shape != (3,) or not np.all(np.isfinite(value)):
        raise ValueError(f"Invalid MRD {name}")
    return value


def _image_axes(image: ismrmrd.Image) -> np.ndarray:
    meta = ismrmrd.Meta.deserialize(image.attribute_string)
    paired = ("ImageRowDir" in meta, "ImageColumnDir" in meta)
    if paired[0] != paired[1] or ("ImageSliceDir" in meta and not all(paired)):
        raise ValueError(
            "MRD pixel orientation requires paired row and column directions"
        )
    if all(paired):
        row = np.asarray(meta["ImageRowDir"], dtype=float)
        column = np.asarray(meta["ImageColumnDir"], dtype=float)
        if row.shape != (3,) or column.shape != (3,):
            raise ValueError("MRD pixel directions require three components")
        normal = np.asarray(
            meta.get("ImageSliceDir", np.cross(row, column)), dtype=float
        )
        if normal.shape != (3,):
            raise ValueError("MRD pixel slice direction requires three components")
        axes = np.column_stack((row, column, normal))
    else:
        axes = np.column_stack(
            [
                _vector(image.getHead(), n)
                for n in ("read_dir", "phase_dir", "slice_dir")
            ]
        )
    if not np.isfinite(axes).all() or not np.allclose(
        axes.T @ axes, np.eye(3), rtol=0, atol=1e-4
    ):
        raise ValueError("MRD image axes must be finite and orthonormal")
    return axes


def _planes(image: ismrmrd.Image) -> list[ismrmrd.Image]:
    """Split packed MRD volumes, whose position denotes their center."""
    data = np.asarray(image.data)
    if data.ndim != 4 or data.shape[0] != 1:
        raise ValueError("Input must be a single-channel reconstructed MRD image")
    if data.shape[1] == 1:
        return [image]
    header = image.getHead()
    spacing = float(header.field_of_view[2]) / data.shape[1]
    if spacing <= 0:
        raise ValueError("Packed MRD volume needs positive slice spacing")
    axes = _image_axes(image)
    planes = []
    for z in range(data.shape[1]):
        plane = ismrmrd.Image.from_array(data[:, z : z + 1].copy(), transpose=False)
        head = copy.deepcopy(header)
        head.data_type = plane.data_type
        head.matrix_size[2] = 1
        head.field_of_view[2] = spacing
        head.position[:] = (
            _vector(header, "position")
            + (z - (data.shape[1] - 1) / 2) * spacing * axes[:, 2]
        )
        head.slice = z
        plane.setHead(head)
        plane.attribute_string = image.attribute_string
        planes.append(plane)
    return planes


def source_identity(images):
    identity = {}
    for key in ("StudyInstanceUID", "FrameOfReferenceUID"):
        values = {
            helpers._meta_text(ismrmrd.Meta.deserialize(im.attribute_string), key)
            for im in images
        }
        values.discard("")
        values.discard(None)
        if len(values) > 1:
            raise ValueError(f"Images have inconsistent {key}")
        if values:
            identity[key] = values.pop()
    return identity


@dataclass(frozen=True)
class TargetVolume:
    magnitude: np.ndarray
    affine: np.ndarray
    anchors: tuple
    identity: dict


def assemble_target(images):
    if not images:
        raise ValueError("A magnitude target volume is required")
    groups = set()
    planes = []
    for im in images:
        head = im.getHead()
        meta = ismrmrd.Meta.deserialize(im.attribute_string)
        if head.image_type != ismrmrd.IMTYPE_MAGNITUDE:
            raise ValueError("Target requires only magnitude MRD images")
        if any(getattr(head, index) != 0 for index in ("repetition", "set", "phase", "average")):
            raise ValueError(
                "Repeated scans and dynamic acquisitions must be sent separately"
            )
        groups.add(
            (
                head.image_series_index,
                head.contrast,
                helpers._meta_text(meta, "SeriesInstanceUID"),
            )
        )
        planes.extend(_planes(im))
    if len(groups) != 1:
        raise ValueError("Send exactly one magnitude target series and contrast")
    reference = planes[0].getHead()
    axes = _image_axes(planes[0])
    shape = np.array(reference.matrix_size[:2], dtype=int)
    fov = _vector(reference, "field_of_view")
    if np.any(shape <= 0) or np.any(fov <= 0):
        raise ValueError("Target matrix size and field of view must be positive")
    for im in planes:
        head = im.getHead()
        if (
            not np.array_equal(head.matrix_size[:2], shape)
            or not np.allclose(_vector(head, "field_of_view"), fov, rtol=0, atol=1e-3)
            or not np.allclose(_image_axes(im), axes, rtol=0, atol=1e-4)
        ):
            raise ValueError("Target slices have inconsistent geometry")
    ordered = sorted(
        planes, key=lambda im: np.dot(_vector(im.getHead(), "position"), axes[:, 2])
    )
    positions = np.array([_vector(im.getHead(), "position") for im in ordered])
    spacing = fov / np.array([*shape, 1])
    if len(ordered) > 1:
        steps = np.diff(positions, axis=0)
        spacing[2] = np.dot(steps[0], axes[:, 2])
        if spacing[2] <= 0 or not np.allclose(
            steps, axes[:, 2] * spacing[2], rtol=0, atol=1e-3
        ):
            raise ValueError(
                "Target slices must have unique regular positions without gaps"
            )
    lps = np.eye(4)
    lps[:3, :3] = axes * spacing
    lps[:3, 3] = positions[0] - axes[:, :2] @ ((shape - 1) * spacing[:2] / 2)
    affine = np.diag([-1, -1, 1, 1]) @ lps
    magnitude = np.stack([im.data[0, 0].T for im in ordered], axis=2).astype(float)
    return TargetVolume(magnitude, affine, tuple(ordered), source_identity(images))
