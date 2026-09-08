#!/usr/bin/env python3
"""OpenRecon module for a fully-sampled Cartesian 3D EPI reconstruction.

Every readout already lands on a regular grid, so reconstruction is a per-coil
3D FFT followed by root-sum-of-squares coil combination. See
``reconstruct_epi_data.py`` for the offline prototype this module streams the
same algorithm from.
"""

import ctypes
from itertools import permutations
import logging
import os
from time import perf_counter
import traceback
import uuid

import ismrmrd
import numpy as np

import constants
import mrdhelper


debugFolder = "/tmp/share/debug"

OPENRECON_DEFAULTS = {
    "config": "epirecon",
    "orientationflipslice": False,
}
OUTPUT_SERIES_DESCRIPTION = "epirecon"
OUTPUT_IMAGE_COMMENT = "Cartesian 3D EPI FFT reconstruction"
OUTPUT_IMAGE_SERIES_INDEX = 1

# Acquisitions carrying any of these flags are not imaging k-space lines (noise
# calibration, EPI ghost-correction navigators, dummy/feedback scans) and must
# be excluded when filling the k-space array.
NON_IMAGING_FLAGS = (
    ismrmrd.ACQ_IS_NOISE_MEASUREMENT,
    ismrmrd.ACQ_IS_PARALLEL_CALIBRATION,
    ismrmrd.ACQ_IS_PHASECORR_DATA,
    ismrmrd.ACQ_IS_NAVIGATION_DATA,
    ismrmrd.ACQ_IS_HPFEEDBACK_DATA,
    ismrmrd.ACQ_IS_RTFEEDBACK_DATA,
    ismrmrd.ACQ_IS_DUMMYSCAN_DATA,
)

# ISMRMRD direction vectors are in the DICOM/Siemens patient coordinate system
# (+x left, +y posterior, +z head). Labels below are (negative, positive).
PATIENT_AXIS_LABELS = (("R", "L"), ("A", "P"), ("F", "H"))

# The FIRE Configurator disables NormOrientation, so nothing downstream rotates
# our images into the DICOM standard display view. The native ICE reconstruction
# is emitted in that view, so this app has to produce it itself, otherwise the
# two series are mirrored relative to each other on screen.
#
# The standard view is the right-handed frame
#   columns increase toward the patient's Left      (+x)
#   rows    increase toward the patient's Posterior (+y)
#   slices  increase toward the patient's Head      (+z)
#
# Targets are ordered (slices, rows, columns) to match the emitted array.
DISPLAY_FRAME_TARGETS = (
    (0.0, 0.0, 1.0),
    (0.0, 1.0, 0.0),
    (1.0, 0.0, 0.0),
)
DISPLAY_FRAME_AXIS_NAMES = ("slices", "rows", "columns")


def _validate_display_frame_targets(targets):
    """Return the handedness of the display targets, refusing a left-handed set.

    The targets describe the emitted DICOM frame, which this app builds
    right-handed under the rule columns x rows = normal. A left-handed target
    set can therefore only be a sign mistake.
    """
    slices, rows, columns = (np.asarray(target, dtype=float) for target in targets)
    handedness = float(np.dot(np.cross(columns, rows), slices))
    if handedness <= 0.0:
        raise ValueError(
            "DISPLAY_FRAME_TARGETS must form a right-handed (columns, rows, "
            f"slices) frame, but columns x rows . slices = {handedness:+.3f}"
        )
    return handedness


DISPLAY_FRAME_HANDEDNESS = _validate_display_frame_targets(DISPLAY_FRAME_TARGETS)

# ICE stacks the frames of an emitted 3D volume against slice_dir, so the
# emitted frame order has to be reversed to compensate. This was measured on
# this scanner/FIRE version while validating a different reconstruction
# (see git history for this file); it has not yet been re-verified against a
# native 3D EPI reconstruction. Confirm it with a test scan -- comparing the
# predicted frame positions this module logs against the scanner's own slice
# positions -- before relying on this module's geometry.
ICE_STACKS_FRAMES_AGAINST_SLICE_DIR = True

# Set on every emitted image to declare whether the frame order above was
# reversed. The compensation makes the pixels disagree with the emitted
# slice_dir on purpose, so this attribute is the contract that lets a consumer
# reading the header instead -- mrd2nifti above all -- undo it rather than
# silently mirroring the volume.
OUTPUT_FRAME_ORDER_REVERSED_ATTRIBUTE = "epireconIceFrameOrderReversed"
SCANNER_DISPLAY_MIN = 0
SCANNER_DISPLAY_MAX = 4096


def _get_config_value(config, key, default, value_type):
    try:
        return mrdhelper.get_json_config_param(config, key, default=default, type=value_type)
    except Exception:
        return default


def _config_bool(config, key, default):
    value = _get_config_value(config, key, default, "bool")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _config_str(config, key, default):
    value = _get_config_value(config, key, default, "str")
    if value is None:
        return default
    return str(value)


def _ensure_debug_folder():
    os.makedirs(debugFolder, exist_ok=True)


def _safe_protocol_name(metadata):
    try:
        protocol_name = getattr(metadata.measurementInformation, "protocolName", "")
        if protocol_name:
            return str(protocol_name)
    except Exception:
        pass
    return OUTPUT_SERIES_DESCRIPTION


def _is_imaging_acquisition(acquisition):
    return not any(acquisition.is_flag_set(flag) for flag in NON_IMAGING_FLAGS)


def _build_kspace_array(acquisitions, encoding):
    """Read filtered ISMRMRD acquisitions into a (coils, partitions, lines, samples) array.

    EPI zig-zags through k-space: alternate readouts are physically acquired
    back to front, and the scanner marks those with ACQ_IS_REVERSE. Placing
    them into the grid without reversing them first would scramble every
    other line of k-space, so that flag has to be honoured even though this
    is otherwise a plain Cartesian recon.
    """
    limits = encoding.encodingLimits
    num_lines = limits.kspace_encoding_step_1.maximum - limits.kspace_encoding_step_1.minimum + 1
    num_partitions = limits.kspace_encoding_step_2.maximum - limits.kspace_encoding_step_2.minimum + 1
    line_offset = limits.kspace_encoding_step_1.minimum
    partition_offset = limits.kspace_encoding_step_2.minimum

    kspace = None
    filled = np.zeros((num_partitions, num_lines), dtype=bool)

    for acquisition in acquisitions:
        readout = np.asarray(acquisition.data, dtype=np.complex64)
        if acquisition.is_flag_set(ismrmrd.ACQ_IS_REVERSE):
            readout = readout[:, ::-1]

        if kspace is None:
            num_coils, num_samples = readout.shape
            kspace = np.zeros(
                (num_coils, num_partitions, num_lines, num_samples),
                dtype=np.complex64,
            )

        line = acquisition.idx.kspace_encode_step_1 - line_offset
        partition = acquisition.idx.kspace_encode_step_2 - partition_offset
        kspace[:, partition, line, :] = readout
        filled[partition, line] = True

    if kspace is None:
        raise ValueError(
            "No imaging acquisitions found in this measurement. Every "
            "acquisition was flagged as noise/navigation/calibration data."
        )

    missing = np.count_nonzero(~filled)
    if missing:
        raise ValueError(
            f"K-space is not fully sampled: {missing} of {filled.size} "
            "(partition, line) positions were never acquired."
        )

    return kspace


def reconstruct_coil_images(kspace):
    """Per-coil 3D inverse FFT from Cartesian k-space to image space."""
    axes = (1, 2, 3)
    shifted = np.fft.ifftshift(kspace, axes=axes)
    images = np.fft.fftshift(np.fft.ifftn(shifted, axes=axes), axes=axes)
    return images.astype(np.complex64)


def crop_readout_oversampling(coil_images, recon_matrix_x):
    """Crop the readout axis back down from an oversampled acquisition matrix."""
    current_size = coil_images.shape[-1]
    if recon_matrix_x <= 0 or recon_matrix_x >= current_size:
        return coil_images

    start = (current_size - recon_matrix_x) // 2
    stop = start + recon_matrix_x
    return coil_images[..., start:stop]


def combine_coils_sos(coil_images):
    """Root-sum-of-squares coil combination."""
    return np.sqrt(np.sum(np.abs(coil_images) ** 2, axis=0)).astype(np.float32)


def _resolve_recon_matrix_x(encoding, num_samples):
    """Target readout size after removing acquisition oversampling.

    siemens_to_ismrmrd sets reconSpace.fieldOfView_mm.x correctly but leaves
    reconSpace.matrixSize.x at 0 for the (oversampled) readout axis, so that
    field can't be trusted on its own. Fall back to deriving the target size
    from the FOV ratio, which is exactly how much of the readout is real
    anatomy versus oversampled padding.
    """
    recon_matrix_x = int(encoding.reconSpace.matrixSize.x)
    if recon_matrix_x > 0:
        return recon_matrix_x

    encoded_fov_x = float(encoding.encodedSpace.fieldOfView_mm.x)
    recon_fov_x = float(encoding.reconSpace.fieldOfView_mm.x)
    if encoded_fov_x > 0 and recon_fov_x > 0:
        return round(num_samples * recon_fov_x / encoded_fov_x)

    return 0


def _format_display_number(value):
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6g}"


def _scale_volume_to_display_range(volume):
    values = np.asarray(volume, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        display = np.zeros(values.shape, dtype=np.uint16)
        return display, {
            "input_min": 0.0,
            "input_max": 0.0,
            "scale": 1.0,
            "display_min": 0,
            "display_max": 0,
            "formula": "value = display",
        }

    input_min = float(np.min(finite))
    input_max = float(np.max(finite))
    input_range = input_max - input_min
    if input_range <= 0.0 or not np.isfinite(input_range):
        display = np.zeros(values.shape, dtype=np.uint16)
        return display, {
            "input_min": input_min,
            "input_max": input_max,
            "scale": 1.0,
            "display_min": 0,
            "display_max": 0,
            "formula": f"value = display + {_format_display_number(input_min)}",
        }

    scale = float(SCANNER_DISPLAY_MAX - SCANNER_DISPLAY_MIN) / input_range
    cleaned = np.nan_to_num(values, nan=input_min, posinf=input_max, neginf=input_min)
    display = np.rint((cleaned - input_min) * scale + SCANNER_DISPLAY_MIN)
    display = np.clip(display, SCANNER_DISPLAY_MIN, SCANNER_DISPLAY_MAX)
    display = display.astype(np.uint16, copy=False)
    scale_text = _format_display_number(scale)
    min_text = _format_display_number(input_min)
    return display, {
        "input_min": input_min,
        "input_max": input_max,
        "scale": scale,
        "display_min": int(np.min(display)) if display.size else 0,
        "display_max": int(np.max(display)) if display.size else 0,
        "formula": f"value = display / {scale_text} + {min_text}",
    }


def _scanner_display_comment(display_meta):
    return (
        f"{OUTPUT_IMAGE_COMMENT}; scanner display uint16 "
        f"{SCANNER_DISPLAY_MIN}-{SCANNER_DISPLAY_MAX}; {display_meta['formula']}"
    )


def _new_dicom_uid():
    return f"2.25.{uuid.uuid4().int}"


def _direction_letters(vector):
    """Return the anatomical letters at the start and end of a direction vector.

    ``(-x, +x)`` yields ``("L", "R")`` because the vector begins on the
    patient's left and ends on the right. The scanner labels the edges of a
    displayed image with exactly these two letters.
    """
    values = np.asarray(vector, dtype=float)
    norm = float(np.linalg.norm(values))
    if norm < 1e-6:
        return ("?", "?")
    unit = values / norm
    axis = int(np.argmax(np.abs(unit)))
    start, end = PATIENT_AXIS_LABELS[axis]
    if unit[axis] < 0:
        start, end = end, start
    return (start, end)


def _direction_label(vector):
    """Describe a patient-space direction vector as, for example, 'R->L'."""
    start, end = _direction_letters(vector)
    if start == "?":
        return "undefined"
    return f"{start}->{end}"


def _format_direction(vector):
    values = np.asarray(vector, dtype=float)
    components = ",".join(f"{float(value):+.4f}" for value in values)
    obliquity = float(np.max(np.abs(values))) if values.size else 0.0
    return f"{_direction_label(values)} [{components}] alignment={obliquity:.4f}"


def _position_label(position):
    """Format a patient-space position the way the scanner prints one, 'R6.3 P2.3 H35.4'."""
    values = np.asarray(position, dtype=float)
    parts = []
    for axis in range(3):
        value = float(values[axis])
        negative, positive = PATIENT_AXIS_LABELS[axis]
        parts.append(f"{positive if value >= 0.0 else negative}{abs(value):.1f}")
    return " ".join(parts)


def _slice_position_label(position):
    """Format the dominant component of a position, matching the scanner 'SP' field."""
    values = np.asarray(position, dtype=float)
    axis = int(np.argmax(np.abs(values)))
    value = float(values[axis])
    negative, positive = PATIENT_AXIS_LABELS[axis]
    return f"{positive if value >= 0.0 else negative}{abs(value):.1f}"


def _unit_vector(vector, fallback=(0.0, 0.0, 1.0)):
    values = np.asarray(vector, dtype=float)
    norm = float(np.linalg.norm(values))
    if norm < 1e-6:
        return np.asarray(fallback, dtype=float)
    return values / norm


def _frame_position(center_position, slice_dir, frame_index, slice_count, slice_spacing_mm):
    """Patient-space centre of one emitted frame.

    ``center_position`` is the centre of the whole volume, which is what the MRD
    header carries, so frame ``frame_index`` of ``slice_count`` sits half a
    volume away from it minus its own offset. ``frame_index`` is zero based;
    the scanner numbers the same frame ``frame_index + 1``.
    """
    offset = (float(frame_index) - (float(slice_count) - 1.0) / 2.0) * float(
        slice_spacing_mm
    )
    return np.asarray(center_position, dtype=float) + offset * _unit_vector(slice_dir)


def _canonicalize_to_display_frame(volume, axis_directions):
    """Rotate a (slices, rows, columns) volume into the DICOM standard display view.

    ``axis_directions`` are the patient-space directions of the volume's own
    axes, in the same (slices, rows, columns) order. The returned directions
    describe the returned volume, so the header stays exactly as honest as it
    was on input: this is a change of display convention, not a correction
    layered on top of one. That also means it cannot move the anatomy, which is
    why the frame-stacking compensation exists as a separate stage.

    The permutation and signs are derived from the acquisition's own vectors
    rather than hardcoded, so obliquity and a non-HFS patient position are
    handled without a second code path.
    """
    directions = [np.asarray(vector, dtype=float) for vector in axis_directions]
    unit_directions = []
    for vector in directions:
        norm = float(np.linalg.norm(vector))
        unit_directions.append(vector / norm if norm > 1e-6 else vector)

    targets = [np.asarray(target, dtype=float) for target in DISPLAY_FRAME_TARGETS]

    # Choose all three axes as one assignment. Greedy target-by-target matching
    # can consume the second-best axis early and force a wrong mapping for a
    # valid oblique rotation.
    permutation = max(
        permutations(range(3)),
        key=lambda candidate: sum(
            abs(float(np.dot(unit_directions[candidate[index]], targets[index])))
            for index in range(3)
        ),
    )
    signs = [
        -1.0
        if float(np.dot(unit_directions[permutation[index]], targets[index])) < 0.0
        else 1.0
        for index in range(3)
    ]

    canonical = np.asarray(volume).transpose(*permutation)
    for output_axis, sign in enumerate(signs):
        if sign < 0.0:
            canonical = np.flip(canonical, axis=output_axis)

    canonical_directions = [
        signs[output_axis] * directions[permutation[output_axis]]
        for output_axis in range(3)
    ]
    return np.ascontiguousarray(canonical), canonical_directions, permutation, signs


def _log_display_frame(permutation, signs, canonical_directions, shape):
    logging.info(
        "DICOM display frame: NormOrientation is disabled on the scanner, so the "
        "volume is rotated into the standard view here (columns to L, rows to P, "
        "slices to H). Direction metadata is transformed with the pixels."
    )
    for output_axis, name in enumerate(DISPLAY_FRAME_AXIS_NAMES):
        logging.info(
            "Display frame axis %d (%s): source axis %d %s -> %s",
            output_axis,
            name,
            permutation[output_axis],
            "reversed" if signs[output_axis] < 0.0 else "kept",
            _format_direction(canonical_directions[output_axis]),
        )
    logging.info("Display frame shape: %s", tuple(int(size) for size in shape))

    slices, rows, columns = canonical_directions
    handedness = float(np.dot(np.cross(_unit_vector(columns), _unit_vector(rows)),
                              _unit_vector(slices)))
    if handedness > 0.0:
        logging.info(
            "Display frame handedness: right-handed (columns x rows . slices = "
            "%+.3f), matching the native reconstruction.",
            handedness,
        )
    else:
        logging.error(
            "Display frame handedness: LEFT-handed (columns x rows . slices = "
            "%+.3f). The scanner will draw this volume from the opposite side of "
            "the native reconstruction. An acquisition frame is always "
            "right-handed, so this means an axis was reversed without its "
            "direction vector, or DISPLAY_FRAME_TARGETS is inconsistent.",
            handedness,
        )


def _compensate_ice_frame_stacking(volume, slice_dir):
    """Reverse the emitted frame order for the scanner, deliberately breaking the header.

    Returns ``(volume, content_slice_dir, reversed_frames)``. ``slice_dir`` comes
    back unchanged when no compensation is applied.

    When it is applied only the pixels move. Negating the vector as well would
    cancel the reversal, because ICE positions the frames from that same vector,
    so there is no emitted volume that is both header-consistent and displayed
    correctly by this FIRE pipeline. This function chooses the scanner, and the
    resulting image therefore carries ``OUTPUT_FRAME_ORDER_REVERSED_ATTRIBUTE``
    so that consumers reading the header instead -- ``mrd2nifti`` above all --
    can undo it. Anything that reads ``slice_dir`` and ignores that attribute
    will place the volume mirrored through-plane.
    """
    if not ICE_STACKS_FRAMES_AGAINST_SLICE_DIR:
        return volume, np.asarray(slice_dir, dtype=float), False

    logging.warning(
        "ICE frame-stacking compensation applied: the emitted frame order is "
        "reversed because ICE positions frames against slice_dir. The emitted "
        "slice_dir still describes the acquisition, so the pixels and the "
        "header disagree by design and '%s' is set to 1 to declare it. The "
        "content runs along %s with increasing frame number. Any consumer that "
        "builds geometry from slice_dir must honour that attribute.",
        OUTPUT_FRAME_ORDER_REVERSED_ATTRIBUTE,
        _direction_label(-np.asarray(slice_dir, dtype=float)),
    )
    return (
        np.ascontiguousarray(np.flip(np.asarray(volume), axis=0)),
        -np.asarray(slice_dir, dtype=float),
        True,
    )


def _log_emitted_geometry(center_position, read_dir, phase_dir, slice_dir, shape, slice_fov_mm):
    """Log what the scanner should display, so one screenshot can confirm or refute it.

    Everything here is derived from the header this app is about to emit. If the
    scanner shows something different, the header was overridden downstream
    rather than computed wrongly here.
    """
    slice_count = int(shape[0])
    spacing = float(slice_fov_mm) / float(slice_count) if slice_count else 0.0

    left, right = _direction_letters(read_dir)
    top, bottom = _direction_letters(phase_dir)
    # The boxed marker is the side the viewer has to look from for the screen
    # basis to be proper, so it follows the DICOM normal columns x rows, not
    # slice_dir.
    view_from, _ = _direction_letters(
        np.cross(_unit_vector(read_dir), _unit_vector(phase_dir))
    )

    logging.info(
        "Predicted scanner display: left edge '%s', right edge '%s', top edge "
        "'%s', bottom edge '%s', viewed from '%s' (the boxed marker). The native "
        "transversal reconstruction shows R, L, A, P and F.",
        left,
        right,
        top,
        bottom,
        view_from,
    )
    logging.info(
        "Emitted volume centre: %s [%s], slice spacing %.5f mm over %d slices",
        _position_label(center_position),
        ",".join(f"{float(value):+.3f}" for value in center_position),
        spacing,
        slice_count,
    )

    if slice_count:
        sample_indices = sorted(
            {0, slice_count // 4, slice_count // 2, (3 * slice_count) // 4, slice_count - 1}
        )
        for index in sample_indices:
            position = _frame_position(
                center_position, slice_dir, index, slice_count, spacing
            )
            logging.info(
                "Predicted frame %d/%d (scanner numbering): SP %s, full position %s",
                index + 1,
                slice_count,
                _slice_position_label(position),
                _position_label(position),
            )
        logging.info(
            "Those positions are where the emitted content sits, with the ICE "
            "frame-stacking compensation (%s) already accounted for. Compare any "
            "one of them against the scanner's slice position for the same frame "
            "number. A match confirms the geometry end to end; a sign difference "
            "means ICE has changed how it stacks frames and "
            "ICE_STACKS_FRAMES_AGAINST_SLICE_DIR must be flipped; a different "
            "magnitude means the volume centre or the field of view disagrees.",
            "on" if ICE_STACKS_FRAMES_AGAINST_SLICE_DIR else "off",
        )


def _log_patient_space_localisation(label, volume, center_position, axis_directions, fov_mm):
    """Log where the signal actually sits in patient coordinates.

    Voxel-index centroids cannot be compared against a scanner screenshot, but
    these positions can. They are what distinguishes a volume that is merely
    stored back to front from one whose content is genuinely in the wrong place.
    ``fov_mm`` is a 3-tuple matching ``volume``'s (slices, rows, columns) axes.
    """
    values = np.asarray(volume, dtype=np.float64)
    if values.ndim != 3 or not values.size:
        logging.info("Patient-space localisation [%s]: unavailable", label)
        return

    total = float(values.sum())
    if total <= 0.0:
        logging.info("Patient-space localisation [%s]: no signal", label)
        return

    centroid_position = np.asarray(center_position, dtype=float).copy()
    for axis, name in enumerate(DISPLAY_FRAME_AXIS_NAMES):
        profile = values.sum(
            axis=tuple(other for other in range(3) if other != axis)
        )
        count = int(profile.size)
        spacing = float(fov_mm[axis]) / float(count)
        indices = np.arange(count, dtype=np.float64)
        centroid_index = float((profile * indices).sum() / profile.sum())
        peak_index = int(np.argmax(profile))

        above = np.flatnonzero(profile >= 0.1 * float(profile.max()))
        extent_mm = (float(above[-1] - above[0]) + 1.0) * spacing if above.size else 0.0

        offset_mm = (centroid_index - (count - 1) / 2.0) * spacing
        direction = _unit_vector(axis_directions[axis])
        centroid_position = centroid_position + offset_mm * direction

        logging.info(
            "Patient-space localisation [%s] %s (%s): centroid index %.2f/%d "
            "(%+.2f mm from centre), peak index %d, signal extent %.1f mm",
            label,
            name,
            _direction_label(direction),
            centroid_index,
            count,
            offset_mm,
            peak_index,
            extent_mm,
        )

    logging.info(
        "Patient-space localisation [%s]: intensity centroid at %s [%s]",
        label,
        _position_label(centroid_position),
        ",".join(f"{float(value):+.3f}" for value in centroid_position),
    )


def _log_volume_statistics(label, volume):
    values = np.asarray(volume, dtype=np.float64)
    if not values.size:
        logging.info("Volume statistics [%s]: empty", label)
        return

    total = float(values.sum())
    if total > 0.0:
        centroid = [
            float(
                (values.sum(axis=tuple(other for other in range(values.ndim) if other != axis))
                 * np.arange(values.shape[axis])).sum()
                / total
            )
            for axis in range(values.ndim)
        ]
        centroid_text = ",".join(
            f"{value:.2f}/{values.shape[axis]}" for axis, value in enumerate(centroid)
        )
    else:
        centroid_text = "undefined"

    logging.info(
        "Volume statistics [%s]: shape=%s dtype=%s min=%.6g max=%.6g mean=%.6g "
        "intensity_centroid_per_axis=%s",
        label,
        tuple(int(size) for size in values.shape),
        np.asarray(volume).dtype,
        float(values.min()),
        float(values.max()),
        float(values.mean()),
        centroid_text,
    )


def _log_reference_geometry(reference_head, metadata):
    try:
        patient_position = str(metadata.measurementInformation.patientPosition)
    except Exception:
        patient_position = "unavailable"

    logging.info(
        "Acquisition geometry: patient_position=%s position=(%s) "
        "patient_table_position=(%s)",
        patient_position,
        ",".join(f"{float(value):+.3f}" for value in reference_head.position),
        ",".join(
            f"{float(value):+.3f}"
            for value in getattr(reference_head, "patient_table_position", ())
        ),
    )
    logging.info("Acquisition read_dir  (MRD x, columns): %s", _format_direction(reference_head.read_dir))
    logging.info("Acquisition phase_dir (MRD y, rows):    %s", _format_direction(reference_head.phase_dir))
    logging.info("Acquisition slice_dir (MRD z, slices):  %s", _format_direction(reference_head.slice_dir))
    logging.info(
        "Acquisition volume centre: %s",
        _position_label(reference_head.position),
    )
    # Siemens builds its PRS frame so that phase x read = slice, which is the
    # opposite cross-product order from DICOM's column x row = normal. Measured
    # data therefore arrives DICOM-left-handed, and that is expected, not a
    # defect: read_dir=(-1,0,0), phase_dir=(0,1,0), slice_dir=(0,0,1) satisfies
    # the Siemens rule exactly. Only the emitted frame has to be DICOM
    # right-handed, and _log_display_frame checks that one.
    prs_handedness = float(
        np.dot(
            np.cross(
                _unit_vector(reference_head.phase_dir),
                _unit_vector(reference_head.read_dir),
            ),
            _unit_vector(reference_head.slice_dir),
        )
    )
    logging.info(
        "Acquisition frame: phase x read . slice = %+.3f (%s under the Siemens "
        "PRS convention). A value near zero would mean the incoming vectors are "
        "not orthonormal and no downstream mapping could be trusted.",
        prs_handedness,
        "consistent" if prs_handedness > 0.0 else "INCONSISTENT",
    )
    logging.info(
        "Geometry stages: 1) the acquisition frame (already partition=slice, "
        "line=phase, sample=read by construction for this Cartesian recon) is "
        "rotated into the DICOM display view, with the direction vectors "
        "transformed together with the pixels, 2) the frame order is reversed "
        "on its own to compensate for how ICE stacks a 3D volume. Stage 1 "
        "cannot change where the anatomy lands, only how it is stored; stage 2 "
        "is the only stage that moves it."
    )


def _log_acquisition_axes(packed_shape, reference_head, flip_slice):
    logging.info(
        "Acquisition-frame volume: shape=%s (reverse_slices=%s), slices along "
        "%s, rows along %s, columns along %s",
        tuple(int(size) for size in packed_shape),
        flip_slice,
        _direction_label(reference_head.slice_dir),
        _direction_label(reference_head.phase_dir),
        _direction_label(reference_head.read_dir),
    )


def process(connection, config, metadata):
    logging.info("Config:\n%s", config)

    try:
        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info(
            "First encoding trajectory=%s matrix=(%s x %s x %s) fov=(%s x %s x %s)mm^3",
            metadata.encoding[0].trajectory,
            metadata.encoding[0].encodedSpace.matrixSize.x,
            metadata.encoding[0].encodedSpace.matrixSize.y,
            metadata.encoding[0].encodedSpace.matrixSize.z,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z,
        )
    except Exception:
        logging.info("Improperly formatted metadata: %s", metadata)

    acquisitions = []
    passthrough_images = []

    try:
        for item in connection:
            if isinstance(item, ismrmrd.Acquisition):
                if _is_imaging_acquisition(item):
                    acquisitions.append(item)

                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_MEASUREMENT):
                    logging.info("Processing %d acquired readouts", len(acquisitions))
                    images = process_raw(acquisitions, connection, config, metadata)
                    connection.send_image(images)
                    acquisitions = []

            elif isinstance(item, ismrmrd.Image):
                passthrough_images.append(item)

            elif item is None:
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        if acquisitions:
            logging.info("Processing %d acquired readouts (end of stream)", len(acquisitions))
            images = process_raw(acquisitions, connection, config, metadata)
            connection.send_image(images)

        if passthrough_images:
            logging.warning(
                "Received %d images instead of raw data; returning them unchanged",
                len(passthrough_images),
            )
            connection.send_image(process_image(passthrough_images, connection, config, metadata))

    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()


def process_raw(group, connection, config, metadata):
    if not group:
        return []

    tic = perf_counter()
    _ensure_debug_folder()

    encoding = metadata.encoding[0]
    orientation_flip_slice = _config_bool(
        config,
        "orientationflipslice",
        OPENRECON_DEFAULTS["orientationflipslice"],
    )
    logging.info("Resolved configuration: orientationflipslice=%s", orientation_flip_slice)

    kspace = _build_kspace_array(group, encoding)
    num_coils, num_partitions, num_lines, num_samples = kspace.shape
    logging.info(
        "Loaded k-space: coils=%d partitions=%d lines=%d samples=%d",
        num_coils,
        num_partitions,
        num_lines,
        num_samples,
    )

    logging.info("Reconstructing with a per-coil 3D FFT")
    coil_images = reconstruct_coil_images(kspace)

    recon_matrix_x = _resolve_recon_matrix_x(encoding, num_samples)
    logging.info("Cropping readout axis from %d to %d samples", num_samples, recon_matrix_x)
    coil_images = crop_readout_oversampling(coil_images, recon_matrix_x)

    logging.info("Combining coils with sum-of-squares")
    output_volume = combine_coils_sos(coil_images)
    logging.info(
        "Reconstructed volume shape (partitions, lines, readout): %s",
        output_volume.shape,
    )

    np.save(os.path.join(debugFolder, "epirecon_kspace.npy"), kspace)
    np.save(os.path.join(debugFolder, "epirecon_coil_images.npy"), coil_images)
    np.save(os.path.join(debugFolder, "epirecon_output_volume.npy"), output_volume)

    reference_head = group[len(group) // 2].getHead()
    recon_fov = encoding.reconSpace.fieldOfView_mm
    output_fov_mm = (float(recon_fov.z), float(recon_fov.y), float(recon_fov.x))

    process_time_ms = (perf_counter() - tic) * 1000.0
    message = f"Cartesian EPI processing time: {process_time_ms:.2f} ms"
    logging.info(message)
    connection.send_logging(constants.MRD_LOGGING_INFO, message)

    return _build_output_images(
        output_volume,
        reference_head,
        metadata,
        output_fov_mm=output_fov_mm,
        flip_slice=orientation_flip_slice,
    )


def process_image(images, connection, config, metadata):
    del connection, config, metadata

    images_out = []
    for image in images:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        meta["Keep_image_geometry"] = 1
        image.attribute_string = meta.serialize()
        images_out.append(image)
    return images_out


def _build_output_images(volume, reference_head, metadata, output_fov_mm, flip_slice=False):
    volume = np.asarray(volume, dtype=np.float32)
    if volume.ndim != 3:
        raise ValueError(f"Reconstructed volume must be 3D, got shape {volume.shape}")

    _log_reference_geometry(reference_head, metadata)
    _log_volume_statistics("reconstructed volume (partition, line, readout)", volume)

    display_volume, display_meta = _scale_volume_to_display_range(volume)
    logging.info(
        "Scanner display scaling: input_min=%.6g input_max=%.6g scale=%s formula='%s'",
        display_meta["input_min"],
        display_meta["input_max"],
        _format_display_number(display_meta["scale"]),
        display_meta["formula"],
    )

    return [
        _build_single_output_image(
            display_volume,
            display_meta,
            reference_head,
            metadata,
            output_fov_mm=output_fov_mm,
            flip_slice=flip_slice,
            series_index=OUTPUT_IMAGE_SERIES_INDEX,
        )
    ]


def _build_single_output_image(
    display_volume,
    display_meta,
    reference_head,
    metadata,
    output_fov_mm,
    flip_slice,
    series_index,
):
    # Stage 1 (a plain optional slice reversal) resolves any ambiguity in the
    # acquisition's own partition order; unlike a non-Cartesian trajectory,
    # Cartesian k-space indices already map directly onto (slice, phase, read)
    # with no per-sequence axis lookup required. Stage 2 transforms the
    # acquisition vectors together with the pixels in
    # _canonicalize_to_display_frame; no display correction changes metadata
    # without changing the corresponding pixels.
    read_dir = np.asarray(reference_head.read_dir, dtype=float)
    phase_dir = np.asarray(reference_head.phase_dir, dtype=float)
    slice_dir = np.asarray(reference_head.slice_dir, dtype=float)
    slice_dir_norm = float(np.linalg.norm(slice_dir))
    if slice_dir_norm < 1e-6:
        logging.warning("Acquisition slice_dir is degenerate; substituting +z (head)")
        slice_dir = np.array([0.0, 0.0, 1.0], dtype=float)
    else:
        slice_dir = slice_dir / slice_dir_norm

    center_position = np.asarray(reference_head.position, dtype=float)

    series_description = f"{_safe_protocol_name(metadata)}_{OUTPUT_SERIES_DESCRIPTION}"
    series_grouping = f"{series_description}_{series_index}"
    series_uid = _new_dicom_uid()
    image_comment = _scanner_display_comment(display_meta)

    # Pack the complete matrix as one explicit 3D MRD image rather than one
    # message per slice, so the DICOM writer produces a single volume matching
    # the native ICE reconstruction contract.
    packed_volume = np.ascontiguousarray(
        np.asarray(display_volume)[::-1, :, :] if flip_slice else np.asarray(display_volume)
    )
    _log_acquisition_axes(packed_volume.shape, reference_head, flip_slice)

    packed_volume, canonical_directions, permutation, signs = (
        _canonicalize_to_display_frame(
            packed_volume,
            (slice_dir, phase_dir, read_dir),
        )
    )
    slice_dir, phase_dir, read_dir = canonical_directions
    canonical_fov_mm = tuple(float(output_fov_mm[permutation[axis]]) for axis in range(3))
    slice_fov_mm, phase_fov_mm, read_fov_mm = canonical_fov_mm
    _log_display_frame(permutation, signs, canonical_directions, packed_volume.shape)

    # Stage 3 compensates for the scanner rather than describing the data, so it
    # is the only place the emitted pixels stop matching the emitted slice_dir.
    packed_volume, content_slice_dir, frames_reversed = _compensate_ice_frame_stacking(
        packed_volume, slice_dir
    )
    _log_emitted_geometry(
        center_position,
        read_dir,
        phase_dir,
        content_slice_dir,
        packed_volume.shape,
        slice_fov_mm,
    )

    slice_count = int(packed_volume.shape[0])
    _log_volume_statistics("packed output", packed_volume)
    _log_patient_space_localisation(
        "packed output",
        packed_volume,
        center_position,
        (content_slice_dir, phase_dir, read_dir),
        canonical_fov_mm,
    )
    image = ismrmrd.Image.from_array(packed_volume, transpose=False)

    new_header = mrdhelper.update_img_header_from_raw(image.getHead(), reference_head)
    new_header.data_type = image.data_type
    new_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    new_header.image_series_index = series_index
    new_header.image_index = 1
    new_header.slice = 0
    new_header.matrix_size = tuple(int(value) for value in image.getHead().matrix_size)
    new_header.position = tuple(float(value) for value in center_position)
    new_header.read_dir = tuple(float(value) for value in read_dir)
    new_header.phase_dir = tuple(float(value) for value in phase_dir)
    new_header.slice_dir = tuple(float(value) for value in slice_dir)
    new_header.field_of_view = (read_fov_mm, phase_fov_mm, slice_fov_mm)
    image.setHead(new_header)
    image.image_series_index = series_index
    image.field_of_view = (
        ctypes.c_float(read_fov_mm),
        ctypes.c_float(phase_fov_mm),
        ctypes.c_float(slice_fov_mm),
    )

    meta = ismrmrd.Meta()
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "FFT", "CARTESIAN"]
    meta["ImageType"] = "DERIVED\\PRIMARY\\M\\epirecon"
    meta["DicomImageType"] = "DERIVED\\PRIMARY\\M\\epirecon"
    meta["ImageTypeValue4"] = "epirecon"
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SequenceDescriptionAdditional"] = OUTPUT_IMAGE_COMMENT
    meta["SeriesDescription"] = series_description
    meta["SequenceDescription"] = series_description
    meta["ProtocolName"] = series_description
    meta["SeriesNumberRangeNameUID"] = series_grouping
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = _new_dicom_uid()
    meta["ImageComment"] = image_comment
    meta["ImageComments"] = image_comment
    # 1 tells ICE to keep the geometry described in this header instead of
    # rebuilding it and applying its own flip/shift. The in-plane vectors
    # describe the emitted pixels. slice_dir does not: stage 3 reverses the
    # frames against it on purpose, because ICE stacks them that way, and
    # OUTPUT_FRAME_ORDER_REVERSED_ATTRIBUTE below declares that so a consumer
    # reading this header can undo it.
    meta["Keep_image_geometry"] = 1
    meta["partition_count"] = 1
    meta["slice_count"] = slice_count
    meta["NumberOfSlices"] = slice_count
    meta["ImagesInAcquisition"] = slice_count
    meta["NumberInSeries"] = 1
    meta["SliceNo"] = 0
    meta["IsmrmrdSliceNo"] = 0
    meta["AnatomicalSliceNo"] = 0
    meta["ChronSliceNo"] = 0
    meta["ProtocolSliceNumber"] = 0
    meta["Actual3DImagePartNumber"] = 0
    meta["Actual3DImaPartNumber"] = 0
    meta["AnatomicalPartitionNo"] = 0
    meta["ImageRowDir"] = [f"{float(value):.18f}" for value in read_dir]
    meta["ImageColumnDir"] = [f"{float(value):.18f}" for value in phase_dir]
    meta["ImageSliceNormDir"] = [f"{float(value):.18f}" for value in slice_dir]
    meta["SlicePosLightMarker"] = [
        f"{float(value):.18f}" for value in new_header.position
    ]
    meta["epireconDisplayScale"] = _format_display_number(display_meta["scale"])
    meta["epireconDisplayInputMin"] = f"{float(display_meta['input_min']):.6g}"
    meta["epireconDisplayInputMax"] = f"{float(display_meta['input_max']):.6g}"
    meta["epireconDisplayMin"] = str(int(display_meta["display_min"]))
    meta["epireconDisplayMax"] = str(int(display_meta["display_max"]))
    meta["epireconDisplayFormula"] = display_meta["formula"]
    meta["epireconOrientationFlipSlice"] = str(int(bool(flip_slice)))
    meta[OUTPUT_FRAME_ORDER_REVERSED_ATTRIBUTE] = str(int(bool(frames_reversed)))
    image.attribute_string = meta.serialize()

    logging.info(
        "Emitting image: series_index=%d series_description='%s' matrix_size=%s "
        "slice_count=%d fov_mm=(read=%.3f,phase=%.3f,slice=%.3f) flip_slice=%s "
        "columns=%s rows=%s slices=%s "
        "Keep_image_geometry=%s series_uid=%s",
        series_index,
        series_description,
        tuple(int(value) for value in new_header.matrix_size),
        slice_count,
        read_fov_mm,
        phase_fov_mm,
        slice_fov_mm,
        flip_slice,
        _direction_label(read_dir),
        _direction_label(phase_dir),
        _direction_label(slice_dir),
        meta["Keep_image_geometry"],
        series_uid,
    )

    return image
