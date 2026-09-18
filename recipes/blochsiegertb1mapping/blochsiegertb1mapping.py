"""OpenRecon Bloch-Siegert B1 mapping pipeline.

At a high level, this script:

1. Loads the reconstructed magnitude and phase frames for each transmit
   channel and identifies the frame groups used for the mapping.
2. Averages the configured pre-dummy and post-dummy reference frames to
   estimate the B0 map from phase evolution.
3. Derives a trusted-phase mask from phase stability within the magnitude mask.
4. Before any polynomial fitting of the BS phase, fits a degree-2 temporal
   phase baseline to the retained reference frames
   (pre-dummy, C/D, and B0-corrected post-reference frames) at trusted voxels,
   after removing each voxel's average phase to avoid wrapping; the average is
   restored to the interpolated baseline before correcting the BS frames.
5. Fills untrusted BS-phase voxels using MATLAB-regionfill-style harmonic
   inpainting, while filtering B0 with the existing trusted-voxel polynomial.
   The previous polynomial BS-phase extrapolation remains in
   `_extrapolate_phase_volume` (and can be restored in
   `_filter_bloch_siegert_maps` if this method is not suitable).
6. Builds a Bloch-Siegert lookup table from the supplied RF pulse and pulse
   width, then interpolates B1 values from the measured B0 and BS phase.
7. Generates the B1 maps and the 1Tx reference-amplitude output using the GUI
   reference amplitude.
8. Applies the magnitude mask to outputs when requested and writes positioned
   per-slice DICOM images.
"""

import base64
import gc
import io
import json
import logging
import math
import os
import re
import traceback
import uuid
from functools import lru_cache

import ismrmrd
import numpy as np

try:
    import constants
except ImportError:
    class constants:
        MRD_LOGGING_ERROR = 3


RECIPE_NAME = "blochsiegertb1mapping"
PHASE_WRAP = 4096.0
BSS_PULSE_WIDTH_MS = 6.0
DELTA_TE_MS = 1.0
PRE_DUMMY = 2
POST_DUMMY = 2
ECHOES_PER_TX = 4
KBS_SCALE = 0.044 / 6.0
SCANNER_DISPLAY_MIN = 0
SCANNER_DISPLAY_MAX = 4095
PROCESSING_INFO_SERIES_INDEX = 180
FILTER_SMOOTH_SIGMA = 0.5

B1_SERIES_INDEX_START = 101
BSP_SERIES_INDEX_START = 120
PHSC_SERIES_INDEX_START = 140
B0_SERIES_INDEX = 160
REF_AMPLITUDE_SERIES_INDEX = 170
FILTER_POLYNOMIAL_ORDER_BY_TX = {1: 10, 8: 10, 16: 10}


class FrameCountMismatchError(ValueError):
    """Raised when a slice cannot be identified as a configured sequence."""

SOURCE_PARENT_REFERENCE_META_KEYS = {
    "DicomEngineDimString",
    "MFInstanceNumber",
    "MultiFrameSOPInstanceUID",
    "PSMultiFrameSOPInstanceUID",
    "PSSeriesInstanceUID",
    "SOPInstanceUID",
}
SOURCE_PARENT_REFERENCE_META_PREFIXES = (
    "ReferencedGSPS",
    "ReferencedImageSequence",
)
SCANNER_WRITE_UNSAFE_META_KEYS = {
    "ImageTypeValue3",
}


def process(connection, config, metadata):
    logging.info("Config: %s", config)
    input_images = []

    try:
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                input_images.append(item)
            else:
                logging.info("Ignoring unsupported MRD message %s", type(item).__name__)

        if not input_images:
            logging.warning("No image messages received; closing without output")
            return

        settings = _settings_from_config(config)
        result = compute_bloch_siegert_maps(input_images, settings)
        output_images = build_output_images(result, settings, input_images)

        logging.info(
            "Bloch-Siegert OpenRecon: slices=%d nframe=%d ntx=%d outputs=%d",
            result["slice_count"],
            result["nframe"],
            result["ntx"],
            len(output_images),
        )
        if not output_images:
            logging.info("No output maps enabled; closing without output")
            return

        _validate_output_images(output_images, input_images)
        _send_images_by_series(connection, output_images)

    except FrameCountMismatchError as error:
        # A wrong dummy-frame setting must not produce mis-indexed maps.
        # Stop cleanly after logging the reason; no output images are sent.
        logging.warning("No maps generated: %s", error)
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        # The scanner may invoke the recon repeatedly in the same worker.
        # Drop the large input/output/result object graphs before closing so
        # NumPy buffers are returned promptly instead of being retained until
        # a later garbage-collection cycle.
        try:
            output_images.clear()
        except (NameError, UnboundLocalError):
            pass
        try:
            input_images.clear()
        except (NameError, UnboundLocalError):
            pass
        try:
            result.clear()
        except (NameError, UnboundLocalError, AttributeError):
            pass
        gc.collect()
        connection.send_close()


def compute_bloch_siegert_maps(input_images, settings=None):
    settings = dict(settings or {})
    pre_dummy = _setting_int(settings, "predummy", default=PRE_DUMMY)
    post_dummy = _setting_int(settings, "postdummy", default=POST_DUMMY)
    expected_frame_counts = (
        _required_frame_count(1, pre_dummy, post_dummy),
        _required_frame_count(8, pre_dummy, post_dummy),
        _required_frame_count(16, pre_dummy, post_dummy),
    )
    magnitude_images, phase_images = _split_magnitude_phase_images(
        input_images,
        minimum_frame_count=min(expected_frame_counts),
    )
    logging.info(
        "Bloch-Siegert frame split: magnitude=%d phase=%d; expected per-slice "
        "counts: 1Tx=%d, 8Tx=%d, 16Tx=%d (pre_dummy=%d, post_dummy=%d)",
        len(magnitude_images),
        len(phase_images),
        expected_frame_counts[0],
        expected_frame_counts[1],
        expected_frame_counts[2],
        pre_dummy,
        post_dummy,
    )
    if not magnitude_images:
        raise ValueError("Bloch-Siegert mapping requires magnitude image messages")
    if not phase_images:
        raise ValueError("Bloch-Siegert mapping requires phase image messages")
    gui_pulse_width = _setting_float(
        settings, "bspulsewidthms", default=BSS_PULSE_WIDTH_MS
    )
    miniice_pulse_width = _miniice_pulse_width(magnitude_images)
    if miniice_pulse_width is not None:
        settings["bspulsewidthms"] = miniice_pulse_width
        logging.info(
            "WIP_ABSpw found in mini-ICE (MR_TAG_SEQ_WIP2): %.6g ms; "
            "overriding GUI RF pulse width %.6g ms",
            miniice_pulse_width,
            gui_pulse_width,
        )
    else:
        logging.info(
            "WIP_ABSpw/MR_TAG_SEQ_WIP2 not found in mini-ICE; using GUI "
            "RF pulse width %.6g ms",
            gui_pulse_width,
        )

    magnitude_groups = _group_frames_by_slice(
        magnitude_images,
        expected_frame_counts,
    )
    phase_groups = _group_frames_by_slice(
        phase_images,
        expected_frame_counts,
    )
    magnitude_groups, phase_groups = _pair_slice_groups(magnitude_groups, phase_groups)

    slice_results = []
    nframe = None
    ntx = None

    for slice_index, (magnitude_group, phase_group) in enumerate(
        zip(magnitude_groups, phase_groups)
    ):
        sequence_nframe, sequence_ntx = _sequence_shape(
            min(len(magnitude_group), len(phase_group)),
            pre_dummy,
            post_dummy,
        )
        if nframe is None:
            nframe = sequence_nframe
            ntx = sequence_ntx
        elif nframe != sequence_nframe or ntx != sequence_ntx:
            raise ValueError(
                "All Bloch-Siegert slices must use the same sequence shape; "
                f"slice 0 has nframe={nframe}, ntx={ntx}, slice {slice_index} "
                f"has nframe={sequence_nframe}, ntx={sequence_ntx}"
            )

        slice_results.append(
            _compute_slice_maps(
                magnitude_group[:nframe],
                phase_group[:nframe],
                ntx,
                settings,
                slice_index,
            )
        )

    b1 = np.concatenate([item["b1"] for item in slice_results], axis=1)
    bsp = np.concatenate([item["bsp"] for item in slice_results], axis=1)
    phsc = np.concatenate([item["phsc"] for item in slice_results], axis=1)
    b0 = np.concatenate([item["b0"] for item in slice_results], axis=0)
    phase_mask = np.concatenate(
        [item["phase_mask"] for item in slice_results],
        axis=0,
    )
    mask_for_magnitude = _fill_holes_2d_or_3d(np.concatenate(
        [item["mask_for_magnitude"] for item in slice_results],
        axis=0,
    ))

    # Preserve only the lightweight geometry anchors; per-slice results are
    # no longer needed after the volume assembly.  Do
    # not keep references to every intermediate slice while filtering and
    # LUT interpolation allocate their own working arrays.
    anchor_images = [group[0] for group in magnitude_groups]
    del magnitude_groups, phase_groups, slice_results
    gc.collect()

    bsp, b0 = _filter_bloch_siegert_maps(
        bsp,
        b0,
        phase_mask,
        mask_for_magnitude,
        apply_bsp_to_entire_volume=_setting_bool(
            settings,
            "applyfilter",
            default=False,
        ),
    )
    pulse_width = _setting_float(
        settings,
        "bspulsewidthms",
        default=BSS_PULSE_WIDTH_MS,
    )
    kbs = KBS_SCALE * pulse_width
    if kbs <= 0:
        raise ValueError(f"bspulsewidthms must be positive, got {pulse_width}")
    # The LUT is defined for nonnegative Bloch-Siegert phase shifts.  Small
    # negative measured shifts are noise/polarity residuals and must match the
    # previous analytical path, which clamped them to zero before calculating
    # B1. Passing them to the scattered interpolant otherwise selects the
    # nearest B1=0 edge and makes the map appear binary.
    raw_negative_bsp = int(np.count_nonzero(np.asarray(bsp) < 0))
    # Keep the post-filter BS phase nonnegative for both the BSp output and
    # the LUT lookup.  The LUT is defined only for nonnegative phase shifts.
    bsp = np.maximum(np.asarray(bsp, dtype=np.float32), 0.0)
    logging.info(
        "B1 LUT phase clamp: raw_negative_values=%d/%d; "
        "lut_input_range=[%.6g,%.6g]",
        raw_negative_bsp,
        int(bsp.size),
        float(np.min(bsp)),
        float(np.max(bsp)),
    )
    b1 = _b1_from_lookup_table(bsp, b0, pulse_width)
    logging.info(
        "Final B1 map after LUT: shape=%s range=[%.6g,%.6g] "
        "unique_values=%d zero_count=%d one_count=%d",
        b1.shape,
        float(np.nanmin(b1)),
        float(np.nanmax(b1)),
        int(np.unique(b1).size),
        int(np.count_nonzero(b1 == 0)),
        int(np.count_nonzero(b1 == 1)),
    )

    return {
        "nframe": nframe,
        "ntx": ntx,
        "slice_count": int(b0.shape[0]),
        "anchor_images": anchor_images,
        "magnitude_images": magnitude_images,
        "phase_images": phase_images,
        "b1": b1.astype(np.float32),
        "bsp": bsp.astype(np.float32),
        "phsc": phsc.astype(np.float32),
        "b0": b0.astype(np.float32),
        "pulse_width": pulse_width,
        "phase_mask": phase_mask.astype(np.uint16),
        "mask_for_magnitude": mask_for_magnitude.astype(np.uint16),
        "mask": mask_for_magnitude.astype(np.uint16),
    }


def build_output_images(result, settings=None, input_images=None):
    settings = dict(settings or {})
    input_images = input_images or result["magnitude_images"] + result["phase_images"]
    series_indices = _allocate_output_series_indices(input_images, result["ntx"])
    anchor = result["anchor_images"][0]
    slice_anchors = result["anchor_images"]
    source_name = _source_series_name(anchor) or RECIPE_NAME
    pulse_width = float(result.get("pulse_width", _setting_float(
        settings, "bspulsewidthms", default=BSS_PULSE_WIDTH_MS
    )))
    apply_mask = _setting_bool(settings, "applymask", default=False)
    output_mask = np.asarray(result["mask_for_magnitude"], dtype=bool)

    def output_volume(volume):
        values = np.asarray(volume)
        if not apply_mask:
            return values
        if values.shape != output_mask.shape:
            raise ValueError(
                "MaskForMagnitude shape does not match output map shape: "
                f"mask={output_mask.shape}, map={values.shape}"
            )
        return np.where(output_mask, values, 0)

    outputs = []
    for tx_index in range(result["ntx"]):
        outputs.append(
            _map_to_mrd_image(
                output_volume(result["b1"][tx_index]),
                anchor,
                slice_anchors,
                series_indices["b1"][tx_index],
                _map_series_name(source_name, "b1", tx_index, result["ntx"]),
                "BSSB1",
                "BlochSiegertB1Map",
                "uT",
                tx_index,
            )
        )

    if _setting_bool(settings, "sendbsp", default=True):
        for tx_index in range(result["ntx"]):
            outputs.append(
                _map_to_mrd_image(
                    output_volume(result["bsp"][tx_index]),
                    anchor,
                    slice_anchors,
                    series_indices["bsp"][tx_index],
                    _map_series_name(source_name, "bsp", tx_index, result["ntx"]),
                    "BSSBSP",
                    "BlochSiegertPhase",
                    "degrees",
                    tx_index,
                )
            )

    if _setting_bool(settings, "sendphsc", default=True):
        for phase_index in range(result["phsc"].shape[0]):
            outputs.append(
                _map_to_mrd_image(
                    output_volume(result["phsc"][phase_index]),
                    anchor,
                    slice_anchors,
                    series_indices["phsc"][phase_index],
                    _map_series_name(
                        source_name,
                        "phsc",
                        phase_index,
                        result["phsc"].shape[0],
                    ),
                    "BSSPHSC",
                    "BlochSiegertTransmitPhase",
                    "degrees",
                    phase_index,
                )
            )

    outputs.append(
        _map_to_mrd_image(
            output_volume(result["b0"]),
            anchor,
            slice_anchors,
            series_indices["b0"],
            f"{source_name} OR B0 Map",
            "BSSB0",
            "BlochSiegertB0Map",
            "Hz",
        )
    )

    if result["ntx"] == 1:
        reference_amp = _setting_float(
            settings,
            "abstxrefamp",
            default=200.0,
        )
        ref_amplitude = np.divide(
            11.74 * reference_amp,
            result["b1"][0],
            out=np.zeros_like(result["b1"][0], dtype=np.float32),
            where=np.asarray(result["b1"][0]) > 0,
        )
        ref_amplitude = np.minimum(ref_amplitude, 1000.0)
        ref_amplitude = np.round(ref_amplitude, 1)
        outputs.append(
            _map_to_mrd_image(
                output_volume(ref_amplitude),
                anchor,
                slice_anchors,
                series_indices["refamp"],
                f"{source_name} OR V_Ref_Amp",
                "BSSREFAMP",
                "BlochSiegertReferenceAmplitude",
                "V",
            )
        )

    outputs.extend(
        _build_processing_info_images(
            result,
            anchor,
            source_name,
            series_indices["processing"],
        )
    )
    outputs.extend(
        _build_processing_info_images(
            result,
            anchor,
            source_name,
            series_indices["processing_b0"],
            map_key="b0",
            map_label="B0",
            units="Hz",
        )
    )

    return outputs


def _diagnostic_quantiles(values):
    values = np.asarray(values, dtype=np.float64)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return float("nan"), float("nan"), float("nan")
    return tuple(float(value) for value in np.percentile(finite, [5, 50, 95]))


def _build_processing_info_images(
    result,
    anchor,
    source_name,
    series_index,
    map_key="b1",
    map_label="B1",
    units="uT",
):
    """Create one masked histogram image per channel in one derived series."""
    mask = np.asarray(result["mask_for_magnitude"], dtype=bool)
    images = []
    series_name = f"{source_name} OR {map_label} Hist"
    volumes = np.asarray(result[map_key], dtype=np.float64)
    if volumes.ndim == 3:
        volumes = volumes[np.newaxis, ...]
    for tx_index, values in enumerate(volumes):
        if values.shape != mask.shape:
            raise ValueError(
                "Processing mask shape does not match B1 volume: "
                f"mask={mask.shape}, {map_label}={values.shape}"
            )
        histogram_mask = mask & np.isfinite(values)
        # Zero B1 values generally represent clamped/untrusted voxels and
        # dominate the nTx=8 histogram.  Exclude them from B1 statistics only;
        # B0 and other map histograms retain their zero values.
        if map_label.lower() == "b1":
            histogram_mask &= np.abs(values) > 1e-6
        finite_values = values[histogram_mask]
        logging.info(
            "%s histogram samples: channel=%d retained=%d excluded_zero=%d",
            map_label,
            tx_index + 1,
            int(finite_values.size),
            int(np.count_nonzero(mask & np.isfinite(values)) - finite_values.size),
        )
        pixels = _histogram_pixels(finite_values, tx_index + 1, map_label, units)
        display_pixels, display_meta = _scale_volume_to_display_range(
            pixels[np.newaxis, ...],
            "gray",
        )
        display_meta["comment"] = (
            "processing-information grayscale image; scanner display uint16 "
            f"{SCANNER_DISPLAY_MIN}-{SCANNER_DISPLAY_MAX}; "
            + display_meta["formula"]
        )
        output = ismrmrd.Image.from_array(display_pixels, transpose=False)
        header = anchor.getHead()
        header.data_type = output.data_type
        header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
        header.image_series_index = int(series_index)
        header.image_index = tx_index + 1
        header.slice = 0
        # Histograms are synthetic display images, not anatomical slices.
        # Use a canonical image orientation so scanner-specific anchor
        # orientations cannot rotate or mirror the text (notably on Cima).
        _set_header_sequence_field(header, "read_dir", [1.0, 0.0, 0.0])
        _set_header_sequence_field(header, "phase_dir", [0.0, 1.0, 0.0])
        _set_header_sequence_field(header, "slice_dir", [0.0, 0.0, 1.0])
        _set_header_sequence_field(header, "position", [0.0, 0.0, 0.0])
        _set_header_sequence_field(header, "matrix_size", [1024, 1024, 1])
        _set_header_sequence_field(header, "field_of_view", [200.0, 200.0, 1.0])
        output.setHead(header)
        output.image_series_index = int(series_index)
        meta = _output_meta(
            anchor,
            header,
            series_index,
            series_name,
            "BSSINFO",
            "BlochSiegertProcessingInfo",
            "gray",
            1,
            # Use a readable scanner display window for histogram images.
            # DICOM convention is WindowCenter (level) followed by
            # WindowWidth: WL=150, WW=350.
            150.0,
            350.0,
            tx_index=tx_index,
            display_meta=display_meta,
            instance_index=tx_index + 1,
            series_image_count=volumes.shape[0],
        )
        meta["ImageComments"] = (
            f"{series_name}; {map_label} histogram and cumulative sum; channel "
            f"{tx_index + 1}; source values in {units}; "
            + meta["BlochSiegertDisplayFormula"]
        )
        meta["ImageComment"] = meta["ImageComments"]
        output.attribute_string = meta.serialize()
        images.append(output)
    return images


def _histogram_pixels(values, channel_index, map_label, units):
    finite_values = np.asarray(values, dtype=np.float64)
    finite_values = finite_values[np.isfinite(finite_values)]
    if finite_values.size == 0:
        finite_values = np.asarray([0.0], dtype=np.float64)
    value_min = float(np.min(finite_values))
    value_max = float(np.max(finite_values))
    # B0 can contain isolated phase-wrap/extrapolation outliers, so use a
    # robust range for that histogram.  Do not do this for B1: clipping its
    # upper percentile can make a correctly scaled 7--12 uT map appear as a
    # sub-unit histogram.  B1 samples have already had zero values removed.
    if map_label.lower() == "b0":
        display_min, display_max = np.percentile(finite_values, [1.0, 99.0])
        if display_min < display_max:
            histogram_values = np.clip(finite_values, display_min, display_max)
            low_outliers = int(np.count_nonzero(finite_values < display_min))
            high_outliers = int(np.count_nonzero(finite_values > display_max))
        else:
            display_min, display_max = value_min, value_max
            histogram_values = finite_values
            low_outliers = high_outliers = 0
    else:
        display_min, display_max = value_min, value_max
        histogram_values = finite_values
        low_outliers = high_outliers = 0
    if value_min == value_max:
        half_width = max(abs(value_min) * 0.05, 0.5)
        histogram_range = (value_min - half_width, value_max + half_width)
    else:
        histogram_range = (float(display_min), float(display_max))
    histogram, edges = np.histogram(
        histogram_values,
        bins=256,
        range=histogram_range,
    )
    centers = (edges[:-1] + edges[1:]) / 2.0
    cumulative = np.cumsum(histogram).astype(np.float64)
    cumulative /= cumulative[-1] if cumulative[-1] else 1.0
    q1, median, q3 = np.percentile(finite_values, [25.0, 50.0, 75.0])
    iqr = q3 - q1

    # Plotting is intentionally imported only when processing images are
    # actually requested.  Importing matplotlib/Pillow at module startup
    # consumes substantial scanner memory before any data arrive.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from PIL import Image

    figure, (histogram_axis, cumulative_axis) = plt.subplots(
        2,
        1,
        figsize=(1024 / 300.0, 1024 / 300.0),
        dpi=300,
    )
    figure.suptitle(
        f"{map_label} Histogram and Cumulative Sum - Channel {channel_index}\n"
        f"IQR: {iqr:.3f} {units}, Median: {median:.3f} {units}; "
        f"display: [{display_min:.3f}, {display_max:.3f}] {units}; "
        f"outliers: {low_outliers + high_outliers}",
        fontsize=8,
    )
    histogram_axis.plot(centers, histogram, color="black")
    # The rendered image is rotated 180 degrees for scanner display.  Reverse
    # the plot axis before rasterization so the value distribution remains
    # left-to-right in the final image.
    histogram_axis.invert_xaxis()
    histogram_axis.set_xlabel(f"{map_label} ({units})")
    histogram_axis.set_ylabel("Count")
    histogram_axis.tick_params(labelsize=6)
    histogram_axis.grid()
    cumulative_axis.plot(centers, cumulative, color="black")
    cumulative_axis.invert_xaxis()
    cumulative_axis.set_xlabel(f"{map_label} ({units})")
    cumulative_axis.set_ylabel("Cumulative fraction")
    cumulative_axis.tick_params(labelsize=6)
    cumulative_axis.grid()
    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(figure)
    buffer.seek(0)
    image = Image.open(buffer).convert("L").resize(
        (1024, 1024),
        resample=Image.Resampling.LANCZOS,
    )
    # The scanner display requires a 180-degree rotation for these synthetic
    # processing images.  Do not add a horizontal mirror here: it makes the
    # lettering unreadable even when the image orientation looks correct.
    pixels = np.asarray(image, dtype=np.uint16)
    pixels = np.rot90(pixels, 2)
    return pixels


def _phase_input_domain(data_min, data_max, phase_wrap):
    tolerance = 0.01
    if data_min >= -math.pi - tolerance and data_max <= math.pi + tolerance:
        return "radians_candidate"
    if phase_wrap <= 0:
        return "native_values"
    if data_min >= -tolerance and data_max <= phase_wrap + tolerance:
        return "unsigned_counts_candidate"
    if data_min >= -phase_wrap - tolerance and data_max <= phase_wrap + tolerance:
        return "signed_or_rescaled_counts_candidate"
    return "unknown"


def _format_frame_indices(indices):
    return "[" + ",".join(str(int(index + 1)) for index in indices) + "]"


def _compute_slice_maps(magnitude_images, phase_images, ntx, settings, slice_index):
    magnitude_stack = np.stack([_image_volume_data(image) for image in magnitude_images])
    phase_stack = np.stack([_image_volume_data(image) for image in phase_images])

    magnitude_nonfinite = int(np.count_nonzero(~np.isfinite(magnitude_stack)))
    phase_nonfinite = int(np.count_nonzero(~np.isfinite(phase_stack)))
    magnitude_stack = np.nan_to_num(magnitude_stack.astype(np.float32), copy=False)
    mask_for_magnitude = _bloch_siegert_magnitude_mask(magnitude_stack, ntx)
    raw_phase_finite = phase_stack[np.isfinite(phase_stack)]
    if raw_phase_finite.size:
        raw_phase_min = float(np.min(raw_phase_finite))
        raw_phase_max = float(np.max(raw_phase_finite))
    else:
        raw_phase_min = 0.0
        raw_phase_max = 0.0
    raw_phase_quantiles = _diagnostic_quantiles(raw_phase_finite)
    phase_wrap = _setting_float(settings, "phasewrap", default=PHASE_WRAP)
    phase_source_meta = _meta_from_image(phase_images[0])
    logging.info(
        "Bloch-Siegert phase input diagnostics: slice=%d raw=[%.6g,%.6g] "
        "raw_q05_50_95=[%.6g,%.6g,%.6g] "
        "domain=%s phasewrap=%.6g source_slope=%s source_intercept=%s",
        slice_index + 1,
        raw_phase_min,
        raw_phase_max,
        *raw_phase_quantiles,
        _phase_input_domain(raw_phase_min, raw_phase_max, phase_wrap),
        phase_wrap,
        _meta_text(phase_source_meta, "RescaleSlope") or "missing",
        _meta_text(phase_source_meta, "RescaleIntercept") or "missing",
    )


    pre_dummy = _setting_int(settings, "predummy", default=PRE_DUMMY)
    post_dummy = _setting_int(settings, "postdummy", default=POST_DUMMY)
    phase_stack = np.nan_to_num(phase_stack.astype(np.float32), copy=False)
    phase_stack = _phase_to_radians(phase_stack, phase_wrap)

    complex_phase = np.exp(1j * phase_stack)

    # The B0 pre-reference excludes up to the first two unstable frames while
    # always retaining the last available pre-reference frame. It is not used
    # by the BSp or B1 calculations.
    b0_pre_start = min(2, pre_dummy)
    b0_pre_stop = pre_dummy + 1
    b0_pre_reference = np.mean(
        complex_phase[b0_pre_start:b0_pre_stop],
        axis=0,
    )
    tx_indices = np.arange(ntx) * ECHOES_PER_TX
    phase_a_indices = pre_dummy + 3 + tx_indices
    phase_b_indices = pre_dummy + 1 + tx_indices
    phase_c_indices = pre_dummy + 2 + tx_indices
    phase_d_indices = pre_dummy + 4 + tx_indices
    phase_a = complex_phase[phase_a_indices]
    phase_b = complex_phase[phase_b_indices]
    phase_c = complex_phase[phase_c_indices]
    phase_d = complex_phase[phase_d_indices]
    post_start = ECHOES_PER_TX * ntx + pre_dummy + 1
    post_stop = post_start + post_dummy + 1
    tx_phase_start = post_stop
    tx_phase_stop = tx_phase_start + ntx
    phase_mask, phase_difference_std_degrees = _phase_stability_mask(
        complex_phase,
        pre_dummy,
        phase_c_indices,
        phase_d_indices,
        mask_for_magnitude,
    )
    b0_phase = _estimate_post_reference_phase(
        complex_phase,
        b0_pre_reference,
        post_start,
        post_stop,
    )
    phase_a, phase_b = _correct_bs_phase_temporal_baseline(
        complex_phase,
        phase_a,
        phase_b,
        phase_c,
        phase_d,
        b0_pre_reference,
        phase_mask,
        b0_pre_start,
        b0_pre_stop,
        post_start,
        post_stop,
        phase_a_indices,
        phase_b_indices,
        phase_c_indices,
        phase_d_indices,
        b0_phase,
    )
    # Scanner DICOM phase has the opposite BSp polarity to the positive phase
    # convention used by the MATLAB-derived B1 calculation. Correct that
    # polarity before applying the negative branch-cut unwrap.
    raw_bsp = -np.angle(phase_a * np.conj(phase_b)).astype(np.float32)
    wrap_threshold = math.radians(-90.0)
    wrap_mask = raw_bsp < wrap_threshold
    bsp = raw_bsp.copy()
    bsp[wrap_mask] += 2 * math.pi

    pulse_width = _setting_float(settings, "bspulsewidthms", default=BSS_PULSE_WIDTH_MS)
    kbs = KBS_SCALE * pulse_width
    if kbs <= 0:
        raise ValueError(f"bspulsewidthms must be positive, got {pulse_width}")
    bsp_for_b1 = np.maximum(bsp, 0.0)
    b1 = np.sqrt(bsp_for_b1 / kbs)

    if slice_index == 0:
        logging.info(
            "Bloch-Siegert frame layout (1-based): b0_pre=%d-%d B=%s C=%s "
            "A=%s D=%s post=%d-%d tx_phase=%d-%d",
            b0_pre_start + 1,
            pre_dummy + 1,
            _format_frame_indices(phase_b_indices),
            _format_frame_indices(phase_c_indices),
            _format_frame_indices(phase_a_indices),
            _format_frame_indices(phase_d_indices),
            post_start + 1,
            post_stop,
            tx_phase_start + 1,
            tx_phase_stop,
        )

    logging.info(
        "Bloch-Siegert input diagnostics: slice=%d magnitude_nonfinite=%d "
        "phase_nonfinite=%d phase_mask_foreground=%d/%d "
        "magnitude_mask_foreground_before_hole_fill=%d/%d "
        "mask_applied_to_maps=%s "
        "phase_difference_std_deg=[%.3f,%.3f]",
        slice_index + 1,
        magnitude_nonfinite,
        phase_nonfinite,
        int(np.count_nonzero(phase_mask)),
        int(phase_mask.size),
        int(np.count_nonzero(mask_for_magnitude)),
        int(mask_for_magnitude.size),
        _setting_bool(settings, "applymask", default=False),
        float(np.min(phase_difference_std_degrees)),
        float(np.max(phase_difference_std_degrees)),
    )
    pre_degrees = np.degrees(np.angle(b0_pre_reference))
    pre_phase_quantiles = _diagnostic_quantiles(pre_degrees[phase_mask])
    pre_coherence_quantiles = _diagnostic_quantiles(
        np.abs(b0_pre_reference)[phase_mask]
    )
    logging.info(
        "Bloch-Siegert masked pre-reference diagnostics: slice=%d voxels=%d "
        "phase_deg_q05_50_95=[%.3f,%.3f,%.3f] "
        "coherence_q05_50_95=[%.6g,%.6g,%.6g]",
        slice_index + 1,
        int(np.count_nonzero(phase_mask)),
        *pre_phase_quantiles,
        *pre_coherence_quantiles,
    )
    for tx_index in range(ntx):
        raw_degrees = np.degrees(raw_bsp[tx_index])
        bsp_degrees = np.degrees(bsp[tx_index])
        negative_for_b1 = bsp[tx_index] < 0
        logging.info(
            "Bloch-Siegert BSp diagnostics: slice=%d tx=%d voxels=%d "
            "raw_deg=[%.3f,%.3f] dicom_polarity_corrected=true wrapped=%d "
            "retained_negative=%d phase_zero=%d b1_clamped=%d "
            "b1_uT=[%.6g,%.6g]",
            slice_index + 1,
            tx_index + 1,
            int(bsp[tx_index].size),
            float(np.min(raw_degrees)),
            float(np.max(raw_degrees)),
            int(np.count_nonzero(wrap_mask[tx_index])),
            int(np.count_nonzero(negative_for_b1)),
            int(np.count_nonzero(bsp_degrees == 0)),
            int(np.count_nonzero(negative_for_b1)),
            float(np.min(b1[tx_index])),
            float(np.max(b1[tx_index])),
        )
        phase_b_quantiles = _diagnostic_quantiles(
            np.degrees(np.angle(phase_b[tx_index]))[phase_mask]
        )
        phase_c_quantiles = _diagnostic_quantiles(
            np.degrees(np.angle(phase_c[tx_index]))[phase_mask]
        )
        phase_a_quantiles = _diagnostic_quantiles(
            np.degrees(np.angle(phase_a[tx_index]))[phase_mask]
        )
        phase_d_quantiles = _diagnostic_quantiles(
            np.degrees(np.angle(phase_d[tx_index]))[phase_mask]
        )
        logging.info(
            "Bloch-Siegert masked phase-term diagnostics: slice=%d tx=%d "
            "B_deg_q05_50_95=[%.3f,%.3f,%.3f] "
            "C_deg_q05_50_95=[%.3f,%.3f,%.3f] "
            "A_deg_q05_50_95=[%.3f,%.3f,%.3f] "
            "D_deg_q05_50_95=[%.3f,%.3f,%.3f]",
            slice_index + 1,
            tx_index + 1,
            *phase_b_quantiles,
            *phase_c_quantiles,
            *phase_a_quantiles,
            *phase_d_quantiles,
        )
        raw_bsp_quantiles = _diagnostic_quantiles(raw_degrees[phase_mask])
        output_bsp_quantiles = _diagnostic_quantiles(bsp_degrees[phase_mask])
        logging.info(
            "Bloch-Siegert masked BSp diagnostics: slice=%d tx=%d voxels=%d "
            "raw_deg_q05_50_95=[%.3f,%.3f,%.3f] "
            "output_deg_q05_50_95=[%.3f,%.3f,%.3f] wrapped=%d "
            "retained_negative_inside=%d retained_negative_outside=%d "
            "phase_zero_inside=%d",
            slice_index + 1,
            tx_index + 1,
            int(np.count_nonzero(phase_mask)),
            *raw_bsp_quantiles,
            *output_bsp_quantiles,
            int(np.count_nonzero(wrap_mask[tx_index] & phase_mask)),
            int(np.count_nonzero(negative_for_b1 & phase_mask)),
            int(np.count_nonzero(negative_for_b1 & ~phase_mask)),
            int(np.count_nonzero((bsp_degrees == 0) & phase_mask)),
        )

    phsc = np.angle(complex_phase[tx_phase_start:tx_phase_stop]).astype(np.float32)

    delta_te_ms = _setting_float(settings, "deltatems", default=DELTA_TE_MS)
    if delta_te_ms <= 0:
        raise ValueError(f"deltatems must be positive, got {delta_te_ms}")
    b0 = (
        b0_phase
        * 1000.0
        / (2.0 * math.pi * delta_te_ms)
    ).astype(np.float32)

    return {
        "bsp": bsp,
        "b1": b1.astype(np.float32),
        "phsc": phsc,
        "b0": b0,
        "phase_mask": phase_mask.astype(np.uint16),
        "mask_for_magnitude": mask_for_magnitude.astype(np.uint16),
    }


def _phase_stability_mask(
    complex_phase,
    pre_dummy,
    phase_c_indices,
    phase_d_indices,
    magnitude_mask,
):
    reference_indices = np.concatenate(
        (
            np.arange(pre_dummy + 1, dtype=int),
            np.asarray(phase_c_indices, dtype=int),
            np.asarray(phase_d_indices, dtype=int),
        )
    )
    reference_frames = complex_phase[reference_indices]
    average_phase = np.mean(reference_frames, axis=0)
    phase_differences = np.angle(reference_frames * np.conj(average_phase))
    phase_difference_std_degrees = np.degrees(
        np.std(phase_differences, axis=0)
    ).astype(np.float32)
    magnitude_mask = np.asarray(magnitude_mask, dtype=bool)
    if magnitude_mask.shape != phase_difference_std_degrees.shape:
        raise ValueError(
            "Magnitude mask and phase standard-deviation map must have the "
            f"same shape; mask={magnitude_mask.shape}, "
            f"phase_std={phase_difference_std_degrees.shape}"
        )
    trusted_std_values = phase_difference_std_degrees[
        magnitude_mask & np.isfinite(phase_difference_std_degrees)
    ]
    if trusted_std_values.size == 0:
        phase_mask = np.zeros_like(magnitude_mask, dtype=bool)
    else:
        # Retain the 80% most phase-stable voxels within the magnitude mask.
        std_threshold = float(np.percentile(trusted_std_values, 80.0))
        phase_mask = magnitude_mask & (
            phase_difference_std_degrees <= std_threshold
        )
    return phase_mask, phase_difference_std_degrees


def _correct_bs_phase_temporal_baseline(
    complex_phase,
    phase_a,
    phase_b,
    phase_c,
    phase_d,
    b0_pre_reference,
    phase_mask,
    b0_pre_start,
    b0_pre_stop,
    post_start,
    post_stop,
    phase_a_indices,
    phase_b_indices,
    phase_c_indices,
    phase_d_indices,
    b0_phase,
):
    """Remove a quadratic temporal phase baseline at trusted voxels."""
    n_tx = phase_a.shape[0]
    time_pre = np.arange(b0_pre_start, b0_pre_stop, dtype=np.float64) + 1.0
    time_post = np.arange(post_start, post_stop, dtype=np.float64) + 1.0
    trusted = np.asarray(phase_mask, dtype=bool)
    corrected_a = np.array(phase_a, copy=True)
    corrected_b = np.array(phase_b, copy=True)
    logging.info(
        "Temporal BS baseline fit positions (1-based): pre=%s C=%s D=%s post=%s",
        _format_frame_indices(np.arange(b0_pre_start, b0_pre_stop)),
        _format_frame_indices(phase_c_indices),
        _format_frame_indices(phase_d_indices),
        _format_frame_indices(np.arange(post_start, post_stop)),
    )
    for tx_index in range(n_tx):
        time_fit = np.concatenate(
            (
                time_pre,
                np.asarray([phase_c_indices[tx_index] + 1.0]),
                np.asarray([phase_d_indices[tx_index] + 1.0]),
                time_post,
            )
        )
        vandermonde = np.column_stack((
            np.ones(time_fit.size),
            time_fit,
            time_fit * time_fit,
        ))
        fit_pinv = np.linalg.pinv(vandermonde)
        post_phase = complex_phase[post_start:post_stop]
        post_offsets = np.arange(1, post_phase.shape[0] + 1, dtype=np.float32)
        post_phase = post_phase * np.exp(-1j * post_offsets[:, None, None, None] * b0_phase)
        fit_samples = np.concatenate(
            (
                complex_phase[b0_pre_start:b0_pre_stop],
                phase_c[tx_index:tx_index + 1],
                phase_d[tx_index:tx_index + 1],
                post_phase * np.exp(-1j * b0_phase),
            ),
            axis=0,
        )
        # Remove the voxel-wise average phase before unwrapping.  This keeps
        # the temporal residual near zero and prevents 2*pi jumps from being
        # interpreted as rapid temporal evolution.
        average_phase = np.angle(np.mean(fit_samples, axis=0))
        relative_samples = fit_samples * np.exp(-1j * average_phase)
        phase_values = np.unwrap(np.angle(relative_samples), axis=0)
        flat = phase_values.reshape(phase_values.shape[0], -1)
        trusted_flat = trusted.reshape(-1)
        coefficients = fit_pinv @ flat[:, trusted_flat]
        average_phase_flat = average_phase.reshape(-1)[trusted_flat]
        for target, target_indices in (
            (corrected_a, phase_a_indices),
            (corrected_b, phase_b_indices),
        ):
            target_phase = np.angle(target[tx_index])
            target_time = np.asarray(target_indices[tx_index], dtype=np.float64) + 1.0
            design = np.asarray([1.0, target_time, target_time * target_time])
            corrected = target[tx_index].copy()
            baseline = average_phase_flat + design @ coefficients
            corrected[trusted] *= np.exp(-1j * baseline)
            target[tx_index] = corrected
    return corrected_a, corrected_b


def _estimate_post_reference_phase(complex_phase, pre_reference, post_start, post_stop):
    """Estimate B0 phase from circularly averaged post-frame increments."""
    post_frames = np.asarray(complex_phase[post_start:post_stop])
    if post_frames.shape[0] == 0:
        raise ValueError("No post-reference frames available for B0 estimation")
    increments = np.empty_like(post_frames, dtype=np.complex64)
    increments[0] = post_frames[0] * np.conj(pre_reference)
    if post_frames.shape[0] > 1:
        increments[1:] = post_frames[1:] * np.conj(post_frames[:-1])
    phase_increments = np.angle(increments)
    b0_phase = np.angle(np.mean(np.exp(1j * phase_increments), axis=0))
    logging.info(
        "B0 post-reference phase increments: frames=%d increment_deg=[%.3f,%.3f] "
        "mean_phase_deg=[%.3f,%.3f]",
        post_frames.shape[0],
        float(np.degrees(np.min(phase_increments))),
        float(np.degrees(np.max(phase_increments))),
        float(np.degrees(np.min(b0_phase))),
        float(np.degrees(np.max(b0_phase))),
    )
    return b0_phase.astype(np.float32)


def _bloch_siegert_magnitude_mask(magnitude_stack, ntx):
    mask_source = np.mean(magnitude_stack[: 2 * ntx + 2], axis=0)
    finite_values = mask_source[np.isfinite(mask_source)]
    if finite_values.size == 0:
        return np.zeros(mask_source.shape, dtype=bool)

    source_mean = float(np.mean(finite_values))
    low_values = finite_values[finite_values < source_mean]
    if low_values.size == 0:
        threshold = source_mean
    else:
        threshold = float(np.mean(low_values) + 2.0 * np.std(low_values))

    return mask_source > threshold


def _filter_bloch_siegert_maps(
    bsp,
    b0,
    phase_mask,
    magnitude_mask,
    apply_bsp_to_entire_volume,
):
    phase_mask = np.asarray(phase_mask, dtype=bool)
    magnitude_mask = np.asarray(magnitude_mask, dtype=bool)
    try:
        polynomial_order = FILTER_POLYNOMIAL_ORDER_BY_TX[bsp.shape[0]]
    except KeyError as error:
        raise ValueError(
            f"Unsupported nTx={bsp.shape[0]} for filtering; expected 1, 8, or 16"
        ) from error
    filtered_bsp = np.empty_like(bsp, dtype=np.float32)

    for tx_index in range(bsp.shape[0]):
        # New path: MATLAB regionfill-style harmonic interpolation of only
        # the untrusted BS-phase voxels.  The former polynomial path is kept
        # above in _extrapolate_phase_volume for easy rollback.
        filtered_bsp[tx_index] = _regionfill_phase_volume(
            bsp[tx_index],
            phase_mask,
            apply_to_entire_volume=apply_bsp_to_entire_volume,
        )

    filtered_b0 = _regionfill_phase_volume(
        b0,
        phase_mask,
        apply_to_entire_volume=True,
    )
    return filtered_bsp, filtered_b0.astype(np.float32)


def _regionfill_phase_volume(volume, mask, apply_to_entire_volume=False):
    """Fill masked-out voxels by solving a slice-wise discrete Laplace equation.

    This is the numerical equivalent of MATLAB's 2-D ``regionfill`` applied
    independently to each slice. Trusted voxels provide Dirichlet boundary
    values; untrusted voxels are iteratively replaced by the mean of their
    four in-plane neighbors. The trusted samples are retained in the output.
    """
    values = np.asarray(volume, dtype=np.float32)
    trusted = np.asarray(mask, dtype=bool)
    if values.shape != trusted.shape:
        raise ValueError(
            f"Regionfill volume/mask shape mismatch: volume={values.shape}, "
            f"mask={trusted.shape}"
        )
    output = np.array(values, copy=True)
    if values.ndim != 3:
        raise ValueError(f"Regionfill expects a 3-D volume, got {values.shape}")

    for slice_index in range(values.shape[0]):
        image = output[slice_index]
        trusted_slice = trusted[slice_index]
        missing = ~trusted_slice
        if not np.any(missing) or not np.any(trusted_slice):
            continue
        finite_trusted = image[trusted_slice & np.isfinite(image)]
        if finite_trusted.size == 0:
            continue
        image[~np.isfinite(image)] = float(np.mean(finite_trusted))
        fill = image.copy()
        fill[missing] = float(np.mean(finite_trusted))
        for _ in range(1000):
            padded = np.pad(fill, 1, mode="edge")
            neighbor_mean = (
                padded[:-2, 1:-1]
                + padded[2:, 1:-1]
                + padded[1:-1, :-2]
                + padded[1:-1, 2:]
            ) * 0.25
            delta = float(np.max(np.abs(neighbor_mean[missing] - fill[missing])))
            fill[missing] = neighbor_mean[missing]
            if delta < 1e-4:
                break
        if apply_to_entire_volume:
            # Regionfill preserves trusted boundary samples by definition;
            # only the previously untrusted region is replaced.
            image[missing] = fill[missing]
        else:
            image[missing] = fill[missing]
        output[slice_index] = image
    return output.astype(np.float32)


@lru_cache(maxsize=8)
def _generate_bloch_siegert_lut(pulse_width):
    lut_path = os.path.join(os.path.dirname(__file__), "blochsiegert_lut.npz")
    tables = np.load(lut_path)
    pulse_widths = np.asarray(tables["pulse_widths"], dtype=np.float64)
    width_index = int(np.argmin(np.abs(pulse_widths - float(pulse_width))))
    selected_width = float(pulse_widths[width_index])
    b0 = np.asarray(tables["b0_offsets"], dtype=np.float32)
    b1 = np.asarray(tables["b1_scales"], dtype=np.float32)
    lut = np.asarray(tables["lut"][width_index], dtype=np.float32)
    if not np.isclose(selected_width, float(pulse_width)):
        logging.warning(
            "B1 LUT pulse width %.6g ms unavailable; using nearest precomputed "
            "table %.6g ms",
            pulse_width,
            selected_width,
        )
    logging.info(
        "B1 LUT generated: pulse_width=%.6g ms shape=%s b0=[%.6g,%.6g] "
        "b1=[%.6g,%.6g] phase=[%.6g,%.6g]",
        selected_width,
        lut.shape,
        float(b0.min()),
        float(b0.max()),
        float(b1.min()),
        float(b1.max()),
        float(lut.min()),
        float(lut.max()),
    )
    return b0, b1, lut


def _simulate_transverse_grid(rf, b0_offsets, b1_scales, dt, gamma):
    """Vectorized Pauly Bloch simulation for the complete B0/B1 grid."""
    alpha = np.full(b0_offsets.shape, 1.0 / math.sqrt(2.0), dtype=np.complex128)
    beta = alpha.copy()
    wz = 2.0 * math.pi * b0_offsets
    scale_t = b1_scales * 1e-6
    for value in rf:
        wx = gamma * float(np.real(value)) * scale_t
        wy = gamma * float(np.imag(value)) * scale_t
        phi = np.sqrt(wx * wx + wy * wy + wz * wz) * dt
        nonzero = phi > 0
        sin_half = np.sin(phi / 2.0)
        nx = np.divide(wx * dt, phi, out=np.zeros_like(phi), where=nonzero)
        ny = np.divide(wy * dt, phi, out=np.zeros_like(phi), where=nonzero)
        nz = np.divide(wz * dt, phi, out=np.zeros_like(phi), where=nonzero)
        alpha_step = np.cos(phi / 2.0) - 1j * nz * sin_half
        beta_step = (ny - 1j * nx) * sin_half
        alpha_step = np.where(nonzero, alpha_step, 1.0)
        beta_step = np.where(nonzero, beta_step, 0.0)
        alpha, beta = (
            alpha_step * alpha - np.conj(beta_step) * beta,
            beta_step * alpha + np.conj(alpha_step) * beta,
        )
    return np.column_stack((2.0 * np.real(np.conj(alpha) * beta),
                            2.0 * np.imag(np.conj(alpha) * beta)))


def _b1_from_lookup_table(bsp, b0, pulse_width):
    # scipy is loaded lazily so the OpenRecon worker has a small startup
    # footprint while waiting for the scanner to begin sending data.
    from scipy.interpolate import LinearNDInterpolator, NearestNDInterpolator

    b0_grid, b1_grid, lut = _generate_bloch_siegert_lut(float(pulse_width))
    points = np.column_stack((
        np.repeat(b0_grid, b1_grid.size),
        lut.reshape(-1),
    ))
    values = np.tile(b1_grid, b0_grid.size)
    linear = LinearNDInterpolator(points, values)
    nearest = NearestNDInterpolator(points, values)
    bsp_array = np.asarray(bsp)
    # B0 is one map per slice, whereas BSp is one map per transmit channel.
    # Broadcast B0 across the channel dimension for nTx>1 LUT queries.
    b0_array = np.broadcast_to(np.asarray(b0), bsp_array.shape)
    b0_flat = b0_array.reshape(-1)
    bsp_flat = bsp_array.reshape(-1)
    result = np.empty(b0_flat.size, dtype=np.float32)
    missing_count = 0
    linear_count = 0
    # nTx=8 produces over one million voxels.  Chunking avoids retaining the
    # full query matrix and both interpolator temporaries at the same time.
    chunk_size = 100_000
    for start in range(0, b0_flat.size, chunk_size):
        stop = min(start + chunk_size, b0_flat.size)
        query_chunk = np.column_stack((b0_flat[start:stop], bsp_flat[start:stop]))
        values_chunk = np.asarray(linear(query_chunk), dtype=np.float32)
        missing = np.isnan(values_chunk)
        linear_count += int(np.count_nonzero(~missing))
        missing_count += int(np.count_nonzero(missing))
        if np.any(missing):
            values_chunk[missing] = nearest(query_chunk[missing])
        result[start:stop] = values_chunk
    logging.info(
        "B1 LUT interpolation input: points=%d query=%d b0=[%.6g,%.6g] "
        "phase=[%.6g,%.6g] linear_hits=%d nearest_fallback=%d",
        points.shape[0],
        b0_flat.size,
        float(np.min(b0_flat)),
        float(np.max(b0_flat)),
        float(np.min(bsp_flat)),
        float(np.max(bsp_flat)),
        linear_count,
        missing_count,
    )
    result = result.reshape(bsp_array.shape)
    logging.info(
        "B1 LUT interpolation output: shape=%s range=[%.6g,%.6g] "
        "unique_values=%d zero_count=%d one_count=%d",
        result.shape,
        float(np.nanmin(result)),
        float(np.nanmax(result)),
        int(np.unique(result).size),
        int(np.count_nonzero(result == 0)),
        int(np.count_nonzero(result == 1)),
    )
    return result


def _extrapolate_phase_volume(
    phase_volume,
    mask,
    polynomial_order,
    zero_non_positive=False,
    apply_to_entire_volume=False,
    smooth_sigma=0.8,
):
    phase_volume = np.asarray(phase_volume, dtype=np.float64)
    mask = np.asarray(mask, dtype=bool)
    original_ndim = phase_volume.ndim
    if original_ndim == 2:
        phase_volume = phase_volume[np.newaxis, ...]
        mask = mask[np.newaxis, ...]
    if phase_volume.ndim != 3 or mask.shape != phase_volume.shape:
        raise ValueError(
            "phase volume and trusted mask must have the same 2D or 3D shape; "
            f"volume={phase_volume.shape}, mask={mask.shape}"
        )
    if polynomial_order < 0:
        raise ValueError(
            f"polynomial_order must be nonnegative, got {polynomial_order}"
        )
    if not np.any(mask):
        raise ValueError("No trusted voxels found in the provided phaseMask")

    working_volume = phase_volume.copy()
    if zero_non_positive:
        working_volume[working_volume <= 0] = 0

    depth, height, width = working_volume.shape
    x_coordinates = _normalized_filter_axis(width)
    y_coordinates = _normalized_filter_axis(height)
    z_coordinates = _normalized_filter_axis(depth)
    valid_z, valid_y, valid_x = np.nonzero(mask)
    valid_vandermonde = _build_3d_vandermonde(
        x_coordinates[valid_x],
        y_coordinates[valid_y],
        z_coordinates[valid_z],
        polynomial_order,
        max_degrees=(width - 1, height - 1, depth - 1),
    )
    coefficients, _, _, _ = np.linalg.lstsq(
        valid_vandermonde,
        working_volume[mask],
        rcond=None,
    )

    output = working_volume.copy()
    if apply_to_entire_volume:
        target_indices = np.arange(working_volume.size, dtype=np.int64)
    else:
        target_indices = np.flatnonzero(~mask)
    _evaluate_polynomial_at_indices(
        output,
        target_indices,
        x_coordinates,
        y_coordinates,
        z_coordinates,
        polynomial_order,
        coefficients,
        max_degrees=(width - 1, height - 1, depth - 1),
    )

    if smooth_sigma > 0:
        from scipy.ndimage import gaussian_filter

        output = gaussian_filter(
            output,
            sigma=float(smooth_sigma),
            mode="nearest",
            truncate=2.0,
        )
    if zero_non_positive:
        output[output <= 0] = 0
    if original_ndim == 2:
        output = output[0]
    return output.astype(np.float32)


def _normalized_filter_axis(size):
    if size == 1:
        # MATLAB linspace(-1, 1, 1) returns its upper endpoint.
        return np.asarray([1.0], dtype=np.float64)
    return np.linspace(-1.0, 1.0, size, dtype=np.float64)


def _polynomial_exponents(order, max_degrees=None):
    exponents = []
    for total_degree in range(order + 1):
        for x_degree in range(total_degree, -1, -1):
            for y_degree in range(total_degree - x_degree, -1, -1):
                z_degree = total_degree - x_degree - y_degree
                if max_degrees is None or all(
                    degree <= maximum
                    for degree, maximum in zip(
                        (x_degree, y_degree, z_degree),
                        max_degrees,
                    )
                ):
                    exponents.append((x_degree, y_degree, z_degree))
    return exponents


def _build_3d_vandermonde(
    x_values,
    y_values,
    z_values,
    order,
    max_degrees=None,
):
    x_values = np.asarray(x_values, dtype=np.float64).reshape(-1)
    y_values = np.asarray(y_values, dtype=np.float64).reshape(-1)
    z_values = np.asarray(z_values, dtype=np.float64).reshape(-1)
    if not (x_values.size == y_values.size == z_values.size):
        raise ValueError("x, y, and z coordinate arrays must have equal lengths")
    return np.column_stack(
        [
            (x_values ** x_degree)
            * (y_values ** y_degree)
            * (z_values ** z_degree)
            for x_degree, y_degree, z_degree in _polynomial_exponents(
                order,
                max_degrees=max_degrees,
            )
        ]
    )


def _evaluate_polynomial_at_indices(
    output,
    target_indices,
    x_coordinates,
    y_coordinates,
    z_coordinates,
    polynomial_order,
    coefficients,
    max_degrees=None,
):
    chunk_size = 65536
    for start in range(0, target_indices.size, chunk_size):
        chunk = target_indices[start:start + chunk_size]
        z_indices, y_indices, x_indices = np.unravel_index(chunk, output.shape)
        vandermonde = _build_3d_vandermonde(
            x_coordinates[x_indices],
            y_coordinates[y_indices],
            z_coordinates[z_indices],
            polynomial_order,
            max_degrees=max_degrees,
        )
        output.reshape(-1)[chunk] = vandermonde @ coefficients


def _phase_to_radians(phase_stack, phase_wrap):
    if phase_wrap <= 0:
        return phase_stack.astype(np.float32)
    return phase_stack.astype(np.float32) * (2.0 * math.pi / phase_wrap) - math.pi


def _split_magnitude_phase_images(images, minimum_frame_count=11):
    magnitude_images = []
    phase_images = []
    for image in images:
        if _is_phase_image(image):
            phase_images.append(image)
        elif _is_magnitude_image(image):
            magnitude_images.append(image)
        else:
            logging.info("Ignoring non-magnitude/non-phase image_type=%s", image.image_type)
    if not phase_images:
        fallback_magnitude, fallback_phase = _fallback_split_magnitude_phase_series(
            magnitude_images,
            minimum_frame_count,
        )
        if fallback_phase:
            logging.warning(
                "No explicit phase MRD image_type was present; treating the first "
                "of two equal-length image series as magnitude and the second as phase"
            )
            return fallback_magnitude, fallback_phase
    return magnitude_images, phase_images


def _fallback_split_magnitude_phase_series(images, minimum_frame_count=11):
    series_groups = []
    group_index_by_key = {}
    for image in images:
        key = _source_series_key(image)
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(series_groups)
            group_index_by_key[key] = group_index
            series_groups.append([])
        series_groups[group_index].append(image)

    if len(series_groups) != 2 or len(series_groups[0]) != len(series_groups[1]):
        return images, []

    series_groups = sorted(series_groups, key=_series_group_sort_key)
    frame_count = len(series_groups[0])
    if frame_count < minimum_frame_count:
        return images, []
    return series_groups[0], series_groups[1]


def _source_series_key(image):
    meta = _meta_from_image(image)
    return (
        int(image.getHead().image_series_index),
        _meta_text(meta, "SeriesInstanceUID"),
        _meta_text(meta, "SeriesNumberRangeNameUID"),
        _meta_text(meta, "SequenceDescription"),
        _meta_text(meta, "ProtocolName"),
    )


def _series_group_sort_key(group):
    first_image = group[0]
    meta = _meta_from_image(first_image)
    series_number = _dicom_json_numeric_value(meta, "00200011")
    if series_number is None:
        series_number = int(first_image.getHead().image_series_index)
    return (series_number, int(first_image.getHead().image_series_index))


def _is_magnitude_image(image):
    image_type = int(getattr(image, "image_type", 0))
    if image_type in (0, int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))):
        return True
    meta = _meta_from_image(image)
    text = " ".join(
        [
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()
    return "MAGNITUDE" in text or re.search(r"(^|\\|\s)M($|\\|\s)", text) is not None


def _is_phase_image(image):
    image_type = int(getattr(image, "image_type", 0))
    if image_type == int(getattr(ismrmrd, "IMTYPE_PHASE", 2)):
        return True
    meta = _meta_from_image(image)
    text = " ".join(
        [
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()
    return "PHASE" in text or re.search(r"(^|\\|\s)P($|\\|\s)", text) is not None


def _group_frames_by_slice(images, expected_frame_counts=(11, 46, 86)):
    minimum_frame_count = min(expected_frame_counts)
    indexed = list(enumerate(images))
    if indexed and all(_is_volume_frame_image(image) for _order, image in indexed):
        logging.info(
            "Grouped %d volume-frame image(s) into one Bloch-Siegert frame group",
            len(indexed),
        )
        return [[image for _order, image in sorted(indexed, key=_frame_sort_key)]]

    candidates = []
    for name, key_func in (
        ("position", _position_group_key),
        ("slice", _slice_group_key),
    ):
        groups = _groups_from_key(indexed, key_func)
        if groups and all(len(group) >= minimum_frame_count for group in groups):
            candidates.append((name, groups))

    chunked = _chunked_frame_groups(indexed, expected_frame_counts)
    if chunked:
        candidates.append(("chunk", chunked))

    if not candidates:
        raise ValueError(
            "Bloch-Siegert mapping requires at least "
            f"{minimum_frame_count} frames per slice for the configured "
            "pre/post dummy counts"
        )

    def score(candidate):
        name, groups = candidate
        group_sizes = {len(group) for group in groups}
        informative_geometry = (
            name != "chunk"
            and len(groups) > 1
            and len(group_sizes) == 1
        )
        exact = sum(1 for group in groups if len(group) in expected_frame_counts)
        return (
            informative_geometry,
            exact,
            name != "chunk",
            len(groups),
            -max(group_sizes),
        )

    selected_name, selected_groups = max(candidates, key=score)
    logging.info(
        "Grouped %d image(s) into %d Bloch-Siegert slice group(s) using %s",
        len(images),
        len(selected_groups),
        selected_name,
    )
    return [
        [image for _order, image in sorted(group, key=_frame_sort_key)]
        for group in sorted(selected_groups, key=_slice_sort_key)
    ]


def _groups_from_key(indexed_images, key_func):
    groups = []
    group_index_by_key = {}
    for item in indexed_images:
        key = key_func(item[1])
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(groups)
            group_index_by_key[key] = group_index
            groups.append([])
        groups[group_index].append(item)
    return groups


def _position_group_key(image):
    header = image.getHead()
    position = tuple(round(float(value), 3) for value in header.position)
    slice_dir = tuple(round(float(value), 3) for value in header.slice_dir)
    return position, slice_dir


def _slice_group_key(image):
    return int(image.getHead().slice)


def _chunked_frame_groups(indexed_images, expected_frame_counts=(11, 46, 86)):
    total = len(indexed_images)
    chunk_size = next(
        (
            frame_count
            for frame_count in sorted(expected_frame_counts, reverse=True)
            if total >= frame_count and total % frame_count == 0
        ),
        None,
    )
    if chunk_size is None:
        return []
    return [
        indexed_images[index : index + chunk_size]
        for index in range(0, total, chunk_size)
    ]


def _frame_sort_key(indexed_image):
    order, image = indexed_image
    header = image.getHead()
    image_index = int(getattr(header, "image_index", 0))
    return (
        image_index if image_index > 0 else order + 1,
        int(getattr(header, "contrast", 0)),
        int(getattr(header, "phase", 0)),
        int(getattr(header, "repetition", 0)),
        int(getattr(header, "set", 0)),
        order,
    )


def _slice_sort_key(group):
    first_image = group[0][1]
    header = first_image.getHead()
    axis = _normalize_vector(header.slice_dir)
    if axis is None:
        axis = np.asarray((0.0, 0.0, 1.0))
    position = np.asarray(header.position, dtype=float)
    return (float(np.dot(position, axis)), group[0][0])


def _pair_slice_groups(magnitude_groups, phase_groups):
    if len(magnitude_groups) != len(phase_groups):
        raise ValueError(
            "Magnitude and phase image groups do not describe the same number of "
            f"slices: {len(magnitude_groups)} magnitude, {len(phase_groups)} phase"
        )

    return magnitude_groups, phase_groups


def _sequence_shape(frame_count, pre_dummy, post_dummy):
    if pre_dummy < 0 or post_dummy < 0:
        raise ValueError(
            "predummy and postdummy must be nonnegative; "
            f"got predummy={pre_dummy}, postdummy={post_dummy}"
        )

    one_tx_frames = _required_frame_count(1, pre_dummy, post_dummy)
    eight_tx_frames = _required_frame_count(8, pre_dummy, post_dummy)
    sixteen_tx_frames = _required_frame_count(16, pre_dummy, post_dummy)
    if frame_count == eight_tx_frames:
        return eight_tx_frames, 8
    if frame_count == sixteen_tx_frames:
        return sixteen_tx_frames, 16
    if frame_count == one_tx_frames:
        return one_tx_frames, 1
    raise FrameCountMismatchError(
        "Bloch-Siegert frame count does not match the configured sequence: "
        f"{one_tx_frames} frames for 1Tx, {eight_tx_frames} frames for 8Tx, "
        f"or {sixteen_tx_frames} frames for 16Tx "
        f"with predummy={pre_dummy} and postdummy={post_dummy}; "
        f"got {frame_count}"
    )


def _required_frame_count(ntx, pre_dummy, post_dummy):
    return (
        ECHOES_PER_TX * ntx
        + ntx
        + (pre_dummy + 1)
        + (post_dummy + 1)
    )


def _is_volume_frame_image(image):
    data = np.asarray(image.data)
    return data.ndim == 4 and data.shape[0] == 1 and data.shape[1] > 1


def _image_volume_data(image):
    data = np.asarray(image.data)
    if data.ndim == 4 and data.shape[0] == 1:
        return data[0]
    data = np.squeeze(data)
    if data.ndim == 2:
        return data[np.newaxis, :, :]
    if data.ndim != 3:
        raise ValueError(
            "Bloch-Siegert input images must be single-channel 2D frames or "
            "single-channel 3D volume frames; "
            f"got data shape {np.asarray(image.data).shape}"
        )
    return data


def _format_display_number(value):
    number = float(value)
    if number == 0.0:
        return "0"
    if number.is_integer():
        return str(int(number))
    return f"{number:.12g}"


def _scale_volume_to_display_range(volume, units):
    values = np.asarray(volume, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        display = np.zeros(values.shape, dtype=np.uint16)
        return display, {
            "input_min": 0.0,
            "input_max": 0.0,
            "scale": 1.0,
            "display_min": SCANNER_DISPLAY_MIN,
            "display_max": SCANNER_DISPLAY_MIN,
            "rescale_slope": 1.0,
            "rescale_intercept": 0.0,
            "formula": f"{units} = display",
        }

    input_min = float(np.min(finite))
    input_max = float(np.max(finite))
    input_range = input_max - input_min
    if input_range <= 0.0 or not np.isfinite(input_range):
        display = np.zeros(values.shape, dtype=np.uint16)
        intercept_text = _format_display_number(input_min)
        return display, {
            "input_min": input_min,
            "input_max": input_max,
            "scale": 1.0,
            "display_min": SCANNER_DISPLAY_MIN,
            "display_max": SCANNER_DISPLAY_MIN,
            "rescale_slope": 1.0,
            "rescale_intercept": input_min,
            "formula": f"{units} = display + {intercept_text}",
        }

    display_range = float(SCANNER_DISPLAY_MAX - SCANNER_DISPLAY_MIN)
    scale = display_range / input_range
    rescale_slope = input_range / display_range
    cleaned = np.nan_to_num(
        values,
        nan=input_min,
        posinf=input_max,
        neginf=input_min,
    )
    display = np.rint((cleaned - input_min) * scale + SCANNER_DISPLAY_MIN)
    display = np.clip(display, SCANNER_DISPLAY_MIN, SCANNER_DISPLAY_MAX)
    display = display.astype(np.uint16, copy=False)
    slope_text = _format_display_number(rescale_slope)
    intercept_text = _format_display_number(input_min)
    return display, {
        "input_min": input_min,
        "input_max": input_max,
        "scale": scale,
        "display_min": int(np.min(display)) if display.size else SCANNER_DISPLAY_MIN,
        "display_max": int(np.max(display)) if display.size else SCANNER_DISPLAY_MIN,
        "rescale_slope": rescale_slope,
        "rescale_intercept": input_min,
        "formula": f"{units} = display * {slope_text} + {intercept_text}",
    }


def _map_to_mrd_image(
    volume,
    anchor_image,
    slice_anchor_images,
    series_index,
    series_name,
    image_type_token,
    map_role,
    units,
    tx_index=None,
    window_center=None,
    window_width=None,
):
    volume = np.asarray(volume)
    if volume.ndim != 3:
        raise ValueError(f"Output map volume must be 3D, got shape {volume.shape}")

    if image_type_token in {"BSSBSP", "BSSPHSC"}:
        physical_volume = np.degrees(volume)
        conversion_comment = "phase converted from radians to degrees; "
    else:
        physical_volume = volume
        conversion_comment = ""

    output_data, display_meta = _scale_volume_to_display_range(
        physical_volume,
        units,
    )
    display_meta["comment"] = (
        conversion_comment
        + f"scanner display uint16 {SCANNER_DISPLAY_MIN}-{SCANNER_DISPLAY_MAX}; "
        + display_meta["formula"]
    )
    if conversion_comment:
        display_meta["conversion"] = "radians to degrees"
    output = ismrmrd.Image.from_array(output_data, transpose=False)

    header = anchor_image.getHead()
    header.data_type = output.data_type
    header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
    header.image_series_index = int(series_index)
    header.image_index = 1
    header.slice = 0
    header.contrast = 0
    output_header = output.getHead()
    _set_header_sequence_field(
        header,
        "matrix_size",
        [int(value) for value in output_header.matrix_size],
    )

    slice_axis = _infer_slice_axis(slice_anchor_images)
    _set_header_sequence_field(
        header,
        "position",
        [float(value) for value in slice_anchor_images[0].getHead().position],
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )
    fov = [float(value) for value in header.field_of_view]
    fov[2] = _output_fov_z(slice_anchor_images, slice_axis, volume.shape[0])
    _set_header_sequence_field(header, "field_of_view", fov)

    output.setHead(header)
    output.image_series_index = int(series_index)
    try:
        output._slice_positions = [
            [float(value) for value in image.getHead().position]
            for image in slice_anchor_images
        ]
        output._slice_dir = [float(value) for value in slice_axis]
    except Exception:
        pass

    # DICOM applies RescaleSlope/RescaleIntercept before WindowCenter/WindowWidth,
    # so window values must use physical units rather than normalized pixels.
    center, width = _window_center_width(physical_volume)
    if window_center is not None:
        center = window_center
    if window_width is not None:
        width = window_width
    logging.info(
        "Bloch-Siegert output scaling: series=%d type=%s tx=%s units=%s "
        "physical=[%s,%s] display=[%d,%d] slope=%s intercept=%s "
        "window=[%.6g,%.6g]",
        series_index,
        image_type_token,
        "-" if tx_index is None else str(tx_index + 1),
        units,
        _format_display_number(display_meta["input_min"]),
        _format_display_number(display_meta["input_max"]),
        display_meta["display_min"],
        display_meta["display_max"],
        _format_display_number(display_meta["rescale_slope"]),
        _format_display_number(display_meta["rescale_intercept"]),
        center,
        width,
    )
    output.attribute_string = _output_meta(
        anchor_image,
        header,
        series_index,
        series_name,
        image_type_token,
        map_role,
        units,
        volume.shape[0],
        center,
        width,
        tx_index,
        display_meta,
    ).serialize()
    return output


def _output_meta(
    source_image,
    header,
    series_index,
    series_name,
    image_type_token,
    map_role,
    units,
    slice_count,
    window_center,
    window_width,
    tx_index=None,
    display_meta=None,
    instance_index=None,
    series_image_count=1,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    _strip_scanner_write_unsafe_meta(meta)
    if "IceMiniHead" in meta:
        del meta["IceMiniHead"]

    series_uid = _derived_series_uid(source_image, series_index, series_name)
    sop_uid = _derived_instance_uid(
        source_image,
        series_uid,
        series_index,
        series_name,
        instance_index=instance_index,
    )
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"

    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "BLOCHSIEGERTB1MAPPING"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    image_comment = series_name
    if display_meta is not None:
        scale_text = _format_display_number(display_meta["scale"])
        image_comment = f"{series_name}; {display_meta['comment']}"
        meta["BlochSiegertDisplayScale"] = scale_text
        meta["BlochSiegertDisplayFormula"] = display_meta["formula"]
        meta["BlochSiegertDisplayInputMin"] = _format_display_number(
            display_meta["input_min"]
        )
        meta["BlochSiegertDisplayInputMax"] = _format_display_number(
            display_meta["input_max"]
        )
        meta["BlochSiegertDisplayMin"] = str(display_meta["display_min"])
        meta["BlochSiegertDisplayMax"] = str(display_meta["display_max"])
        # The DICOM writer maps these MRD image attributes to
        # (0028,1053) Rescale Slope and (0028,1052) Rescale Intercept.
        meta["RescaleSlope"] = _format_display_number(
            display_meta["rescale_slope"]
        )
        meta["RescaleIntercept"] = _format_display_number(
            display_meta["rescale_intercept"]
        )
        if "conversion" in display_meta:
            meta["BlochSiegertDisplayConversion"] = display_meta["conversion"]
    meta["ImageComments"] = image_comment
    meta["ImageComment"] = image_comment
    meta["SeriesNumberRangeNameUID"] = _derived_series_grouping(
        series_name,
        series_index,
    )
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["Keep_image_geometry"] = "0"
    meta["partition_count"] = "1"
    meta["slice_count"] = str(int(slice_count))
    meta["NumberOfSlices"] = str(int(slice_count))
    meta["ImagesInAcquisition"] = str(int(slice_count))
    meta["NumberInSeries"] = str(int(series_image_count))
    meta["SliceNo"] = "0"
    meta["AnatomicalSliceNo"] = "0"
    meta["ChronSliceNo"] = "0"
    meta["ProtocolSliceNumber"] = "0"
    meta["Actual3DImagePartNumber"] = "0"
    meta["AnatomicalPartitionNo"] = "0"
    meta["BlochSiegertOutput"] = map_role
    meta["BlochSiegertUnits"] = units
    if tx_index is not None:
        meta["BlochSiegertTxIndex"] = str(int(tx_index + 1))
    meta["WindowCenter"] = f"{float(window_center):.6g}"
    meta["WindowWidth"] = f"{float(window_width):.6g}"
    meta.update(_header_geometry_meta(header))
    _strip_scanner_write_unsafe_meta(meta)
    return meta


def _header_geometry_meta(header):
    return {
        "ImageRowDir": [f"{float(value):.18f}" for value in header.read_dir],
        "ImageColumnDir": [f"{float(value):.18f}" for value in header.phase_dir],
        "ImageSliceNormDir": [f"{float(value):.18f}" for value in header.slice_dir],
        "SlicePosLightMarker": [f"{float(value):.18f}" for value in header.position],
    }


def _allocate_output_series_indices(input_images, ntx):
    used = {int(image.getHead().image_series_index) for image in input_images}

    def reserve(preferred):
        series_index = int(preferred)
        while series_index in used:
            series_index += 1
        used.add(series_index)
        return series_index

    return {
        "b1": [reserve(B1_SERIES_INDEX_START + index) for index in range(ntx)],
        "bsp": [reserve(BSP_SERIES_INDEX_START + index) for index in range(ntx)],
        "phsc": [reserve(PHSC_SERIES_INDEX_START + index) for index in range(ntx)],
        "b0": reserve(B0_SERIES_INDEX),
        "refamp": reserve(REF_AMPLITUDE_SERIES_INDEX),
        "processing": reserve(PROCESSING_INFO_SERIES_INDEX),
        "processing_b0": reserve(PROCESSING_INFO_SERIES_INDEX + 1),
    }


def _map_series_name(source_name, map_name, index, count):
    labels = {
        "b1": "B1 map",
        "bsp": "BS Phase",
        "phsc": "Tx Phase",
    }
    label = labels.get(map_name, map_name)
    if count == 1:
        return f"{source_name} OR {label}"
    return f"{source_name} OR {label} Tx{index + 1}"


def _derived_series_grouping(series_name, series_index):
    return f"{_sanitize_identity_text(series_name)}_{int(series_index)}"


def _derived_series_uid(source_image, series_index, series_name):
    seed = "|".join(
        [
            RECIPE_NAME,
            _source_series_uid(source_image) or _source_series_name(source_image) or "source",
            str(int(series_index)),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_instance_uid(
    source_image,
    series_uid,
    series_index,
    series_name,
    instance_index=None,
):
    seed = "|".join(
        [
            f"{RECIPE_NAME}-instance",
            series_uid,
            _source_sop_uid(source_image) or "source",
            str(int(series_index)),
            series_name,
            "" if instance_index is None else str(int(instance_index)),
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _source_series_name(source_image):
    meta = _meta_from_image(source_image)
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _meta_text(meta, key)
        if value:
            return value

    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _minihead_string_value(minihead, key)
        if value:
            return value
    return ""


def _source_series_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SeriesInstanceUID") or _minihead_string_value(
        _decode_ice_minihead(_meta_text(meta, "IceMiniHead")),
        "SeriesInstanceUID",
    )


def _source_sop_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SOPInstanceUID") or _minihead_string_value(
        _decode_ice_minihead(_meta_text(meta, "IceMiniHead")),
        "SOPInstanceUID",
    )


def _strip_source_parent_refs(meta):
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta[key]
            continue
        if any(key.startswith(prefix) for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES):
            del meta[key]


def _strip_scanner_write_unsafe_meta(meta):
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        if key in meta:
            del meta[key]


def _validate_output_images(output_images, input_images):
    errors = []
    input_series_indices = {
        int(image.getHead().image_series_index)
        for image in input_images
    }
    input_series_uids = {
        _source_series_uid(image)
        for image in input_images
        if _source_series_uid(image)
    }
    seen_series_uids = {}
    seen_sop_uids = {}

    for index, image in enumerate(output_images):
        header = image.getHead()
        series_index = int(header.image_series_index)
        meta = _meta_from_image(image)
        series_uid = _meta_text(meta, "SeriesInstanceUID")
        sop_uid = _meta_text(meta, "SOPInstanceUID")
        keep_geometry = _meta_int(meta, "Keep_image_geometry")
        image_type_token = _meta_text(meta, "ImageTypeValue4")

        if series_index in input_series_indices:
            errors.append(f"image {index} reuses input image_series_index {series_index}")
        if keep_geometry != 0:
            errors.append(f"image {index} has Keep_image_geometry={keep_geometry}, expected 0")
        if _meta_text(meta, "IceMiniHead"):
            errors.append(f"image {index} keeps source IceMiniHead on derived output")
        if _meta_text(meta, "ImageTypeValue3"):
            errors.append(f"image {index} keeps unsafe ImageTypeValue3")
        if not series_uid:
            errors.append(f"image {index} is missing SeriesInstanceUID")
        if series_uid in input_series_uids:
            errors.append(f"image {index} reuses input SeriesInstanceUID {series_uid}")
        if not sop_uid:
            errors.append(f"image {index} is missing SOPInstanceUID")
        if series_uid:
            previous = seen_series_uids.setdefault(series_uid, series_index)
            if previous != series_index:
                errors.append(
                    f"SeriesInstanceUID {series_uid} is shared by series "
                    f"{previous} and {series_index}"
                )
        if sop_uid:
            previous = seen_sop_uids.setdefault(sop_uid, index)
            if previous != index:
                errors.append(f"image {index} duplicates SOPInstanceUID from image {previous}")
        if int(header.image_index) < 1:
            errors.append(f"image {index} has image_index {header.image_index}, expected >= 1")
        if int(header.slice) != 0:
            errors.append(f"image {index} has slice {header.slice}, expected 0")
        data = np.asarray(image.data)
        if data.dtype != np.uint16:
            errors.append(f"image {index} {image_type_token} data is not uint16")
        data_min = int(np.min(data)) if data.size else SCANNER_DISPLAY_MIN
        data_max = int(np.max(data)) if data.size else SCANNER_DISPLAY_MIN
        if data_min < SCANNER_DISPLAY_MIN or data_max > SCANNER_DISPLAY_MAX:
            errors.append(f"image {index} {image_type_token} data is outside 0..4095")
        if data_min != data_max and (
            data_min != SCANNER_DISPLAY_MIN or data_max != SCANNER_DISPLAY_MAX
        ):
            errors.append(
                f"image {index} nonconstant {image_type_token} data does not use 0..4095"
            )
        formula = _meta_text(meta, "BlochSiegertDisplayFormula")
        if not formula or formula not in _meta_text(meta, "ImageComments"):
            errors.append(
                f"image {index} {image_type_token} ImageComments is missing "
                "its inverse formula"
            )
        if _meta_int(meta, "BlochSiegertDisplayMin") != data_min:
            errors.append(
                f"image {index} {image_type_token} display minimum metadata is wrong"
            )
        if _meta_int(meta, "BlochSiegertDisplayMax") != data_max:
            errors.append(
                f"image {index} {image_type_token} display maximum metadata is wrong"
            )
        for key in (
            "BlochSiegertDisplayInputMin",
            "BlochSiegertDisplayInputMax",
            "RescaleSlope",
            "RescaleIntercept",
        ):
            if not _meta_text(meta, key):
                errors.append(f"image {index} {image_type_token} output is missing {key}")
        if image_type_token in {"BSSBSP", "BSSPHSC"}:
            comments = _meta_text(meta, "ImageComments")
            if "radians to degrees" not in comments:
                errors.append(
                    f"image {index} phase ImageComments is missing its conversion"
                )

    if errors:
        raise ValueError(
            "Invalid blochsiegertb1mapping output series contract before send: "
            + "; ".join(errors)
        )


def _send_images_by_series(connection, images):
    images = _split_output_volumes(images)
    batches = []
    batch_by_series = {}
    for image in images:
        series_index = int(image.getHead().image_series_index)
        if series_index not in batch_by_series:
            batch_by_series[series_index] = []
            batches.append(batch_by_series[series_index])
        batch_by_series[series_index].append(image)
    for batch in batches:
        connection.send_image(batch)


def _split_output_volumes(images):
    """Transmit each multi-slice derived volume as positioned 2D images."""
    split_images = []
    for image in images:
        positions = getattr(image, "_slice_positions", None)
        data = np.asarray(image.data)
        if data.ndim == 5 and data.shape[:2] == (1, 1):
            volume_data = data[0, 0]
        elif data.ndim == 4 and data.shape[0] == 1:
            volume_data = data[0]
        else:
            volume_data = data
        if not positions or volume_data.shape[0] <= 1:
            split_images.append(image)
            continue
        series_index = int(image.getHead().image_series_index)
        total = min(len(positions), volume_data.shape[0])
        parent_meta = _meta_from_image(image)
        series_uid = _meta_text(parent_meta, "SeriesInstanceUID")
        for index in range(total):
            slice_image = ismrmrd.Image.from_array(
                volume_data[index], transpose=False
            )
            header = image.getHead()
            header.image_index = index + 1
            header.slice = index
            _set_header_sequence_field(header, "position", positions[index])
            slice_header = slice_image.getHead()
            _set_header_sequence_field(
                header,
                "matrix_size",
                [int(slice_header.matrix_size[0]), int(slice_header.matrix_size[1]), 1],
            )
            if header.field_of_view[2] > 0 and total > 0:
                header.field_of_view[2] = float(header.field_of_view[2]) / total
            slice_image.setHead(header)
            slice_image.image_series_index = series_index
            slice_image.image_index = index + 1
            meta = ismrmrd.Meta.deserialize(parent_meta.serialize())
            meta["NumberInSeries"] = str(total)
            meta["ImagesInAcquisition"] = str(total)
            meta["NumberOfSlices"] = "1"
            if series_uid:
                meta["SeriesInstanceUID"] = series_uid
            source_uid = _meta_text(parent_meta, "SOPInstanceUID") or "source"
            meta["SOPInstanceUID"] = (
                f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, source_uid + '|' + str(index + 1)).int}"
            )
            meta["SlicePosLightMarker"] = [f"{float(v):.18f}" for v in positions[index]]
            meta["SliceNo"] = str(index)
            meta["AnatomicalSliceNo"] = str(index)
            meta["ChronSliceNo"] = str(index)
            slice_image.attribute_string = meta.serialize()
            split_images.append(slice_image)
    return split_images


def _settings_from_config(config):
    return {
        "sendbsp": _config_bool(config, "sendbsp", default=True),
        "sendphsc": _config_bool(config, "sendphsc", default=True),
        "applymask": _config_bool(config, "applymask", default=False),
        "applyfilter": _config_bool(config, "applyfilter", default=False),
        "abstxrefamp": _config_float(config, "abstxrefamp", default=200.0),
        "bspulsewidthms": _config_int(
            config,
            "bspulsewidthms",
            default=6,
        ),
        "deltatems": _config_float(
            config,
            "deltatems",
            default=DELTA_TE_MS,
        ),
        "predummy": _config_int(
            config,
            "predummy",
            default=PRE_DUMMY,
        ),
        "postdummy": _config_int(
            config,
            "postdummy",
            default=POST_DUMMY,
        ),
        "phasewrap": _config_float(config, "phasewrap", default=PHASE_WRAP),
    }


def _config_parameters(config):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return {}
    if not isinstance(config, dict):
        return {}
    parameters = config.get("parameters", config)
    return parameters if isinstance(parameters, dict) else {}


def _config_bool(config, key, default=False):
    return _coerce_bool(_config_parameters(config).get(key, default), default)


def _config_float(config, key, default=0.0):
    value = _config_parameters(config).get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _setting_bool(settings, key, default=False):
    return _coerce_bool(settings.get(key, default), default)


def _setting_float(settings, key, default=0.0):
    try:
        return float(settings.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def _config_int(config, key, default=0):
    value = _config_parameters(config).get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _setting_int(settings, key, default=0):
    try:
        return int(settings.get(key, default))
    except (TypeError, ValueError):
        return int(default)


def _coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _fill_holes_2d_or_3d(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim == 2:
        return _fill_holes_2d(mask)
    if mask.ndim != 3:
        raise ValueError(
            f"_fill_holes_2d_or_3d expects a 2D or 3D mask, got {mask.shape}"
        )
    slice_filled = np.stack(
        [_fill_holes_2d(slice_mask) for slice_mask in mask],
        axis=0,
    )
    if mask.shape[0] == 1:
        return slice_filled
    return _fill_holes_3d(slice_filled)


def _fill_holes_2d(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim != 2:
        raise ValueError(f"_fill_holes_2d expects a 2D mask, got {mask.shape}")

    inverse = ~mask
    visited = np.zeros(mask.shape, dtype=bool)
    stack = []
    height, width = mask.shape
    for row in range(height):
        for col in (0, width - 1):
            if inverse[row, col] and not visited[row, col]:
                visited[row, col] = True
                stack.append((row, col))
    for col in range(width):
        for row in (0, height - 1):
            if inverse[row, col] and not visited[row, col]:
                visited[row, col] = True
                stack.append((row, col))

    while stack:
        row, col = stack.pop()
        for next_row, next_col in (
            (row - 1, col),
            (row + 1, col),
            (row, col - 1),
            (row, col + 1),
        ):
            if (
                0 <= next_row < height
                and 0 <= next_col < width
                and inverse[next_row, next_col]
                and not visited[next_row, next_col]
            ):
                visited[next_row, next_col] = True
                stack.append((next_row, next_col))

    holes = inverse & ~visited
    return mask | holes


def _fill_holes_3d(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim != 3:
        raise ValueError(f"_fill_holes_3d expects a 3D mask, got {mask.shape}")

    inverse = ~mask
    visited = np.zeros(mask.shape, dtype=bool)
    stack = []
    depth, height, width = mask.shape

    def add_boundary_voxel(z_index, row, col):
        if inverse[z_index, row, col] and not visited[z_index, row, col]:
            visited[z_index, row, col] = True
            stack.append((z_index, row, col))

    for row in range(height):
        for col in range(width):
            add_boundary_voxel(0, row, col)
            add_boundary_voxel(depth - 1, row, col)
    for z_index in range(depth):
        for col in range(width):
            add_boundary_voxel(z_index, 0, col)
            add_boundary_voxel(z_index, height - 1, col)
        for row in range(height):
            add_boundary_voxel(z_index, row, 0)
            add_boundary_voxel(z_index, row, width - 1)

    while stack:
        z_index, row, col = stack.pop()
        for next_z, next_row, next_col in (
            (z_index - 1, row, col),
            (z_index + 1, row, col),
            (z_index, row - 1, col),
            (z_index, row + 1, col),
            (z_index, row, col - 1),
            (z_index, row, col + 1),
        ):
            if (
                0 <= next_z < depth
                and 0 <= next_row < height
                and 0 <= next_col < width
                and inverse[next_z, next_row, next_col]
                and not visited[next_z, next_row, next_col]
            ):
                visited[next_z, next_row, next_col] = True
                stack.append((next_z, next_row, next_col))

    holes = inverse & ~visited
    return mask | holes


def _infer_slice_axis(images):
    if images:
        axis = _normalize_vector(images[0].getHead().slice_dir)
        if axis is not None:
            return axis
    if len(images) > 1:
        positions = [np.asarray(image.getHead().position, dtype=float) for image in images]
        delta = positions[-1] - positions[0]
        axis = _normalize_vector(delta)
        if axis is not None:
            return axis
    return np.asarray((0.0, 0.0, 1.0), dtype=float)


def _normalize_vector(values):
    vector = np.asarray(values, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm <= 0:
        return None
    return vector / norm


def _output_fov_z(slice_anchor_images, slice_axis, output_slice_count):
    source_fov_z = float(slice_anchor_images[0].getHead().field_of_view[2])
    if len(slice_anchor_images) <= 1:
        return source_fov_z

    projections = [
        float(np.dot(np.asarray(image.getHead().position, dtype=float), slice_axis))
        for image in slice_anchor_images
    ]
    projections = sorted(projections)
    spacings = np.diff(projections)
    spacings = spacings[np.abs(spacings) > 1e-6]
    if spacings.size:
        return float(np.median(np.abs(spacings)) * output_slice_count)
    return float(source_fov_z * output_slice_count)


def _window_center_width(data):
    values = np.asarray(data, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 1.0
    data_min = float(np.min(finite))
    data_max = float(np.max(finite))
    width = data_max - data_min
    if width <= 0:
        width = 1.0
    return data_min + width / 2.0, width


def _set_header_sequence_field(header, field_name, values):
    target = getattr(header, field_name)
    for index, value in enumerate(values):
        target[index] = value


def _meta_from_image(image):
    if not getattr(image, "attribute_string", ""):
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _meta_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    text = str(value or "").strip()
    return "" if text.upper() == "N/A" else text


def _meta_int(meta, key):
    value = _meta_text(meta, key)
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _dicom_json_numeric_value(meta, tag):
    text = _meta_text(meta, "DicomJson")
    if not text:
        return None
    try:
        dicom_json = json.loads(base64.b64decode(text).decode("utf-8"))
    except Exception:
        return None
    values = dicom_json.get(tag, {}).get("Value", [])
    if not values:
        return None
    try:
        return float(values[0])
    except (TypeError, ValueError):
        return None


def _decode_ice_minihead(value):
    if not value:
        return ""
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _miniice_pulse_width(images):
    """Read WIP_ABSpw (MR_TAG_SEQ_WIP2) from the first available mini-ICE header."""
    for image in images:
        meta = _meta_from_image(image)
        minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
        value = _minihead_numeric_value(minihead, "WIP_ABSpw")
        if value is None:
            value = _minihead_numeric_value(minihead, "MR_TAG_SEQ_WIP2")
        if value is not None and value > 0:
            return value
    return None


def _log_reference_metadata(image, target_values):
    """Log occurrences of configured reference values in incoming metadata."""
    meta = _meta_from_image(image)
    logging.info(
        "Available ISMRMRD metadata variable names (%d): %s",
        len(meta.keys()),
        ", ".join(sorted(str(key) for key in meta.keys())) or "none",
    )
    for key in sorted(meta.keys(), key=str):
        value = _meta_text(meta, key).replace("\n", "\\n")
        logging.info("ISMRMRD metadata value: %s=%s", key, value[:500])
    targets = tuple(
        round(float(value), 1) for value in target_values if float(value) > 0
    )
    number_pattern = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)")
    matches = []
    for key in meta.keys():
        text = _meta_text(meta, key)
        numbers = [float(value) for value in number_pattern.findall(text)]
        if any(round(number, 1) == target for number in numbers for target in targets):
            matches.append(f"{key}={text[:200]}")
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    minihead_names = sorted(
        set(
            re.findall(
                r'<Param(?:String|Double|Float|Long|Int|Bool)\."([^"]+)">',
                minihead,
            )
        )
    )
    logging.info(
        "Available Mini-ICE parameter names (%d): %s",
        len(minihead_names),
        ", ".join(minihead_names) or "none",
    )
    for line in minihead.splitlines():
        stripped = line.strip()
        if stripped and "<Param" in stripped:
            logging.info("Mini-ICE parameter value: %s", stripped[:500])
    mini_matches = [
        line.strip()[:300]
        for line in minihead.splitlines()
        if any(
            round(float(value), 1) == target
            for value in number_pattern.findall(line)
            for target in targets
        )
    ]
    dicom_matches = []
    private_meta_value = None
    for tag, value in meta.items():
        normalized_tag = re.sub(r"[^0-9a-fA-F]", "", str(tag)).lower()
        if normalized_tag == "00211038":
            private_meta_value = value
            break
    dicom_json = _meta_text(meta, "DicomJson")
    if dicom_json:
        try:
            payload = json.loads(base64.b64decode(dicom_json).decode("utf-8"))
            logging.info(
                "Available DICOM JSON tag names (%d): %s",
                len(payload),
                ", ".join(sorted(str(key) for key in payload)) or "none",
            )
            private_value = private_meta_value
            # DICOM JSON producers use several spellings for private tags
            # (e.g. ``00211038``, ``0021,1038`` or ``(0021,1038)``).  Match
            # by the hexadecimal digits so formatting does not hide a value.
            for tag, entry in payload.items():
                normalized_tag = re.sub(r"[^0-9a-fA-F]", "", str(tag)).lower()
                if normalized_tag != "00211038" or not isinstance(entry, dict):
                    continue
                values = entry.get("Value", [])
                if values:
                    private_value = values[0]
                    break
            try:
                private_numeric = float(private_value)
            except (TypeError, ValueError):
                private_numeric = None
            logging.info(
                "Reference-amplitude DICOM private tag (0021,1038): value=%s "
                "configured_ref_amplitude=%s match_to_GUI_precision=%s",
                private_value if private_value is not None else "not present",
                targets[0] if targets else "none",
                private_numeric is not None
                and round(private_numeric, 1) in targets,
            )
            for tag, value in payload.items():
                text = json.dumps(value, separators=(",", ":"))
                if any(
                    round(float(value), 1) == target
                    for value in number_pattern.findall(text)
                    for target in targets
                ):
                    dicom_matches.append(f"{tag}={text[:200]}")
        except Exception as error:
            logging.info("Reference metadata DicomJson decode failed: %s", error)
    else:
        try:
            private_numeric = float(private_meta_value)
        except (TypeError, ValueError):
            private_numeric = None
        logging.info(
            "Reference-amplitude DICOM private tag (0021,1038): value=%s "
            "configured_ref_amplitude=%s match_to_GUI_precision=%s "
            "(DicomJson not present)",
            private_meta_value if private_meta_value is not None else "not present",
            targets[0] if targets else "none",
            private_numeric is not None and round(private_numeric, 1) in targets,
        )
    logging.info(
        "Reference-amplitude value search targets=%s: image_meta=%s mini_ICE=%s "
        "dicom_json=%s",
        targets,
        matches or "none",
        mini_matches or "none",
        dicom_matches or "none",
    )


def _minihead_numeric_value(minihead, key):
    if not minihead:
        return None
    pattern = (
        rf'<Param(?:String|Double|Float|Long|Int)\."{re.escape(key)}">'
        rf'\s*\{{[^}}]*?(?:"value"\s*:\s*)?'
        rf'"?([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)"?'
    )
    match = re.search(pattern, minihead)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _minihead_string_value(minihead, key):
    match = re.search(
        rf'<ParamString\."{re.escape(key)}">\s*{{\s*"([^"]*)"',
        minihead,
    )
    return match.group(1).strip() if match else ""


def _sanitize_identity_text(value):
    return (
        str(value or "")
        .strip()
        .replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )
