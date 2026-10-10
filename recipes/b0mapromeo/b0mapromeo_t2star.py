"""Monoexponential magnitude T2* fitting and scanner millisecond images."""

import copy

import ismrmrd
import numpy as np
import openreconi2iexample as helpers
from b0_geometry import _image_axes


def fit_t2star(
    magnitude: np.ndarray, echo_times_ms: list[float], support: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Fit log signal over positive finite echoes inside the B0 foreground."""
    magnitude = np.asarray(magnitude)
    times = np.asarray(echo_times_ms, dtype=np.float64)
    support = np.asarray(support, dtype=bool)
    if magnitude.ndim != 4 or support.shape != magnitude.shape[:3]:
        raise ValueError("T2* requires XYZ-echo magnitude and matching XYZ support")
    if (times.shape != (magnitude.shape[3],) or len(times) < 2
            or not np.all(np.isfinite(times)) or np.any(times <= 0)
            or np.any(np.diff(times) <= 0)):
        raise ValueError("T2* requires positive increasing echo times in ms")
    values_ms = np.zeros(support.shape, dtype=np.float32)
    validity = np.zeros(support.shape, dtype=bool)
    for z in range(magnitude.shape[2]):
        plane = magnitude[:, :, z, :].astype(np.float64)
        samples = np.isfinite(plane) & (plane > 0)
        count = samples.sum(axis=-1)
        log_signal = np.zeros_like(plane)
        np.log(plane, out=log_signal, where=samples)
        divisor = np.maximum(count, 1)
        mean_time = np.sum(np.where(samples, times, 0), axis=-1) / divisor
        mean_log = log_signal.sum(axis=-1) / divisor
        centered_time = np.where(samples, times - mean_time[..., None], 0)
        centered_log = np.where(samples, log_signal - mean_log[..., None], 0)
        denominator = np.sum(centered_time * centered_time, axis=-1)
        numerator = np.sum(centered_time * centered_log, axis=-1)
        slope = np.zeros(count.shape, dtype=np.float64)
        np.divide(numerator, denominator, out=slope, where=denominator > 0)
        varying = (np.max(np.where(samples, log_signal, -np.inf), axis=-1)
                   > np.min(np.where(samples, log_signal, np.inf), axis=-1))
        valid = (support[:, :, z] & (count >= 2) & varying
                 & (slope < 0) & np.isfinite(slope))
        values = np.zeros(count.shape, dtype=np.float64)
        with np.errstate(over="ignore", divide="ignore"):
            np.divide(-1, slope, out=values, where=valid)
        valid &= (np.isfinite(values) & (values > 0)
                  & (values <= np.finfo(np.float32).max))
        values_ms[:, :, z] = np.where(valid, values, 0)
        validity[:, :, z] = valid & (values_ms[:, :, z] > 0)
    return values_ms, validity


def output_t2star_images(
    values_ms: np.ndarray, valid: np.ndarray,
    anchors: list[ismrmrd.Image], series_index: int,
) -> list[ismrmrd.Image]:
    """Encode a separate unsigned T2* series with zero reserved for padding."""
    if not 0 <= series_index <= 65535:
        raise ValueError("T2* output series index exceeds the MRD uint16 range")
    values_ms = np.asarray(values_ms)
    valid = np.asarray(valid, dtype=bool)
    if (values_ms.ndim != 3 or valid.shape != values_ms.shape
            or len(anchors) != values_ms.shape[2]):
        raise ValueError("T2* output geometry and anchors must match")
    if np.any(~np.isfinite(values_ms[valid])) or np.any(values_ms[valid] <= 0):
        raise ValueError("Valid T2* output values must be finite and positive")
    maximum = float(np.max(values_ms[valid])) if np.any(valid) else 0
    slope = max(1.0, maximum / 4094)
    pixels = np.where(valid, np.clip(np.rint(values_ms / slope) + 1, 1, 4095), 0)
    pixels = pixels.astype(np.uint16)
    name = "T2STAR ms"
    identity = helpers._build_output_series_identity_from_name(
        anchors[0], series_index, name
    )
    comment = "T2* ms; unweighted log-linear magnitude fit; positive finite echoes >=2"
    outputs = []
    for z, source in enumerate(anchors):
        output = ismrmrd.Image.from_array(
            pixels[:, :, z].T[None, None], transpose=False
        )
        header = copy.deepcopy(source.getHead())
        header.data_type = output.data_type
        output.setHead(header)
        helpers._stamp_output_image(
            output, source, series_index, z, name, ["Image", "Quantitative"],
            "T2STAR_MAP", ["T2STAR", "MS"], series_identity=identity,
            extra_meta={
                "ImageComment": comment, "ImageComments": comment,
                "RescaleSlope": str(slope), "RescaleIntercept": str(-slope),
                "RescaleType": "ms", "T2StarMapUnits": "ms",
                "T2StarFitMethod": "unweighted log-linear monoexponential",
                "WindowCenter": "50", "WindowWidth": "100",
                "PixelPaddingValue": "0", "PixelPaddingRangeLimit": "0",
                "NumberOfSlices": str(len(anchors)),
                "ImagesInAcquisition": str(len(anchors)),
                "slice_count": str(len(anchors)), "NumberInSeries": str(z + 1),
            },
        )
        source_meta = ismrmrd.Meta.deserialize(source.attribute_string)
        if "ImageRowDir" in source_meta or "ImageColumnDir" in source_meta:
            axes = _image_axes(source)
            meta = ismrmrd.Meta.deserialize(output.attribute_string)
            meta["ImageSliceDir"] = axes[:, 2].tolist()
            meta["ImageSliceNormDir"] = axes[:, 2].tolist()
            output.attribute_string = meta.serialize()
        outputs.append(output)
    return outputs
