"""OpenRecon (MRD image-in/image-out) module for mobstr3D single-slice DENSE preprocessing.

Receives the reconstructed cine DENSE images of one or more slices (magnitude plus x, y
and z encoded phase), and processes each slice as soon as all of its frames have
arrived: nnU-Net modelESv2 segmentation, automatic seed selection and mobstr3D's
spatiotemporal quality-guided phase unwrapping (mobstr3d_slice.py). Returns, per slice:

- the received images (``sendoriginal``),
- the nnU-Net labels (0 background, 1 myocardium, 2 blood pool) with the endocardial
  and epicardial contours of each frame in ``Mobstr3DContours`` (base64 JSON),
- x, y and z displacement maps in mm (stored = mm * 100 + 2048, 0 outside the unwrapped
  myocardium, with matching RescaleSlope/RescaleIntercept). The unwrapping seed and
  per-frame anchor points, slice location and DENSE encoding are in ``Mobstr3DSlice``.

DENSE images are identified, and their encoding frequency, scale, RCswap/RCSflip and
cardiac frame read, from the ImageComments the DENSE reconstruction writes, e.g.
``DENSE x-enc pha - Scale:2.309401 EncFreq:0.06 ... Phs:3/29 RCswap:1 RCSflip:0/0/0``.

Parameters (OpenReconLabel.json): ``frameofseed``, ``endframe``, ``phasequality``,
``contourmethod`` and ``sendoriginal``.
"""
import base64
import json
import logging
import time
import traceback
from copy import deepcopy

import ismrmrd
import numpy as np

import constants
import mrdhelper
import openreconi2iexample as shared

import mobstr3d_slice as pipeline

DISPLACEMENT_COUNTS_PER_MM = 100
DISPLACEMENT_OFFSET = 2048
LABELS = "0=background;1=myocardium;2=bloodpool"
_segmenter = None


def segmenter():
    global _segmenter
    if _segmenter is None:
        _segmenter = pipeline.Segmenter()
        logging.info("mobstr3D: nnU-Net modelESv2 loaded on %s", _segmenter.device)
    return _segmenter


def parse_settings(config):
    settings = pipeline.Settings(
        frame_of_seed=mrdhelper.get_json_config_param(config, "frameofseed", default=pipeline.Settings.frame_of_seed, type="int"),
        end_frame=mrdhelper.get_json_config_param(config, "endframe", default=pipeline.Settings.end_frame, type="int"),
        phase_quality_method=mrdhelper.get_json_config_param(config, "phasequality", default=pipeline.Settings.phase_quality_method, type="choice"),
        label_to_contour_method=mrdhelper.get_json_config_param(config, "contourmethod", default=pipeline.Settings.label_to_contour_method, type="choice"),
    ).validate()
    send_original = mrdhelper.get_json_config_param(config, "sendoriginal", default=True, type="bool")
    return settings, send_original


def _meta(image):
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _dicom_value(meta, tag):
    """A value from the DicomJson header dicom2mrd stores, or None."""
    encoded = meta.get("DicomJson")
    if not encoded:
        return None
    try:
        value = json.loads(base64.b64decode(encoded))[tag]["Value"][0]
    except (KeyError, IndexError, ValueError, TypeError):
        return None
    return value


def image_comment(image):
    meta = _meta(image)
    for key in ("ImageComments", "ImageComment"):
        value = meta.get(key)
        if value:
            return value if isinstance(value, str) else " ".join(map(str, value))
    return _dicom_value(meta, "00204000") or ""


def _location(image):
    head = image.getHead()
    return pipeline.slice_location(head.position, head.read_dir, head.phase_dir)


def _inverted(image):
    """mobstr3D's inverted-slice-location flag: scanner SliceLocation if known, else the slice normal."""
    meta = _meta(image)
    header_location = meta.get("SliceLocation") or _dicom_value(meta, "00201041")
    if isinstance(header_location, list):
        header_location = header_location[0]
    computed = _location(image)
    if header_location not in (None, ""):
        return pipeline.slice_inverted(computed, float(header_location))
    head = image.getHead()
    return float(np.dot(np.cross(head.read_dir, head.phase_dir), head.slice_dir)) < 0


class SliceCollector:
    """Buffers DENSE images per slice location until every encoding has every frame."""

    def __init__(self):
        self.slices = {}

    def add(self, image):
        """Store a DENSE image; return its slice key once that slice is complete, else None."""
        comment = image_comment(image)
        encoding = pipeline.encoding_from_comment(comment)
        if encoding is None:
            return None, False
        frame = pipeline.frame_from_comment(comment)
        index, total = frame if frame else (int(image.getHead().phase), None)
        key = round(_location(image), 2)
        entry = self.slices.setdefault(key, {"images": {}, "frames": None, "comment": None})
        entry["images"].setdefault(encoding, {})[index] = image
        if total:
            entry["frames"] = total
        if encoding != "MAG":
            entry["comment"] = comment
        complete = entry["frames"] is not None and all(
            len(entry["images"].get(enc, {})) >= entry["frames"] for enc in pipeline.ENCODINGS)
        return key, complete

    def pop(self, key):
        return self.slices.pop(key)


def dense_slice(entry, label):
    if entry["comment"] is None:
        raise ValueError(f"{label}: no DENSE phase images received")
    magnitude = entry["images"].get("MAG")
    if not magnitude:
        raise ValueError(f"{label}: no DENSE magnitude images received")
    anchor = next(iter(magnitude.values()))
    head = anchor.getHead()
    fov = np.asarray(head.field_of_view, dtype=float)
    rows, cols = np.asarray(anchor.data).shape[-2:]
    images = {enc: {frame: np.asarray(image.data)[0, 0] for frame, image in frames.items()}
              for enc, frames in entry["images"].items()}
    return pipeline.DenseSlice(
        images=images,
        pixel_spacing=(fov[1] / rows, fov[0] / cols),  # DICOM PixelSpacing order: row, column
        slice_location=_location(anchor),
        inverted=_inverted(anchor),
        parameters=pipeline.DenseParameters.from_comment(entry["comment"]),
        label=label,
    )


def _plane_image(plane, source):
    output = ismrmrd.Image.from_array(plane[None, None], transpose=False)
    constructor = output.getHead()
    header = deepcopy(source.getHead())
    header.data_type = constructor.data_type
    header.channels = constructor.channels
    header.matrix_size = constructor.matrix_size
    output.setHead(header)
    return output


def _stamp_derived(output, source, series_index, index, identity, token, extra):
    """Derived image in its source frame's geometry and storage slot (a cine of one slice)."""
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = shared._source_geometry_header_image_index(source, index)
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    output.setHead(header)
    output.image_series_index = series_index
    storage = shared._original_storage_fields(source, index, output)
    output.attribute_string = shared._segment_source_geometry_meta(
        source, output, series_index, index, identity["series_name"], extra, identity, storage,
        data_role="Image", image_processing_history=["PYTHON", "MOBSTR3D"],
        image_type=f"DERIVED\\PRIMARY\\M\\{token}", image_type_value4=token, scanner_postprocessing=False,
    ).serialize()
    return output


def _encode(payload):
    return base64.b64encode(json.dumps(pipeline.jsonable(payload), separators=(",", ":")).encode()).decode()


def original_series(entry, next_index):
    outputs = []
    for encoding in pipeline.ENCODINGS:
        frames = entry["images"].get(encoding, {})
        if not frames:
            continue
        ordered = [frames[frame] for frame in sorted(frames)]
        series_index = next_index()
        identity = shared._build_output_series_identity(ordered[0], series_index, "original", "source")
        batch = []
        for index, source in enumerate(ordered):
            output = deepcopy(source)
            shared._stamp_original_image(output, source, series_index, index, identity["series_name"], identity)
            batch.append(output)
        outputs.append(batch)
    return outputs


def label_series(result, sources, series_index):
    identity = shared._build_output_series_identity(sources[0], series_index, "mobstr3d-labels", "source")
    outputs = []
    for index, (frame, source) in enumerate(zip(result.frames, sources)):
        key = f"Frame_{frame}"
        output = _plane_image(result.labels[index].astype(np.uint16), source)
        contours = {side: result.contours[side][key] for side in ("endo", "epi")}
        extra = {"Mobstr3DFrame": str(frame), "Mobstr3DLabels": LABELS, "Mobstr3DContours": _encode(contours)}
        shared._stamp_segment_source_geometry_image(output, source, series_index, index, identity["series_name"],
                                                    extra, identity)
        outputs.append(output)
    return outputs


def displacement_series(result, dense, sources, axis, series_index):
    component = "xyz".index(axis)
    identity = shared._build_output_series_identity(sources[0], series_index, f"mobstr3d-d{axis}-mm", "source")
    token = f"MOBSTR3D_D{axis.upper()}"
    slope = 1 / DISPLACEMENT_COUNTS_PER_MM
    intercept = -DISPLACEMENT_OFFSET / DISPLACEMENT_COUNTS_PER_MM
    comment = f"mobstr3D d{axis} mm = stored * {slope} - {-intercept}; 0 = not unwrapped"
    summary = {
        "slice_location_mm": dense.slice_location,
        "slice_location_inverted": dense.inverted,
        "pixel_spacing_mm": dense.pixel_spacing,
        "encoding_frequency": dense.parameters.encoding_frequency,
        "scale": dense.parameters.scale,
        "rc_swap": dense.parameters.rc_swap,
        "rcs_flip": dense.parameters.rcs_flip,
        "frames": result.frames,
    }
    outputs = []
    clipped = 0
    for index, (frame, source) in enumerate(zip(result.frames, sources)):
        values = result.displacement[index, :, :, component]
        valid = ~np.isnan(values)
        stored = np.rint(np.where(valid, values, 0) * DISPLACEMENT_COUNTS_PER_MM) + DISPLACEMENT_OFFSET
        clipped += int(np.sum(valid & ((stored < 1) | (stored > 4095))))
        stored = np.where(valid, np.clip(stored, 1, 4095), 0).astype(np.uint16)
        output = _plane_image(stored, source)
        seeds = result.seed_points.get(f"Frame_{frame}", {})
        extra = {
            "ImageComments": comment,
            "ImageComment": comment,
            "RescaleSlope": str(slope),
            "RescaleIntercept": str(intercept),
            "RescaleType": "mm",
            "WindowCenter": "0",
            "WindowWidth": "20",
            "PixelPaddingValue": "0",
            "Mobstr3DFrame": str(frame),
            "Mobstr3DSlice": _encode({**summary, "frame": frame, "seed_points": seeds.get(axis, [])}),
        }
        outputs.append(_stamp_derived(output, source, series_index, index, identity, token, extra))
    if clipped:
        logging.warning("mobstr3D: %d d%s values exceeded +/-%.2f mm and were clipped", clipped, axis,
                        (4095 - DISPLACEMENT_OFFSET) / DISPLACEMENT_COUNTS_PER_MM)
    return outputs


def process_entry(connection, entry, label, settings, send_original, next_index):
    started = time.time()
    dense = dense_slice(entry, label)
    result = pipeline.process_slice(dense, settings, segmenter())
    sources = [entry["images"]["MAG"][frame] for frame in result.frames]
    if send_original:
        for batch in original_series(entry, next_index):
            connection.send_image(batch)
    connection.send_image(label_series(result, sources, next_index()))
    for axis in "xyz":
        connection.send_image(displacement_series(result, dense, sources, axis, next_index()))
    logging.info("mobstr3D: %s at %.2f mm processed (%d frames) in %.1f s", label, dense.slice_location,
                 len(result.frames), time.time() - started)


def process(connection, config, metadata):
    collector = SliceCollector()
    used = set()
    counter = iter(range(100, 65536))
    done = 0

    def next_index():
        index = next(counter)
        while index in used:
            index = next(counter)
        used.add(index)
        return index

    def run(key, entry):
        nonlocal done
        label = f"Slice_{done}"
        done += 1
        try:
            process_entry(connection, entry, label, settings, send_original, next_index)
        except Exception:
            message = f"mobstr3D: {label} at {key} mm failed\n{traceback.format_exc()}"
            logging.error(message)
            connection.send_logging(constants.MRD_LOGGING_ERROR, message)

    try:
        settings, send_original = parse_settings(config)
        logging.info("mobstr3D: %s, sendoriginal=%s", settings, send_original)
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                raise ValueError("mobstr3D accepts reconstructed MRD images, not raw acquisitions")
            used.add(int(item.image_series_index))
            key, complete = collector.add(item)
            if key is None:
                logging.warning("mobstr3D: ignoring a non-DENSE image (series %d)", item.image_series_index)
            elif complete:
                run(key, collector.pop(key))
        for key in list(collector.slices):
            logging.warning("mobstr3D: slice at %s mm ended before all frames arrived; processing what was received", key)
            run(key, collector.pop(key))
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()
