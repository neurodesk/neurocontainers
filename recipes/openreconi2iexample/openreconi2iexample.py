"""Minimal configurable OpenRecon image-to-image example."""
import base64
import json
import logging
import re
import traceback
import uuid

import constants
import ismrmrd
import numpy as np


INVERT_SERIES_INDEX = 99
ORIGINAL_SERIES_INDEX = 100
SEGMENT_SERIES_INDEX = 101
UPSAMPLED_SERIES_INDEX = 102
MIP_SERIES_INDEX = 103
METRICS_SERIES_INDEX = 120
INVERT_SERIES_NAME = "openrecon_invert"
ORIGINAL_SERIES_NAME = "openrecon_original"
SEGMENT_SERIES_NAME = "openrecon_segment"
UPSAMPLED_SERIES_NAME = "openrecon_upsampled"
MIP_SERIES_NAME = "openrecon_mip"
METRICS_SERIES_NAME = "openrecon_metrics"
SEGMENTATION_LUT = "MicroDeltaHotMetal.pal"
SEGMENT_SOURCE_GEOMETRY_META_KEY = "SegmentSourceGeometry"
SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE = (
    f"DERIVED\\PRIMARY\\SEGMENTATION\\{SEGMENT_SERIES_NAME}"
)
SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4 = SEGMENT_SERIES_NAME
SEGMENT_POSTPROCESSING_META_KEY = "SegmentPostProcessing"
SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY = "SegmentPostProcessingChildRole"
SEGMENT_POSTPROCESSING_IMAGE_TYPE = f"DERIVED\\PRIMARY\\M\\{SEGMENT_SERIES_NAME}"
SEGMENT_POSTPROCESSING_IMAGE_TYPE_VALUE4 = SEGMENT_SERIES_NAME
SEGMENT_POSTPROCESSING_DISALLOWED_DIXON_TOKENS = {
    "DIXON",
    "FAT",
    "FAT_FRAC",
    "WATER",
}
METRICS_FIELDNAMES = (
    "region",
    "source",
    "voxels",
    "volume_mm3",
    "volume_ml",
    "voxel_mm3",
    "threshold",
)
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
ORIGINAL_SINGLE_PARTITION_FIELDS = (
    "Actual3DImagePartNumber",
    "AnatomicalPartitionNo",
)
ORIGINAL_RESTAMPED_STORAGE_FIELDS = (
    "Actual3DImagePartNumber",
    "AnatomicalPartitionNo",
    "AnatomicalSliceNo",
    "ChronSliceNo",
    "NumberInSeries",
    "ProtocolSliceNumber",
    "SliceNo",
    "IsmrmrdSliceNo",
)
MINIHEAD_STORAGE_FIELD_SPECS = (
    ("Actual3DImagePartNumber", "DICOM", "Actual3DImagePartNumber"),
    ("Actual3DImaPartNumber", "DICOM", "Actual3DImagePartNumber"),
    ("NumberInSeries", "DICOM", "NumberInSeries"),
    ("ProtocolSliceNumber", "DICOM", "ProtocolSliceNumber"),
    ("SliceNo", "DICOM", "SliceNo"),
    ("AnatomicalPartitionNo", "CONTROL", "AnatomicalPartitionNo"),
    ("AnatomicalSliceNo", "CONTROL", "AnatomicalSliceNo"),
    ("ChronSliceNo", "CONTROL", "ChronSliceNo"),
    ("IsmrmrdSliceNo", "CONTROL", "IsmrmrdSliceNo"),
)
SCANNER_PARTITION_INDEX = 0
SLICE_POSITION_TOLERANCE_MM = 1e-4
EXTRA_ORIGINAL_SERIES_INDEX_START = 104
EXTRA_SEGMENT_SERIES_INDEX_START = 110
SOURCE_VOLUME_GROUP_FIELDS = (
    ("contrast", "c"),
    ("phase", "ph"),
    ("repetition", "rep"),
    ("set", "set"),
    ("average", "avg"),
)


def process(connection, config, metadata):
    logging.info("Config: %s", config)
    input_images = []
    magnitude_images = []

    try:
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                continue

            input_images.append(item)
            if item.image_type in (ismrmrd.IMTYPE_MAGNITUDE, 0):
                magnitude_images.append(item)

        if not input_images:
            logging.warning("No image messages received; closing without output")
            return

        send_original = _config_bool(config, "sendoriginal", default=False)
        send_invert = _config_bool_any(config, ("invert", "sendinvert"), default=False)
        send_upsampled = _config_bool_any(
            config,
            ("upsampled", "sendupsampled", "sendinterpolated"),
            default=False,
        )
        send_segment = _config_bool_any(
            config,
            ("segment", "sendsegment", "sendthreshold"),
            default=False,
        )
        send_segment_postprocessing = _config_bool_any(
            config,
            (
                "segmentpostprocessing",
                "sendsegmentpostprocessing",
                "segmentationpostprocessing",
                "sendsegmentationpostprocessing",
            ),
            default=False,
        )
        use_segmentation_colormap = _config_bool_any(
            config,
            ("segmentationcolormap", "sendsegmentationcolormap"),
            default=False,
        )
        send_mip = _config_bool_any(
            config,
            ("mip", "sendmip", "sendthresholdmip"),
            default=False,
        )
        send_metrics = _config_bool_any(
            config,
            ("sendmetrics", "metrics", "sendregionmetrics"),
            default=False,
        )
        output_images = []
        original_images = []
        if send_original:
            original_images = _restamp_originals(input_images)
            output_images.extend(original_images)
        inverted_images = []
        if send_invert:
            inverted_images = _invert_images(magnitude_images)
            output_images.extend(inverted_images)
        computed_segment_images = []
        segment_images = []
        metrics_rows = []
        if send_segment or send_metrics:
            if send_segment and send_segment_postprocessing:
                computed_segment_images = _segment_images_for_postprocessing(
                    magnitude_images,
                    use_colormap=use_segmentation_colormap,
                    metrics_rows=metrics_rows if send_metrics else None,
                )
            else:
                computed_segment_images = _segment_images(
                    magnitude_images,
                    use_colormap=use_segmentation_colormap,
                    metrics_rows=metrics_rows if send_metrics else None,
                )
        if send_segment:
            segment_images = computed_segment_images
            output_images.extend(segment_images)
        upsampled_images = []
        if send_upsampled:
            upsampled_images = _upsampled_images(magnitude_images)
            output_images.extend(upsampled_images)
        mip_images = []
        if send_mip:
            mip_images = _mip_image(magnitude_images)
            output_images.extend(mip_images)
        metrics_images = []
        if send_metrics:
            metrics_images = _metrics_report_images(magnitude_images, metrics_rows)
            output_images.extend(metrics_images)

        logging.info(
            "Configured outputs: original=%s invert=%s upsampled=%s segment=%s "
            "segmentpostprocessing=%s segmentationcolormap=%s mip=%s metrics=%s",
            send_original,
            send_invert,
            send_upsampled,
            send_segment,
            send_segment_postprocessing,
            use_segmentation_colormap,
            send_mip,
            send_metrics,
        )
        logging.info(
            "Sending %d original image(s), %d inverted image(s), "
            "%d upsampled image(s), %d segmentation image(s), %d MIP image(s), "
            "and %d metrics image(s)",
            len(original_images),
            len(inverted_images),
            len(upsampled_images),
            len(segment_images),
            len(mip_images),
            len(metrics_images),
        )
        if not output_images:
            logging.info("No output options enabled; closing without output")
            return

        _validate_output_images(output_images, input_images)
        _log_output_images(output_images)
        send_batches = _output_send_batches(
            output_images,
            original_images,
            split_originals=bool(
                original_images
                and segment_images
                and send_segment
                and send_segment_postprocessing
            ),
        )
        if len(send_batches) > 1:
            logging.info(
                "Sending source-geometry originals separately from "
                "segmentpostprocessing outputs across %d MRD image messages",
                len(send_batches),
            )
        for batch_index, batch in enumerate(send_batches, start=1):
            if len(send_batches) > 1:
                logging.info(
                    "Sending MRD image message batch %d/%d with %d image(s)",
                    batch_index,
                    len(send_batches),
                    len(batch),
                )
            connection.send_image(batch)

    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
    finally:
        connection.send_close()


def _output_send_batches(output_images, original_images, split_originals=False):
    if not split_originals:
        return [output_images]

    original_ids = {id(image) for image in original_images}
    original_batch = [
        image for image in output_images
        if id(image) in original_ids
    ]
    remaining_batch = [
        image for image in output_images
        if id(image) not in original_ids
    ]
    return [
        batch for batch in (original_batch, remaining_batch)
        if batch
    ]


def _invert_images(images):
    if not images:
        return []

    data_min = min(float(np.min(np.asarray(image.data))) for image in images)
    data_max = max(float(np.max(np.asarray(image.data))) for image in images)
    window_width = max(data_max - data_min, 1.0)
    window_center = data_min + window_width / 2.0

    series_identity = _build_output_series_identity(
        images[0],
        INVERT_SERIES_INDEX,
        "inverted",
        INVERT_SERIES_NAME,
    )
    inverted_items = []
    for source_image in images:
        source_data = np.asarray(source_image.data)
        inverted = data_min + data_max - source_data.astype(np.float32)
        if np.issubdtype(source_data.dtype, np.integer):
            inverted = np.rint(inverted).astype(source_data.dtype)
        else:
            inverted = inverted.astype(source_data.dtype)
        inverted_items.append((source_image, inverted))

    if _source_geometry_needs_explicit_volume(images):
        return [
            _pack_explicit_volume(
                inverted_items,
                INVERT_SERIES_INDEX,
                series_identity,
                "Image",
                INVERT_SERIES_NAME,
                ["PYTHON", "OPENRECON_INVERT_VOLUME"],
                {
                    "WindowCenter": str(window_center),
                    "WindowWidth": str(window_width),
                },
                "inverted output",
            )
        ]

    outputs = []
    for output_index, (source_image, inverted) in enumerate(inverted_items):
        output = ismrmrd.Image.from_array(inverted, transpose=False)
        header = source_image.getHead()
        header.data_type = output.data_type
        output.setHead(header)
        _stamp_output_image(
            output,
            source_image,
            INVERT_SERIES_INDEX,
            output_index,
            series_identity["series_name"],
            "Image",
            INVERT_SERIES_NAME,
            ["PYTHON", "OPENRECON_INVERT"],
            {
                "WindowCenter": str(window_center),
                "WindowWidth": str(window_width),
            },
            series_identity=series_identity,
        )
        outputs.append(output)

    return outputs


def _segment_images(images, use_colormap=False, metrics_rows=None):
    if not images:
        return []

    source_groups = _original_source_groups(images)
    if len(source_groups) > 1:
        logging.info(
            "segmentation split %d received image(s) into %d source volume group(s)",
            len(images),
            len(source_groups),
        )
    segment_suffixes = _segment_group_suffixes(source_groups)
    outputs = []
    thresholds = []
    for group_index, group_images in enumerate(source_groups):
        _validate_original_source_geometry(group_images, "segmentation")
        threshold = _bright_foreground_threshold(group_images)
        thresholds.append(threshold)
        series_index = _segment_series_index(group_index)
        series_identity = _build_output_series_identity(
            group_images[0],
            series_index,
            segment_suffixes[group_index],
            SEGMENT_SERIES_NAME,
        )
        segment_items = []
        for source_image in group_images:
            source_data = np.asarray(source_image.data)
            foreground = source_data.astype(np.float32) >= threshold
            segmentation = _largest_connected_component_per_plane(foreground).astype(
                np.uint16
            )
            segment_items.append((source_image, segmentation))

        metric_row = None
        if metrics_rows is not None:
            metric_row = _region_metrics_row(
                group_images,
                group_index,
                segment_suffixes[group_index],
                threshold,
                segment_items,
            )
            metrics_rows.append(metric_row)

        extra_meta = {
            "WindowCenter": "0.5",
            "WindowWidth": "1",
        }
        if metric_row:
            metrics_comment = _format_region_volume_comment(metric_row)
            extra_meta["ImageComments"] = metrics_comment
            extra_meta["ImageComment"] = metrics_comment
        if len(source_groups) > 1:
            extra_meta["SourceVolumeGroup"] = segment_suffixes[group_index]
            for field_name, _label in SOURCE_VOLUME_GROUP_FIELDS:
                extra_meta[f"Source{field_name.title()}"] = str(
                    _source_volume_field_value(group_images[0], field_name)
                )
        if use_colormap:
            extra_meta["LUTFileName"] = SEGMENTATION_LUT

        for output_index, (source_image, segmentation) in enumerate(segment_items):
            output = ismrmrd.Image.from_array(segmentation, transpose=False)
            header = source_image.getHead()
            header.data_type = output.data_type
            output.setHead(header)
            _stamp_segment_source_geometry_image(
                output,
                source_image,
                series_index,
                output_index,
                series_identity["series_name"],
                extra_meta,
                series_identity,
            )
            outputs.append(output)

    logging.info(
        "Created %d source-geometry segmentation image(s) from %d source image(s) "
        "across %d source volume group(s) with threshold(s) %s, "
        "segmentpostprocessing=False, and "
        "segmentationcolormap=%s",
        len(outputs),
        len(images),
        len(source_groups),
        ", ".join(f"{threshold:.6g}" for threshold in thresholds),
        use_colormap,
    )
    return outputs


def _segment_images_for_postprocessing(
    images,
    use_colormap=False,
    metrics_rows=None,
):
    if not images:
        return []

    source_groups = _original_source_groups(images)
    if len(source_groups) > 1:
        logging.info(
            "segmentpostprocessing split %d received image(s) into %d source "
            "volume group(s)",
            len(images),
            len(source_groups),
        )
    segment_suffixes = _segment_group_suffixes(source_groups)
    outputs = []
    thresholds = []

    for group_index, group_images in enumerate(source_groups):
        _validate_original_source_geometry(group_images)
        threshold = _bright_foreground_threshold(group_images)
        thresholds.append(threshold)
        series_index = _segment_series_index(group_index)
        series_identity = _build_output_series_identity(
            group_images[0],
            series_index,
            segment_suffixes[group_index],
            SEGMENT_SERIES_NAME,
        )
        segment_items = []
        for source_image in group_images:
            source_data = np.asarray(source_image.data)
            foreground = source_data.astype(np.float32) >= threshold
            segmentation = _largest_connected_component_per_plane(foreground).astype(
                np.uint16
            )
            segment_items.append((source_image, segmentation))

        metric_row = None
        if metrics_rows is not None:
            metric_row = _region_metrics_row(
                group_images,
                group_index,
                segment_suffixes[group_index],
                threshold,
                segment_items,
            )
            metrics_rows.append(metric_row)

        extra_meta = {
            "WindowCenter": "0.5",
            "WindowWidth": "1",
            SEGMENT_POSTPROCESSING_META_KEY: "1",
        }
        if metric_row:
            metrics_comment = _format_region_volume_comment(metric_row)
            extra_meta["ImageComments"] = metrics_comment
            extra_meta["ImageComment"] = metrics_comment
        if len(source_groups) > 1:
            extra_meta["SourceVolumeGroup"] = segment_suffixes[group_index]
            for field_name, _label in SOURCE_VOLUME_GROUP_FIELDS:
                extra_meta[f"Source{field_name.title()}"] = str(
                    _source_volume_field_value(group_images[0], field_name)
                )
        if use_colormap:
            extra_meta["LUTFileName"] = SEGMENTATION_LUT

        for output_index, (source_image, segmentation) in enumerate(segment_items):
            output = ismrmrd.Image.from_array(segmentation, transpose=False)
            header = source_image.getHead()
            header.data_type = output.data_type
            output.setHead(header)
            _stamp_segment_postprocessing_image(
                output,
                source_image,
                series_index,
                output_index,
                series_identity["series_name"],
                extra_meta,
                series_identity,
            )
            outputs.append(output)

    logging.info(
        "Created %d source-geometry segmentation image(s) from %d source image(s) "
        "across %d source volume group(s) with threshold(s) %s, "
        "segmentpostprocessing=True, and segmentationcolormap=%s",
        len(outputs),
        len(images),
        len(source_groups),
        ", ".join(f"{threshold:.6g}" for threshold in thresholds),
        use_colormap,
    )
    return outputs


def _region_metrics_row(group_images, group_index, region_name, threshold, segment_items):
    voxel_count = int(
        sum(
            int(np.count_nonzero(np.asarray(segmentation)))
            for _image, segmentation in segment_items
        )
    )
    voxel_volume_mm3 = _source_voxel_volume_mm3(group_images)
    volume_mm3 = float(voxel_count * voxel_volume_mm3)
    source_name = _source_series_name(group_images[0]) or "source"
    return {
        "region": region_name,
        "source": source_name,
        "series": _segment_series_index(group_index),
        "voxels": voxel_count,
        "voxel_mm3": voxel_volume_mm3,
        "volume_mm3": volume_mm3,
        "volume_ml": volume_mm3 / 1000.0,
        "threshold": float(threshold),
    }


def _source_voxel_volume_mm3(images):
    if not images:
        return 0.0

    header = images[0].getHead()
    matrix = [max(int(value), 1) for value in header.matrix_size]
    fov = [float(value) for value in header.field_of_view]
    voxel_x = fov[0] / matrix[0] if matrix[0] else 1.0
    voxel_y = fov[1] / matrix[1] if matrix[1] else 1.0
    slice_axis = _infer_slice_axis([image.getHead() for image in images])
    voxel_z = _explicit_volume_output_spacing(images, slice_axis)
    return abs(float(voxel_x * voxel_y * voxel_z))


def _format_metric_number(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return str(value)
    if np.isposinf(value):
        return "inf"
    if np.isneginf(value):
        return "-inf"
    if not np.isfinite(value):
        return "nan"
    return f"{value:.6g}"


def _format_region_volume_comment(row):
    return (
        "Region volume: "
        f"{_format_metric_number(row['volume_ml'])} mL "
        f"({_format_metric_number(row['volume_mm3'])} mm3; "
        f"{int(row['voxels'])} voxels)"
    )


def _format_metrics_summary_comment(rows, max_chars=1200):
    rows = list(rows or [])
    if not rows:
        return ""
    if len(rows) == 1:
        return _format_region_volume_comment(rows[0])

    total_volume_ml = sum(float(row["volume_ml"]) for row in rows)
    parts = [
        f"{row['region']}={_format_metric_number(row['volume_ml'])} mL"
        for row in rows[:8]
    ]
    if len(rows) > 8:
        parts.append(f"... {len(rows) - 8} more")
    text = (
        "Region volumes: "
        + ", ".join(parts)
        + f"; total={_format_metric_number(total_volume_ml)} mL"
    )
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def _metrics_report_images(images, metrics_rows):
    metrics_rows = list(metrics_rows or [])
    if not images or not metrics_rows:
        if not metrics_rows:
            logging.info("sendmetrics requested but no region metrics were available")
        return []

    source_header = images[0].getHead()
    report_width = max(int(source_header.matrix_size[0]), 1)
    report_height = max(int(source_header.matrix_size[1]), 1)
    page = _render_metrics_table_page(
        metrics_rows,
        width=report_width,
        height=report_height,
    )
    page = _orient_metrics_report_page_for_scanner(page)
    height, width = int(page.shape[0]), int(page.shape[1])
    series_identity = _build_output_series_identity(
        images[0],
        METRICS_SERIES_INDEX,
        "metrics",
        METRICS_SERIES_NAME,
    )
    report_source = _metrics_report_source_image(images[0], width, height)
    metrics_comment = _format_metrics_summary_comment(metrics_rows)
    extra_meta = {
        "WindowCenter": "2040",
        "WindowWidth": "4080",
        "ImageComments": metrics_comment,
        "ImageComment": metrics_comment,
        "MetricsRows": str(len(metrics_rows)),
    }
    output = _pack_explicit_volume(
        [(report_source, page.reshape(1, 1, height, width))],
        METRICS_SERIES_INDEX,
        series_identity,
        "Segmentation",
        METRICS_SERIES_NAME,
        ["PYTHON", "OPENRECON_METRICS"],
        extra_meta,
        "metrics report output",
    )
    logging.info(
        "Created metrics report image with %d row(s), total volume=%s mL",
        len(metrics_rows),
        _format_metric_number(sum(float(row["volume_ml"]) for row in metrics_rows)),
    )
    return [output]


def _orient_metrics_report_page_for_scanner(page):
    return np.rot90(np.asarray(page), 2).copy()


def _metrics_report_source_image(source_image, width, height):
    data = np.zeros((1, 1, height, width), dtype=np.uint16)
    report_source = ismrmrd.Image.from_array(data, transpose=False)
    header = source_image.getHead()
    header.data_type = report_source.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_index = 1
    header.slice = 0
    header.contrast = 0
    _set_metrics_report_header_geometry(header, width, height)
    report_source.setHead(header)
    report_source.attribute_string = source_image.attribute_string
    return report_source


def _set_metrics_report_header_geometry(header, width, height):
    _set_header_sequence_field(header, "matrix_size", [width, height, 1])
    _set_header_sequence_field(
        header,
        "field_of_view",
        [float(width), float(height), 1.0],
    )
    _set_header_sequence_field(header, "position", [0.0, 0.0, 0.0])
    _set_header_sequence_field(header, "read_dir", [1.0, 0.0, 0.0])
    _set_header_sequence_field(header, "phase_dir", [0.0, 1.0, 0.0])
    _set_header_sequence_field(header, "slice_dir", [0.0, 0.0, 1.0])


def _render_metrics_table_page(metrics_rows, width=1024, height=None):
    from PIL import Image, ImageDraw, ImageFont

    width = max(int(width), 1)
    header_lines = 3
    if height is None:
        margin = 24
        row_height = 24
        height = max(
            512,
            margin * 2 + row_height * (len(metrics_rows) + header_lines + 2),
        )
    else:
        height = max(int(height), 1)
        margin = max(2, min(24, width // 32, height // 24))
        visible_rows = max(len(metrics_rows) + header_lines + 2, 1)
        row_height = max(8, min(24, (height - 2 * margin) // visible_rows))
    image = Image.new("L", (width, height), 0)
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()

    draw.text((margin, margin), "OpenRecon Region Metrics", fill=255, font=font)
    draw.text(
        (margin, margin + row_height),
        f"Rows: {len(metrics_rows)}    Total volume: "
        f"{_format_metric_number(sum(float(row['volume_ml']) for row in metrics_rows))} mL",
        fill=220,
        font=font,
    )

    table_top = margin + row_height * header_lines
    base_column_widths = {
        "region": 150,
        "source": 250,
        "voxels": 90,
        "volume_mm3": 135,
        "volume_ml": 120,
        "voxel_mm3": 115,
        "threshold": 120,
    }
    available_width = max(width - 2 * margin, len(METRICS_FIELDNAMES))
    scale = min(1.0, available_width / sum(base_column_widths.values()))
    column_widths = {
        field: max(8, int(base_column_widths[field] * scale))
        for field in METRICS_FIELDNAMES
    }
    columns = [(field, column_widths[field]) for field in METRICS_FIELDNAMES]
    x = margin
    for field, width_px in columns:
        draw.text((x, table_top), field, fill=255, font=font)
        x += width_px
    draw.line(
        (
            margin,
            table_top + row_height - 6,
            width - margin,
            table_top + row_height - 6,
        ),
        fill=120,
    )

    y = table_top + row_height
    for row in metrics_rows:
        x = margin
        for field, width_px in columns:
            value = row.get(field, "")
            if isinstance(value, float):
                value = _format_metric_number(value)
            text = str(value)
            if len(text) > 32:
                text = text[:29] + "..."
            draw.text((x, y), text, fill=220, font=font)
            x += width_px
        y += row_height

    return np.asarray(image, dtype=np.uint16) * 16


def _bright_foreground_threshold(images):
    values = []
    for image in images:
        data = np.asarray(image.data, dtype=np.float32)
        values.append(data[np.isfinite(data)])
    finite_values = np.concatenate(values) if values else np.asarray([], dtype=np.float32)
    if finite_values.size == 0:
        return np.inf

    data_min = float(np.min(finite_values))
    data_max = float(np.max(finite_values))
    if data_max <= data_min:
        return np.inf

    background = float(np.percentile(finite_values, 50))
    upper = float(np.percentile(finite_values, 95))
    if upper <= background:
        upper = data_max

    threshold = background + 0.5 * (upper - background)
    if threshold >= data_max:
        threshold = background + 0.5 * (data_max - background)
    return threshold


def _largest_connected_component_per_plane(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim < 2:
        return _largest_connected_component_2d(mask.reshape(1, -1)).reshape(mask.shape)

    result = np.zeros(mask.shape, dtype=bool)
    planes = mask.reshape((-1,) + mask.shape[-2:])
    result_planes = result.reshape((-1,) + mask.shape[-2:])
    for index, plane in enumerate(planes):
        result_planes[index] = _largest_connected_component_2d(plane)
    return result


def _largest_connected_component_2d(mask):
    mask = np.asarray(mask, dtype=bool)
    height, width = mask.shape
    foreground = mask.ravel()
    foreground_indices = np.flatnonzero(foreground)
    if foreground_indices.size == 0:
        return np.zeros(mask.shape, dtype=bool)

    visited = np.zeros(foreground.size, dtype=bool)
    best_component = []
    for seed in foreground_indices:
        if visited[seed]:
            continue

        component = []
        stack = [int(seed)]
        visited[seed] = True
        while stack:
            current = stack.pop()
            component.append(current)
            row, col = divmod(current, width)
            neighbors = []
            if row > 0:
                neighbors.append(current - width)
            if row + 1 < height:
                neighbors.append(current + width)
            if col > 0:
                neighbors.append(current - 1)
            if col + 1 < width:
                neighbors.append(current + 1)
            for neighbor in neighbors:
                if foreground[neighbor] and not visited[neighbor]:
                    visited[neighbor] = True
                    stack.append(neighbor)

        if len(component) > len(best_component):
            best_component = component

    result = np.zeros(foreground.size, dtype=bool)
    result[best_component] = True
    return result.reshape(mask.shape)


def _mip_image(images):
    if not images:
        return []

    stack = np.stack([np.asarray(image.data) for image in images])
    # Stack shape is [image, cha, z, y, x]. Collapse source slices and any
    # per-image z dimension to one projected output slice.
    mip = stack.max(axis=(0, 2))
    mip = mip[:, np.newaxis, :, :]
    mip = _cast_like(mip, np.asarray(images[0].data))

    output = ismrmrd.Image.from_array(mip, transpose=False)
    header = images[0].getHead()
    header.data_type = output.data_type
    output.setHead(header)
    series_identity = _build_output_series_identity(
        images[0],
        MIP_SERIES_INDEX,
        "mip",
        MIP_SERIES_NAME,
    )
    _stamp_output_image(
        output,
        images[0],
        MIP_SERIES_INDEX,
        0,
        series_identity["series_name"],
        "Image",
        MIP_SERIES_NAME,
        ["PYTHON", "OPENRECON_MIP"],
        series_identity=series_identity,
    )
    logging.info(
        "Created maximum intensity projection from %d image(s)",
        len(images),
    )
    return [output]


def _upsampled_images(images):
    if not images:
        return []

    images, slice_axis = _ordered_upsample_sources(images)
    series_identity = _build_output_series_identity(
        images[0],
        UPSAMPLED_SERIES_INDEX,
        "upsampled",
        UPSAMPLED_SERIES_NAME,
    )
    fallback_half_step = _interpolated_half_step(images)
    output_slice_count = 2 * len(images)
    output_spacing = _upsampled_output_spacing(images, slice_axis, fallback_half_step)
    output_half_step = output_spacing * slice_axis
    source_slice_count_hint = _source_slice_count_hint(images)
    logging.info(
        "upsampled: input_slices=%d output_slices=%d output_spacing=%.6f "
        "source_slice_count_hint=%s",
        len(images),
        output_slice_count,
        output_spacing,
        source_slice_count_hint if source_slice_count_hint is not None else "unknown",
    )
    if (
        source_slice_count_hint is not None
        and output_slice_count > source_slice_count_hint
    ):
        logging.warning(
            "upsampled output_slices=%d exceeds source slice_count hint=%d; "
            "using explicit output geometry",
            output_slice_count,
            source_slice_count_hint,
        )
    origin = _header_position(images[0].getHead())
    slice_images = []
    for index, image in enumerate(images):
        current_position = origin + (2 * index * output_spacing * slice_axis)
        if index + 1 < len(images):
            next_image = images[index + 1]
            midpoint = 0.5 * (
                np.asarray(image.data, dtype=np.float32)
                + np.asarray(next_image.data, dtype=np.float32)
            )
        else:
            midpoint = np.asarray(image.data)
        midpoint_position = current_position + output_half_step

        slice_images.append(
            _make_upsampled_image(
                image,
                np.asarray(image.data),
                2 * index,
                current_position,
                output_half_step,
                output_slice_count,
                slice_axis=slice_axis,
                slice_thickness=output_spacing,
                series_identity=series_identity,
            )
        )

        slice_images.append(
            _make_upsampled_image(
                image,
                _cast_like(midpoint, np.asarray(image.data)),
                2 * index + 1,
                midpoint_position,
                output_half_step,
                output_slice_count,
                slice_axis=slice_axis,
                slice_thickness=output_spacing,
                series_identity=series_identity,
            )
        )

    logging.info(
        "Created %d upsampled slice(s) from %d source image(s)",
        len(slice_images),
        len(images),
    )
    _validate_unique_projected_positions(slice_images, slice_axis, "upsampled output")
    output = _pack_upsampled_volume(
        slice_images,
        images[0],
        output_slice_count,
        output_spacing,
        slice_axis,
        series_identity,
    )
    logging.info(
        "Packed %d upsampled slice(s) into one volume image with matrix_size=%s "
        "and field_of_view=%s",
        output_slice_count,
        _format_vector(output.getHead().matrix_size),
        _format_vector(output.getHead().field_of_view),
    )
    return [output]


def _interpolated_images(images):
    return _upsampled_images(images)


def _make_upsampled_image(
    source_image,
    data,
    output_index,
    position,
    half_step,
    output_slice_count=None,
    slice_axis=None,
    slice_thickness=None,
    series_identity=None,
):
    output_slice_count = _require_output_slice_count(output_slice_count)
    output = ismrmrd.Image.from_array(data, transpose=False)
    header = source_image.getHead()
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_index = output_index + 1
    header.slice = output_index

    fov = [float(value) for value in header.field_of_view]
    if slice_axis is None:
        slice_axis = _normalize_vector(half_step)
    if slice_axis is None:
        slice_axis = _infer_slice_axis([header])

    step_length = (
        float(slice_thickness)
        if slice_thickness is not None
        else float(np.linalg.norm(half_step))
    )
    if step_length > 0:
        fov[2] = step_length
    _set_header_sequence_field(header, "field_of_view", fov)
    _set_header_sequence_field(
        header,
        "position",
        [float(value) for value in position],
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )

    output.setHead(header)
    geometry_meta = _explicit_header_geometry_meta(header)
    extra_meta = {
        "Keep_image_geometry": str(int(0)),
        "partition_count": str(int(1)),
        "slice_count": str(int(output_slice_count)),
        "NumberOfSlices": str(int(output_slice_count)),
        "ImagesInAcquisition": str(int(output_slice_count)),
    }
    extra_meta.update(geometry_meta)
    _stamp_output_image(
        output,
        source_image,
        UPSAMPLED_SERIES_INDEX,
        output_index,
        (
            series_identity["series_name"]
            if series_identity
            else _derived_series_name(
                source_image,
                "upsampled",
                UPSAMPLED_SERIES_NAME,
            )
        ),
        "Image",
        UPSAMPLED_SERIES_NAME,
        ["PYTHON", "OPENRECON_UPSAMPLED"],
        # The source MiniHead can carry the original SLC bounds; explicit
        # geometry lets the host accept the larger upsampled slice range.
        extra_meta=extra_meta,
        patch_minihead=False,
        series_identity=series_identity,
    )
    return output


def _make_interpolated_image(
    source_image,
    data,
    output_index,
    position,
    half_step,
    output_slice_count=None,
):
    output_slice_count = _require_output_slice_count(output_slice_count)
    return _make_upsampled_image(
        source_image,
        data,
        output_index,
        position,
        half_step,
        output_slice_count,
    )


def _pack_upsampled_volume(
    slice_images,
    source_image,
    output_slice_count,
    output_spacing,
    slice_axis,
    series_identity,
):
    output_slice_count = _require_output_slice_count(output_slice_count)
    if len(slice_images) != output_slice_count:
        raise ValueError(
            "upsampled volume slice count mismatch: "
            f"{len(slice_images)} != {output_slice_count}"
        )

    volume_slices = []
    for index, image in enumerate(slice_images):
        data = np.asarray(image.data)
        if data.ndim != 4 or data.shape[0] != 1 or data.shape[1] != 1:
            raise ValueError(
                "upsampled slice images must be single-channel 2D images; "
                f"slice {index} has shape {data.shape}"
            )
        volume_slices.append(data[0, 0])

    volume_data = np.stack(volume_slices, axis=0)
    output = ismrmrd.Image.from_array(volume_data, transpose=False)
    header = slice_images[0].getHead()
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_index = 1
    header.slice = 0
    _set_header_sequence_field(
        header,
        "matrix_size",
        [int(value) for value in output.getHead().matrix_size],
    )

    fov = [float(value) for value in header.field_of_view]
    fov[2] = float(output_spacing * output_slice_count)
    _set_header_sequence_field(header, "field_of_view", fov)
    _set_header_sequence_field(
        header,
        "position",
        [float(value) for value in _header_position(slice_images[0].getHead())],
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )
    output.setHead(header)

    extra_meta = {
        "Keep_image_geometry": str(int(0)),
        "partition_count": str(int(1)),
        "slice_count": str(int(output_slice_count)),
        "NumberOfSlices": str(int(output_slice_count)),
        "ImagesInAcquisition": str(int(output_slice_count)),
    }
    extra_meta.update(_explicit_header_geometry_meta(header))
    _stamp_output_image(
        output,
        source_image,
        UPSAMPLED_SERIES_INDEX,
        0,
        (
            series_identity["series_name"]
            if series_identity
            else _derived_series_name(
                source_image,
                "upsampled",
                UPSAMPLED_SERIES_NAME,
            )
        ),
        "Image",
        UPSAMPLED_SERIES_NAME,
        ["PYTHON", "OPENRECON_UPSAMPLED_VOLUME"],
        extra_meta=extra_meta,
        patch_minihead=False,
        series_identity=series_identity,
    )
    return output


def _restamp_originals(images):
    if not images:
        return []

    outputs = []
    source_groups = _original_source_groups(images)
    if len(source_groups) > 1:
        logging.info(
            "sendoriginal split %d received image(s) into %d source volume group(s)",
            len(images),
            len(source_groups),
        )
    original_suffixes = _original_group_suffixes(source_groups)

    for group_index, group_images in enumerate(source_groups):
        series_index = _grouped_series_index(ORIGINAL_SERIES_INDEX, group_index)
        if not _is_original_series_index(series_index):
            raise ValueError(
                "sendoriginal produced too many source volume groups for the "
                f"reserved original series range; group {group_index} would use "
                f"image_series_index {series_index}"
            )
        _validate_original_source_geometry(group_images)
        series_identity = _build_output_series_identity(
            group_images[0],
            series_index,
            original_suffixes[group_index],
            ORIGINAL_SERIES_NAME,
        )

        for output_index, source_image in enumerate(group_images):
            output = ismrmrd.Image.from_array(
                np.asarray(source_image.data).copy(),
                transpose=False,
            )
            header = source_image.getHead()
            header.data_type = output.data_type
            output.setHead(header)
            _stamp_original_image(
                output,
                source_image,
                series_index,
                output_index,
                series_identity["series_name"],
                series_identity=series_identity,
            )
            outputs.append(output)

    return outputs


def _validate_original_source_geometry(group_images, label="sendoriginal"):
    source_slice_count = _source_geometry_slice_limit(group_images)
    if source_slice_count is None or len(group_images) <= source_slice_count:
        return
    raise ValueError(
        f"{label} source geometry advertises "
        f"{source_slice_count} slice/partition slot(s), but {len(group_images)} "
        "image(s) were received. Refusing to change this output into "
        "explicit-volume geometry because the scanner needs the source-native "
        "2D image stream."
    )


def _is_original_series_index(series_index):
    return (
        int(series_index) == ORIGINAL_SERIES_INDEX
        or EXTRA_ORIGINAL_SERIES_INDEX_START
        <= int(series_index)
        < EXTRA_SEGMENT_SERIES_INDEX_START
    )


def _is_segment_postprocessing_output(meta):
    return _meta_int(meta, SEGMENT_POSTPROCESSING_META_KEY) == 1


def _is_segment_source_geometry_output(meta):
    return _meta_int(meta, SEGMENT_SOURCE_GEOMETRY_META_KEY) == 1


def _source_image_groups(images):
    groups = []
    group_index_by_key = {}
    for image in images:
        key = _source_group_key(image)
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(groups)
            group_index_by_key[key] = group_index
            groups.append([])
        groups[group_index].append(image)
    return groups


def _original_source_groups(images):
    images = list(images)
    if len(images) < 2:
        return [images]

    source_groups = _source_image_groups(images)
    split_groups = []
    for group_images in source_groups:
        if len(group_images) < 2:
            split_groups.append(group_images)
            continue

        slice_axis = _infer_slice_axis([image.getHead() for image in group_images])
        projections = np.asarray(
            [_projected_position(image, slice_axis) for image in group_images],
            dtype=float,
        )
        duplicate_positions = _duplicate_projected_position_count(projections)
        if not duplicate_positions:
            split_groups.append(group_images)
            continue

        volume_groups = _source_volume_groups(group_images)
        if len(volume_groups) > 1:
            logging.warning(
                "sendoriginal source geometry has %d duplicate projected slice "
                "position(s) across %d image(s); split into %d source volume group(s)",
                duplicate_positions,
                len(group_images),
                len(volume_groups),
            )
            split_groups.extend(volume_groups)
            continue

        logging.warning(
            "sendoriginal source geometry has %d duplicate projected slice "
            "position(s), but no distinct source volume groups were detected",
            duplicate_positions,
        )
        split_groups.append(group_images)

    return split_groups


def _source_volume_groups(images):
    groups = []
    group_index_by_key = {}
    for image in images:
        key = _source_volume_group_key(image)
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(groups)
            group_index_by_key[key] = group_index
            groups.append([])
        groups[group_index].append(image)
    return groups


def _source_group_key(image):
    meta = _meta_from_image(image)
    minihead = _image_minihead(image)
    source_name = _source_series_name(image)
    series_uid = _meta_text(meta, "SeriesInstanceUID") or _minihead_string_value(
        minihead,
        "SeriesInstanceUID",
    )
    series_grouping = _meta_text(
        meta,
        "SeriesNumberRangeNameUID",
    ) or _minihead_string_value(
        minihead,
        "SeriesNumberRangeNameUID",
    )
    image_type = _meta_text(meta, "ImageType") or _minihead_string_value(
        minihead,
        "ImageType",
    )
    data_shape = tuple(int(value) for value in np.asarray(image.data).shape)
    return (
        series_uid,
        series_grouping,
        source_name,
        image_type,
        data_shape,
    )


def _source_volume_group_key(image):
    return (
        _source_group_key(image),
        tuple(
            _source_volume_field_value(image, field_name)
            for field_name, _label in SOURCE_VOLUME_GROUP_FIELDS
        ),
    )


def _original_group_suffixes(source_groups):
    if len(source_groups) == 1:
        return ["original"]

    source_key_counts = {}
    for group_images in source_groups:
        key = _source_group_key(group_images[0])
        source_key_counts[key] = source_key_counts.get(key, 0) + 1

    suffixes = []
    seen = set()
    for group_index, group_images in enumerate(source_groups):
        source_key = _source_group_key(group_images[0])
        if source_key_counts[source_key] == 1:
            suffix = "original"
        else:
            label = _source_volume_group_label(group_images[0]) or f"g{group_index}"
            suffix = f"original-{label}"
        suffix_key = (source_key, suffix)
        if suffix_key in seen:
            suffix = f"original-g{group_index}"
            suffix_key = (source_key, suffix)
        seen.add(suffix_key)
        suffixes.append(suffix)
    return suffixes


def _segment_group_suffixes(source_groups):
    if len(source_groups) == 1:
        return ["segment"]

    suffixes = []
    seen = set()
    for group_index, group_images in enumerate(source_groups):
        label = _source_volume_group_label(group_images[0]) or f"g{group_index}"
        suffix = f"segment-{label}"
        if suffix in seen:
            suffix = f"segment-g{group_index}"
        seen.add(suffix)
        suffixes.append(suffix)
    return suffixes


def _source_volume_group_label(image):
    parts = []
    for field_name, label in SOURCE_VOLUME_GROUP_FIELDS:
        value = _source_volume_field_value(image, field_name)
        if field_name == "contrast" or value != 0:
            parts.append(f"{label}{value}")
    return "-".join(parts)


def _source_volume_field_value(image, field_name):
    try:
        return int(getattr(image.getHead(), field_name))
    except Exception:
        return 0


def _grouped_series_index(base_series_index, group_index):
    if group_index == 0:
        return base_series_index
    return EXTRA_ORIGINAL_SERIES_INDEX_START + group_index - 1


def _segment_series_index(group_index):
    if group_index == 0:
        return SEGMENT_SERIES_INDEX
    return EXTRA_SEGMENT_SERIES_INDEX_START + group_index - 1


def _pack_explicit_volume(
    source_items,
    series_index,
    series_identity,
    data_role,
    image_type_token,
    history,
    extra_meta=None,
    label="explicit output",
):
    source_items = list(source_items)
    if not source_items:
        raise ValueError(f"{label} has no source images to pack")

    ordered_items, slice_axis = _ordered_volume_items(source_items, label)
    output_slice_count = len(ordered_items)
    output_spacing = _explicit_volume_output_spacing(
        [source_image for source_image, _data in ordered_items],
        slice_axis,
    )

    volume_slices = []
    for index, (_source_image, data) in enumerate(ordered_items):
        data = np.asarray(data)
        if data.ndim != 4 or data.shape[0] != 1 or data.shape[1] != 1:
            raise ValueError(
                f"{label} explicit volume requires single-channel 2D source "
                f"images; image {index} has shape {data.shape}"
            )
        volume_slices.append(data[0, 0])

    volume_data = np.stack(volume_slices, axis=0)
    output = ismrmrd.Image.from_array(volume_data, transpose=False)
    source_image = ordered_items[0][0]
    header = source_image.getHead()
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_index = 1
    header.slice = 0
    header.contrast = 0
    _set_header_sequence_field(
        header,
        "matrix_size",
        [int(value) for value in output.getHead().matrix_size],
    )

    fov = [float(value) for value in header.field_of_view]
    fov[2] = float(output_spacing * output_slice_count)
    _set_header_sequence_field(header, "field_of_view", fov)
    _set_header_sequence_field(
        header,
        "position",
        [float(value) for value in _header_position(source_image.getHead())],
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )
    output.setHead(header)

    explicit_meta = {
        "Keep_image_geometry": str(int(0)),
        "partition_count": str(int(1)),
        "slice_count": str(int(output_slice_count)),
        "NumberOfSlices": str(int(output_slice_count)),
        "ImagesInAcquisition": str(int(output_slice_count)),
    }
    explicit_meta.update(_explicit_header_geometry_meta(header))
    explicit_meta.update(extra_meta or {})
    _stamp_output_image(
        output,
        source_image,
        series_index,
        0,
        series_identity["series_name"],
        data_role,
        image_type_token,
        history,
        extra_meta=explicit_meta,
        patch_minihead=False,
        series_identity=series_identity,
    )
    logging.info(
        "Packed %d %s image(s) into one explicit volume with matrix_size=%s "
        "and field_of_view=%s",
        output_slice_count,
        label,
        _format_vector(output.getHead().matrix_size),
        _format_vector(output.getHead().field_of_view),
    )
    return output


def _stamp_output_image(
    output,
    source_image,
    series_index,
    output_index,
    series_name,
    data_role,
    image_type_token,
    history,
    extra_meta=None,
    patch_minihead=True,
    series_identity=None,
):
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = output_index + 1
    header.slice = output_index
    header.contrast = 0
    if output.data_type in (ismrmrd.DATATYPE_CXFLOAT, ismrmrd.DATATYPE_CXDOUBLE):
        header.image_type = ismrmrd.IMTYPE_COMPLEX
    else:
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    output.setHead(header)
    output.image_series_index = series_index
    output.attribute_string = _output_meta(
        source_image,
        series_index,
        output_index,
        series_name,
        data_role,
        image_type_token,
        history,
        extra_meta or {},
        patch_minihead,
        series_identity,
    ).serialize()
    return output


def _stamp_original_image(
    output,
    source_image,
    series_index,
    output_index,
    series_name,
    series_identity=None,
):
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = output_index + 1
    output.setHead(header)
    storage_fields = _original_storage_fields(output, output_index)
    output.image_series_index = series_index
    output.attribute_string = _original_passthrough_meta(
        source_image,
        series_index,
        output_index,
        series_name,
        series_identity,
        storage_fields,
    ).serialize()
    return output


def _stamp_segment_postprocessing_image(
    output,
    source_image,
    series_index,
    output_index,
    series_name,
    extra_meta,
    series_identity=None,
):
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = _source_geometry_header_image_index(source_image, output_index)
    if output.data_type in (ismrmrd.DATATYPE_CXFLOAT, ismrmrd.DATATYPE_CXDOUBLE):
        header.image_type = ismrmrd.IMTYPE_COMPLEX
    else:
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    output.setHead(header)
    storage_fields = _original_storage_fields(output, output_index)
    output.image_series_index = series_index
    output.attribute_string = _segment_postprocessing_meta(
        source_image,
        output,
        series_index,
        output_index,
        series_name,
        extra_meta,
        series_identity,
        storage_fields,
    ).serialize()
    return output


def _stamp_segment_source_geometry_image(
    output,
    source_image,
    series_index,
    output_index,
    series_name,
    extra_meta,
    series_identity=None,
):
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = _source_geometry_header_image_index(source_image, output_index)
    if output.data_type in (ismrmrd.DATATYPE_CXFLOAT, ismrmrd.DATATYPE_CXDOUBLE):
        header.image_type = ismrmrd.IMTYPE_COMPLEX
    else:
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    output.setHead(header)
    storage_fields = _original_storage_fields(output, output_index)
    output.image_series_index = series_index
    output.attribute_string = _segment_source_geometry_meta(
        source_image,
        output,
        series_index,
        output_index,
        series_name,
        extra_meta,
        series_identity,
        storage_fields,
    ).serialize()
    return output


def _source_geometry_header_image_index(source_image, output_index):
    source_image_index = int(source_image.getHead().image_index)
    if source_image_index >= 1:
        return source_image_index
    return output_index + 1


def _original_passthrough_meta(
    source_image,
    series_index,
    output_index,
    series_name,
    series_identity,
    storage_fields,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    if series_identity is None:
        series_identity = _build_output_series_identity_from_name(
            source_image,
            series_index,
            series_name,
        )
    series_name = series_identity["series_name"]
    series_uid = series_identity["series_uid"]
    series_grouping = series_identity["series_grouping"]
    sop_uid = _derived_instance_uid(
        source_image,
        series_index,
        series_name,
        output_index,
        series_uid,
    )

    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        if not _meta_text(meta, key):
            meta[key] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    _set_meta_scalar(meta, "Keep_image_geometry", 1)
    _ensure_original_storage_meta(meta, source_image, output_index, storage_fields)

    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    _strip_scanner_write_unsafe_meta(meta)
    if minihead:
        patched_minihead, changed = _patch_original_ice_minihead(
            minihead,
            series_grouping,
            series_uid,
            sop_uid,
            storage_fields,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _segment_source_geometry_meta(
    source_image,
    output_image,
    series_index,
    output_index,
    series_name,
    extra_meta,
    series_identity,
    storage_fields,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    if series_identity is None:
        series_identity = _build_output_series_identity_from_name(
            source_image,
            series_index,
            series_name,
        )
    series_name = series_identity["series_name"]
    series_uid = series_identity["series_uid"]
    series_grouping = series_identity["series_grouping"]
    sop_uid = _derived_instance_uid(
        source_image,
        series_index,
        series_name,
        output_index,
        series_uid,
    )
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    image_type = SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE
    image_type_value4 = SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4
    child_role = _segment_postprocessing_child_role(series_index)
    exam_data_role = _format_exam_data_role_sequential_number(child_role)

    meta["DataRole"] = "Segmentation"
    meta["ImageProcessingHistory"] = ["PYTHON", "OPENRECON_SEGMENT_SOURCE_GEOMETRY"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ExamDataRole"] = exam_data_role
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["ImageComment"] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    meta["ImageTypeValue4"] = image_type_value4
    meta["ComplexImageComponent"] = "MAGNITUDE"
    _set_meta_field(meta, "SequenceDescriptionAdditional", "openrecon")
    _set_meta_scalar(meta, "Keep_image_geometry", 1)
    _set_meta_scalar(meta, SEGMENT_SOURCE_GEOMETRY_META_KEY, 1)
    _set_meta_scalar(meta, SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY, child_role)
    _ensure_original_storage_meta(meta, output_image, output_index, storage_fields)
    for key, value in (extra_meta or {}).items():
        if value is not None:
            meta[key] = value
    _strip_scanner_write_unsafe_meta(meta)

    if minihead:
        patched_minihead, changed = _patch_segment_postprocessing_ice_minihead(
            minihead,
            series_name,
            series_grouping,
            series_uid,
            sop_uid,
            image_type,
            image_type_value4,
            exam_data_role,
            storage_fields,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _segment_postprocessing_meta(
    source_image,
    output_image,
    series_index,
    output_index,
    series_name,
    extra_meta,
    series_identity,
    storage_fields,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    if series_identity is None:
        series_identity = _build_output_series_identity_from_name(
            source_image,
            series_index,
            series_name,
        )
    series_name = series_identity["series_name"]
    series_uid = series_identity["series_uid"]
    series_grouping = series_identity["series_grouping"]
    sop_uid = _derived_instance_uid(
        source_image,
        series_index,
        series_name,
        output_index,
        series_uid,
    )
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    (
        image_type,
        dicom_image_type,
        image_type_value4_tokens,
    ) = _source_postprocessing_image_type_identity(meta, minihead)
    child_role = _segment_postprocessing_child_role(series_index)
    exam_data_role = _format_exam_data_role_sequential_number(child_role)

    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "OPENRECON_SEGMENT_POSTPROCESSING"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = dicom_image_type
    meta["ExamDataRole"] = exam_data_role
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    meta["ImageTypeValue4"] = image_type_value4_tokens
    meta["ComplexImageComponent"] = "MAGNITUDE"
    _set_meta_scalar(meta, "Keep_image_geometry", 1)
    _set_meta_scalar(meta, SEGMENT_POSTPROCESSING_META_KEY, 1)
    _set_meta_scalar(meta, SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY, child_role)
    _ensure_original_storage_meta(meta, output_image, output_index, storage_fields)
    for key, value in (extra_meta or {}).items():
        if value is not None:
            meta[key] = value
    _strip_scanner_write_unsafe_meta(meta)

    if minihead:
        patched_minihead, changed = _patch_segment_postprocessing_ice_minihead(
            minihead,
            series_name,
            series_grouping,
            series_uid,
            sop_uid,
            image_type,
            image_type_value4_tokens,
            exam_data_role,
            storage_fields,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _output_meta(
    source_image,
    series_index,
    output_index,
    series_name,
    data_role,
    image_type_token,
    history,
    extra_meta,
    patch_minihead,
    series_identity,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    _strip_scanner_write_unsafe_meta(meta)
    if series_identity is None:
        series_identity = _build_output_series_identity_from_name(
            source_image,
            series_index,
            series_name,
        )
    series_name = series_identity["series_name"]
    series_uid = series_identity["series_uid"]
    series_grouping = series_identity["series_grouping"]
    sop_uid = _derived_instance_uid(
        source_image,
        series_index,
        series_name,
        output_index,
        series_uid,
    )
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"
    meta["DataRole"] = data_role
    meta["ImageProcessingHistory"] = history
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["ImageComment"] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    _set_meta_field(meta, "SequenceDescriptionAdditional", "openrecon")
    _set_meta_scalar(meta, "Keep_image_geometry", 1)
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    _set_output_position_meta(meta, output_index)
    for key, value in extra_meta.items():
        if value is not None:
            meta[key] = value
    _strip_scanner_write_unsafe_meta(meta)

    if not patch_minihead:
        if "IceMiniHead" in meta:
            del meta["IceMiniHead"]
        return meta

    if minihead:
        patched_minihead, changed = _patch_ice_minihead(
            minihead,
            series_name,
            series_grouping,
            series_uid,
            sop_uid,
            image_type,
            image_type_token,
            output_index,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


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


def _set_output_position_meta(meta, output_index):
    for key in ("Actual3DImagePartNumber", "AnatomicalPartitionNo"):
        _set_meta_scalar(meta, key, SCANNER_PARTITION_INDEX)
    for key, value in (
        ("AnatomicalSliceNo", output_index),
        ("ChronSliceNo", output_index),
        ("NumberInSeries", output_index + 1),
        ("ProtocolSliceNumber", output_index),
        ("SliceNo", output_index),
        ("IsmrmrdSliceNo", output_index),
    ):
        _set_meta_scalar(meta, key, value)


def _original_storage_fields(source_image, output_index):
    header = source_image.getHead()
    header_slice = int(header.slice)
    header_image_index = max(int(header.image_index), 1)
    stream_index = header_image_index - 1
    slice_index = header_slice if header_slice >= 0 else output_index
    return {
        "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
        "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
        "AnatomicalSliceNo": slice_index,
        "ChronSliceNo": stream_index,
        "NumberInSeries": header_image_index,
        "ProtocolSliceNumber": slice_index,
        "SliceNo": slice_index,
        "IsmrmrdSliceNo": slice_index,
    }


def _ensure_original_storage_meta(
    meta,
    source_image,
    output_index,
    storage_fields=None,
):
    if storage_fields is None:
        storage_fields = _original_storage_fields(source_image, output_index)
    for key in storage_fields:
        _set_meta_scalar(meta, key, storage_fields[key])


def _meta_from_image(image):
    if not image.attribute_string:
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _derived_series_name(source_image, suffix, fallback_base=INVERT_SERIES_NAME):
    source_name = _source_series_name(source_image)
    if source_name:
        return f"{source_name}-{suffix}"
    return f"{fallback_base}-{suffix}"


def _build_output_series_identity(
    anchor_image,
    series_index,
    suffix,
    fallback_base,
):
    return _build_output_series_identity_from_name(
        anchor_image,
        series_index,
        _derived_series_name(anchor_image, suffix, fallback_base),
    )


def _build_output_series_identity_from_name(anchor_image, series_index, series_name):
    return {
        "series_name": series_name,
        "series_grouping": _derived_series_grouping(series_name, series_index),
        "series_uid": _derived_series_uid(anchor_image, series_index, series_name),
    }


def _derived_series_grouping(series_name, series_index):
    return f"{_sanitize_identity_text(series_name)}_{series_index}"


def _derived_series_uid(source_image, series_index, series_name):
    meta = _meta_from_image(source_image)
    base_uid = _meta_text(meta, "SeriesInstanceUID")
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    if not base_uid and minihead:
        base_uid = _minihead_string_value(minihead, "SeriesInstanceUID")

    seed = "|".join(
        [
            "openreconi2iexample",
            base_uid or _source_series_name(source_image) or "source",
            str(series_index),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_instance_uid(
    source_image,
    series_index,
    series_name,
    output_index,
    series_uid=None,
):
    meta = _meta_from_image(source_image)
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    source_instance_uid = _meta_text(meta, "SOPInstanceUID")
    if not source_instance_uid and minihead:
        source_instance_uid = _minihead_string_value(minihead, "SOPInstanceUID")
    if series_uid is None:
        series_uid = _derived_series_uid(source_image, series_index, series_name)

    seed = "|".join(
        [
            "openreconi2iexample-instance",
            series_uid,
            source_instance_uid or str(output_index),
            str(output_index),
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
    if minihead:
        for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
            value = _minihead_string_value(minihead, key)
            if value:
                return value

    return ""


def _meta_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    text = str(value or "").strip()
    return "" if text.upper() == "N/A" else text


def _meta_values(meta, key):
    value = meta.get(key)
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        value = [value]
    values = []
    for item in value:
        text = str(item or "").strip()
        if text and text.upper() != "N/A":
            values.append(text)
    return values


def _normalise_identity_tokens(value):
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple)):
        values = value
    else:
        values = [value]
    tokens = []
    for item in values:
        token = _sanitize_identity_text(item)
        if token:
            tokens.append(token)
    return tokens


def _source_postprocessing_image_type_identity(source_meta, minihead):
    image_type = (
        _meta_text(source_meta, "ImageType")
        or _minihead_string_value(minihead, "ImageType")
        or SEGMENT_POSTPROCESSING_IMAGE_TYPE
    )
    dicom_image_type = _meta_text(source_meta, "DicomImageType") or image_type
    image_type_value4_tokens = (
        _minihead_array_tokens(minihead, "ImageTypeValue4")
        or _meta_values(source_meta, "ImageTypeValue4")
        or [SEGMENT_POSTPROCESSING_IMAGE_TYPE_VALUE4]
    )
    return image_type, dicom_image_type, image_type_value4_tokens


def _segment_postprocessing_child_role(series_index):
    return int(series_index)


def _has_dixon_image_type_token(values):
    for value in values:
        for token in re.split(r"[^A-Za-z0-9_]+", str(value or "").upper()):
            if token in SEGMENT_POSTPROCESSING_DISALLOWED_DIXON_TOKENS:
                return True
    return False


def _has_magnitude_image_type_value3_token(values):
    for value in values:
        parts = str(value or "").upper().split("\\")
        if len(parts) >= 3 and parts[2] == "M":
            return True
    return False


def _format_exam_data_role_sequential_number(value):
    return (
        '<DataRole Version="DR2.0">\n'
        ' <Categories>\n'
        '  <Category Name="SequentialNumber">\n'
        f"   <CategoryEntry>{int(value)}</CategoryEntry>\n"
        "  </Category>\n"
        " </Categories>\n"
        "</DataRole>"
    )


def _exam_data_role_sequential_number(value):
    match = re.search(
        r"SequentialNumber.*?<CategoryEntry>\s*(-?\d+)\s*</CategoryEntry>",
        str(value or ""),
        flags=re.DOTALL,
    )
    return int(match.group(1)) if match else None


def _decode_ice_minihead(value):
    if not value:
        return ""
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _minihead_string_value(minihead, key):
    match = re.search(
        rf'<ParamString\."{re.escape(key)}">\s*{{\s*"([^"]*)"',
        minihead,
    )
    return match.group(1).strip() if match else ""


def _minihead_exam_data_role(minihead):
    match = re.search(
        r'<ParamString\."ExamDataRole">\s*\{\s*"(.*?</DataRole>)"\s*\}',
        minihead,
        flags=re.DOTALL,
    )
    if match:
        return match.group(1).replace('""', '"').strip()
    return _minihead_string_value(minihead, "ExamDataRole")


def _minihead_long_value(minihead, key):
    match = re.search(
        rf'<ParamLong\."{re.escape(key)}">\s*{{\s*(-?\d*)\s*}}',
        minihead,
    )
    value = match.group(1).strip() if match else ""
    return int(value) if value not in {"", "-"} else None


def _minihead_array_tokens(minihead, key):
    block_match = re.search(
        rf'<ParamArray\."{re.escape(key)}">\s*{{.*?^\s*}}',
        minihead,
        flags=re.DOTALL | re.MULTILINE,
    )
    if not block_match:
        return []
    return [
        token.strip()
        for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_match.group(0))
    ]


def _minihead_param_map_line_span(minihead_text, map_name):
    lines = minihead_text.splitlines(keepends=True)
    map_pattern = re.compile(rf'<ParamMap\."{re.escape(map_name)}">')

    for map_index, line in enumerate(lines):
        if not map_pattern.search(line):
            continue

        open_index = None
        for candidate_index in range(map_index + 1, len(lines)):
            stripped = lines[candidate_index].strip()
            if stripped == "{":
                open_index = candidate_index
                break
            if stripped.startswith('<ParamMap."'):
                break
        if open_index is None:
            continue

        depth = 1
        for close_index in range(open_index + 1, len(lines)):
            stripped = lines[close_index].strip()
            if stripped == "{":
                depth += 1
            elif stripped == "}":
                depth -= 1
                if depth == 0:
                    return lines, map_index, open_index, close_index

    return lines, None, None, None


def _minihead_param_map_text(minihead_text, map_name):
    lines, map_index, _open_index, close_index = _minihead_param_map_line_span(
        minihead_text,
        map_name,
    )
    if map_index is None:
        return ""
    return "".join(lines[map_index : close_index + 1])


def _minihead_param_map_has_long_param(minihead_text, map_name, name):
    map_text = _minihead_param_map_text(minihead_text, map_name)
    if not map_text:
        return False
    return bool(re.search(rf'<ParamLong\."{re.escape(name)}">', map_text))


def _patch_ice_minihead(
    minihead_text,
    series_name,
    series_grouping,
    series_uid,
    sop_uid,
    image_type,
    image_type_token,
    output_index,
):
    current_text = minihead_text
    current_text, changed = _remove_minihead_string_param(
        current_text,
        "ImageTypeValue3",
    )
    for name, value in (
        ("SeriesDescription", series_name),
        ("SequenceDescription", series_name),
        ("ProtocolName", series_name),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
        ("ImageType", image_type),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change
    current_text, did_change = _replace_or_append_minihead_array_token(
        current_text,
        "ImageTypeValue4",
        image_type_token,
    )
    changed = changed or did_change
    for name in ("Actual3DImagePartNumber", "AnatomicalPartitionNo"):
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            name,
            SCANNER_PARTITION_INDEX,
        )
        changed = changed or did_change
    for name, value in (
        ("AnatomicalSliceNo", output_index),
        ("ChronSliceNo", output_index),
        ("NumberInSeries", output_index + 1),
        ("ProtocolSliceNumber", output_index),
        ("SliceNo", output_index),
    ):
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change
    return current_text, changed


def _patch_original_ice_minihead(
    minihead_text,
    series_grouping,
    series_uid,
    sop_uid,
    storage_fields,
):
    current_text = minihead_text
    changed = False
    current_text, did_change = _strip_scanner_write_unsafe_minihead(current_text)
    changed = changed or did_change
    for name, value in (
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change
    current_text, did_change = _patch_minihead_storage_fields(
        current_text,
        storage_fields,
    )
    changed = changed or did_change
    return current_text, changed


def _patch_segment_postprocessing_ice_minihead(
    minihead_text,
    series_name,
    series_grouping,
    series_uid,
    sop_uid,
    image_type,
    image_type_value4_tokens,
    exam_data_role,
    storage_fields,
):
    current_text = minihead_text
    changed = False
    for name, value in (
        ("SeriesDescription", series_name),
        ("SequenceDescription", series_name),
        ("ProtocolName", series_name),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
        ("ImageType", image_type),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change
    current_text, did_change = _replace_or_append_minihead_array_tokens(
        current_text,
        "ImageTypeValue4",
        image_type_value4_tokens,
    )
    changed = changed or did_change
    current_text, did_change = _replace_or_insert_minihead_exam_data_role(
        current_text,
        exam_data_role,
    )
    changed = changed or did_change
    current_text, did_change = _strip_scanner_write_unsafe_minihead(current_text)
    changed = changed or did_change
    current_text, did_change = _patch_minihead_storage_fields(
        current_text,
        storage_fields,
    )
    changed = changed or did_change
    return current_text, changed


def _patch_minihead_storage_fields(
    minihead_text,
    storage_fields,
    preserve_existing=False,
):
    current_text = minihead_text
    changed = False
    for minihead_name, map_name, source_field in MINIHEAD_STORAGE_FIELD_SPECS:
        value = storage_fields.get(source_field)
        if preserve_existing:
            current_text, did_change = _ensure_minihead_long_param(
                current_text,
                minihead_name,
                value,
                map_name=map_name,
            )
        else:
            current_text, did_change = _replace_or_insert_minihead_long_param(
                current_text,
                minihead_name,
                value,
                map_name=map_name,
            )
        changed = changed or did_change
    return current_text, changed


def _remove_minihead_string_param(minihead_text, name):
    pattern = re.compile(
        rf'^\s*<ParamString\."{re.escape(name)}">\s*\{{\s*"[^"]*"\s*\}}\s*\n?',
        flags=re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _remove_minihead_array_param(minihead_text, name):
    pattern = re.compile(
        rf'^\s*<ParamArray\."{re.escape(name)}">\s*\{{.*?^\s*\}}\s*\n?',
        flags=re.DOTALL | re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _strip_scanner_write_unsafe_minihead(minihead_text):
    current_text = minihead_text
    changed = False
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        current_text, did_change = _remove_minihead_string_param(current_text, key)
        changed = changed or did_change
        current_text, did_change = _remove_minihead_array_param(current_text, key)
        changed = changed or did_change
    return current_text, changed


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_identity_text(value)
    if not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*")([^"]*)("\s*\}})'
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        if all(match.group(2) == value for match in matches):
            return minihead_text, False
        return (
            pattern.sub(
                lambda match: f"{match.group(1)}{value}{match.group(3)}",
                minihead_text,
            ),
            True,
        )

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


def _minihead_string_literal(value):
    return str(value).replace('"', '""')


def _replace_or_insert_minihead_exam_data_role(minihead_text, exam_data_role):
    if not exam_data_role:
        return minihead_text, False

    literal = _minihead_string_literal(exam_data_role)
    pattern = re.compile(
        r'(<ParamString\."ExamDataRole">\s*\{\s*")(.*?</DataRole>)("\s*\})',
        flags=re.DOTALL,
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        if all(match.group(2).replace('""', '"') == exam_data_role for match in matches):
            return minihead_text, False
        return (
            pattern.sub(
                lambda match: f"{match.group(1)}{literal}{match.group(3)}",
                minihead_text,
            ),
            True,
        )

    return _insert_minihead_string_param_in_map(
        minihead_text,
        "DICOM",
        "ExamDataRole",
        exam_data_role,
    )


def _insert_minihead_string_param_in_map(minihead_text, map_name, name, value):
    literal = _minihead_string_literal(value)
    lines, _map_index, open_index, close_index = _minihead_param_map_line_span(
        minihead_text,
        map_name,
    )
    if close_index is None:
        appended_param = f'\n<ParamString."{name}">\t{{ "{literal}" }}\n'
        return minihead_text.rstrip() + appended_param, True

    indent = ""
    for line in lines[open_index + 1 : close_index]:
        match = re.match(r'(\s*)<Param(?:Long|String|Array)\.', line)
        if match:
            indent = match.group(1)
            break
    if not indent:
        indent_match = re.match(r"(\s*)", lines[open_index])
        indent = indent_match.group(1) if indent_match else ""

    line_ending = "\r\n" if "\r\n" in minihead_text else "\n"
    param_line = f'{indent}<ParamString."{name}">\t{{ "{literal}" }}{line_ending}'
    lines.insert(close_index, param_line)
    return "".join(lines), True


def _replace_existing_minihead_long_param(minihead_text, name, value):
    if value is None:
        return minihead_text, False, False

    value = int(value)
    pattern = re.compile(
        rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})'
    )
    matches = list(pattern.finditer(minihead_text))
    if not matches:
        return minihead_text, False, False
    if all((match.group(2) or "").strip() == str(value) for match in matches):
        return minihead_text, False, True
    return (
        pattern.sub(
            lambda match: f"{match.group(1)}{value}{match.group(3)}",
            minihead_text,
        ),
        True,
        True,
    )


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    if value is None:
        return minihead_text, False

    value = int(value)
    current_text, changed, found = _replace_existing_minihead_long_param(
        minihead_text,
        name,
        value,
    )
    if found:
        return current_text, changed

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_or_insert_minihead_long_param(
    minihead_text,
    name,
    value,
    map_name=None,
):
    if value is None:
        return minihead_text, False

    value = int(value)
    current_text, changed, found = _replace_existing_minihead_long_param(
        minihead_text,
        name,
        value,
    )
    if not map_name:
        if found:
            return current_text, changed
        return _replace_or_append_minihead_long_param(current_text, name, value)

    if _minihead_param_map_has_long_param(current_text, map_name, name):
        return current_text, changed

    current_text, did_change = _insert_minihead_long_param_in_map(
        current_text,
        map_name,
        name,
        value,
    )
    return current_text, changed or did_change


def _insert_minihead_long_param_in_map(minihead_text, map_name, name, value):
    lines, _map_index, open_index, close_index = _minihead_param_map_line_span(
        minihead_text,
        map_name,
    )
    if close_index is None:
        return _replace_or_append_minihead_long_param(minihead_text, name, value)

    indent = ""
    for line in lines[open_index + 1 : close_index]:
        match = re.match(r'(\s*)<Param(?:Long|String|Array)\.', line)
        if match:
            indent = match.group(1)
            break
    if not indent:
        indent_match = re.match(r"(\s*)", lines[open_index])
        indent = indent_match.group(1) if indent_match else ""

    line_ending = "\r\n" if "\r\n" in minihead_text else "\n"
    param_line = f'{indent}<ParamLong."{name}">{{ {int(value)} }}{line_ending}'
    lines.insert(close_index, param_line)
    return "".join(lines), True


def _ensure_minihead_long_param(
    minihead_text,
    name,
    value,
    minimum=None,
    map_name=None,
):
    if value is None:
        return minihead_text, False
    current_value = _minihead_long_value(minihead_text, name)
    if current_value is not None and (minimum is None or current_value >= minimum):
        if not map_name or _minihead_param_map_has_long_param(
            minihead_text,
            map_name,
            name,
        ):
            return minihead_text, False
    return _replace_or_insert_minihead_long_param(
        minihead_text,
        name,
        value,
        map_name=map_name,
    )


def _replace_or_append_minihead_array_token(minihead_text, name, target_token):
    return _replace_or_append_minihead_array_tokens(minihead_text, name, [target_token])


def _replace_or_append_minihead_array_tokens(minihead_text, name, target_tokens):
    target_tokens = _normalise_identity_tokens(target_tokens)
    if not target_tokens:
        return minihead_text, False

    line_ending = "\r\n" if "\r\n" in minihead_text else "\n"
    block_pattern = re.compile(
        rf'(<ParamArray\."{re.escape(name)}">\s*\{{)(.*?)(^\s*\}})',
        flags=re.DOTALL | re.MULTILINE,
    )
    block_match = block_pattern.search(minihead_text)
    if not block_match:
        token_lines = "".join(
            f'{line_ending}\t{{ "{token}" }}' for token in target_tokens
        )
        appended_param = (
            f'{line_ending}<ParamArray."{name}">\t{{'
            f"{token_lines}{line_ending}}}{line_ending}"
        )
        return minihead_text.rstrip() + appended_param, True

    block_text = block_match.group(0)
    tokens = _minihead_array_tokens(block_text, name)
    if tokens == target_tokens:
        return minihead_text, False

    token_pattern = re.compile(r'\{\s*"[^"]*"\s*\}')
    token_matches = list(token_pattern.finditer(block_text))
    token_lines = "".join(
        f'{line_ending}\t{{ "{token}" }}' for token in target_tokens
    )
    if not token_matches:
        stripped_block = block_text.rstrip()
        close_index = stripped_block.rfind("}")
        if close_index >= 0:
            replacement_block = (
                stripped_block[:close_index]
                + token_lines
                + line_ending
                + stripped_block[close_index:]
            )
        else:
            replacement_block = stripped_block + token_lines
    else:
        first_token = token_matches[0]
        last_token = token_matches[-1]
        replacement_block = (
            block_text[:first_token.start()]
            + token_lines.lstrip(line_ending)
            + block_text[last_token.end():]
        )
    return (
        minihead_text[:block_match.start()]
        + replacement_block
        + minihead_text[block_match.end():],
        True,
    )


def _sanitize_identity_text(value):
    return (
        str(value or "")
        .strip()
        .replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _set_meta_field(meta, key, value):
    meta[key] = value


def _set_meta_scalar(meta, key, value):
    meta[key] = str(int(value))


def _validate_output_images(output_images, input_images):
    errors = []
    seen_image_keys = {}
    seen_storage_keys = {}
    seen_minihead_storage_keys = {}
    seen_sop_uids = {}
    source_image_count = len(input_images)
    source_slice_count = _source_geometry_slice_limit(input_images) or source_image_count
    input_identity = _identity_values(input_images)
    input_has_minihead = any(_image_minihead(image) for image in input_images)
    series_identity = {}
    series_by_uid = {}

    for index, image in enumerate(output_images):
        header = image.getHead()
        image_key = (
            int(image.image_series_index),
            int(header.slice),
            int(header.image_index),
        )
        if image_key in seen_image_keys:
            errors.append(
                f"image {index} duplicates output image key {image_key} "
                f"from image {seen_image_keys[image_key]}"
            )
        else:
            seen_image_keys[image_key] = index

        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        keep_image_geometry = _meta_int(meta, "Keep_image_geometry")
        is_original_output = _is_original_series_index(int(image.image_series_index))
        if (
            input_has_minihead
            and not minihead
            and keep_image_geometry != 0
            and not is_original_output
        ):
            errors.append(f"image {index} is missing IceMiniHead")

        identity = {
            "series_description": _meta_text(meta, "SeriesDescription"),
            "sequence_description": _meta_text(meta, "SequenceDescription"),
            "protocol_name": _meta_text(meta, "ProtocolName"),
            "series_grouping": _meta_text(meta, "SeriesNumberRangeNameUID"),
            "series_uid": _meta_text(meta, "SeriesInstanceUID"),
            "sop_uid": _meta_text(meta, "SOPInstanceUID"),
            "minihead_sequence_description": _minihead_string_value(
                minihead, "SequenceDescription"
            ),
            "minihead_protocol_name": _minihead_string_value(minihead, "ProtocolName"),
            "minihead_series_grouping": _minihead_string_value(
                minihead, "SeriesNumberRangeNameUID"
            ),
            "minihead_series_uid": _minihead_string_value(minihead, "SeriesInstanceUID"),
            "minihead_sop_uid": _minihead_string_value(minihead, "SOPInstanceUID"),
        }
        _validate_identity_fields(
            index,
            identity,
            input_identity,
            errors,
            allow_reused_descriptors=is_original_output,
        )
        _validate_storage_fields(
            index,
            image,
            meta,
            minihead,
            input_identity,
            seen_storage_keys,
            seen_minihead_storage_keys,
            seen_sop_uids,
            _series_slice_limit(
                int(image.image_series_index),
                source_slice_count,
                source_image_count,
            ),
            errors,
        )

        series_key = int(image.image_series_index)
        comparable_identity = (
            identity["sequence_description"],
            identity["protocol_name"],
            identity["series_grouping"],
            identity["series_uid"],
        )
        previous_identity = series_identity.setdefault(series_key, comparable_identity)
        if previous_identity != comparable_identity:
            errors.append(
                f"image {index} in image_series_index {series_key} has "
                f"inconsistent identity values: {comparable_identity} != "
                f"{previous_identity}"
            )
        series_uid = identity["series_uid"]
        if series_uid:
            previous_series = series_by_uid.setdefault(series_uid, series_key)
            if previous_series != series_key:
                errors.append(
                    f"SeriesInstanceUID {series_uid} is shared by "
                    f"image_series_index {previous_series} and {series_key}"
                )

    if errors:
        raise ValueError(
            "Invalid openreconi2iexample output series contract before send: "
            + "; ".join(errors)
        )


def _validate_identity_fields(
    index,
    identity,
    input_identity,
    errors,
    allow_reused_descriptors=False,
):
    required = (
        "series_description",
        "sequence_description",
        "protocol_name",
        "series_grouping",
        "series_uid",
        "sop_uid",
    )
    for key in required:
        if not identity[key]:
            errors.append(f"image {index} is missing Meta {key}")

    for meta_key, minihead_key in (
        ("sequence_description", "minihead_sequence_description"),
        ("protocol_name", "minihead_protocol_name"),
        ("series_grouping", "minihead_series_grouping"),
        ("series_uid", "minihead_series_uid"),
        ("sop_uid", "minihead_sop_uid"),
    ):
        meta_value = identity[meta_key]
        minihead_value = identity[minihead_key]
        if minihead_value and meta_value != minihead_value:
            errors.append(
                f"image {index} has Meta/IceMiniHead {meta_key} mismatch: "
                f"{meta_value} != {minihead_value}"
            )

    for key in (
        "sequence_description",
        "protocol_name",
        "series_grouping",
        "series_uid",
        "minihead_sequence_description",
        "minihead_protocol_name",
        "minihead_series_grouping",
        "minihead_series_uid",
        "sop_uid",
        "minihead_sop_uid",
    ):
        if allow_reused_descriptors and key in (
            "sequence_description",
            "protocol_name",
            "minihead_sequence_description",
            "minihead_protocol_name",
        ):
            continue
        value = identity[key]
        if value and value in input_identity:
            errors.append(f"image {index} reuses input identity {key}={value}")


def _validate_source_like_minihead_storage_maps(index, minihead, context, errors):
    if not minihead:
        return

    for field, map_name, _source_field in MINIHEAD_STORAGE_FIELD_SPECS:
        if not _minihead_param_map_has_long_param(minihead, map_name, field):
            errors.append(
                f"image {index} is missing {context} IceMiniHead "
                f'{map_name} ParamLong "{field}"'
            )


def _validate_storage_fields(
    index,
    image,
    meta,
    minihead,
    input_identity,
    seen_storage_keys,
    seen_minihead_storage_keys,
    seen_sop_uids,
    series_slice_limit,
    errors,
):
    header = image.getHead()
    header_slice = int(header.slice)
    header_image_index = int(header.image_index)
    header_matrix_z = int(header.matrix_size[2])
    keep_image_geometry = _meta_int(meta, "Keep_image_geometry")
    image_type_value4 = _meta_text(meta, "ImageTypeValue4")
    is_original_output = _is_original_series_index(int(image.image_series_index))
    is_segment_source_geometry_output = _is_segment_source_geometry_output(meta)
    is_segment_postprocessing_output = _is_segment_postprocessing_output(meta)
    is_source_like_output = (
        is_original_output
        or is_segment_source_geometry_output
        or is_segment_postprocessing_output
    )

    if header_image_index < 1:
        errors.append(
            f"image {index} has image_index {header_image_index}, expected >= 1"
        )

    if (
        series_slice_limit
        and not 0 <= header_slice < series_slice_limit
    ):
        errors.append(
            f"image {index} has slice {header_slice} outside source image "
            f"bounds [0..{series_slice_limit})"
        )

    if is_segment_source_geometry_output:
        image_type = _meta_text(meta, "ImageType")
        dicom_image_type = _meta_text(meta, "DicomImageType")
        minihead_image_type = _minihead_string_value(minihead, "ImageType")
        minihead_image_type_value4 = _minihead_array_tokens(
            minihead,
            "ImageTypeValue4",
        )
        expected_child_role = _segment_postprocessing_child_role(
            int(image.image_series_index)
        )
        child_role_value = _meta_int(meta, SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY)
        meta_exam_data_role = _exam_data_role_sequential_number(
            _meta_text(meta, "ExamDataRole")
        )
        minihead_exam_data_role = _exam_data_role_sequential_number(
            _minihead_exam_data_role(minihead)
        )
        if _meta_text(meta, "DataRole") != "Segmentation":
            errors.append(
                f"image {index} has segment source-geometry DataRole="
                f"{_meta_text(meta, 'DataRole')}, expected Segmentation"
            )
        if image_type != SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE:
            errors.append(
                f"image {index} has segment source-geometry ImageType="
                f"{image_type}, expected {SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE}"
            )
        if dicom_image_type != SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE:
            errors.append(
                f"image {index} has segment source-geometry DicomImageType="
                f"{dicom_image_type}, expected {SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE}"
            )
        if minihead and minihead_image_type and minihead_image_type != image_type:
            errors.append(
                f"image {index} has segment source-geometry IceMiniHead ImageType="
                f"{minihead_image_type}, expected {image_type}"
            )
        if image_type_value4 != SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4:
            errors.append(
                f"image {index} has segment source-geometry ImageTypeValue4="
                f"{image_type_value4}, expected "
                f"{SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4}"
            )
        if minihead and minihead_image_type_value4 != [
            SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4
        ]:
            errors.append(
                f"image {index} has segment source-geometry IceMiniHead "
                f"ImageTypeValue4={minihead_image_type_value4}, expected "
                f"{[SEGMENT_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4]}"
            )
        if _has_dixon_image_type_token(
            [image_type, dicom_image_type, image_type_value4]
            + minihead_image_type_value4
        ):
            errors.append(
                f"image {index} has Dixon image-type identity on "
                "segment source-geometry output"
            )
        if _has_magnitude_image_type_value3_token(
            [image_type, dicom_image_type, minihead_image_type]
        ):
            errors.append(
                f"image {index} has M image-type value 3 on "
                "segment source-geometry output"
            )
        if child_role_value != expected_child_role:
            errors.append(
                f"image {index} has segment source-geometry child role "
                f"{child_role_value}, expected {expected_child_role}"
            )
        if meta_exam_data_role != expected_child_role:
            errors.append(
                f"image {index} has segment source-geometry Meta ExamDataRole "
                f"SequentialNumber={meta_exam_data_role}, expected "
                f"{expected_child_role}"
            )
        if minihead and minihead_exam_data_role != expected_child_role:
            errors.append(
                f"image {index} has segment source-geometry IceMiniHead ExamDataRole "
                f"SequentialNumber={minihead_exam_data_role}, expected "
                f"{expected_child_role}"
            )
        expected_position_fields = {
            "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
            "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
            "AnatomicalSliceNo": header_slice,
            "ChronSliceNo": header_image_index - 1,
            "NumberInSeries": header_image_index,
            "ProtocolSliceNumber": header_slice,
            "SliceNo": header_slice,
            "IsmrmrdSliceNo": header_slice,
        }
        for field in (
            "Actual3DImagePartNumber",
            "AnatomicalPartitionNo",
            "AnatomicalSliceNo",
            "ChronSliceNo",
            "NumberInSeries",
            "ProtocolSliceNumber",
            "SliceNo",
            "IsmrmrdSliceNo",
        ):
            expected = expected_position_fields[field]
            meta_value = _meta_int(meta, field)
            if meta_value is None:
                errors.append(
                    f"image {index} is missing segment source-geometry Meta {field}"
                )
            elif meta_value != expected:
                errors.append(
                    f"image {index} has segment source-geometry Meta {field}="
                    f"{meta_value}, expected {expected}"
                )
            minihead_value = _minihead_long_value(minihead, field)
            if minihead and minihead_value is None:
                errors.append(
                    f"image {index} is missing segment source-geometry IceMiniHead "
                    f"{field}"
                )
            elif minihead_value is not None and minihead_value != expected:
                errors.append(
                    f"image {index} has segment source-geometry IceMiniHead "
                    f"{field}={minihead_value}, expected {expected}"
                )
        actual_part_value = _minihead_long_value(minihead, "Actual3DImaPartNumber")
        if minihead and actual_part_value is None:
            errors.append(
                f"image {index} is missing segment source-geometry IceMiniHead "
                "Actual3DImaPartNumber"
            )
        elif (
            actual_part_value is not None
            and actual_part_value != SCANNER_PARTITION_INDEX
        ):
            errors.append(
                f"image {index} has segment source-geometry IceMiniHead "
                f"Actual3DImaPartNumber={actual_part_value}, "
                f"expected {SCANNER_PARTITION_INDEX}"
            )
        _validate_source_like_minihead_storage_maps(
            index,
            minihead,
            "segment source-geometry",
            errors,
        )

    elif is_segment_postprocessing_output:
        image_type = _meta_text(meta, "ImageType")
        dicom_image_type = _meta_text(meta, "DicomImageType")
        image_type_value4_values = _meta_values(meta, "ImageTypeValue4")
        minihead_image_type = _minihead_string_value(minihead, "ImageType")
        minihead_image_type_value4 = _minihead_array_tokens(
            minihead,
            "ImageTypeValue4",
        )
        expected_child_role = _segment_postprocessing_child_role(
            int(image.image_series_index)
        )
        child_role_value = _meta_int(meta, SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY)
        meta_exam_data_role = _exam_data_role_sequential_number(
            _meta_text(meta, "ExamDataRole")
        )
        minihead_exam_data_role = _exam_data_role_sequential_number(
            _minihead_exam_data_role(minihead)
        )
        if _meta_text(meta, "DataRole") != "Image":
            errors.append(
                f"image {index} has segment postprocessing DataRole="
                f"{_meta_text(meta, 'DataRole')}, expected Image"
            )
        if not image_type:
            errors.append(
                f"image {index} is missing segment postprocessing ImageType"
            )
        if not dicom_image_type:
            errors.append(
                f"image {index} is missing segment postprocessing DicomImageType"
            )
        if minihead and minihead_image_type and minihead_image_type != image_type:
            errors.append(
                f"image {index} has segment postprocessing IceMiniHead ImageType="
                f"{minihead_image_type}, expected {image_type}"
            )
        if not image_type_value4_values:
            errors.append(
                f"image {index} is missing segment postprocessing ImageTypeValue4"
            )
        if minihead and not minihead_image_type_value4:
            errors.append(
                f"image {index} is missing segment postprocessing IceMiniHead "
                "ImageTypeValue4"
            )
        if (
            minihead
            and minihead_image_type_value4
            and image_type_value4_values != minihead_image_type_value4
        ):
            errors.append(
                f"image {index} has segment postprocessing Meta ImageTypeValue4="
                f"{image_type_value4_values}, expected IceMiniHead "
                f"{minihead_image_type_value4}"
            )
        if child_role_value != expected_child_role:
            errors.append(
                f"image {index} has segment postprocessing child role "
                f"{child_role_value}, expected {expected_child_role}"
            )
        if meta_exam_data_role != expected_child_role:
            errors.append(
                f"image {index} has segment postprocessing Meta ExamDataRole "
                f"SequentialNumber={meta_exam_data_role}, expected "
                f"{expected_child_role}"
            )
        if minihead and minihead_exam_data_role != expected_child_role:
            errors.append(
                f"image {index} has segment postprocessing IceMiniHead ExamDataRole "
                f"SequentialNumber={minihead_exam_data_role}, expected "
                f"{expected_child_role}"
            )
        expected_position_fields = {
            "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
            "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
            "AnatomicalSliceNo": header_slice,
            "ChronSliceNo": header_image_index - 1,
            "NumberInSeries": header_image_index,
            "ProtocolSliceNumber": header_slice,
            "SliceNo": header_slice,
            "IsmrmrdSliceNo": header_slice,
        }
        for field in (
            "Actual3DImagePartNumber",
            "AnatomicalPartitionNo",
            "AnatomicalSliceNo",
            "ChronSliceNo",
            "NumberInSeries",
            "ProtocolSliceNumber",
            "SliceNo",
            "IsmrmrdSliceNo",
        ):
            expected = expected_position_fields[field]
            meta_value = _meta_int(meta, field)
            if meta_value is None:
                errors.append(
                    f"image {index} is missing segment postprocessing Meta {field}"
                )
            elif meta_value != expected:
                errors.append(
                    f"image {index} has segment postprocessing Meta {field}="
                    f"{meta_value}, expected {expected}"
                )
            minihead_value = _minihead_long_value(minihead, field)
            if minihead and minihead_value is None:
                errors.append(
                    f"image {index} is missing segment postprocessing IceMiniHead "
                    f"{field}"
                )
            elif minihead_value is not None and minihead_value != expected:
                errors.append(
                    f"image {index} has segment postprocessing IceMiniHead "
                    f"{field}={minihead_value}, expected {expected}"
                )
        actual_part_value = _minihead_long_value(minihead, "Actual3DImaPartNumber")
        if minihead and actual_part_value is None:
            errors.append(
                f"image {index} is missing segment postprocessing IceMiniHead "
                "Actual3DImaPartNumber"
            )
        elif (
            actual_part_value is not None
            and actual_part_value != SCANNER_PARTITION_INDEX
        ):
            errors.append(
                f"image {index} has segment postprocessing IceMiniHead "
                f"Actual3DImaPartNumber={actual_part_value}, "
                f"expected {SCANNER_PARTITION_INDEX}"
            )
        _validate_source_like_minihead_storage_maps(
            index,
            minihead,
            "segment postprocessing",
            errors,
        )

    elif not is_original_output:
        expected_position_fields = {
            "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
            "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
            "AnatomicalSliceNo": header_slice,
            "ChronSliceNo": header_slice,
            "NumberInSeries": int(header.image_index),
            "ProtocolSliceNumber": header_slice,
            "SliceNo": header_slice,
            "IsmrmrdSliceNo": header_slice,
        }

        for field, expected in expected_position_fields.items():
            meta_value = _meta_int(meta, field)
            if meta_value is None:
                errors.append(f"image {index} is missing Meta {field}")
            elif meta_value != expected:
                errors.append(
                    f"image {index} has Meta {field}={meta_value}, expected {expected}"
                )
            if field == "IsmrmrdSliceNo":
                continue
            minihead_value = _minihead_long_value(minihead, field)
            if minihead and minihead_value is None:
                errors.append(f"image {index} is missing IceMiniHead {field}")
            elif minihead_value is not None and minihead_value != expected:
                errors.append(
                    f"image {index} has IceMiniHead {field}={minihead_value}, "
                    f"expected {expected}"
                )

    if keep_image_geometry is None:
        errors.append(f"image {index} is missing Meta Keep_image_geometry")
    elif is_original_output and keep_image_geometry != 1:
        errors.append(
            f"image {index} is an original pass-through output with "
            f"Keep_image_geometry={keep_image_geometry}, expected 1"
        )
    elif is_segment_source_geometry_output and keep_image_geometry != 1:
        errors.append(
            f"image {index} is a segment source-geometry output with "
            f"Keep_image_geometry={keep_image_geometry}, expected 1"
        )
    elif is_segment_postprocessing_output and keep_image_geometry != 1:
        errors.append(
            f"image {index} is a segment postprocessing output with "
            f"Keep_image_geometry={keep_image_geometry}, expected 1"
        )
    elif is_original_output:
        expected_original_fields = {
            "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
            "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
            "AnatomicalSliceNo": header_slice,
            "ChronSliceNo": header_image_index - 1,
            "NumberInSeries": header_image_index,
            "ProtocolSliceNumber": header_slice,
            "SliceNo": header_slice,
            "IsmrmrdSliceNo": header_slice,
        }
        for field, expected in expected_original_fields.items():
            meta_value = _meta_int(meta, field)
            if meta_value is None:
                errors.append(f"image {index} is missing original Meta {field}")
            elif meta_value != expected:
                errors.append(
                    f"image {index} has original Meta {field}={meta_value}, "
                    f"expected {expected}"
                )
            minihead_value = _minihead_long_value(minihead, field)
            if minihead and minihead_value is None:
                errors.append(f"image {index} is missing original IceMiniHead {field}")
            elif minihead_value is not None and minihead_value != expected:
                errors.append(
                    f"image {index} has original IceMiniHead {field}="
                    f"{minihead_value}, expected {expected}"
                )
        actual_part_value = _minihead_long_value(minihead, "Actual3DImaPartNumber")
        if minihead and actual_part_value is None:
            errors.append(
                f"image {index} is missing original IceMiniHead "
                "Actual3DImaPartNumber"
            )
        elif (
            actual_part_value is not None
            and actual_part_value != SCANNER_PARTITION_INDEX
        ):
            errors.append(
                f"image {index} has original IceMiniHead "
                f"Actual3DImaPartNumber={actual_part_value}, "
                f"expected {SCANNER_PARTITION_INDEX}"
            )
        _validate_source_like_minihead_storage_maps(
            index,
            minihead,
            "original",
            errors,
        )
    elif keep_image_geometry == 0:
        partition_count = _meta_int(meta, "partition_count")
        if partition_count is None:
            errors.append(f"image {index} is missing Meta partition_count")
        elif partition_count < 1:
            errors.append(
                f"image {index} has Meta partition_count={partition_count}, "
                "expected at least 1"
            )

        slice_count = _meta_int(meta, "slice_count")
        if slice_count is None:
            errors.append(f"image {index} is missing Meta slice_count")
        elif header_slice >= slice_count:
            errors.append(
                f"image {index} has slice {header_slice} outside explicit "
                f"slice_count {slice_count}"
            )
        elif header_matrix_z > 1 and header_matrix_z != slice_count:
            errors.append(
                f"image {index} has matrix_size[2]={header_matrix_z}, "
                f"expected explicit slice_count {slice_count}"
            )

        number_of_slices = _meta_int(meta, "NumberOfSlices")
        if header_matrix_z > 1 and number_of_slices != slice_count:
            errors.append(
                f"image {index} has Meta NumberOfSlices={number_of_slices}, "
                f"expected {slice_count}"
            )

    sop_uid = _meta_text(meta, "SOPInstanceUID")
    minihead_sop_uid = _minihead_string_value(minihead, "SOPInstanceUID")
    if sop_uid:
        previous_index = seen_sop_uids.setdefault(sop_uid, index)
        if previous_index != index:
            errors.append(
                f"SOPInstanceUID {sop_uid} is shared by output images "
                f"{previous_index} and {index}"
            )
    if minihead_sop_uid:
        previous_index = seen_sop_uids.setdefault(minihead_sop_uid, index)
        if previous_index != index:
            errors.append(
                f"IceMiniHead SOPInstanceUID {minihead_sop_uid} is shared by "
                f"output images {previous_index} and {index}"
            )
    if not is_source_like_output:
        minihead_image_type_value4 = _minihead_array_tokens(minihead, "ImageTypeValue4")
        if not image_type_value4:
            errors.append(f"image {index} is missing Meta ImageTypeValue4")
        if minihead and image_type_value4 not in minihead_image_type_value4:
            errors.append(
                f"image {index} has IceMiniHead ImageTypeValue4 "
                f"{minihead_image_type_value4}, expected {image_type_value4}"
            )
    for field in SCANNER_WRITE_UNSAFE_META_KEYS:
        if _meta_text(meta, field):
            errors.append(f"image {index} has unsafe scanner Meta {field}")
        if _minihead_string_value(minihead, field) or _minihead_array_tokens(
            minihead,
            field,
        ):
            errors.append(f"image {index} has unsafe scanner IceMiniHead {field}")

    storage_key = (
        _meta_text(meta, "SeriesInstanceUID"),
        _meta_int(meta, "SliceNo"),
        _meta_int(meta, "ChronSliceNo"),
        _meta_int(meta, "NumberInSeries"),
    )
    missing_storage_fields = [
        field
        for field, value in zip(
            ("SeriesInstanceUID", "SliceNo", "ChronSliceNo", "NumberInSeries"),
            storage_key,
        )
        if value is None
    ]
    if missing_storage_fields:
        errors.append(
            f"image {index} is missing scanner storage key field(s) "
            f"{', '.join(missing_storage_fields)}"
        )
    previous_index = seen_storage_keys.setdefault(storage_key, index)
    if previous_index != index:
        errors.append(
            f"image {index} duplicates scanner storage key {storage_key} "
            f"from image {previous_index}"
        )
    minihead_storage_key = (
        _minihead_string_value(minihead, "SeriesInstanceUID"),
        _minihead_long_value(minihead, "SliceNo"),
        _minihead_long_value(minihead, "ChronSliceNo"),
        _minihead_long_value(minihead, "NumberInSeries"),
    )
    if minihead and all(
        value is not None and value != "" for value in minihead_storage_key
    ):
        previous_index = seen_minihead_storage_keys.setdefault(
            minihead_storage_key,
            index,
        )
        if previous_index != index:
            errors.append(
                f"image {index} duplicates scanner MiniHead storage key "
                f"{minihead_storage_key} from image {previous_index}"
            )
    for field in ("SOPInstanceUID",):
        value = _meta_text(meta, field)
        if value and value in input_identity:
            errors.append(f"image {index} reuses input storage identity {field}={value}")


def _series_slice_limit(series_index, source_slice_count, source_image_count):
    if source_slice_count <= 0:
        return 0
    if series_index == UPSAMPLED_SERIES_INDEX:
        return 2 * source_image_count
    if series_index == MIP_SERIES_INDEX:
        return 1
    return source_slice_count


def _identity_values(images):
    values = set()
    for image in images:
        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        for key in (
            "SeriesDescription",
            "SequenceDescription",
            "ProtocolName",
            "SeriesNumberRangeNameUID",
            "SeriesInstanceUID",
            "SOPInstanceUID",
        ):
            value = _meta_text(meta, key)
            if value:
                values.add(value)
            minihead_value = _minihead_string_value(minihead, key)
            if minihead_value:
                values.add(minihead_value)
    return values


def _meta_int(meta, key):
    text = _meta_text(meta, key)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _image_minihead(image):
    return _decode_ice_minihead(_meta_text(_meta_from_image(image), "IceMiniHead"))


def _ordered_upsample_sources(images):
    images = list(images)
    if len(images) < 2:
        return images, _infer_slice_axis([image.getHead() for image in images])

    headers = [image.getHead() for image in images]
    slice_axis = _infer_slice_axis(headers)
    projections = np.asarray(
        [_projected_position(image, slice_axis) for image in images],
        dtype=float,
    )
    duplicate_positions = _duplicate_projected_position_count(projections)
    median_spacing = _median_projected_spacing(projections)
    increasing = _is_projected_order_monotonic(projections, increasing=True)
    decreasing = _is_projected_order_monotonic(projections, increasing=False)
    logging.info(
        "upsampled source geometry: count=%d axis=%s projected_range=[%.6f, %.6f] "
        "median_spacing=%.6f duplicate_positions=%d receive_order_monotonic=%s",
        len(images),
        _format_vector(slice_axis),
        float(np.min(projections)),
        float(np.max(projections)),
        median_spacing,
        duplicate_positions,
        increasing or decreasing,
    )
    if duplicate_positions:
        logging.warning(
            "upsampled source geometry has %d duplicate projected slice position(s)",
            duplicate_positions,
        )
        raise ValueError(
            "upsampled source geometry has duplicate projected slice position(s)"
        )
    if increasing:
        return images, slice_axis

    sort_indices = sorted(
        range(len(images)),
        key=lambda index: (
            round(float(projections[index]), 4),
            int(headers[index].slice),
            int(headers[index].image_index),
            index,
        ),
    )
    order_reason = (
        "decreasing in physical position"
        if decreasing
        else "not monotonic in physical position"
    )
    logging.warning(
        "upsampled source slices are %s; "
        "sorting by projected position before interpolation: first mappings %s",
        order_reason,
        ", ".join(
            f"out{output_index}->in{input_index}"
            for output_index, input_index in enumerate(sort_indices[:24])
        ),
    )
    if len(sort_indices) > 24:
        logging.warning(
            "upsampled source sort mapping omitted %d additional slice(s)",
            len(sort_indices) - 24,
        )
    return [images[index] for index in sort_indices], slice_axis


def _ordered_volume_items(source_items, label):
    source_items = list(source_items)
    if len(source_items) < 2:
        images = [source_image for source_image, _data in source_items]
        return source_items, _infer_slice_axis([image.getHead() for image in images])

    images = [source_image for source_image, _data in source_items]
    headers = [image.getHead() for image in images]
    slice_axis = _infer_slice_axis(headers)
    projections = np.asarray(
        [_projected_position(image, slice_axis) for image in images],
        dtype=float,
    )
    duplicate_positions = _duplicate_projected_position_count(projections)
    median_spacing = _median_projected_spacing(projections)
    increasing = _is_projected_order_monotonic(projections, increasing=True)
    decreasing = _is_projected_order_monotonic(projections, increasing=False)
    logging.info(
        "%s source geometry: count=%d axis=%s projected_range=[%.6f, %.6f] "
        "median_spacing=%.6f duplicate_positions=%d receive_order_monotonic=%s",
        label,
        len(source_items),
        _format_vector(slice_axis),
        float(np.min(projections)),
        float(np.max(projections)),
        median_spacing,
        duplicate_positions,
        increasing or decreasing,
    )
    if duplicate_positions:
        raise ValueError(
            f"{label} source geometry has duplicate projected slice position(s); "
            "cannot pack a safe explicit volume"
        )
    if increasing:
        return source_items, slice_axis

    sort_indices = sorted(
        range(len(source_items)),
        key=lambda index: (
            round(float(projections[index]), 4),
            int(headers[index].slice),
            int(headers[index].image_index),
            index,
        ),
    )
    order_reason = (
        "decreasing in physical position"
        if decreasing
        else "not monotonic in physical position"
    )
    logging.warning(
        "%s source slices are %s; sorting by projected position before "
        "packing explicit volume: first mappings %s",
        label,
        order_reason,
        ", ".join(
            f"out{output_index}->in{input_index}"
            for output_index, input_index in enumerate(sort_indices[:24])
        ),
    )
    if len(sort_indices) > 24:
        logging.warning(
            "%s source sort mapping omitted %d additional slice(s)",
            label,
            len(sort_indices) - 24,
        )
    return [source_items[index] for index in sort_indices], slice_axis


def _validate_unique_projected_positions(images, slice_axis, label):
    if len(images) < 2:
        return

    projections = np.asarray(
        [_projected_position(image, slice_axis) for image in images],
        dtype=float,
    )
    sort_indices = sorted(range(len(images)), key=lambda index: projections[index])
    duplicates = []
    for previous_index, current_index in zip(sort_indices, sort_indices[1:]):
        if (
            abs(projections[current_index] - projections[previous_index])
            <= SLICE_POSITION_TOLERANCE_MM
        ):
            duplicates.append((previous_index, current_index, projections[current_index]))

    if duplicates:
        details = ", ".join(
            f"{previous}/{current}@{projection:.6f}"
            for previous, current, projection in duplicates[:8]
        )
        if len(duplicates) > 8:
            details += f", ... {len(duplicates) - 8} more"
        raise ValueError(
            f"{label} has duplicate projected slice position(s): {details}"
        )

    sorted_projections = projections[sort_indices]
    sorted_diffs = np.diff(sorted_projections)
    current_increasing = _is_projected_order_monotonic(
        projections,
        increasing=True,
    )
    current_decreasing = _is_projected_order_monotonic(
        projections,
        increasing=False,
    )
    logging.info(
        "%s geometry: count=%d projected_range=[%.6f, %.6f] min_spacing=%.6f "
        "output_order_monotonic=%s",
        label,
        len(images),
        float(np.min(projections)),
        float(np.max(projections)),
        float(np.min(np.abs(sorted_diffs))) if sorted_diffs.size else 0.0,
        current_increasing or current_decreasing,
    )
    if not (current_increasing or current_decreasing):
        logging.warning("%s positions are not monotonic in output order", label)


def _duplicate_projected_position_count(projections):
    if len(projections) < 2:
        return 0
    sorted_diffs = np.diff(np.sort(np.asarray(projections, dtype=float)))
    return int(np.sum(np.abs(sorted_diffs) <= SLICE_POSITION_TOLERANCE_MM))


def _median_projected_spacing(projections):
    if len(projections) < 2:
        return 0.0
    sorted_diffs = np.diff(np.sort(np.asarray(projections, dtype=float)))
    nonzero_diffs = np.abs(
        sorted_diffs[np.abs(sorted_diffs) > SLICE_POSITION_TOLERANCE_MM]
    )
    if nonzero_diffs.size == 0:
        return 0.0
    return float(np.median(nonzero_diffs))


def _is_projected_order_monotonic(projections, increasing):
    if len(projections) < 2:
        return True
    diffs = np.diff(np.asarray(projections, dtype=float))
    if increasing:
        return bool(np.all(diffs > SLICE_POSITION_TOLERANCE_MM))
    return bool(np.all(diffs < -SLICE_POSITION_TOLERANCE_MM))


def _log_output_images(output_images):
    for index, image in enumerate(output_images):
        header = image.getHead()
        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        logging.info(
            "OPENRECONI2I_OUTPUT index=%d series=%d image_index=%d slice=%d "
            "matrix_size=%s position=%s fov=%s meta_slice_pos=%s keep_geometry=%s "
            "partition_count=%s slice_count=%s name=%s series_uid=%s sop_uid=%s "
            "minihead_slice=%s minihead_chron_slice=%s minihead_sop_uid=%s",
            index,
            int(image.image_series_index),
            int(header.image_index),
            int(header.slice),
            _format_vector(header.matrix_size),
            _format_vector(_header_vector(header, "position")),
            _format_vector(_header_vector(header, "field_of_view")),
            _meta_vector_text(meta, "SlicePosLightMarker"),
            _meta_text(meta, "Keep_image_geometry"),
            _meta_text(meta, "partition_count"),
            _meta_text(meta, "slice_count"),
            _meta_text(meta, "SeriesDescription"),
            _meta_text(meta, "SeriesInstanceUID"),
            _meta_text(meta, "SOPInstanceUID"),
            _minihead_long_value(minihead, "SliceNo"),
            _minihead_long_value(minihead, "ChronSliceNo"),
            _minihead_string_value(minihead, "SOPInstanceUID"),
        )


def _explicit_header_geometry_meta(header):
    # The MRD ImageHeader is the geometry contract; these Meta copies are kept
    # aligned for scanner-side consumers and log/debug inspection.
    return {
        "ImageRowDir": _meta_vector(_header_vector(header, "read_dir")),
        "ImageColumnDir": _meta_vector(_header_vector(header, "phase_dir")),
        "ImageSliceNormDir": _meta_vector(_header_vector(header, "slice_dir")),
        "SlicePosLightMarker": _meta_vector(_header_vector(header, "position")),
    }


def _infer_slice_axis(headers):
    for header in headers:
        axis = _normalize_vector(_header_vector(header, "slice_dir"))
        if axis is not None:
            return axis

    if len(headers) > 1:
        axis = _normalize_vector(
            _header_vector(headers[-1], "position")
            - _header_vector(headers[0], "position")
        )
        if axis is not None:
            return axis

    return np.array([0.0, 0.0, 1.0], dtype=float)


def _projected_position(image, slice_axis):
    return float(np.dot(_header_position(image.getHead()), slice_axis))


def _project_position_on_slice_axis(position, origin, slice_axis):
    position = np.asarray(position, dtype=float)
    origin = np.asarray(origin, dtype=float)
    offset = position - origin
    return origin + float(np.dot(offset, slice_axis)) * slice_axis


def _half_step_on_slice_axis(half_step, slice_axis):
    half_step = np.asarray(half_step, dtype=float)
    signed_length = float(np.dot(half_step, slice_axis))
    if abs(signed_length) <= SLICE_POSITION_TOLERANCE_MM:
        signed_length = float(np.linalg.norm(half_step))
    return signed_length * slice_axis


def _header_vector(header, field_name):
    try:
        values = np.asarray(getattr(header, field_name), dtype=float)
    except Exception:
        return np.zeros(3, dtype=float)
    if values.size < 3:
        padded = np.zeros(3, dtype=float)
        padded[: values.size] = values
        return padded
    return values[:3]


def _normalize_vector(vector):
    vector = np.asarray(vector, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-8:
        return None
    return vector / norm


def _format_vector(vector):
    return "[" + ", ".join(f"{float(value):.6f}" for value in vector) + "]"


def _meta_vector(values):
    return [f"{float(value):.18f}" for value in values]


def _meta_vector_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(str(item) for item in value) + "]"
    if value is None:
        return ""
    return str(value)


def _upsampled_output_spacing(images, slice_axis, fallback_half_step):
    if len(images) > 1:
        source_spacing = _median_projected_spacing(
            [_projected_position(image, slice_axis) for image in images]
        )
        if source_spacing > SLICE_POSITION_TOLERANCE_MM:
            return 0.5 * source_spacing

    fallback_spacing = float(np.linalg.norm(fallback_half_step))
    if fallback_spacing > SLICE_POSITION_TOLERANCE_MM:
        return fallback_spacing
    return 1.0


def _explicit_volume_output_spacing(images, slice_axis):
    if len(images) > 1:
        source_spacing = _median_projected_spacing(
            [_projected_position(image, slice_axis) for image in images]
        )
        if source_spacing > SLICE_POSITION_TOLERANCE_MM:
            return source_spacing

    header = images[0].getHead()
    source_spacing = float(header.field_of_view[2]) if header.field_of_view[2] else 0.0
    if source_spacing > SLICE_POSITION_TOLERANCE_MM:
        return source_spacing
    return 1.0


def _interpolated_half_step(images):
    if len(images) > 1:
        first = _header_position(images[0].getHead())
        second = _header_position(images[1].getHead())
        step = 0.5 * (second - first)
        if float(np.linalg.norm(step)) > 0:
            return step

    header = images[0].getHead()
    slice_dir = np.asarray(header.slice_dir, dtype=float)
    norm = float(np.linalg.norm(slice_dir))
    if norm == 0:
        slice_dir = np.array([0.0, 0.0, 1.0], dtype=float)
    else:
        slice_dir = slice_dir / norm

    spacing = float(header.field_of_view[2]) if header.field_of_view[2] else 1.0
    return 0.5 * spacing * slice_dir


def _header_position(header):
    return np.asarray(header.position, dtype=float)


def _source_slice_count_hint(images):
    meta_keys = ("slice_count", "SliceCount", "NumberOfSlices", "NoOfSlices")
    minihead_keys = meta_keys + ("sSliceArray.lSize",)
    return _first_positive_source_hint(images, meta_keys, minihead_keys)


def _source_partition_count_hint(images):
    meta_keys = ("partition_count", "PartitionCount", "NoOfPartitions")
    return _first_positive_source_hint(images, meta_keys, meta_keys)


def _source_geometry_slice_limit(images):
    slice_hint = _source_slice_count_hint(images)
    partition_hint = _source_partition_count_hint(images)
    if (
        partition_hint is not None
        and partition_hint > 1
        and len(images) > partition_hint
        and (slice_hint is None or slice_hint >= len(images))
    ):
        return partition_hint
    if slice_hint is not None:
        return slice_hint
    if partition_hint is not None and partition_hint > 1:
        return partition_hint
    return None


def _source_geometry_needs_explicit_volume(images):
    source_slice_count = _source_geometry_slice_limit(images)
    if source_slice_count is None or len(images) <= source_slice_count:
        return False
    logging.warning(
        "source geometry advertises %d slice/partition slot(s), but %d image(s) "
        "were received; packing output as explicit volume geometry",
        source_slice_count,
        len(images),
    )
    return True


def _first_positive_source_hint(images, meta_keys, minihead_keys=None):
    minihead_keys = minihead_keys or meta_keys
    for image in images:
        meta = _meta_from_image(image)
        for key in meta_keys:
            value = _meta_int(meta, key)
            if value is not None and value > 0:
                return value

        minihead = _image_minihead(image)
        for key in minihead_keys:
            value = _minihead_long_value(minihead, key)
            if value is not None and value > 0:
                return value

    return None


def _require_output_slice_count(output_slice_count):
    if output_slice_count is None:
        raise ValueError("output_slice_count is required for explicit output geometry")
    try:
        output_slice_count = int(output_slice_count)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "output_slice_count must be a positive integer for explicit output geometry"
        ) from exc
    if output_slice_count < 1:
        raise ValueError(
            "output_slice_count must be a positive integer for explicit output geometry"
        )
    return output_slice_count


def _set_header_sequence_field(image_header, field_name, values):
    values = list(values)
    current_value = getattr(image_header, field_name)
    try:
        current_value[:] = values
    except Exception:
        setattr(image_header, field_name, tuple(values))


def _cast_like(data, reference):
    if np.issubdtype(reference.dtype, np.integer):
        info = np.iinfo(reference.dtype)
        return np.clip(np.rint(data), info.min, info.max).astype(reference.dtype)
    return data.astype(reference.dtype)


def _config_bool(config, key, default=False):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return default

    raw = None
    if isinstance(config, dict):
        raw = config.get(key)
        parameters = config.get("parameters")
        if raw is None and isinstance(parameters, dict):
            raw = parameters.get(key)

    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)

    text = str(raw).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return default


def _config_bool_any(config, keys, default=False):
    for key in keys:
        value = _config_bool(config, key, default=None)
        if value is not None:
            return value
    return default
