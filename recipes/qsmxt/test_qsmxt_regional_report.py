import builtins
import io
import logging
from pathlib import Path

import ismrmrd
import numpy as np
import pytest

import qsmxt
import qsmxt_regional_report as regional
from test_qsmxt_openrecon import (
    FakeConnection, FakeMetadata, _dual_input_images, _fake_qsmxt_binary, _input_images,
)


NATIVE_HEADER = (
    "map\tunit\tindex\tname\tn_voxels\tn_valid\tvolume_mm3\tmean\tsd\tmedian\t"
    "min\tmax\tp5\tp95\treference\treference_offset_ppm\n"
)
NATIVE_ROW = (
    "Chimap\tppm\t10\tleft thalamus\t36\t32\t1187.4\t0.012000\t0.030000\t"
    "0.011000\t-0.010000\t0.090000\t-0.005000\t0.080000\tmean\t0.002000\n"
)


def _write_tsv(output_dir, raw, relative="sub-01/anat/sub-01_acq-gre_desc-segmentation_stats.tsv"):
    path = output_dir / "derivatives/qsmxt" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw.encode("utf-8"))
    return path


def _build_pages(output_dir, *, requested_reference="mean"):
    return regional.build_pages(
        output_dir, input_variant="distortion-corrected",
        requested_reference=requested_reference,
    )


def _text_recorder(monkeypatch):
    from PIL import ImageDraw

    rendered = []
    original = ImageDraw.ImageDraw.text

    def record(draw, xy, text, *args, **kwargs):
        rendered.append(text)
        return original(draw, xy, text, *args, **kwargs)

    monkeypatch.setattr(ImageDraw.ImageDraw, "text", record)
    return rendered


def test_native_chimap_values_render_and_raw_tsv_is_preserved(tmp_path, monkeypatch, caplog):
    rendered = _text_recorder(monkeypatch)
    raw = (NATIVE_HEADER + NATIVE_ROW + NATIVE_ROW.replace(
        "Chimap\tppm", "desc-paramagnetic_Chimap\tppm",
    ) + NATIVE_ROW.replace("Chimap\tppm", "R2starmap\tHz")).replace("\n", "\r\n")
    _write_tsv(tmp_path, raw, "sub-01/ses-02/anat/sub-01_ses-02_desc-segmentation_stats.tsv")
    _write_tsv(tmp_path, "unrelated aggregate", "desc-segmentation_stats.tsv")

    with caplog.at_level(logging.INFO):
        pages = _build_pages(tmp_path, requested_reference="ventricles")

    record = next(record for record in caplog.records if "raw TSV" in record.msg)
    assert record.args[-1] == raw
    assert "input_variant=distortion-corrected requested_reference=ventricles" in record.message
    assert rendered.count("10  left thalamus") == 1
    assert {"0.012000", "0.030000", "0.011000", "32/36"} <= set(rendered)
    assert "Requested QSM reference: ventricles" in rendered
    assert "Native QSM reference: mean" in rendered
    assert "Native reference offset: 0.002000 ppm" in rendered
    assert len(pages) == 1
    assert pages[0].dtype == np.uint16
    assert pages[0].shape == (1800, 1600)
    assert pages[0].min() == 0 and pages[0].max() == 4095


@pytest.mark.parametrize("reference,offset", [("", ""), ("none", ""), ("", "0.000000")])
def test_zero_mean_and_missing_native_provenance_are_not_invented(
    tmp_path, monkeypatch, reference, offset,
):
    rendered = _text_recorder(monkeypatch)
    fields = NATIVE_ROW.rstrip("\n").split("\t")
    fields[7] = "0.000000"
    fields[-2:] = [reference, offset]
    _write_tsv(tmp_path, NATIVE_HEADER + "\t".join(fields) + "\n")

    _build_pages(tmp_path, requested_reference="left-thalamus")

    assert "0.000000" in rendered
    assert f"Native QSM reference: {reference or 'not recorded'}" in rendered
    assert f"Native reference offset: {offset + ' ppm' if offset else 'not recorded'}" in rendered
    assert "Native QSM reference: left-thalamus" not in rendered


@pytest.mark.parametrize("field,value", [
    ("map", "chimap"), ("unit", "Hz"), ("index", "0"), ("name", ""),
    ("n_voxels", "0"), ("n_valid", "0"), ("n_valid", "37"), ("n_valid", "1.5"),
    ("mean", "nan"), ("sd", "inf"), ("sd", "-0.1"), ("median", "-inf"),
    ("reference_offset_ppm", "nan"), ("mean", "invalid"),
])
def test_invalid_native_statistics_fail_after_raw_logging(tmp_path, caplog, field, value):
    values = dict(zip(NATIVE_HEADER.strip().split("\t"), NATIVE_ROW.strip().split("\t")))
    values[field] = value
    raw = NATIVE_HEADER + "\t".join(values.values()) + "\n"
    _write_tsv(tmp_path, raw)
    with caplog.at_level(logging.INFO), pytest.raises(ValueError):
        _build_pages(tmp_path)
    assert any(record.args[-1] == raw for record in caplog.records if "raw TSV" in record.msg)


@pytest.mark.parametrize("raw", [
    "", NATIVE_HEADER, NATIVE_HEADER + NATIVE_ROW + NATIVE_ROW,
    NATIVE_HEADER + NATIVE_ROW.rstrip("\n").rsplit("\t", 1)[0] + "\n",
    NATIVE_HEADER.replace("mean", "median") + NATIVE_ROW,
    NATIVE_HEADER + NATIVE_ROW + NATIVE_ROW.replace("\t10\t", "\t11\t").replace("\tmean\t", "\tnone\t"),
    NATIVE_HEADER + NATIVE_ROW + NATIVE_ROW.replace("\t10\t", "\t11\t").replace("0.002000", "0.003000"),
])
def test_malformed_duplicate_or_inconsistent_runs_are_rejected(tmp_path, raw):
    _write_tsv(tmp_path, raw)
    with pytest.raises(ValueError):
        _build_pages(tmp_path)


def test_missing_and_ambiguous_per_run_tsv_fail_explicitly(tmp_path):
    _write_tsv(tmp_path, NATIVE_HEADER + NATIVE_ROW, "desc-segmentation_stats.tsv")
    with pytest.raises(ValueError, match="found 0"):
        _build_pages(tmp_path)
    _write_tsv(tmp_path, NATIVE_HEADER + NATIVE_ROW)
    _write_tsv(tmp_path, NATIVE_HEADER + NATIVE_ROW, "sub-02/anat/sub-02_desc-segmentation_stats.tsv")
    with pytest.raises(ValueError, match="found 2"):
        _build_pages(tmp_path)


def test_raw_data_is_logged_before_renderer_import(tmp_path, monkeypatch, caplog):
    raw = NATIVE_HEADER + NATIVE_ROW
    _write_tsv(tmp_path, raw)
    original_import = builtins.__import__

    def fail_renderer(name, *args, **kwargs):
        if name == "matplotlib":
            assert any(record.args[-1] == raw for record in caplog.records if "raw TSV" in record.msg)
            raise ImportError("renderer unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_renderer)
    with caplog.at_level(logging.INFO), pytest.raises(ImportError, match="renderer unavailable"):
        _build_pages(tmp_path)


def test_all_regions_paginate_with_long_labels_and_repeated_provenance(tmp_path, monkeypatch):
    rendered = _text_recorder(monkeypatch)
    reference = ",".join(["left-thalamus", "right-caudate"] * 7)
    rows = []
    for index in range(1, 51):
        fields = NATIVE_ROW.rstrip("\n").split("\t")
        fields[2] = str(index)
        fields[3] = f"region {index} with a long anatomical label that wraps onto another line"
        fields[-2] = reference
        rows.append("\t".join(fields) + "\n")
    _write_tsv(tmp_path, NATIVE_HEADER + "".join(rows))

    pages = _build_pages(tmp_path, requested_reference=reference)

    assert len(pages) > 1
    assert len({page.shape for page in pages}) == 1
    assert rendered.count("Regional QSM table") == len(pages)
    assert rendered.count("Native reference offset: 0.002000 ppm") == len(pages)
    assert rendered.count("0.012000") == 50
    assert rendered.count("32/36") == 50
    for index in range(1, 51):
        assert any(f"{index}  region {index} " in text for text in rendered)
    assert f"Page {len(pages)}/{len(pages)}" in rendered


def _conversion(images):
    return {
        "anchor_image": images[0], "input_variant": "distortion-corrected",
        "input_short_label": "DC",
    }


def test_report_is_one_prerotated_explicit_volume_with_clean_identity():
    images = _input_images()
    source_meta = qsmxt._meta_from_image(images[0])
    source_meta.update({
        "PatientID": "synthetic", "StudyInstanceUID": "1.2.3.4",
        "SeriesInstanceUID": "1.2.3.4.5", "SOPInstanceUID": "1.2.3.4.5.6",
        "PSSeriesInstanceUID": "1.2.3.4.5", "ReferencedImageSequence0": "source",
        "IceMiniHead": "unsafe", "RescaleSlope": "0.01", "RescaleIntercept": "-2",
        "QSMxTUnits": "ppm", "QSMxTDisplayFormula": "physical", "ImageTypeValue3": "P",
    })
    images[0].attribute_string = source_meta.serialize()
    pages = (np.arange(20, dtype=np.uint16).reshape(4, 5), np.full((4, 5), 4095, dtype=np.uint16))
    report = qsmxt._build_regional_report_image(pages, _conversion(images), 190)
    wire = io.BytesIO()
    report.serialize_into(wire.write)
    wire.seek(0)
    report = ismrmrd.Image.deserialize_from(wire.read)
    qsmxt._validate_regional_report_image(report, images)

    header = report.getHead()
    meta = qsmxt._meta_from_image(report)
    assert report.data.shape == (1, 2, 4, 5)
    np.testing.assert_array_equal(report.data[0], np.stack([np.rot90(page, 2) for page in pages]))
    assert tuple(header.matrix_size) == (5, 4, 2)
    assert tuple(header.field_of_view) == (5, 4, 2)
    assert tuple(header.position) == (0, 0, 0)
    assert tuple(header.read_dir) == (1, 0, 0)
    assert tuple(header.phase_dir) == (0, 1, 0)
    assert tuple(header.slice_dir) == (0, 0, 1)
    assert header.channels == 1 and header.slice == 0 and header.image_index == 1
    assert meta["DataRole"] == "Segmentation"
    assert meta["SeriesDescription"] == "QSMxT Regional QSM table DC"
    assert meta["PatientID"] == "synthetic" and meta["StudyInstanceUID"] == "1.2.3.4"
    assert meta["SeriesInstanceUID"] != "1.2.3.4.5"
    assert meta["SOPInstanceUID"] != "1.2.3.4.5.6"
    assert meta["slice_count"] == meta["NumberOfSlices"] == meta["ImagesInAcquisition"] == "2"
    assert meta["Keep_image_geometry"] == "0" and meta["partition_count"] == "1"
    assert meta["WindowCenter"] == "2048" and meta["WindowWidth"] == "4096"
    for key in ("IceMiniHead", "ImageTypeValue3", "RescaleSlope", "RescaleIntercept",
                "QSMxTUnits", "QSMxTDisplayFormula", "PSSeriesInstanceUID", "ReferencedImageSequence0"):
        assert key not in meta
    with pytest.raises(ValueError, match="Quantitative"):
        qsmxt._validate_output_images([report], images)


@pytest.mark.parametrize("field,value", [
    ("DataRole", ["Image", "Quantitative"]), ("Keep_image_geometry", "1"),
    ("partition_count", "2"), ("slice_count", "1"), ("NumberOfSlices", "1"),
    ("ImagesInAcquisition", "1"), ("SliceNo", "1"), ("WindowCenter", "2048.5"),
    ("WindowWidth", "1"), ("IceMiniHead", "source"), ("QSMxTUnits", "ppm"),
    ("RescaleSlope", "1"), ("QSMxTInputSeries", "both"),
    ("SeriesDescription", "QSMxT Regional QSM table ND"), ("SeriesInstanceUID", ""),
    ("SeriesNumberRangeNameUID", "source"), ("ImageRowDir", ["0", "1", "0"]),
    ("ImageType", "ORIGINAL\\PRIMARY\\M"), ("ComplexImageComponent", "PHASE"),
])
def test_report_validator_rejects_broken_metadata_contract(field, value):
    images = _input_images()
    report = qsmxt._build_regional_report_image(
        (np.zeros((4, 5), dtype=np.uint16),) * 2, _conversion(images), 190,
    )
    meta = qsmxt._meta_from_image(report)
    meta[field] = value
    report.attribute_string = meta.serialize()
    with pytest.raises(ValueError):
        qsmxt._validate_regional_report_image(report, images)


@pytest.mark.parametrize("field,value", [
    ("image_series_index", 1), ("slice", 1), ("image_index", 2), ("channels", 2),
    ("field_of_view", (5, 4, 3)), ("position", (1, 0, 0)), ("read_dir", (0, 1, 0)),
    ("matrix_size", (5, 4, 3)),
])
def test_report_validator_rejects_broken_header_contract(field, value):
    images = _input_images()
    report = qsmxt._build_regional_report_image(
        (np.zeros((4, 5), dtype=np.uint16),) * 2, _conversion(images), 190,
    )
    header = report.getHead()
    setattr(header, field, value)
    report.setHead(header)
    with pytest.raises(ValueError):
        qsmxt._validate_regional_report_image(report, images)


@pytest.mark.parametrize("dtype,value", [(np.uint16, 4096), (np.float32, 12)])
def test_report_validator_rejects_invalid_pixel_storage(dtype, value):
    images = _input_images()
    report = qsmxt._build_regional_report_image(
        (np.zeros((4, 5), dtype=np.uint16),), _conversion(images), 190,
    )
    invalid = ismrmrd.Image.from_array(np.full((1, 4, 5), value, dtype=dtype), transpose=False)
    header = report.getHead()
    header.data_type = invalid.data_type
    invalid.setHead(header)
    invalid.attribute_string = report.attribute_string
    with pytest.raises(ValueError):
        qsmxt._validate_regional_report_image(invalid, images)


def _analysis_binary(tmp_path):
    fake = _fake_qsmxt_binary(tmp_path)
    with fake.open("a") as stream:
        stream.write(
            "\nif '--do-analysis' in sys.argv:\n"
            "    assert '--no-qsm' not in sys.argv\n"
            "    stats = dest.with_name(dest.name.replace('_Chimap.nii', '_desc-segmentation_stats.tsv'))\n"
            f"    stats.write_text({NATIVE_HEADER + NATIVE_ROW!r})\n"
            "mask = dest.with_name(dest.name.replace('_Chimap.nii', '_mask.nii'))\n"
            "nib.save(nib.Nifti1Image(np.ones(img.shape), affine), str(mask))\n"
        )
    return fake


@pytest.mark.parametrize("selection,expected_variants", [
    ("distortion-corrected", ["DC"]), ("non-distortion-corrected", ["ND"]),
    ("both", ["DC", "ND"]),
])
def test_process_delivers_maps_then_tables_with_unique_identities(
    tmp_path, monkeypatch, selection, expected_variants,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    binary = _analysis_binary(tmp_path)
    images = _dual_input_images()
    for image in images:
        header = image.getHead()
        header.image_series_index += qsmxt.OUTPUT_SERIES_START - 1
        image.setHead(header)
    connection = FakeConnection(images)

    qsmxt.process(connection, {"parameters": {
        "qsmxtbinary": str(binary), "inputseries": selection,
        "echotimesms": "10,20", "regionalqsmtable": True,
    }}, FakeMetadata())

    assert connection.closed and not connection.logs
    batches = connection.sent_batches[4:]
    assert len(batches) == len(expected_variants) * 2
    for variant, map_batch, report_batch in zip(expected_variants, batches[::2], batches[1::2]):
        assert qsmxt._meta_from_image(map_batch[0])["QSMxTOutput"] == "qsm"
        assert len(report_batch) == 1
        qsmxt._validate_regional_report_image(report_batch[0], connection.images)
        assert qsmxt._meta_from_image(report_batch[0])["SeriesDescription"].endswith(variant)
    outputs = [batch[0] for batch in connection.sent_batches]
    for key in ("SeriesInstanceUID", "SOPInstanceUID", "SeriesNumberRangeNameUID"):
        assert len({qsmxt._meta_from_image(image)[key] for image in outputs}) == len(outputs)
    assert len({int(image.getHead().image_series_index) for image in outputs}) == len(outputs)
    assert list((tmp_path / "work").iterdir()) == []


def test_non_qsm_map_selection_still_runs_analysis_and_returns_table(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    binary = _analysis_binary(tmp_path)
    settings = qsmxt._settings_from_config({"sendoutputs": "mask", "regionalqsmtable": True})
    command = qsmxt._qsmxt_command(Path("bids"), Path("output"), settings)
    assert "--do-analysis" in command and "--no-qsm" not in command
    connection = FakeConnection(_input_images())

    qsmxt.process(connection, {"qsmxtbinary": str(binary), "echotimesms": "10,20",
                              "sendoutputs": "mask", "regionalqsmtable": True}, FakeMetadata())

    assert connection.closed and not connection.logs
    assert [qsmxt._meta_from_image(batch[0]).get("QSMxTOutput") for batch in connection.sent_batches[2:]] == [
        "mask", "regionalqsmtable",
    ]


@pytest.mark.parametrize("failure", ["missing", "parse", "render", "validate"])
def test_optional_report_failure_retains_maps_and_continues_next_pair(
    tmp_path, monkeypatch, caplog, failure,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    binary = _analysis_binary(tmp_path)
    original_build = regional.build_pages
    original_validate = qsmxt._validate_regional_report_image

    def fail_first(output_dir, **kwargs):
        if kwargs["input_variant"] == "distortion-corrected":
            stats = next(output_dir.rglob("*_desc-segmentation_stats.tsv"))
            if failure == "missing":
                stats.unlink()
            elif failure == "parse":
                stats.write_text(NATIVE_HEADER + NATIVE_ROW.replace("0.012000", "nan"))
        return original_build(output_dir, **kwargs)

    original_render = regional._render_pages

    def render(table):
        if failure == "render" and table.input_variant == "distortion-corrected":
            raise RuntimeError("renderer unavailable")
        return original_render(table)

    def validate(report, images):
        variant = qsmxt._meta_from_image(report)["QSMxTInputSeries"]
        if failure == "validate" and variant == "distortion-corrected":
            raise ValueError("bad report")
        return original_validate(report, images)

    monkeypatch.setattr(regional, "build_pages", fail_first)
    monkeypatch.setattr(regional, "_render_pages", render)
    monkeypatch.setattr(qsmxt, "_validate_regional_report_image", validate)
    connection = FakeConnection(_dual_input_images())
    with caplog.at_level(logging.INFO):
        qsmxt.process(connection, {"qsmxtbinary": str(binary), "echotimesms": "10,20",
                                  "inputseries": "both", "regionalqsmtable": True}, FakeMetadata())

    assert connection.closed and len(connection.logs) == 1
    assert connection.logs[0][0] == qsmxt.constants.MRD_LOGGING_ERROR
    assert "Optional regional QSM table missing for distortion-corrected" in connection.logs[0][1]
    assert [qsmxt._meta_from_image(batch[0])["SeriesDescription"] for batch in connection.sent_batches[4:]] == [
        "QSMxT QSM DC", "QSMxT QSM ND", "QSMxT Regional QSM table ND",
    ]
    raw_records = [record for record in caplog.records if "raw TSV" in record.msg]
    assert len(raw_records) == (1 if failure == "missing" else 2)


def test_report_transport_failure_is_not_report_preparation_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    binary = _analysis_binary(tmp_path)

    class FailedTransport(FakeConnection):
        def send_image(self, images):
            if (
                isinstance(images, ismrmrd.Image)
                and qsmxt._meta_from_image(images).get("QSMxTOutput") == "regionalqsmtable"
            ):
                raise RuntimeError("report transport failed")
            super().send_image(images)

    connection = FailedTransport(_dual_input_images())
    qsmxt.process(connection, {"qsmxtbinary": str(binary), "echotimesms": "10,20",
                              "inputseries": "both", "regionalqsmtable": True}, FakeMetadata())

    assert connection.closed and len(connection.logs) == 1
    assert "report transport failed" in connection.logs[0][1]
    assert "Optional regional QSM table missing" not in connection.logs[0][1]
    assert len(connection.sent_batches) == 5


def test_defaults_saved_swi_choices_and_noqsm_conflict(tmp_path, monkeypatch):
    assert qsmxt._settings_from_config({})["regional_qsm_table"] is False
    for choice in ("laplacian", "laplacian-slice", "romeo"):
        settings = qsmxt._settings_from_config({"swiunwrapping": choice})
        command = qsmxt._qsmxt_command(Path("bids"), Path("output"), settings)
        assert command[command.index("--swi-unwrapping") + 1] == choice
        assert "--do-analysis" not in command
    connection = FakeConnection(_input_images())
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    qsmxt.process(connection, {"regionalqsmtable": True, "noqsm": True}, FakeMetadata())
    assert connection.closed and len(connection.sent_batches) == 2
    assert "Regional QSM table requires QSM" in connection.logs[0][1]
    assert not (tmp_path / "work").exists()
