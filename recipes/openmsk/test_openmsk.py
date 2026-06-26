import base64
import json
import os
import sys
import types
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np
import pydicom


def _install_openrecon_stubs():
    sys.modules.setdefault("constants", types.SimpleNamespace(MRD_LOGGING_ERROR=3))

    def get_json_config_param(config, key, default=None, type=None):
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                return default
        if not isinstance(config, dict):
            return default
        if key in config:
            return config[key]
        parameters = config.get("parameters")
        if isinstance(parameters, dict) and key in parameters:
            return parameters[key]
        return default

    sys.modules.setdefault(
        "mrdhelper",
        types.SimpleNamespace(get_json_config_param=get_json_config_param),
    )


_install_openrecon_stubs()
import openmsk  # noqa: E402


def first(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def make_image():
    image = ismrmrd.Image.from_array(np.ones((4, 4), dtype=np.uint16), transpose=False)
    header = image.getHead()
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_series_index = 1
    header.image_index = 1
    header.slice = 0
    header.field_of_view[:] = [4.0, 4.0, 1.0]
    header.matrix_size[:] = [4, 4, 1]
    image.setHead(header)
    return image


def make_source_image_with_minihead():
    image = make_image()
    minihead = """<ParamString."SeriesDescription">\t{ "source_series" }
<ParamString."SequenceDescription">\t{ "source_series" }
<ParamString."ProtocolName">\t{ "source_series" }
<ParamString."SeriesNumberRangeNameUID">\t{ "source_group" }
<ParamString."SeriesInstanceUID">\t{ "1.2.3" }
<ParamString."SOPInstanceUID">\t{ "1.2.3.4" }
<ParamString."ImageType">\t{ "ORIGINAL\\PRIMARY\\M\\NONE" }
<ParamString."ImageTypeValue3">\t{ "M" }
<ParamArray."ImageTypeValue3">
{
    <DefaultSize> 1
    <MaxSize> 2147483647
    <Default> <ParamString."">{ }
    { "M" }
}
<ParamArray."ImageTypeValue4">
{
    <DefaultSize> 1
    <MaxSize> 2147483647
    <Default> <ParamString."">{ }
    { "NONE" }
}
<ParamLong."SliceNo">\t{ 99 }
"""
    meta = ismrmrd.Meta()
    meta["SeriesDescription"] = "source_series"
    meta["SequenceDescription"] = "source_sequence"
    meta["ProtocolName"] = "source_protocol"
    meta["SeriesNumberRangeNameUID"] = "source_group"
    meta["SOPInstanceUID"] = "1.2.3.4"
    meta["DicomImageType"] = "ORIGINAL\\PRIMARY\\M\\NONE"
    meta["ImageType"] = "NONE"
    meta["ImageTypeValue3"] = "M"
    meta["ImageTypeValue4"] = "NONE"
    meta["IceMiniHead"] = base64.b64encode(minihead.encode("utf-8")).decode("ascii")
    image.attribute_string = meta.serialize()
    return image


def decoded_minihead(meta):
    return base64.b64decode(first(meta, "IceMiniHead")).decode("utf-8")


def exam_role_number(value):
    return f"<CategoryEntry>{int(value)}</CategoryEntry>"


def test_segment_meta_patches_scanner_visible_minihead_identity():
    source = make_source_image_with_minihead()

    meta = openmsk._derived_meta(
        source,
        openmsk.SEGMENT_SERIES_NAME,
        "2.25.123",
        openmsk.SEGMENT_SERIES_INDEX,
        2,
        openmsk.SEGMENT_IMAGE_TYPE,
        "Segmentation",
        "OpenMSK segmentation",
        7,
        source_geometry_segment=True,
        slice_count=80,
    )
    minihead = decoded_minihead(meta)

    assert first(meta, "DataRole") == "Segmentation"
    assert first(meta, "SeriesDescription") == openmsk.SEGMENT_SERIES_NAME
    assert first(meta, "SeriesNumberRangeNameUID") == "openmsk_segmentation_101"
    assert first(meta, "SOPInstanceUID") != "1.2.3.4"
    assert first(meta, "ImageTypeValue3") is None
    assert first(meta, "SegmentSourceGeometry") == "1"
    assert first(meta, "SegmentOutputGeometry") == "2d"
    assert first(meta, "SegmentPostProcessingChildRole") == str(openmsk.SEGMENT_SERIES_INDEX)
    assert exam_role_number(openmsk.SEGMENT_SERIES_INDEX) in first(meta, "ExamDataRole")
    assert first(meta, "NumberInSeries") == "3"
    assert first(meta, "SliceNo") == "2"
    assert '<ParamString."SeriesDescription">\t{ "openmsk_segmentation" }' in minihead
    assert '<ParamString."SeriesNumberRangeNameUID">\t{ "openmsk_segmentation_101" }' in minihead
    assert '<ParamString."SOPInstanceUID">\t{ "1.2.3.4" }' not in minihead
    assert "ImageTypeValue3" not in minihead
    assert '<ParamString."ExamDataRole">' in minihead
    assert exam_role_number(openmsk.SEGMENT_SERIES_INDEX) in minihead
    assert '<Default> <ParamString."">{ }' in minihead
    assert '    { "openmsk_segmentation" }' in minihead
    assert '    "openmsk_segmentation"' not in minihead
    assert '<ParamLong."SliceNo">\t{ 2 }' in minihead


def test_original_passthrough_preserves_source_identity_and_valid_paramarray():
    source = make_source_image_with_minihead()

    restamped = openmsk._restamp_images(
        [source],
        openmsk.ORIGINAL_SERIES_INDEX,
        "openmsk_original",
        "ORIGINAL",
        "OpenMSK original",
    )

    meta = ismrmrd.Meta.deserialize(restamped[0].attribute_string)
    minihead = decoded_minihead(meta)

    assert first(meta, "SeriesDescription") == "source_series"
    assert first(meta, "SequenceDescription") == "source_sequence"
    assert first(meta, "ProtocolName") == "source_protocol"
    assert first(meta, "SeriesNumberRangeNameUID") == "openmsk_original_100"
    assert first(meta, "SOPInstanceUID") != "1.2.3.4"
    assert first(meta, "DicomImageType") == "ORIGINAL\\PRIMARY\\M\\NONE"
    assert first(meta, "ImageType") == "NONE"
    assert first(meta, "ImageTypeValue3") is None
    assert first(meta, "ImageTypeValue4") == "NONE"
    assert '<ParamString."SeriesDescription">\t{ "source_series" }' in minihead
    assert '<ParamString."SequenceDescription">\t{ "source_series" }' in minihead
    assert '<ParamString."ProtocolName">\t{ "source_series" }' in minihead
    assert '<ParamString."SeriesNumberRangeNameUID">\t{ "openmsk_original_100" }' in minihead
    assert '<ParamString."SOPInstanceUID">\t{ "1.2.3.4" }' not in minihead
    assert "ImageTypeValue3" not in minihead
    assert '<ParamArray."ImageTypeValue4">' in minihead
    assert '<DefaultSize> 1' in minihead
    assert '<MaxSize> 2147483647' in minihead
    assert '<Default> <ParamString."">{ }' in minihead
    assert '    { "NONE" }' in minihead
    assert '    "NONE"' not in minihead


class FakeConnection:
    def __init__(self, items):
        self.items = list(items)
        self.closed = False
        self.logs = []

    def __iter__(self):
        return iter(self.items + [None])

    def send_logging(self, level, message):
        self.logs.append((level, message))

    def send_close(self):
        self.closed = True


def test_process_default_sends_segmentation_before_optional_postprocessing(monkeypatch):
    events = []
    source = make_image()
    source.attribute_string = ismrmrd.Meta().serialize()

    def fake_write_run_config(tmpdir, *_args):
        path = Path(tmpdir) / "openmsk_config.json"
        path.write_text("{}")
        return path

    monkeypatch.setattr(openmsk, "_write_source_nifti", lambda images, *_args: (images, (4, 4, 1)))
    monkeypatch.setattr(openmsk, "_write_run_config", fake_write_run_config)
    monkeypatch.setattr(openmsk, "_run_kneepipeline_segmentation", lambda *_args: True)
    monkeypatch.setattr(
        openmsk,
        "_find_single_output",
        lambda _output_dir, pattern: Path("openmsk_echo1_all-labels.nii.gz")
        if pattern == "*_all-labels.nii.gz"
        else None,
    )
    monkeypatch.setattr(openmsk, "_nifti_to_mrd_images", lambda *_args, **_kwargs: [make_image()])
    monkeypatch.setattr(openmsk, "_run_kneepipeline_postprocessing", lambda *_args: events.append("postprocessing") or True)
    monkeypatch.setattr(openmsk, "_send_images", lambda _connection, _images, context: events.append(context))

    connection = FakeConnection([source])
    openmsk.process(connection, {}, metadata=types.SimpleNamespace(encoding=[]))

    assert events == ["original_passthrough", "openmsk_segmentation"]
    assert connection.closed
    assert connection.logs == []


def test_process_runs_t2_postprocessing_for_qdess_segmentation_result(monkeypatch):
    events = []
    postprocessing_args = []
    source = make_image()
    source.attribute_string = ismrmrd.Meta().serialize()

    def fake_write_run_config(tmpdir, *_args):
        path = Path(tmpdir) / "openmsk_config.json"
        path.write_text("{}")
        return path

    def fake_postprocessing(_output_dir, _config_path, compute_thickness, compute_t2):
        postprocessing_args.append((compute_thickness, compute_t2))
        return True

    monkeypatch.setattr(openmsk, "_write_source_nifti", lambda images, *_args: (images, (4, 4, 1)))
    monkeypatch.setattr(openmsk, "_write_run_config", fake_write_run_config)
    monkeypatch.setattr(
        openmsk,
        "_run_kneepipeline_segmentation",
        lambda *_args: {"ok": True, "segmentation": {"is_qdess": True, "skip_steps": []}},
    )
    monkeypatch.setattr(
        openmsk,
        "_find_single_output",
        lambda _output_dir, pattern: Path("openmsk_echo1_all-labels.nii.gz")
        if pattern == "*_all-labels.nii.gz"
        else None,
    )
    monkeypatch.setattr(openmsk, "_nifti_to_mrd_images", lambda *_args, **_kwargs: [make_image()])
    monkeypatch.setattr(openmsk, "_run_kneepipeline_postprocessing", fake_postprocessing)
    monkeypatch.setattr(openmsk, "_send_images", lambda _connection, _images, context: events.append(context))

    connection = FakeConnection([source])
    openmsk.process(connection, {}, metadata=types.SimpleNamespace(encoding=[]))

    assert events == ["original_passthrough", "openmsk_segmentation"]
    assert postprocessing_args == [(False, True)]
    assert connection.closed
    assert connection.logs == []


def test_process_uses_synthetic_qdess_dicom_when_two_echoes_available(monkeypatch):
    events = []
    segmentation_inputs = []
    writer_echo_counts = []
    echo1 = make_image()
    echo2 = make_image()
    echo1.image_index = 100
    echo2.image_index = 200
    echo1.attribute_string = ismrmrd.Meta().serialize()
    echo2.attribute_string = ismrmrd.Meta().serialize()

    def fake_write_run_config(tmpdir, *_args):
        path = Path(tmpdir) / "openmsk_config.json"
        path.write_text("{}")
        return path

    def fake_writer(echo_groups, output_dir, *_args):
        writer_echo_counts.append(sorted(len(images) for images in echo_groups.values()))
        output_dir.mkdir()
        return output_dir

    def fake_segmentation(input_path, output_dir, *_args):
        segmentation_inputs.append(Path(input_path))
        Path(output_dir, "openmsk_qdess_dicom_all-labels.nii.gz").write_text("labels")
        return {"ok": True, "segmentation": {"is_qdess": True, "skip_steps": []}}

    monkeypatch.setattr(openmsk, "_write_source_nifti", lambda images, *_args: (images, (4, 4, 1)))
    monkeypatch.setattr(openmsk, "_write_run_config", fake_write_run_config)
    monkeypatch.setattr(openmsk, "_write_synthetic_qdess_dicom_input", fake_writer)
    monkeypatch.setattr(openmsk, "_run_kneepipeline_segmentation", fake_segmentation)
    monkeypatch.setattr(openmsk, "_run_kneepipeline_postprocessing", lambda *_args: True)
    monkeypatch.setattr(openmsk, "_nifti_to_mrd_images", lambda *_args, **_kwargs: [make_image()])
    monkeypatch.setattr(openmsk, "_send_images", lambda _connection, _images, context: events.append(context))

    connection = FakeConnection([echo1, echo2])
    openmsk.process(
        connection,
        {"parameters": {"qdess_gl_area": 3132.0, "qdess_tg_us": 1560.0}},
        metadata=types.SimpleNamespace(encoding=[]),
    )

    assert writer_echo_counts == [[1, 1]]
    assert segmentation_inputs[0].name == "openmsk_qdess_dicom"
    assert events == ["original_passthrough", "openmsk_segmentation"]
    assert connection.closed
    assert connection.logs == []


def test_process_can_skip_originals_when_requested(monkeypatch):
    events = []
    source = make_image()
    source.attribute_string = ismrmrd.Meta().serialize()

    def fake_write_run_config(tmpdir, *_args):
        path = Path(tmpdir) / "openmsk_config.json"
        path.write_text("{}")
        return path

    monkeypatch.setattr(openmsk, "_write_source_nifti", lambda images, *_args: (images, (4, 4, 1)))
    monkeypatch.setattr(openmsk, "_write_run_config", fake_write_run_config)
    monkeypatch.setattr(openmsk, "_run_kneepipeline_segmentation", lambda *_args: True)
    monkeypatch.setattr(
        openmsk,
        "_find_single_output",
        lambda _output_dir, pattern: Path("openmsk_echo1_all-labels.nii.gz")
        if pattern == "*_all-labels.nii.gz"
        else None,
    )
    monkeypatch.setattr(openmsk, "_nifti_to_mrd_images", lambda *_args, **_kwargs: [make_image()])
    monkeypatch.setattr(openmsk, "_send_images", lambda _connection, _images, context: events.append(context))

    connection = FakeConnection([source])
    openmsk.process(
        connection,
        {"parameters": {"sendoriginal": False}},
        metadata=types.SimpleNamespace(encoding=[]),
    )

    assert events == ["openmsk_segmentation"]
    assert connection.closed
    assert connection.logs == []


def test_process_sends_subregions_and_metrics_report_when_outputs_exist(monkeypatch):
    events = []
    source = make_image()
    source.attribute_string = ismrmrd.Meta().serialize()

    def fake_write_run_config(tmpdir, *_args):
        path = Path(tmpdir) / "openmsk_config.json"
        path.write_text("{}")
        return path

    def fake_segmentation(_input_path, output_dir, *_args):
        Path(output_dir, "openmsk_echo1_all-labels.nii.gz").write_text("labels")
        return True

    def fake_postprocessing(output_dir, *_args):
        Path(output_dir, "openmsk_echo1_subregions-labels.nii.gz").write_text("subregions")
        Path(output_dir, "openmsk_echo1_thickness_results.json").write_text(
            json.dumps({"fem_cart_mm_mean": 1.2345})
        )
        Path(output_dir, "openmsk_echo1_thickness_results.csv").write_text(
            "fem_cart_mm_mean\n1.2345\n"
        )
        return True

    monkeypatch.setattr(openmsk, "_write_source_nifti", lambda images, *_args: (images, (4, 4, 1)))
    monkeypatch.setattr(openmsk, "_write_run_config", fake_write_run_config)
    monkeypatch.setattr(openmsk, "_run_kneepipeline_segmentation", fake_segmentation)
    monkeypatch.setattr(openmsk, "_run_kneepipeline_postprocessing", fake_postprocessing)
    monkeypatch.setattr(openmsk, "_nifti_to_mrd_images", lambda *_args, **_kwargs: [make_image()])
    monkeypatch.setattr(openmsk, "_build_metrics_report_images", lambda *_args, **_kwargs: [make_image()])
    monkeypatch.setattr(openmsk, "_send_images", lambda _connection, _images, context: events.append(context))

    connection = FakeConnection([source])
    openmsk.process(
        connection,
        {"parameters": {"computethickness": True}},
        metadata=types.SimpleNamespace(encoding=[]),
    )

    assert events == [
        "original_passthrough",
        "openmsk_segmentation",
        "openmsk_subregions",
        "openmsk_metrics_report",
    ]
    assert connection.closed
    assert connection.logs == []


def test_write_run_config_preserves_requested_segmentation_model(tmp_path, monkeypatch):
    source_config = {
        "default_seg_model": "acl_qdess_bone_july_2024",
        "models": {
            "acl_qdess_bone_july_2024": "/opt/DOSMA_WEIGHTS/default.h5",
            "goyal_sagittal": "/opt/DOSMA_WEIGHTS/sagittal_best_model.h5",
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(source_config))
    monkeypatch.setattr(openmsk, "KNEEPIPELINE_CONFIG", config_path)

    run_config_path = openmsk._write_run_config(
        tmp_path,
        "goyal_sagittal",
        run_nsm=False,
        run_bscore=False,
    )
    run_config = json.loads(run_config_path.read_text())

    assert run_config["default_seg_model"] == "goyal_sagittal"


def test_openrecon_label_keeps_packaged_model_choices():
    label = json.loads(Path("OpenReconLabel.json").read_text())
    params = {param["id"]: param for param in label["parameters"]}

    assert params["sendoriginal"]["default"] is True
    assert [value["id"] for value in params["segmodel"]["values"]] == [
        "acl_qdess_bone_july_2024",
        "goyal_sagittal",
        "nnunet_knee",
    ]
    for key in (
        "qdess_tr_ms",
        "qdess_te1_ms",
        "qdess_te2_ms",
        "qdess_flip_angle_deg",
        "qdess_gl_area",
        "qdess_tg_us",
    ):
        assert params[key]["type"] == "double"


def test_kneepipeline_subprocess_env_prepends_numpy_compat_path(monkeypatch):
    monkeypatch.setattr(openmsk, "NNUNET_NUMPY_COMPAT_PATH", "/opt/openmsk_compat")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(["/opt/openmsk_compat", "/already"]))

    env = openmsk._kneepipeline_subprocess_env()

    assert env["PYTHONPATH"].split(os.pathsep) == ["/opt/openmsk_compat", "/already"]


def test_run_kneepipeline_segmentation_returns_step_summary(tmp_path, monkeypatch):
    input_path = tmp_path / "openmsk_echo1.nii.gz"
    config_path = tmp_path / "config.json"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    input_path.write_text("image")
    config_path.write_text("{}")

    def fake_run(cmd, **_kwargs):
        summary_path = Path(cmd[4]) / openmsk.OPENMSK_SEGMENTATION_SUMMARY
        summary_path.write_text(
            json.dumps(
                {
                    "segmentation": {
                        "is_qdess": True,
                        "skip_steps": [],
                        "model_name": "acl_qdess_bone_july_2024",
                    }
                }
            )
        )
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(openmsk.subprocess, "run", fake_run)

    result = openmsk._run_kneepipeline_segmentation(
        input_path,
        output_dir,
        "acl_qdess_bone_july_2024",
        config_path,
    )

    assert result["ok"] is True
    assert result["returncode"] == 0
    assert openmsk._segmentation_is_qdess(result)
    assert not openmsk._segmentation_skips_step(result, "t2_mapping")


def test_synthetic_qdess_dicom_contains_t2_inputs(tmp_path):
    echo1 = make_image()
    echo2 = make_image()
    echo1.image_index = 100
    echo2.image_index = 200
    metadata = types.SimpleNamespace(
        measurementInformation=types.SimpleNamespace(protocolName="qDESS_test"),
        sequenceParameters=types.SimpleNamespace(TR=[], TE=[], flipAngle_deg=[]),
    )
    echo_groups = {0: [echo1], 1: [echo2]}
    params = openmsk._resolve_qdess_parameters(
        {
            "parameters": {
                "qdess_tr_ms": 26.0,
                "qdess_te1_ms": 7.5,
                "qdess_te2_ms": 41.5,
                "qdess_flip_angle_deg": 31.0,
                "qdess_gl_area": 3133.0,
                "qdess_tg_us": 1559.0,
            }
        },
        metadata,
        echo_groups,
    )

    dicom_dir = openmsk._write_synthetic_qdess_dicom_input(
        echo_groups,
        tmp_path / "qdess_dicom",
        metadata,
        {},
        params,
    )

    files = sorted(dicom_dir.glob("*.dcm"))
    assert len(files) == 2
    first = pydicom.dcmread(files[0])
    second = pydicom.dcmread(files[1])
    assert int(first.EchoNumbers) == 1
    assert int(second.EchoNumbers) == 2
    assert float(first.EchoTime) == 7.5
    assert float(second.EchoTime) == 41.5
    assert float(first.RepetitionTime) == 26.0
    assert float(first.FlipAngle) == 31.0
    assert float(first[openmsk.QDESS_GL_AREA_TAG].value) == 3133.0
    assert float(first[openmsk.QDESS_TG_TAG].value) == 1559.0
    assert first.SeriesDescription == "qDESS_test"


def test_nifti_to_mrd_reindexes_rotated_labels_to_source_grid(tmp_path, monkeypatch):
    source = make_image()

    reference_xyz = np.array(
        [
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16],
        ],
        dtype=np.int16,
    )[:, :, None]
    reference_path = tmp_path / "source.nii.gz"
    nib.save(nib.Nifti1Image(reference_xyz, np.eye(4)), reference_path)

    rotated_xyz = np.zeros_like(reference_xyz)
    size = reference_xyz.shape[0]
    for x_index in range(size):
        for y_index in range(size):
            rotated_xyz[size - 1 - y_index, x_index, 0] = reference_xyz[x_index, y_index, 0]
    rotated_affine = np.array(
        [
            [0, 1, 0, 0],
            [-1, 0, 0, size - 1],
            [0, 0, 1, 0],
            [0, 0, 0, 1],
        ],
        dtype=float,
    )
    label_path = tmp_path / "source_all-labels.nii.gz"
    nib.save(nib.Nifti1Image(rotated_xyz, rotated_affine), label_path)

    def fail_resample(*_args, **_kwargs):
        raise AssertionError("pure orientation changes must not interpolate label data")

    monkeypatch.setattr(openmsk, "resample_from_to", fail_resample)

    outputs = openmsk._nifti_to_mrd_images(
        label_path,
        [source],
        openmsk.SEGMENT_SERIES_INDEX,
        openmsk.SEGMENT_SERIES_NAME,
        openmsk.SEGMENT_IMAGE_TYPE,
        data_role="Segmentation",
        dtype=np.int16,
        comment="OpenMSK segmentation",
        source_geometry_segment=True,
        reference_nifti_path=reference_path,
    )

    expected_yx = reference_xyz[:, :, 0].T
    np.testing.assert_array_equal(np.squeeze(outputs[0].data), expected_yx)


def test_metrics_report_images_include_metrics_metadata(tmp_path):
    source = make_source_image_with_minihead()
    metrics_json = tmp_path / "openmsk_echo1_thickness_results.json"
    metrics_json.write_text(json.dumps({"fem_cart_mm_mean": 1.2345}))
    metrics_outputs = [
        {
            "label": "thickness",
            "json_path": metrics_json,
            "csv_path": None,
            "payload": {"fem_cart_mm_mean": 1.2345},
            "rows": [],
        }
    ]

    images = openmsk._build_metrics_report_images(
        metrics_outputs,
        [source],
        "thickness: fem_cart_mm_mean=1.234",
    )

    assert len(images) == 1
    image = images[0]
    header = image.getHead()
    meta = ismrmrd.Meta.deserialize(image.attribute_string)

    assert int(header.image_series_index) == openmsk.METRICS_REPORT_SERIES_INDEX
    assert first(meta, "SeriesDescription") == openmsk.METRICS_REPORT_SERIES_NAME
    assert first(meta, "DataRole") == "Image"
    assert first(meta, "Keep_image_geometry") == "0"
    assert first(meta, "OpenMSKMetricsRows") == "1"
    assert "fem_cart_mm_mean" in first(meta, "ImageComments")
    assert np.asarray(image.data).max() > 0


def test_collect_metrics_outputs_reads_csv_when_json_absent(tmp_path):
    metrics_csv = tmp_path / "openmsk_echo1_thickness_results.csv"
    metrics_csv.write_text("fem_cart_mm_mean,med_tib_cart_mm_mean\n1.2345,2.5\n")

    outputs = openmsk._collect_metrics_outputs(tmp_path)
    comment = openmsk._collect_metrics_comment(tmp_path)
    rows = openmsk._metrics_report_rows(outputs)

    assert outputs[0]["label"] == "thickness"
    assert outputs[0]["rows"] == [{"fem_cart_mm_mean": "1.2345", "med_tib_cart_mm_mean": "2.5"}]
    assert "fem_cart_mm_mean=1.234" in comment
    assert {"source": "thickness.csv", "metric": "fem_cart_mm_mean", "value": "1.2345"} in rows


def test_collect_metrics_outputs_reports_json_and_csv_when_both_exist(tmp_path):
    metrics_json = tmp_path / "openmsk_echo1_thickness_results.json"
    metrics_csv = tmp_path / "openmsk_echo1_thickness_results.csv"
    metrics_json.write_text(json.dumps({"fem_cart_mm_mean": 1.25}))
    metrics_csv.write_text("med_tib_cart_mm_mean\n2.5\n")

    outputs = openmsk._collect_metrics_outputs(tmp_path)
    comment = openmsk._collect_metrics_comment(tmp_path)
    rows = openmsk._metrics_report_rows(outputs)

    assert "thickness.json: fem_cart_mm_mean=1.25" in comment
    assert "thickness.csv: med_tib_cart_mm_mean=2.5" in comment
    assert {"source": "thickness.json", "metric": "fem_cart_mm_mean", "value": "1.25"} in rows
    assert {"source": "thickness.csv", "metric": "med_tib_cart_mm_mean", "value": "2.5"} in rows
