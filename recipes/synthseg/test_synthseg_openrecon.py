import ast
import copy
import ctypes
import itertools
import json
import re
import uuid
from pathlib import Path

import numpy as np
import pytest


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "synthseg.py"
LABEL_PATH = RECIPE_DIR / "OpenReconLabel.json"


def _load_runtime_helpers_for_test(function_names, assignments=()):
    tree = ast.parse(WRAPPER_PATH.read_text())
    helper_nodes = []
    wanted = set(function_names)
    wanted_assignments = set(assignments)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            if names & wanted_assignments:
                helper_nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in wanted:
            helper_nodes.append(node)

    class FakeMrdHelper:
        @staticmethod
        def extract_minihead_string_param(_minihead_text, _name):
            return ""

    class FakeMeta(dict):
        def serialize(self):
            return json.dumps(dict(self))

        @staticmethod
        def deserialize(value):
            if isinstance(value, FakeMeta):
                return FakeMeta(value)
            if isinstance(value, dict):
                return FakeMeta(value)
            return FakeMeta(json.loads(value or "{}"))

    class FakeHead:
        def __init__(self):
            self.data_type = 2
            self.image_type = 1
            self.image_series_index = 1
            self.image_index = 1
            self.slice = 0
            self.contrast = 0
            self.matrix_size = [2, 2, 1]
            self.field_of_view = [2.0, 2.0, 1.0]
            self.position = [0.0, 0.0, 0.0]
            self.read_dir = [1.0, 0.0, 0.0]
            self.phase_dir = [0.0, 1.0, 0.0]
            self.slice_dir = [0.0, 0.0, 1.0]
            self.measurement_uid = 42
            self.patient_table_position = [0.0, 0.0, 0.0]
            self.acquisition_time_stamp = 0
            self.physiology_time_stamp = [0, 0, 0]
            self.user_int = [0] * 8
            self.user_float = [0.0] * 8

    class FakeImage:
        def __init__(self, data):
            self.data = np.array(data, copy=True)
            self.data_type = 2
            self._head = FakeHead()
            if self.data.ndim >= 2:
                rows, cols = self.data.shape[-2:]
                self._head.matrix_size = [int(cols), int(rows), 1]
                self._head.field_of_view = [float(cols), float(rows), 1.0]
            self.image_series_index = self._head.image_series_index
            self.attribute_string = "{}"

        @staticmethod
        def from_array(data, transpose=False):
            return FakeImage(data)

        def setHead(self, head):
            self._head = copy.deepcopy(head)
            self.image_series_index = self._head.image_series_index

        def getHead(self):
            return copy.deepcopy(self._head)

    class FakeIsmrmrd:
        Image = FakeImage
        Meta = FakeMeta
        DATATYPE_CXFLOAT = 7
        DATATYPE_CXDOUBLE = 8
        IMTYPE_COMPLEX = 2
        IMTYPE_MAGNITUDE = 1

    namespace = {
        "base64": __import__("base64"),
        "copy": copy,
        "ctypes": ctypes,
        "ismrmrd": FakeIsmrmrd,
        "json": json,
        "logging": type(
            "Logger",
            (),
            {
                "info": staticmethod(lambda *args, **kwargs: None),
                "warning": staticmethod(lambda *args, **kwargs: None),
            },
        ),
        "mrdhelper": FakeMrdHelper,
        "np": np,
        "ndi": type("FakeNdi", (), {"zoom": staticmethod(lambda *args, **kwargs: None)}),
        "os": __import__("os"),
        "Path": Path,
        "re": re,
        "itertools": itertools,
        "uuid": uuid,
    }
    exec(
        compile(
            ast.Module(body=helper_nodes, type_ignores=[]),
            str(WRAPPER_PATH),
            "exec",
        ),
        namespace,
    )
    namespace["FakeImage"] = FakeImage
    namespace["FakeMeta"] = FakeMeta
    return namespace


def _helpers():
    return _load_runtime_helpers_for_test(
        [
            "_build_reformatted_images",
            "_copy_meta",
            "_decode_ice_minihead",
            "_derived_synthseg_instance_uid",
            "_derived_synthseg_series_uid",
            "_diagnostic_reformat_target_shape",
            "_encode_ice_minihead",
            "_env_positive_float",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_format_vector",
            "_get_meta_text",
            "_ice_compatible_target_shape",
            "_meta_from_image",
            "_resize_2d_nearest",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_square_pixel_target_shape",
            "_stamp_synthseg_output_image",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "OPENRECON_REFORMAT_DOWNSAMPLE_ENV",
            "OPENRECON_SEGMENT_SOURCE_GEOMETRY_SERIES_SUFFIX",
            "OPENRECON_SERIES_SUFFIX",
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
            "SYNTHSEG_OUTPUT_GEOMETRY_2D",
            "SYNTHSEG_REFORMAT_ORIENTATION_META_KEY",
            "SYNTHSEG_REFORMAT_SLICE_COUNT_META_KEY",
            "SYNTHSEG_REFORMAT_SLICE_INDEX_META_KEY",
            "SYNTHSEG_SEGMENT_OUTPUT_GEOMETRY_META_KEY",
            "SYNTHSEG_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY",
            "SYNTHSEG_SEGMENT_POSTPROCESSING_META_KEY",
            "SYNTHSEG_SEGMENT_SOURCE_GEOMETRY_META_KEY",
            "SYNTHSEG_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "SYNTHSEG_SEGMENTATION_LABEL",
            "SYNTHSEG_SEGMENTATION_TYPE_TOKEN",
            "SYNTHSEG_SOURCE_GEOMETRY_IMAGE_TYPE",
            "SYNTHSEG_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4",
        ],
    )


def _source_image(helpers):
    image = helpers["FakeImage"](np.zeros((1, 1, 2, 2), dtype=np.int16))
    head = image.getHead()
    head.image_series_index = 1
    head.image_index = 1
    head.slice = 0
    image.setHead(head)
    image.attribute_string = helpers["FakeMeta"](
        {
            "SeriesDescription": "source_t1w",
            "SequenceDescription": "source_t1w",
            "ProtocolName": "source_t1w",
            "SeriesNumberRangeNameUID": "source_group",
            "SeriesInstanceUID": "1.2.3",
            "SOPInstanceUID": "1.2.3.4",
            "ImageType": "ORIGINAL\\PRIMARY\\M\\ND",
            "DicomImageType": "ORIGINAL\\PRIMARY\\M\\ND",
            "ImageTypeValue3": "M",
            "ImageTypeValue4": "ND",
            "Keep_image_geometry": "1",
        }
    ).serialize()
    return image


def test_reformat_outputs_use_2d_segmentation_header_contract():
    helpers = _helpers()
    source_image = _source_image(helpers)
    source_identity = {
        "series_description": "source_t1w",
        "parent_grouping": "source_group",
        "series_uid": "1.2.3",
        "sop_uid": "1.2.3.4",
        "source_type_token": "ND",
    }
    output_identity = {
        "series_description": "source_t1w_synthseg_coronal",
        "sequence_description": "source_t1w_synthseg_coronal",
        "grouping": "source_group_synthseg_coronal",
        "display_token": "synthseg",
        "type_token": "SYNTHSEG",
        "image_comment": "synthseg_coronal",
        "series_uid": "2.25.999",
    }

    images = helpers["_build_reformatted_images"](
        volume_yxz=np.arange(12, dtype=np.int16).reshape((3, 2, 2)),
        head_template=source_image.getHead(),
        source_image=source_image,
        source_identity=source_identity,
        output_identity=output_identity,
        voxel_size=np.array([1.0, 1.0, 1.0]),
        fov=np.array([2.0, 3.0, 2.0]),
        orientation="coronal",
        series_index=4,
        max_val=4095,
    )

    assert len(images) == 3
    for index, image in enumerate(images):
        head = image.getHead()
        meta = helpers["FakeMeta"].deserialize(image.attribute_string)

        assert image.data.shape == (1, 1, 2, 2)
        assert head.image_series_index == 4
        assert head.image_index == index + 1
        assert head.slice == index
        assert meta["LUTFileName"] == "MicroDeltaHotMetal.pal"
        assert meta["DataRole"] == "Segmentation"
        assert meta["Keep_image_geometry"] == "1"
        assert meta["SegmentSourceGeometry"] == "1"
        assert "SegmentSourceImageHeader" not in meta
        assert meta["SegmentOutputGeometry"] == "2d"
        assert meta["SynthSegReformatOrientation"] == "coronal"
        assert meta["SynthSegReformatSliceIndex"] == str(index)
        assert meta["SynthSegReformatSliceCount"] == "3"
        assert meta["SegmentPostProcessingChildRole"] == "4"
        assert "<CategoryEntry>4</CategoryEntry>" in meta["ExamDataRole"]
        assert meta["SliceNo"] == str(index)
        assert meta["ChronSliceNo"] == str(index)
        assert meta["NumberInSeries"] == str(index + 1)
        assert "ImageTypeValue3" not in meta
        assert "partition_count" not in meta
        assert "slice_count" not in meta
        assert "NumberOfSlices" not in meta
        assert "ImagesInAcquisition" not in meta


def _openrecon_helpers():
    return _load_runtime_helpers_for_test(
        [
            "_openrecon_run_name",
            "_resolve_openrecon_parcellation",
            "_parse_model_list",
            "_safe_path_component",
            "iter_openrecon_parameter_combinations",
        ],
        assignments=[
            "OPENRECON_DEFAULTS",
            "OPENRECON_PARCELLATIONS",
            "OPENRECON_COMBINATION_PARAMETER_VALUES",
            "OPENRECON_MODEL_DEFAULT",
            "OPENRECON_MODEL_VALUES",
            "OPENRECON_OUTPUT_NAME_PARAM",
            "SYNTHSEG_MODEL_FILES",
        ],
    )


def _synthseg_command_helpers():
    return _load_runtime_helpers_for_test(
        [
            "_build_synthseg_command",
            "_resolve_synthseg_crop_options",
        ],
        assignments=[
            "SYNTHSEG_COMMAND",
            "SYNTHSEG_CROP_MULTIPLE",
        ],
    )


def _mp2rage_selection_helpers():
    return _load_runtime_helpers_for_test(
        [
            "_image_identity_values",
            "_meta_from_image",
            "_normalized_identity_text",
            "_select_anatomical_input_images",
        ],
        assignments=["MP2RAGE_IDENTITY_META_KEYS"],
    )


def _selection_image(
    helpers,
    description,
    protocol="T1_MP2RAGE",
    series_index=1,
):
    image = helpers["FakeImage"](np.zeros((1, 1, 2, 2), dtype=np.int16))
    head = image.getHead()
    head.image_series_index = series_index
    image.setHead(head)
    image.attribute_string = helpers["FakeMeta"](
        {
            "SeriesDescription": description,
            "ProtocolName": protocol,
        }
    ).serialize()
    return image


def test_mp2rage_selection_keeps_only_uni_den_images():
    helpers = _mp2rage_selection_helpers()
    images = [
        _selection_image(helpers, "T1_MP2RAGE_INV1"),
        _selection_image(helpers, "T1_MP2RAGE_UNI-DEN"),
        _selection_image(helpers, "T1_MP2RAGE_UNI_DEN"),
        _selection_image(helpers, "T1_MP2RAGE_INV2"),
    ]

    selected = helpers["_select_anatomical_input_images"](images)

    assert selected == images[1:3]


def test_mp2rage_selection_uses_first_series_without_uni_den():
    helpers = _mp2rage_selection_helpers()
    images = [
        _selection_image(helpers, "T1_MP2RAGE_INV1", series_index=4),
        _selection_image(helpers, "T1_MP2RAGE_INV1", series_index=4),
        _selection_image(helpers, "T1_MP2RAGE_UNI", series_index=5),
    ]

    selected = helpers["_select_anatomical_input_images"](images)

    assert selected == images[:2]


def test_non_mp2rage_selection_preserves_all_magnitude_images():
    helpers = _mp2rage_selection_helpers()
    images = [
        _selection_image(helpers, "MPRAGE_T1W", protocol="MPRAGE"),
        _selection_image(helpers, "GRE_T1W", protocol="GRE"),
    ]

    assert helpers["_select_anatomical_input_images"](images) == images


def test_synthseg_crop_options_are_mutually_exclusive_and_aligned():
    helpers = _synthseg_command_helpers()
    resolve = helpers["_resolve_synthseg_crop_options"]

    assert resolve(-1) == (False, 0)
    assert resolve(0) == (True, 0)
    assert resolve(192) == (False, 192)
    assert resolve(193) == (False, 224)
    assert resolve(-2) == (False, 0)


def test_synthseg_command_uses_autocrop_or_manual_crop():
    helpers = _synthseg_command_helpers()
    build_command = helpers["_build_synthseg_command"]
    common = {
        "model": "synthseg",
        "fast": True,
        "parcellation": True,
        "use_gpu": False,
        "threads": 8,
    }

    automatic = build_command(
        Path("input.nii.gz"),
        Path("output.nii.gz"),
        autocrop=True,
        crop_size=0,
        **common,
    )
    assert "--autocrop" in automatic
    assert "--crop" not in automatic
    assert "--cpu" in automatic

    manual = build_command(
        Path("input.nii.gz"),
        Path("output.nii.gz"),
        autocrop=False,
        crop_size=192,
        **common,
    )
    assert "--autocrop" not in manual
    crop_index = manual.index("--crop")
    assert manual[crop_index + 1] == "192"


def test_openrecon_defaults_match_the_scanner_label():
    """The wrapper reads every scanner parameter by id, so the two must agree.

    A parameter present in only one of the two files is silently ignored at the
    scanner: the GUI would offer a control the wrapper never reads, or the
    wrapper would fall back to a default the operator cannot change.
    """
    helpers = _openrecon_helpers()
    label = json.loads(LABEL_PATH.read_text())

    label_ids = [parameter["id"] for parameter in label["parameters"]]
    assert len(label_ids) == len(set(label_ids)), "duplicate parameter ids in label"
    assert set(label_ids) == set(helpers["OPENRECON_DEFAULTS"])
    assert len(label_ids) == 14
    assert "ssdebugthresholdsegment" in label_ids

    for parameter in label["parameters"]:
        default = helpers["OPENRECON_DEFAULTS"][parameter["id"]]
        if parameter["type"] == "boolean":
            assert isinstance(default, bool)
            assert default == parameter["default"]
        elif parameter["type"] == "int":
            assert isinstance(default, int) and not isinstance(default, bool)
            assert default == parameter["default"]
            assert parameter["minimum"] <= default <= parameter["maximum"]
        elif parameter["type"] == "choice":
            assert default == parameter["default"]
            assert default in {value["id"] for value in parameter["values"]}


def test_label_model_choices_have_installed_weights():
    helpers = _openrecon_helpers()
    label = json.loads(LABEL_PATH.read_text())
    ssmodel = next(p for p in label["parameters"] if p["id"] == "ssmodel")

    choice_ids = {value["id"] for value in ssmodel["values"]}
    assert choice_ids == set(helpers["SYNTHSEG_MODEL_FILES"])
    assert choice_ids == set(helpers["OPENRECON_MODEL_VALUES"])
    assert helpers["OPENRECON_MODEL_DEFAULT"] in choice_ids


@pytest.mark.parametrize("choice, expected", [
    ("none", (False, False)),
    ("desikan", (True, False)),
    ("glasser", (False, True)),
    ("both", (True, True)),
    (False, (False, False)),
    (True, (True, False)),
    ("false", (False, False)),
    ("true", (True, False)),
])
def test_parcellation_choice_resolves_scanner_and_saved_configs(choice, expected):
    resolve = _openrecon_helpers()["_resolve_openrecon_parcellation"]
    assert resolve(choice) == expected


@pytest.mark.parametrize("choice", ["unknown", "", None, 1, [], {}])
def test_parcellation_choice_rejects_invalid_configs(choice):
    resolve = _openrecon_helpers()["_resolve_openrecon_parcellation"]
    with pytest.raises(ValueError, match="Invalid ssparc choice"):
        resolve(choice)


def test_parameter_matrix_only_emits_runnable_combinations():
    """SynthSeg-robust forces fast mode, so the matrix must not claim otherwise."""
    helpers = _openrecon_helpers()
    combinations = list(helpers["iter_openrecon_parameter_combinations"]())

    assert combinations
    choices = {"none", "desikan", "glasser", "both"}
    assert {config["parameters"]["ssparc"] for _, config in combinations} == choices
    for name, config in combinations:
        parameters = config["parameters"]
        assert parameters["ssoutputname"] == name
        assert f"_parc-{parameters['ssparc']}_" in name
        assert parameters["ssmodel"] in helpers["OPENRECON_MODEL_VALUES"]
        if parameters["ssmodel"] == "robust":
            assert parameters["ssfast"] is True

    robust_names = {
        name
        for name, config in combinations
        if config["parameters"]["ssmodel"] == "robust"
    }
    assert all("_fast1" in name for name in robust_names)


def _inference_helpers():
    import logging
    import shutil
    import subprocess
    from time import perf_counter

    helpers = _load_runtime_helpers_for_test(
        ["_run_synthseg_command", "_log_synthseg_resources", "_log_directory_contents"]
    )
    helpers.update(
        logging=logging, shutil=shutil, subprocess=subprocess, perf_counter=perf_counter
    )
    return helpers


def test_gpu_failure_retries_cpu_without_partial_outputs(tmp_path, caplog):
    import logging
    import sys

    helpers = _inference_helpers()
    helpers["_log_synthseg_resources"] = lambda *args: None
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    program = tmp_path / "inference.py"
    program.write_text('''
import json, os, sys
from pathlib import Path
out = Path(sys.argv[1])
with (out.parent / "attempts.jsonl").open("a") as log:
    log.write(json.dumps(sys.argv[2:]) + "\\n")
if "--cpu" not in sys.argv:
    (out / "partial.csv").write_text("incomplete")
    (out / "base.nii.gz").write_text("incomplete")
    (out / "desikan.nii.gz").write_text("incomplete")
    print("ResourceExhaustedError: GPU OOM", file=sys.stderr)
    sys.exit(1)
assert os.environ["CUDA_VISIBLE_DEVICES"] == "-1"
assert not any(out.iterdir())
(out / "seg.nii.gz").write_text("completed on CPU")
''')
    command = [sys.executable, str(program), str(output_dir), "--fast", "--parc", "--qc", "--base-output", str(output_dir / "base.nii.gz")]
    with caplog.at_level(logging.INFO):
        helpers["_run_synthseg_command"](command, output_dir)
    attempts = [json.loads(line) for line in (tmp_path / "attempts.jsonl").read_text().splitlines()]
    assert attempts == [command[3:], command[3:] + ["--cpu"]]
    assert (output_dir / "seg.nii.gz").read_text() == "completed on CPU"
    assert "GPU OOM" in (tmp_path / "synthseg_gpu.stderr.log").read_text()
    assert (tmp_path / "synthseg_cpu.stdout.log").exists()
    assert "retrying once on CPU" in caplog.text
    assert "device=cpu finished" in caplog.text


def test_inference_attempt_counts_and_final_error(tmp_path):
    import subprocess
    import sys

    import pytest

    helpers = _inference_helpers()
    helpers["_log_synthseg_resources"] = lambda *args: None
    for cpu, exitcode, expected_attempts in [(False, 0, 1), (True, 0, 1), (False, 3, 2), (True, 3, 1)]:
        work = tmp_path / f"{cpu}-{exitcode}"
        out = work / "output"
        out.mkdir(parents=True)
        program = (
            "import sys; from pathlib import Path; "
            f"p=Path({str(work / 'attempts')!r}); "
            "p.open('a').write('attempt\\n'); "
            "print('failure detail', file=sys.stderr); "
            f"sys.exit({exitcode})"
        )
        command = [sys.executable, "-c", program] + (["--cpu"] if cpu else [])
        if exitcode:
            with pytest.raises(subprocess.CalledProcessError) as error:
                helpers["_run_synthseg_command"](command, out)
            assert error.value.returncode == exitcode
            assert "--cpu" in error.value.cmd
            assert "failure detail" in error.value.stderr
        else:
            helpers["_run_synthseg_command"](command, out)
        assert len((work / "attempts").read_text().splitlines()) == expected_attempts


def test_gpu_telemetry_is_best_effort(monkeypatch, caplog):
    import logging
    import os
    import subprocess

    helpers = _inference_helpers()
    for failure in [FileNotFoundError("no nvidia-smi"), subprocess.TimeoutExpired("nvidia-smi", 2)]:
        def fail(*args, **kwargs):
            raise failure
        monkeypatch.setattr(subprocess, "run", fail)
        with caplog.at_level(logging.INFO):
            helpers["_log_synthseg_resources"](os.getpid(), True)
        assert "GPU telemetry unavailable" in caplog.text
        assert "VmRSS:" in caplog.text


def _output_helpers(additional=()):
    tree = ast.parse(WRAPPER_PATH.read_text())
    definitions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    assignments = {
        target.id: node for node in tree.body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
    }
    wanted = {'_build_openrecon_output_identity', '_build_synthseg_segmentation_images',
              '_build_synthseg_original_images', '_validate_synthseg_output_contract'}
    wanted.update(additional)
    constants = set()
    while True:
        referenced = {
            node.id for name in wanted | constants
            for node in ast.walk((definitions | assignments)[name])
            if isinstance(node, ast.Name)
        }
        next_functions = wanted | (referenced & definitions.keys())
        next_constants = constants | (referenced & assignments.keys())
        if (next_functions, next_constants) == (wanted, constants):
            break
        wanted, constants = next_functions, next_constants
    return _load_runtime_helpers_for_test(wanted, constants)


@pytest.mark.parametrize("segmentation_header", [False, True])
def test_label_products_keep_pixels_geometry_and_distinct_scanner_identity(segmentation_header):
    helpers = _output_helpers()
    source = _source_image(helpers)
    head = source.getHead()
    head.position = [12.0, -7.0, 34.0]
    head.read_dir = [0.0, 1.0, 0.0]
    head.phase_dir = [-1.0, 0.0, 0.0]
    source.setHead(head)
    source_identity = {'series_description': 'source_t1w', 'parent_grouping': 'source_group',
                       'series_uid': '1.2.3', 'sop_uid': '1.2.3.4', 'source_type_token': 'ND'}
    labels = np.array([0, 7, 189, 360], dtype=np.int16).reshape(2, 2, 1)
    outputs = []
    for series, product in [(3, 'synthseg'), (4, 'desikan'), (5, 'glasser_approx')]:
        identity = helpers['_build_openrecon_output_identity'](
            source_identity, series_index=series, product=product,
        )
        images = helpers['_build_synthseg_segmentation_images'](
            labels, [source], source_identity, identity, series, 360,
            segmentation_header=segmentation_header,
        )
        outputs.extend(images)
        image = images[0]
        meta = helpers['FakeMeta'].deserialize(image.attribute_string)
        np.testing.assert_array_equal(image.data[0, 0], labels[:, :, 0])
        np.testing.assert_array_equal(image.getHead().position, source.getHead().position)
        assert meta['LUTFileName'] == 'MicroDeltaHotMetal.pal'
        assert meta['SeriesDescription'] == f'source_t1w_{product}'
        assert meta['SeriesNumberRangeNameUID'] == f'source_group_{product}'
        if segmentation_header:
            assert meta['ImageType'] == f'DERIVED\\PRIMARY\\SEGMENTATION\\{product}_source_geometry'
            assert meta['ImageTypeValue4'] == f'{product}_source_geometry'
        else:
            assert meta['ImageType'] == 'ORIGINAL\\PRIMARY\\M\\ND'
            assert meta['ImageTypeValue4'] == ['ND']
        assert meta['LabelProduct'] == product
        assert meta['ImageProcessingHistory'][1] == product.upper()
    metas = [helpers['FakeMeta'].deserialize(image.attribute_string) for image in outputs]
    assert metas[0]['SeriesInstanceUID'] != metas[1]['SeriesInstanceUID']
    assert metas[0]['SOPInstanceUID'] != metas[1]['SOPInstanceUID']
    helpers['_validate_synthseg_output_contract'](outputs, {1}, source_identity, 'test products')
    originals = helpers['_build_synthseg_original_images']([source], [source], source_identity, 2)
    assert 'LUTFileName' not in helpers['FakeMeta'].deserialize(originals[0].attribute_string)


@pytest.fixture
def process_runtime(monkeypatch, tmp_path):
    import logging
    import shutil
    import sys
    import traceback
    import types
    import ismrmrd
    import nibabel as nib
    import scipy.ndimage as ndi

    helpers = _output_helpers({'process_image'})
    helpers.update(ismrmrd=ismrmrd, nib=nib, ndi=ndi, logging=logging,
                   shutil=shutil, traceback=traceback, debugFolder=str(tmp_path))
    helpers['mrdhelper'] = types.SimpleNamespace(
        get_json_config_param=lambda config, key, default=None, **kwargs: config['parameters'].get(key, default),
        extract_minihead_string_param=lambda *args: '',
    )
    helpers['_resolve_synthseg_models'] = lambda *args: []
    base = np.array([0, 2, 3, 42, 17, 53, 3, 42] * 4, dtype=np.int16).reshape(4, 4, 2)
    # The parcellation network can disagree with the segmentation hemisphere.
    desikan = np.array([0, 2, 2001, 1001, 17, 53, 1002, 2002] * 4, dtype=np.int16).reshape(4, 4, 2)
    glasser = np.where(base == 3, 7, np.where(base == 42, 189, 0)).astype(np.int16)
    maps = dict(synthseg=base, desikan=desikan, glasser_approx=glasser)

    def inference(command, output_dir):
        affine = nib.load(command[command.index('--i') + 1]).affine
        labels = desikan if '--parc' in command else base
        nib.save(nib.Nifti1Image(labels, affine), command[command.index('--o') + 1])
        if '--base-output' in command:
            nib.save(nib.Nifti1Image(base, affine), command[command.index('--base-output') + 1])

    def register(input_path, segmentation, output_path, **kwargs):
        segmentation_image = nib.load(segmentation)
        np.testing.assert_array_equal(segmentation_image.dataobj, base)
        nib.save(nib.Nifti1Image(glasser, segmentation_image.affine), output_path)

    helpers['_run_synthseg_command'] = inference
    monkeypatch.setitem(sys.modules, 'glasser', types.SimpleNamespace(register_glasser=register))
    images = []
    for index in range(2):
        image = ismrmrd.Image.from_array(np.ones((1, 1, 4, 4), dtype=np.int16), transpose=False)
        head = image.getHead()
        head.field_of_view = (4., 4., 1.)
        head.position = (0., 0., float(index))
        head.read_dir, head.phase_dir, head.slice_dir = (1., 0., 0.), (0., 1., 0.), (0., 0., 1.)
        head.image_series_index, head.image_index, head.slice = 1, index + 1, index
        image.setHead(head)
        meta = ismrmrd.Meta()
        meta['SeriesDescription'], meta['SeriesNumberRangeNameUID'] = 't1w', 't1w_group'
        meta['SeriesInstanceUID'] = '1.2.3'
        meta['ImageType'], meta['ImageTypeValue4'] = 'ORIGINAL\\PRIMARY\\M\\ND', 'ND'
        image.attribute_string = meta.serialize()
        images.append(image)
    return helpers, images, maps


@pytest.mark.parametrize('choice,products', [
    ('none', ['synthseg']), ('desikan', ['synthseg', 'desikan']),
    ('glasser', ['synthseg', 'glasser_approx']),
    ('both', ['synthseg', 'desikan', 'glasser_approx']),
])
@pytest.mark.parametrize('segment_header', [False, True])
@pytest.mark.parametrize('extras', [False, True])
def test_process_image_returns_base_and_selected_products(process_runtime, choice, products, segment_header, extras):
    import ismrmrd
    helpers, sources, maps = process_runtime
    config = {'parameters': dict(helpers['OPENRECON_DEFAULTS'], ssparc=choice,
                                 sendoriginal=extras, sssegmentheader=segment_header,
                                 ssreslicesagittal=extras, ssreslicecoronal=extras)}
    outputs = helpers['process_image'](sources, None, config, None)
    series = {}
    for image in outputs:
        series.setdefault(image.getHead().image_series_index, []).append(image)
    metas = [ismrmrd.Meta.deserialize(images[0].attribute_string) for images in series.values()]
    native = [(images, meta) for images, meta in zip(series.values(), metas)
              if meta.get('LabelProduct') and not meta.get('SynthSegReformatOrientation')]
    assert [meta['LabelProduct'] for _, meta in native] == products
    for images, meta in native:
        product = meta['LabelProduct']
        np.testing.assert_array_equal(np.stack([image.data[0, 0].T for image in images], axis=-1), maps[product])
        assert meta['SeriesDescription'] == f't1w_{product}'
        assert meta['SeriesNumberRangeNameUID'] == f't1w_{product}'
        assert meta['LUTFileName'] == 'MicroDeltaHotMetal.pal'
        if product == 'glasser_approx':
            assert meta['WindowWidth'] == '361'
        expected_type = f'DERIVED\\PRIMARY\\SEGMENTATION\\{product}_source_geometry' if segment_header else 'ORIGINAL\\PRIMARY\\M\\ND'
        assert meta['ImageType'] == expected_type
        for source, image in zip(sources, images):
            assert tuple(image.getHead().position) == tuple(source.getHead().position)
    assert len({meta['SeriesInstanceUID'] for meta in metas}) == len(series)
    assert len({ismrmrd.Meta.deserialize(image.attribute_string)['SOPInstanceUID'] for image in outputs}) == len(outputs)
    assert list(series) == list(range(2, 2 + len(series)))
    if extras:
        assert metas[0].get('LabelProduct') is None
        for source, image in zip(sources, next(iter(series.values()))):
            np.testing.assert_array_equal(source.data, image.data)
        reformats = {(meta['LabelProduct'], meta['SynthSegReformatOrientation']) for meta in metas if meta.get('SynthSegReformatOrientation')}
        assert reformats == {(product, orientation) for product in products if product != 'glasser_approx'
                             for orientation in ('sagittal', 'coronal')}


@pytest.mark.parametrize('missing_name', ['input_synthseg.nii.gz', 'input_desikan.nii.gz'])
def test_process_image_rejects_missing_required_product(process_runtime, missing_name):
    helpers, sources, _ = process_runtime
    inference = helpers['_run_synthseg_command']

    def incomplete_inference(command, output_dir):
        inference(command, output_dir)
        (output_dir / missing_name).unlink()

    helpers['_run_synthseg_command'] = incomplete_inference
    config = {'parameters': dict(helpers['OPENRECON_DEFAULTS'], ssparc='desikan')}
    with pytest.raises(FileNotFoundError, match=missing_name):
        helpers['process_image'](sources, None, config, None)


@pytest.mark.parametrize('invalid', ['shape', 'fractional', 'overflow'])
def test_process_image_rejects_invalid_label_product(process_runtime, invalid):
    import nibabel as nib
    helpers, sources, _ = process_runtime
    inference = helpers['_run_synthseg_command']

    def invalid_inference(command, output_dir):
        inference(command, output_dir)
        path = output_dir / 'input_synthseg.nii.gz'
        image = nib.load(path)
        labels = np.asarray(image.dataobj).astype(np.float32)
        if invalid == 'shape':
            labels = labels[:2]
        else:
            labels[0, 0, 0] = 0.5 if invalid == 'fractional' else 32768
        nib.save(nib.Nifti1Image(labels, image.affine), path)

    helpers['_run_synthseg_command'] = invalid_inference
    config = {'parameters': dict(helpers['OPENRECON_DEFAULTS'], ssparc='desikan')}
    with pytest.raises(ValueError, match='Label product'):
        helpers['process_image'](sources, None, config, None)


def test_missing_source_type_uses_each_product_identity(process_runtime):
    import ismrmrd
    helpers, sources, _ = process_runtime
    for source in sources:
        meta = ismrmrd.Meta.deserialize(source.attribute_string)
        del meta['ImageType']
        del meta['ImageTypeValue4']
        source.attribute_string = meta.serialize()
    config = {'parameters': dict(helpers['OPENRECON_DEFAULTS'], ssparc='both', sendoriginal=False)}
    outputs = helpers['process_image'](sources, None, config, None)
    for image in outputs:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        product = meta['LabelProduct']
        assert meta['ImageType'] == f'DERIVED\\PRIMARY\\SEGMENTATION\\{product}_source_image_header'
        assert meta['ImageTypeValue4'] == f'{product}_source_image_header'
