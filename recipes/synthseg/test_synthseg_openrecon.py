import ast
import copy
import ctypes
import itertools
import json
import re
import uuid
from pathlib import Path

import numpy as np


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
            "_parse_model_list",
            "_safe_path_component",
            "iter_openrecon_parameter_combinations",
        ],
        assignments=[
            "OPENRECON_DEFAULTS",
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


def test_parameter_matrix_only_emits_runnable_combinations():
    """SynthSeg-robust forces fast mode, so the matrix must not claim otherwise."""
    helpers = _openrecon_helpers()
    combinations = list(helpers["iter_openrecon_parameter_combinations"]())

    assert combinations
    for name, config in combinations:
        parameters = config["parameters"]
        assert parameters["ssoutputname"] == name
        assert parameters["ssmodel"] in helpers["OPENRECON_MODEL_VALUES"]
        if parameters["ssmodel"] == "robust":
            assert parameters["ssfast"] is True

    robust_names = {
        name
        for name, config in combinations
        if config["parameters"]["ssmodel"] == "robust"
    }
    assert all("_fast1" in name for name in robust_names)
