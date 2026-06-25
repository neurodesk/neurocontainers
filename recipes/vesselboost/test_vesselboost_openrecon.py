import ast
import copy
import ctypes
import json
import re
import uuid
from pathlib import Path

import numpy as np


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "vesselboost.py"


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
        "re": re,
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
            "_derived_vesselboost_instance_uid",
            "_derived_vesselboost_series_uid",
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
            "_stamp_vesselboost_output_image",
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
            "VESSELBOOST_OUTPUT_GEOMETRY_2D",
            "VESSELBOOST_REFORMAT_ORIENTATION_META_KEY",
            "VESSELBOOST_REFORMAT_SLICE_COUNT_META_KEY",
            "VESSELBOOST_REFORMAT_SLICE_INDEX_META_KEY",
            "VESSELBOOST_SEGMENT_OUTPUT_GEOMETRY_META_KEY",
            "VESSELBOOST_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY",
            "VESSELBOOST_SEGMENT_POSTPROCESSING_META_KEY",
            "VESSELBOOST_SEGMENT_SOURCE_GEOMETRY_META_KEY",
            "VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "VESSELBOOST_SEGMENTATION_LABEL",
            "VESSELBOOST_SEGMENTATION_TYPE_TOKEN",
            "VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE",
            "VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4",
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
            "SeriesDescription": "source_tof",
            "SequenceDescription": "source_tof",
            "ProtocolName": "source_tof",
            "SeriesNumberRangeNameUID": "source_group",
            "SeriesInstanceUID": "1.2.3",
            "SOPInstanceUID": "1.2.3.4",
            "ImageType": "ORIGINAL\\PRIMARY\\M\\TOF",
            "DicomImageType": "ORIGINAL\\PRIMARY\\M\\TOF",
            "ImageTypeValue3": "M",
            "ImageTypeValue4": "TOF",
            "Keep_image_geometry": "1",
        }
    ).serialize()
    return image


def test_reformat_outputs_use_2d_segmentation_header_contract():
    helpers = _helpers()
    source_image = _source_image(helpers)
    source_identity = {
        "series_description": "source_tof",
        "parent_grouping": "source_group",
        "series_uid": "1.2.3",
        "sop_uid": "1.2.3.4",
        "source_type_token": "TOF",
    }
    output_identity = {
        "series_description": "source_tof_vesselboost_coronal",
        "sequence_description": "source_tof_vesselboost_coronal",
        "grouping": "source_group_vesselboost_coronal",
        "display_token": "vesselboost",
        "type_token": "VESSELBOOST",
        "image_comment": "vesselboost_coronal",
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
        assert meta["VesselBoostReformatOrientation"] == "coronal"
        assert meta["VesselBoostReformatSliceIndex"] == str(index)
        assert meta["VesselBoostReformatSliceCount"] == "3"
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
