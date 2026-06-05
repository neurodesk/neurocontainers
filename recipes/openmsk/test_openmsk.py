import base64
import json
import sys
import types
from pathlib import Path

import ismrmrd
import numpy as np


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
    "M"
}
<ParamArray."ImageTypeValue4">
{
    "NONE"
}
<ParamLong."SliceNo">\t{ 99 }
"""
    meta = ismrmrd.Meta()
    meta["SeriesDescription"] = "source_series"
    meta["SeriesNumberRangeNameUID"] = "source_group"
    meta["SOPInstanceUID"] = "1.2.3.4"
    meta["ImageTypeValue3"] = "M"
    meta["IceMiniHead"] = base64.b64encode(minihead.encode("utf-8")).decode("ascii")
    image.attribute_string = meta.serialize()
    return image


def decoded_minihead(meta):
    return base64.b64decode(first(meta, "IceMiniHead")).decode("utf-8")


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
    assert first(meta, "NumberInSeries") == "3"
    assert first(meta, "SliceNo") == "2"
    assert '<ParamString."SeriesDescription">\t{ "openmsk_segmentation" }' in minihead
    assert '<ParamString."SeriesNumberRangeNameUID">\t{ "openmsk_segmentation_101" }' in minihead
    assert '<ParamString."SOPInstanceUID">\t{ "1.2.3.4" }' not in minihead
    assert "ImageTypeValue3" not in minihead
    assert '<ParamLong."SliceNo">\t{ 2 }' in minihead


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
