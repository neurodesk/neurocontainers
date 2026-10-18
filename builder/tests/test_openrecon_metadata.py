import base64
import importlib.util
import json
import re
from pathlib import Path

import pytest


_path = Path(__file__).resolve().parents[2] / "macros/openrecon/openrecon_metadata.py"
_spec = importlib.util.spec_from_file_location("openrecon_metadata", _path)
metadata = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(metadata)


def test_derived_identity_preserves_scaling_and_records_processing(monkeypatch):
    monkeypatch.setenv("OPENRECON_SOFTWARE_VERSION", "2.3.4")
    monkeypatch.setenv("OPENRECON_CONTAINER_VERSION", "2.3.4")
    source = {
        "ImageType": "ORIGINAL\\PRIMARY\\M\\ND",
        "SeriesInstanceUID": "1.2.3",
        "SOPInstanceUID": "1.2.3.1",
        "SoftwareVersions": "scanner-version",
        "ImageTypeValue3": "M",
        "IceMiniHead": "old geometry",
    }
    output = dict(source, RescaleSlope="0.001", ImageComment="ppm = display / 1000")
    processing = metadata.processing_metadata("example", str(_path), {"model": "fold0"})
    series_uid = metadata.new_uid()

    metadata.stamp_derived(output, source, "OR_TEST_MAP", series_uid, processing)

    assert output["ImageType"] == "DERIVED\\PRIMARY\\M\\OR_TEST_MAP"
    assert output["DicomImageType"] == output["ImageType"]
    assert output["SeriesInstanceUID"] == series_uid
    assert "SOPInstanceUID" not in output
    assert re.fullmatch(r"2\.25\.\d{1,39}", output["OpenReconImageUID"])
    assert output["SoftwareVersions"] == "scanner-version"
    assert output["RescaleSlope"] == "0.001"
    assert output["OpenReconGeometrySourceSOPInstanceUID"] == "1.2.3.1"
    assert output["OpenReconSoftwareVersion"] == "2.3.4"
    assert json.loads(output["OpenReconParameters"]) == {"model": "fold0"}
    assert "product=OR_TEST_MAP" in output["DerivationDescription"]
    assert output["ImageComment"].startswith(
        "ppm = display / 1000; OpenRecon example 2.3.4"
    )
    assert "ImageTypeValue3" not in output
    assert "IceMiniHead" not in output
    assert source["ImageType"] == "ORIGINAL\\PRIMARY\\M\\ND"

    second = dict(source)
    metadata.stamp_derived(second, source, "OR_TEST_MAP", series_uid, processing)
    assert second["SeriesInstanceUID"] == output["SeriesInstanceUID"]
    assert second["OpenReconImageUID"] != output["OpenReconImageUID"]


@pytest.mark.parametrize(
    "token", ["", "lowercase", "OPENRECON_TOO_LONG", "WITH-HYPHEN"]
)
def test_invalid_scanner_product_is_rejected(token):
    with pytest.raises(ValueError, match="ImageType product"):
        metadata.stamp_derived({}, {}, token, metadata.new_uid(), {})


def test_source_reference_can_be_read_from_scanner_minihead():
    source = {
        "IceMiniHead": base64.b64encode(
            b'<ParamString."SOPInstanceUID">{ "1.2.3.4" }'
        ).decode()
    }
    output = {}
    processing = metadata.processing_metadata("example", str(_path), {})
    metadata.stamp_derived(
        output, source, "OR_TEST_MAP", metadata.new_uid(), processing
    )
    assert output["OpenReconGeometrySourceSOPInstanceUID"] == "1.2.3.4"


def test_large_parameters_stay_complete_outside_the_short_derivation_description():
    parameters = {"model_provenance": "x" * 12000}
    processing = metadata.processing_metadata("example", str(_path), parameters)
    output = {"SOPClassUID": "1.2.840.10008.5.1.4.1.1.4"}
    source = dict(output)
    metadata.stamp_derived(
        output, source, "OR_TEST_MAP", metadata.new_uid(), processing
    )
    assert len(output["DerivationDescription"]) <= 1024
    assert json.loads(output["OpenReconParameters"]) == parameters
    assert "SOPClassUID" not in output
    assert output["OpenReconGeometrySourceSOPClassUID"] == "1.2.840.10008.5.1.4.1.1.4"


def test_image_type_can_preserve_the_multi_value_mrd_encoding():
    output = {}
    processing = metadata.processing_metadata("example", str(_path), {})
    metadata.stamp_derived(
        output,
        {},
        "OR_TEST_MAP",
        metadata.new_uid(),
        processing,
        image_type_as_list=True,
    )
    assert output["ImageType"] == ["DERIVED", "PRIMARY", "M", "OR_TEST_MAP"]
    assert output["DicomImageType"] == "DERIVED\\PRIMARY\\M\\OR_TEST_MAP"
