"""Metadata for processed MRD images returned to the OpenRecon scanner writer.

Source references here are MRD provenance, not a DICOM Source Image Sequence.
The scanner writer determines which attributes reach the exported DICOM.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import uuid
from pathlib import Path


def new_uid() -> str:
    return f"2.25.{uuid.uuid4().int}"


def processing_metadata(software: str, adapter: str, parameters: dict) -> dict[str, str]:
    encoded = json.dumps(parameters, sort_keys=True, separators=(",", ":"), default=str)
    return {
        "OpenReconSoftware": software,
        "OpenReconSoftwareVersion": os.environ.get(
            "OPENRECON_SOFTWARE_VERSION", "unknown",
        ),
        "OpenReconContainerVersion": os.environ.get(
            "OPENRECON_CONTAINER_VERSION", "unknown",
        ),
        "OpenReconAdapterSHA256": hashlib.sha256(Path(adapter).read_bytes()).hexdigest(),
        "OpenReconHelperSHA256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "OpenReconParameters": encoded,
        "OpenReconParametersSHA256": hashlib.sha256(encoded.encode()).hexdigest(),
    }


def _text(meta, name: str) -> str:
    value = meta.get(name, "")
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    return str(value or "")


def _source_uid(meta, name: str) -> str:
    value = _text(meta, name)
    if value:
        return value
    try:
        minihead = base64.b64decode(_text(meta, "IceMiniHead")).decode()
    except (ValueError, UnicodeError):
        return ""
    match = re.search(rf'<ParamString\."{name}">\s*\{{\s*"([^"]*)"', minihead)
    return match.group(1) if match else ""


def stamp_derived(
    meta, source_meta, product: str, series_uid: str, processing: dict,
    *, image_type_as_list: bool = False,
) -> None:
    """Stamp scanner metadata; the final writer owns multi-frame SOP allocation."""
    if not re.fullmatch(r"[A-Z0-9_]{1,16}", product):
        raise ValueError(f"Invalid OpenRecon ImageType product token: {product!r}")
    for key in list(meta):
        if key in {
            "IceMiniHead", "ImageTypeValue3", "DicomEngineDimString", "MFInstanceNumber",
            "MultiFrameSOPInstanceUID", "PSMultiFrameSOPInstanceUID", "PSSeriesInstanceUID",
            "SOPClassUID", "SOPInstanceUID",
        } or key.startswith(("ReferencedGSPS", "ReferencedImageSequence", "OpenRecon")):
            del meta[key]
    meta.update(processing)
    image_type = f"DERIVED\\PRIMARY\\M\\{product}"
    meta.update({
        "ImageType": image_type.split("\\") if image_type_as_list else image_type,
        "DicomImageType": image_type,
        "ImageTypeValue4": product,
        "ComplexImageComponent": "MAGNITUDE",
        "SeriesInstanceUID": series_uid,
        "OpenReconImageUID": new_uid(),
        "OpenReconProduct": product,
    })
    for name in ("SeriesInstanceUID", "SOPInstanceUID", "SOPClassUID"):
        value = _source_uid(source_meta, name)
        if value:
            meta[f"OpenReconGeometrySource{name}"] = value
    summary = (
        f"OpenRecon {processing['OpenReconSoftware']} "
        f"{processing['OpenReconSoftwareVersion']}; product={product}; "
        f"parameters_sha256={processing['OpenReconParametersSHA256']}"
    )
    meta["DerivationDescription"] = (
        f"{summary}; adapter_sha256={processing['OpenReconAdapterSHA256']}; "
        f"helper_sha256={processing['OpenReconHelperSHA256']}"
    )
    comment = _text(meta, "ImageComments") or _text(meta, "ImageComment")
    for name in ("ImageComment", "ImageComments"):
        meta[name] = f"{comment}; {summary}" if comment else summary
