"""Resolve versioned model records from Zenodo's published record API."""

import re

import requests

from .update_sources import configure_read_retries


def validate_record_source(config: dict) -> None:
    if set(config) != {"method", "record"} or not re.fullmatch(
        r"[1-9][0-9]*", str(config.get("record", ""))
    ):
        raise ValueError("zenodo requires a published numeric record identifier")


def observe_record(config: dict):
    from .update_observations import SourceObservation

    validate_record_source(config)
    url = f"https://zenodo.org/api/records/{config['record']}/versions/latest"
    with requests.Session() as session:
        configure_read_retries(session)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, dict) or not re.fullmatch(r"[1-9][0-9]*", str(data.get("id", ""))):
        raise ValueError("Zenodo returned no published record ID")
    if not isinstance(data.get("files"), list) or not data["files"]:
        raise ValueError("Zenodo record has no published files")
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("Zenodo record has no usable metadata")
    version = str(metadata.get("version", "")).lstrip("vV")
    if not re.fullmatch(r"[0-9][A-Za-z0-9._+-]*", version):
        raise ValueError("Zenodo record has no usable model version")
    return SourceObservation(
        str(data["id"]),
        f"https://zenodo.org/records/{data['id']}",
        metadata={"version": version},
    )
