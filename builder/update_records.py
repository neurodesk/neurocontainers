"""Resolve versioned model records from Zenodo's published record API."""

import re
from urllib.parse import urlsplit

import requests

from .update_sources import configure_read_retries


def validate_record_source(config: dict) -> None:
    if (
        config.get("method") != "zenodo"
        or set(config) - {"method", "record", "required_files"}
        or not re.fullmatch(r"[1-9][0-9]*", str(config.get("record", "")))
    ):
        raise ValueError("zenodo requires a published numeric record identifier")
    if "required_files" in config:
        files = config["required_files"]
        if not isinstance(files, list) or not files or any(
            not isinstance(name, str) or not name.strip() for name in files
        ):
            raise ValueError("zenodo required_files must be a nonempty list of filenames")


def _compatible_record(data: dict, required_files: list[str]) -> bool:
    files = data.get("files", [])
    return isinstance(files, list) and set(required_files).issubset(
        file.get("key") for file in files if isinstance(file, dict)
    )


def _latest_compatible_record(session, config: dict, latest: dict) -> dict:
    required = config.get("required_files", [])
    if not required or _compatible_record(latest, required):
        return latest
    url = f"https://zenodo.org/api/records/{config['record']}/versions"
    visited = set()
    while url:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.netloc != "zenodo.org" or url in visited:
            raise ValueError("Zenodo returned an invalid version pagination URL")
        visited.add(url)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        page = response.json()
        records = page.get("hits", {}).get("hits") if isinstance(page, dict) else None
        if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
            raise ValueError("Zenodo returned an invalid version list")
        for record in records:
            if _compatible_record(record, required):
                return record
        url = page.get("links", {}).get("next")
    raise ValueError("Zenodo has no published record containing the required files")


def observe_record(config: dict):
    from .update_observations import SourceObservation

    validate_record_source(config)
    url = f"https://zenodo.org/api/records/{config['record']}/versions/latest"
    with requests.Session() as session:
        configure_read_retries(session)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            data = _latest_compatible_record(session, config, data)
    if not isinstance(data, dict) or not re.fullmatch(r"[1-9][0-9]*", str(data.get("id", ""))):
        raise ValueError("Zenodo returned no published record ID")
    if not isinstance(data.get("files"), list) or not data["files"]:
        raise ValueError("Zenodo record has no published files")
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("Zenodo record has no usable metadata")
    version = str(metadata["version"]).lstrip("vV") if "version" in metadata else None
    if version is not None and not re.fullmatch(r"[0-9][A-Za-z0-9._+-]*", version):
        raise ValueError("Zenodo record has no usable model version")
    return SourceObservation(
        str(data["id"]),
        f"https://zenodo.org/records/{data['id']}",
        metadata={"version": version} if version is not None else {},
    )
