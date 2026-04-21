#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import json
import re
from pathlib import Path
from typing import Any


MARKERS = (
    "INPUT_VOLUME_SUMMARY",
    "MUSCLEMAP_OUTPUT_IDENTITY",
    "SEND_BATCH_SUMMARY",
)


def _strip_line_suffix(payload: str) -> str:
    return payload.rstrip("|\uFFFD").strip()


def _extract_json_events(text: str, marker: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in text.splitlines():
        needle = f"{marker} "
        if needle not in line:
            continue
        payload = line.split(needle, 1)[1]
        payload = _strip_line_suffix(payload)
        try:
            events.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    return events


def _extract_digest(text: str) -> str | None:
    match = re.search(r"musclemap@sha256:([0-9a-f]{64})", text)
    return match.group(1) if match else None


def _extract_metadata_blob(text: str) -> dict[str, Any] | None:
    pattern = re.compile(
        r'com\.siemens-healthineers\.magneticresonance\.openrecon\.metadata:1\.1\.0": "([A-Za-z0-9+/=]+)"'
    )
    for match in pattern.finditer(text):
        try:
            decoded = base64.b64decode(match.group(1)).decode("utf-8", errors="replace")
            payload = json.loads(decoded)
        except Exception:
            continue
        if payload.get("general", {}).get("id") == "musclemap":
            return payload
    return None


def _extract_run_times(text: str) -> dict[str, str | None]:
    started = None
    finished = None
    for line in text.splitlines():
        if started is None and "Marshaller:firstCallImpl() - Image: musclemap V1" in line:
            started = line.split("|", 2)[1]
        if "REQUEST_SHUTDOWN" in line and "musclemap@sha256:" in line:
            finished = line.split("|", 2)[1]
            break
    return {
        "scanner_start": started,
        "scanner_shutdown_request": finished,
    }


def _pick_latest_water_input(inputs: list[dict[str, Any]]) -> dict[str, Any] | None:
    water_events = [
        event
        for event in inputs
        if event.get("resolved_ImageTypeValue4") == "WATER" and event.get("should_process") is True
    ]
    return water_events[-1] if water_events else None


def _pick_latest_segmentation_output(outputs: list[dict[str, Any]]) -> dict[str, Any] | None:
    musclemap_events = [
        event
        for event in outputs
        if event.get("display_label") == "Musclemap"
    ]
    return musclemap_events[-1] if musclemap_events else None


def _pick_latest_send_batch(summaries: list[dict[str, Any]]) -> dict[str, Any] | None:
    connection_events = [
        event
        for event in summaries
        if event.get("context") == "connection_total"
    ]
    return connection_events[-1] if connection_events else (summaries[-1] if summaries else None)


def _extract_series_uid_events(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.splitlines():
        if "Creating new Series Number" in line or "Created new SeriesInstanceUID" in line:
            lines.append(line)
    return lines


def build_summary(text: str) -> dict[str, Any]:
    inputs = _extract_json_events(text, "INPUT_VOLUME_SUMMARY")
    outputs = _extract_json_events(text, "MUSCLEMAP_OUTPUT_IDENTITY")
    summaries = _extract_json_events(text, "SEND_BATCH_SUMMARY")
    metadata_blob = _extract_metadata_blob(text)

    return {
        "image_digest": _extract_digest(text),
        "container_metadata": metadata_blob,
        "run_times": _extract_run_times(text),
        "latest_water_input": _pick_latest_water_input(inputs),
        "latest_segmentation_output": _pick_latest_segmentation_output(outputs),
        "latest_send_batch_summary": _pick_latest_send_batch(summaries),
        "series_uid_events": _extract_series_uid_events(text),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize MuscleMap Composer naming evidence from a scanner log.")
    parser.add_argument("logfile", type=Path)
    args = parser.parse_args()

    text = args.logfile.read_text(errors="replace")
    summary = build_summary(text)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
