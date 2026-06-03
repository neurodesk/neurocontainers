from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from typing import Any


VOLATILE_LABELS = {
    "GITHUB_REPOSITORY",
    "GITHUB_SHA",
}


def normalize_inspect_data(image: dict[str, Any]) -> dict[str, Any]:
    config = dict(image.get("Config") or {})
    labels = dict(config.get("Labels") or {})
    for label in VOLATILE_LABELS:
        labels.pop(label, None)
    config["Labels"] = labels

    return {
        "Config": config,
        "RootFS": image.get("RootFS"),
    }


def fingerprint_inspect_data(image: dict[str, Any]) -> str:
    payload = normalize_inspect_data(image)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def inspect_image(image_ref: str) -> dict[str, Any]:
    result = subprocess.run(
        ["docker", "inspect", image_ref],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    inspected = json.loads(result.stdout)
    if not inspected:
        raise ValueError(f"No docker inspect data found for {image_ref}")
    return inspected[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print a stable Docker image fingerprint for release build comparisons."
    )
    parser.add_argument("image", help="Docker image reference to inspect")
    args = parser.parse_args(argv)

    try:
        print(fingerprint_inspect_data(inspect_image(args.image)))
    except (subprocess.CalledProcessError, ValueError, json.JSONDecodeError) as exc:
        print(f"Failed to fingerprint {args.image}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
