"""Extract validated Boutiques descriptors from a selected local SIF."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_descriptors(payload: bytes) -> dict[str, dict]:
    try:
        from boutiques.validator import validate_descriptor
    except ImportError as exc:
        raise RuntimeError(
            "Install neurocontainers[boutiques] to validate descriptors"
        ) from exc
    descriptors = {}
    with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if member.isdir():
                continue
            if not member.isfile() or path.is_absolute() or ".." in path.parts:
                raise ValueError(f"Unsafe descriptor archive entry: {member.name}")
            if path.suffix != ".json":
                continue
            if (
                len(path.parts) != 1
                or member.size > 1024 * 1024
                or path.name in descriptors
            ):
                raise ValueError(f"Invalid descriptor archive entry: {member.name}")
            descriptor = json.load(archive.extractfile(member))
            validate_descriptor(descriptor)
            descriptors[path.name] = descriptor
    if not descriptors:
        raise ValueError("The selected image has no /boutique/*.json descriptors")
    return descriptors


def extract(
    image: Path,
    output: Path,
    *,
    expected_sha256: str | None = None,
    expected_tool: str | None = None,
    expected_version: str | None = None,
    runtime: str = "apptainer",
) -> dict:
    image = image.resolve(strict=True)
    if not image.is_file():
        raise ValueError("Select a SIF file, not an extracted CVMFS sandbox directory")
    digest = file_digest(image)
    if expected_sha256 and digest != expected_sha256:
        raise ValueError("Selected image does not match the expected SHA-256")
    result = subprocess.run(
        [
            runtime,
            "exec",
            "--cleanenv",
            "--containall",
            str(image),
            "tar",
            "-C",
            "/boutique",
            "-cf",
            "-",
            ".",
        ],
        capture_output=True,
        check=True,
    )
    descriptors = read_descriptors(result.stdout)
    if expected_tool and not any(
        item["name"] == expected_tool for item in descriptors.values()
    ):
        raise ValueError(f"Image does not provide the expected tool {expected_tool}")
    selected = {
        name: item
        for name, item in descriptors.items()
        if not expected_tool or item["name"] == expected_tool
    }
    if expected_version and any(
        item.get("tool-version") != expected_version for item in selected.values()
    ):
        raise ValueError(
            "Descriptor tool version does not match the expected installed version"
        )
    if "index.json" in selected:
        raise ValueError("index.json is reserved for the extraction index")
    for name in [*selected, "index.json"]:
        if (output / name).exists() or (output / name).is_symlink():
            raise FileExistsError(
                f"Refusing to overwrite extraction output: {output / name}"
            )
    output.mkdir(parents=True, exist_ok=True)
    entries = []
    for name, descriptor in selected.items():
        destination = output / name
        bound = {
            **descriptor,
            "container-image": {"type": "singularity", "image": image.name},
        }
        destination.write_text(json.dumps(bound, indent=2) + "\n")
        entries.append(
            {
                "name": descriptor["name"],
                "tool_version": descriptor.get("tool-version"),
                "descriptor": name,
                "sha256": file_digest(destination),
            }
        )
    index = {
        "schema_version": 1,
        "image": {"path": str(image), "sha256": digest},
        "tools": entries,
    }
    (output / "index.json").write_text(json.dumps(index, indent=2) + "\n")
    return index


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--expected-tool")
    parser.add_argument("--expected-version")
    parser.add_argument(
        "--runtime", default="apptainer", choices=("apptainer", "singularity")
    )
    args = parser.parse_args()
    print(
        json.dumps(
            extract(
                args.image,
                args.output,
                expected_sha256=args.expected_sha256,
                expected_tool=args.expected_tool,
                expected_version=args.expected_version,
                runtime=args.runtime,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
