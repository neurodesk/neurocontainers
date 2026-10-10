import io
import json
import tarfile
from types import SimpleNamespace

import pytest

pytest.importorskip("boutiques")

from builder import boutiques


def descriptor():
    return {
        "name": "demo",
        "tool-version": "1.2.3",
        "schema-version": "0.5",
        "description": "Demo CLI",
        "command-line": "demo [VERSION]",
        "inputs": [
            {
                "id": "version",
                "name": "Version",
                "type": "Flag",
                "value-key": "[VERSION]",
                "command-line-flag": "--version",
                "optional": True,
            }
        ],
    }


def archive(entries):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as stream:
        for name, value in entries:
            data = json.dumps(value).encode()
            member = tarfile.TarInfo(name)
            member.size = len(data)
            stream.addfile(member, io.BytesIO(data))
    return output.getvalue()


def test_schema_rejects_invalid_descriptor_and_empty_image():
    with pytest.raises(Exception, match="command-line"):
        boutiques.read_descriptors(archive([("demo.json", {"name": "demo"})]))
    with pytest.raises(ValueError, match="no /boutique"):
        boutiques.read_descriptors(archive([]))


@pytest.mark.parametrize("name", ["../demo.json", "/demo.json", "nested/demo.json"])
def test_extraction_rejects_unsafe_paths(name):
    with pytest.raises(ValueError, match="archive entry"):
        boutiques.read_descriptors(archive([(name, descriptor())]))


def test_index_binds_image_and_extracted_descriptor_without_overwriting(
    tmp_path, monkeypatch
):
    image = tmp_path / "demo.sif"
    image.write_bytes(b"image fixture")
    monkeypatch.setattr(
        boutiques.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout=archive([("./demo.json", descriptor())])
        ),
    )
    destination = tmp_path / "output"
    index = boutiques.extract(
        image, destination, expected_tool="demo", expected_version="1.2.3"
    )
    assert index["schema_version"] == 1
    assert index["image"]["sha256"] == boutiques.file_digest(image)
    bound = json.loads((destination / "demo.json").read_text())
    assert bound["container-image"] == {"type": "singularity", "image": "demo.sif"}
    assert index["tools"][0]["sha256"] == boutiques.file_digest(
        destination / "demo.json"
    )
    with pytest.raises(FileExistsError):
        boutiques.extract(image, destination)
    (destination / "demo.json").unlink()
    (destination / "demo.json").symlink_to(tmp_path / "not-created.json")
    with pytest.raises(FileExistsError):
        boutiques.extract(image, destination)
    assert not (tmp_path / "not-created.json").exists()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"expected_version": "9.9"},
        {"expected_tool": "other"},
        {"expected_sha256": "0" * 64},
    ],
)
def test_wrong_image_or_tool_identity_produces_no_output(tmp_path, monkeypatch, kwargs):
    image = tmp_path / "demo.sif"
    image.write_bytes(b"image fixture")
    monkeypatch.setattr(
        boutiques.subprocess,
        "run",
        lambda *args, **kw: SimpleNamespace(
            stdout=archive([("demo.json", descriptor())])
        ),
    )
    with pytest.raises(ValueError):
        boutiques.extract(image, tmp_path / "output", **kwargs)
    assert not (tmp_path / "output").exists()
