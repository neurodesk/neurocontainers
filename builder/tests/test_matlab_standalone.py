"""Candidate packaging checks run without MATLAB or publication credentials."""

import hashlib
import json
import os
from pathlib import Path
import tarfile

import pytest

from builder import matlab_standalone as standalone


def request(software="fieldtrip"):
    return {"software": software, "source_ref": "a" * 40,
            "source_version": "20250825" if software == "fieldtrip" else "v9.1.0",
            "spm_ref": "b" * 40 if software == "physio" else "", "runtime_release": "R2023b"}


def compiled_fixture(tmp_path, software="fieldtrip", *, exit_code=0, marker=True):
    value = request(software)
    workspace = tmp_path / "workspace"
    source = workspace / "_matlab_source"
    source.mkdir(parents=True)
    (source / "LICENSE").write_text("Source license\n")
    if software == "physio":
        spm = workspace / "_matlab_spm"
        spm.mkdir()
        (spm / "LICENCE.txt").write_text("SPM license\n")
    output = tmp_path / "output"
    compiled = output / "compiled"
    compiled.mkdir(parents=True)
    executable = "fieldtrip" if software == "fieldtrip" else "spm12"
    header = bytearray(20)
    header[:6] = b"\x7fELF\x02\x01"
    header[18:20] = b"\x3e\x00"
    (compiled / executable).write_bytes(header)
    (compiled / executable).chmod(0o755)
    message = standalone.MARKERS[software] if marker else "runtime did not reach its assertion"
    (compiled / f"run_{executable}.sh").write_text(f"#!/bin/bash\nprintf '%s\\n' '{message}'\nexit {exit_code}\n")
    (compiled / "readme.txt").write_text("MATLAB Runtime R2023b is required.\n")
    if software == "physio":
        (compiled / "spm12.ctf").write_bytes(b"test CTF bytes")
    (output / "compiler-report.json").write_text(json.dumps({
        "software": software, "runtime_release": "R2023b", "matlab_root": str(tmp_path / "matlab"),
    }))
    return value, workspace, output


@pytest.mark.parametrize("field,value", [
    ("software", "samsrfx"), ("software", []), ("source_ref", "main"),
    ("source_ref", "A" * 40), ("source_version", "latest"),
    ("source_version", "20250825\nmalicious"), ("runtime_release", "R2020b"),
    ("runtime_release", "latest"), ("spm_ref", "b" * 40),
])
def test_request_rejects_unreviewed_source_and_runtime_identifiers(field, value):
    with pytest.raises(ValueError):
        standalone.validate_request({**request(), field: value})


def test_physio_requires_both_official_source_revisions():
    assert standalone.validate_request(request("physio"))["spm_ref"] == "b" * 40
    with pytest.raises(ValueError, match="SPM12"):
        standalone.validate_request({**request("physio"), "spm_ref": ""})


@pytest.mark.parametrize("annotated", [False, True])
def test_checkout_verifies_official_tag_resolves_to_requested_commit(monkeypatch, tmp_path, annotated):
    calls = []

    def check_output(command, **kwargs):
        calls.append(command)
        if "rev-parse" in command:
            return "a" * 40 + "\n"
        tag = "refs/tags/20250825"
        if annotated:
            return f"{'c' * 40}\t{tag}\n{'a' * 40}\t{tag}^{{}}\n"
        return f"{'a' * 40}\t{tag}\n"

    monkeypatch.setattr(standalone.subprocess, "check_output", check_output)
    standalone.verify_checkout(tmp_path, "fieldtrip/fieldtrip", "a" * 40, "20250825")
    assert calls[1][2] == "https://github.com/fieldtrip/fieldtrip.git"
    with pytest.raises(ValueError, match="requested commit"):
        standalone.verify_checkout(tmp_path, "fieldtrip/fieldtrip", "b" * 40, "20250825")


def test_checkout_rejects_tag_moved_to_another_commit(monkeypatch, tmp_path):
    monkeypatch.setattr(standalone.subprocess, "check_output", lambda command, **kwargs:
                        "a" * 40 if "rev-parse" in command else f"{'b' * 40}\trefs/tags/20250825\n")
    with pytest.raises(ValueError, match="official tag"):
        standalone.verify_checkout(tmp_path, "fieldtrip/fieldtrip", "a" * 40, "20250825")


@pytest.mark.parametrize("defect", ["architecture", "permission", "runtime", "readme_size", "ctf", "symlink"])
def test_compiled_inputs_must_match_the_runtime_and_linux_abi(tmp_path, defect):
    value, _, output = compiled_fixture(tmp_path, "physio")
    directory = output / "compiled"
    if defect == "architecture":
        (directory / "spm12").write_bytes(b"not an x86 ELF executable")
    elif defect == "permission":
        (directory / "spm12").chmod(0o644)
    elif defect == "runtime":
        (directory / "readme.txt").write_text("Runtime R2022a")
    elif defect == "readme_size":
        (directory / "readme.txt").write_text("R2023b " * 10000)
    elif defect == "ctf":
        (directory / "spm12.ctf").unlink()
    else:
        (directory / "spm12.ctf").unlink()
        (directory / "spm12.ctf").symlink_to(directory / "readme.txt")
    with pytest.raises(ValueError):
        standalone.validate_compiled(directory, value)


@pytest.mark.parametrize("exit_code,marker", [(1, True), (0, False)])
def test_runtime_check_requires_exit_success_and_completed_assertions(tmp_path, exit_code, marker):
    value, workspace, output = compiled_fixture(tmp_path, exit_code=exit_code, marker=marker)
    with pytest.raises(ValueError, match="runtime smoke"):
        standalone.package(value, workspace, output)
    assert (output / "runtime-smoke.log").is_file()
    assert not (output / "manifest.json").exists()
    assert not list(output.glob("*.tar.gz"))


@pytest.mark.parametrize("software", ["fieldtrip", "physio"])
def test_package_hashes_real_bytes_preserves_layout_and_is_rerunnable(tmp_path, software):
    value, workspace, output = compiled_fixture(tmp_path, software)
    first = standalone.package(value, workspace, output)
    archive = output / first["archive"]
    assert first["sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    assert first["swift_object"].startswith(f"matlab-candidates/{software}/{'a' * 40}/2023b/")
    prefix = "fieldtrip" if software == "fieldtrip" else "spm12"
    with tarfile.open(archive) as bundle:
        license_file = bundle.extractfile(f"{prefix}/LICENSES/source-LICENSE")
        assert license_file.read() == b"Source license\n"
        assert bundle.getmember(f"{prefix}/{prefix}").mode & 0o111
        assert all(member.uid == member.gid == member.mtime == 0 for member in bundle)
    for path in (output / "compiled").rglob("*"):
        os.utime(path, (1700000000, 1700000000))
    assert standalone.package(value, workspace, output)["sha256"] == first["sha256"]
    assert standalone.verify_candidate(output) == first


def test_archive_rejects_symlinks_without_creating_an_artifact(tmp_path):
    directory = tmp_path / "compiled"
    directory.mkdir()
    (directory / "outside").symlink_to("/etc/passwd")
    archive = tmp_path / "candidate.tar.gz"
    with pytest.raises(ValueError, match="symlinks"):
        standalone.write_archive(directory, archive, "fieldtrip")
    assert not archive.exists()


@pytest.mark.parametrize("defect", ["bytes", "digest", "destination", "archive", "repository", "smoke", "log", "checksum"])
def test_publisher_rejects_changed_bytes_provenance_and_release_destinations(tmp_path, defect):
    value, workspace, output = compiled_fixture(tmp_path)
    manifest = standalone.package(value, workspace, output)
    if defect == "bytes":
        with (output / manifest["archive"]).open("ab") as handle:
            handle.write(b"modified")
    elif defect == "digest":
        manifest["sha256"] = "x" * 64
    elif defect == "destination":
        manifest["swift_object"] = "fieldtrip20250825_mcr2023b.tar.gz"
    elif defect == "archive":
        manifest["archive"] = "../outside.tar.gz"
    elif defect == "repository":
        manifest["source_repository"] = "someone/fork"
    elif defect == "smoke":
        manifest["runtime_smoke"]["passed"] = False
    elif defect == "log":
        (output / "runtime-smoke.log").write_text("not completed")
    else:
        (output / "SHA256SUMS").write_text("wrong checksum")
    (output / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        standalone.verify_candidate(output)


def test_package_requires_both_physio_and_spm_license_files(tmp_path):
    value, workspace, output = compiled_fixture(tmp_path, "physio")
    (workspace / "_matlab_spm/LICENCE.txt").unlink()
    with pytest.raises(ValueError, match="spm12 has no"):
        standalone.package(value, workspace, output)
    assert not (output / "manifest.json").exists()
