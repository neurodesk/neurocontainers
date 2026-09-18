"""Validate, exercise, and package standalone MATLAB compilation candidates."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile


REPOSITORIES = {"fieldtrip": "fieldtrip/fieldtrip", "physio": "ComputationalPsychiatry/PhysIO"}
SHA = re.compile(r"[0-9a-f]{40}")
RELEASE = re.compile(r"R20\d{2}[ab]")
MARKERS = {"fieldtrip": "NEURO_FIELDTRIP_SMOKE_OK", "physio": "NEURO_PHYSIO_SMOKE_OK"}


def validate_request(value: dict) -> dict:
    if set(value) != {"software", "source_ref", "source_version", "spm_ref", "runtime_release"}:
        raise ValueError("compilation request has unexpected or missing fields")
    if any(not isinstance(field, str) for field in value.values()):
        raise ValueError("compilation request fields must be strings")
    software = value["software"]
    if software not in REPOSITORIES:
        raise ValueError("only FieldTrip and PhysIO/SPM have verified upstream compile entrypoints; "
                         "SamSrfX is unsupported")
    if not isinstance(value["source_ref"], str) or not SHA.fullmatch(value["source_ref"]):
        raise ValueError("source_ref must be a full lowercase commit SHA")
    pattern = r"\d{8}" if software == "fieldtrip" else r"v\d+(?:\.\d+)+"
    if not isinstance(value["source_version"], str) or not re.fullmatch(pattern, value["source_version"]):
        raise ValueError("source_version must be an exact official FieldTrip date tag or PhysIO vX.Y.Z tag")
    release = value["runtime_release"]
    if not isinstance(release, str) or not RELEASE.fullmatch(release) or release < "R2021a":
        raise ValueError("runtime_release must be R2021a or newer for matlab-actions v3")
    if software == "physio":
        if not isinstance(value["spm_ref"], str) or not SHA.fullmatch(value["spm_ref"]):
            raise ValueError("PhysIO requires an exact SPM12 commit SHA")
    elif value["spm_ref"]:
        raise ValueError("spm_ref only applies to PhysIO")
    return value


def read_request(path: Path) -> dict:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("compilation request must be an object")
    return validate_request(value)


def verify_checkout(directory: Path, repo: str, revision: str, tag: str | None = None) -> None:
    actual = subprocess.check_output(["git", "-C", str(directory), "rev-parse", "HEAD"], text=True).strip()
    if actual != revision:
        raise ValueError(f"{repo} checkout disagrees with its requested commit")
    if tag is not None:
        refs = subprocess.check_output([
            "git", "ls-remote", f"https://github.com/{repo}.git",
            f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}",
        ], text=True)
        tagged = {line.split()[1]: line.split()[0] for line in refs.splitlines() if line.strip()}
        resolved = tagged.get(f"refs/tags/{tag}^{{}}", tagged.get(f"refs/tags/{tag}"))
        if resolved != revision:
            raise ValueError(f"official tag {repo}:{tag} does not resolve to the requested commit")


def prepare(request: dict, workspace: Path) -> None:
    source = workspace / "_matlab_source"
    verify_checkout(source, REPOSITORIES[request["software"]], request["source_ref"], request["source_version"])
    if request["software"] == "fieldtrip":
        required = [source / "utilities/ft_compile_standalone.m", source / "utilities/ft_standalone.m"]
    else:
        spm = workspace / "_matlab_spm"
        verify_checkout(spm, "spm/spm12", request["spm_ref"])
        required = [source / name for name in (
            "tapas_physio_init.m", "tapas_physio_new.m", "tapas_physio_cfg_matlabbatch.m")]
        required.append(spm / "config/spm_make_standalone.m")
    for path in required:
        if not path.is_file():
            raise ValueError(f"upstream compilation input is missing: {path.relative_to(workspace)}")


def validate_compiled(directory: Path, request: dict) -> str:
    executable = "fieldtrip" if request["software"] == "fieldtrip" else "spm12"
    required = [executable, f"run_{executable}.sh", "readme.txt"]
    if executable == "spm12":
        required.append("spm12.ctf")
    for name in required:
        path = directory / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size == 0:
            raise ValueError(f"compiled package is missing a regular, nonempty {name}")
    with (directory / executable).open("rb") as handle:
        header = handle.read(20)
    if len(header) < 20 or header[:6] != b"\x7fELF\x02\x01" or header[18:20] != b"\x3e\x00":
        raise ValueError("compiled application is not a Linux x86_64 ELF executable")
    if not os.access(directory / executable, os.X_OK):
        raise ValueError("compiled application has no executable permission")
    if (directory / "readme.txt").stat().st_size > 64 * 1024:
        raise ValueError("compiler readme exceeds 64 KiB")
    readme = (directory / "readme.txt").read_text(encoding="utf-8")
    releases = set(re.findall(r"\bR20\d{2}[ab]\b", readme))
    if releases != {request["runtime_release"]}:
        raise ValueError("compiler readme runtime disagrees with the requested MATLAB release")
    return executable


def run_smoke(directory: Path, request: dict, matlab_root: Path, output: Path) -> dict:
    executable = validate_compiled(directory, request)
    marker = MARKERS[request["software"]]
    if request["software"] == "fieldtrip":
        script = output / "fieldtrip-smoke.m"
        script.write_text(
            "raw.label={'channel1'}; raw.fsample=100; raw.time={(0:99)/100};\n"
            "raw.trial={3+sin(2*pi*5*raw.time{1})};\n"
            "cfg=[]; cfg.demean='yes'; result=ft_preprocessing(cfg,raw);\n"
            "assert(isequal(size(result.trial{1}),[1 100]));\n"
            "assert(abs(mean(result.trial{1}))<1e-10);\n"
            f"disp('{marker}');\n"
        )
        arguments = [str(script)]
    else:
        arguments = ["eval", "physio=tapas_physio_new(); assert(isstruct(physio) && isfield(physio,'model')); "
                     f"disp('{marker}');"]
    command = ["bash", str(directory / f"run_{executable}.sh"), str(matlab_root), *arguments]
    environment = {**os.environ, "MCR_CACHE_ROOT": str(output / "mcr-cache"), "MCR_INHIBIT_CTF_LOCK": "1"}
    completed = subprocess.run(command, cwd=output, env=environment, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
    (output / "runtime-smoke.log").write_text(completed.stdout)
    if completed.returncode != 0 or marker not in completed.stdout:
        raise ValueError("compiled runtime smoke check failed; see runtime-smoke.log")
    return {"passed": True, "marker": marker, "exit_code": completed.returncode}


def _copy_licenses(workspace: Path, destination: Path, software: str) -> None:
    sources = [("source", workspace / "_matlab_source")]
    if software == "physio":
        sources.append(("spm12", workspace / "_matlab_spm"))
    destination.mkdir(exist_ok=True)
    for prefix, root in sources:
        found = [path for path in root.iterdir()
                 if path.is_file() and not path.is_symlink()
                 and re.fullmatch(r"(?:COPYING|LICENSE|LICENCE)(?:\.[A-Za-z]+)?", path.name, re.I)]
        if not found:
            raise ValueError(f"{prefix} has no recognized source license file")
        for path in found:
            shutil.copyfile(path, destination / f"{prefix}-{path.name}")


def write_archive(directory: Path, archive: Path, prefix: str) -> str:
    paths = sorted(directory.rglob("*"))
    if any(path.is_symlink() or (not path.is_file() and not path.is_dir()) for path in paths):
        raise ValueError("compiled package cannot contain symlinks or special files")
    temporary = archive.with_suffix(archive.suffix + ".tmp")
    with temporary.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as bundle:
            for path in paths:
                info = bundle.gettarinfo(str(path), arcname=f"{prefix}/{path.relative_to(directory).as_posix()}")
                info.uid = info.gid = info.mtime = 0
                info.uname = info.gname = ""
                if path.is_file():
                    with path.open("rb") as contents:
                        bundle.addfile(info, contents)
                else:
                    bundle.addfile(info)
    temporary.replace(archive)
    return file_digest(archive)


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def archive_name(request: dict) -> str:
    version = request["source_version"]
    runtime = request["runtime_release"][1:]
    if request["software"] == "fieldtrip":
        return f"fieldtrip{version}_mcr{runtime}.tar.gz"
    return f"physio-{version}-spm12-{request['spm_ref'][:12]}_mcr{runtime}.tar.gz"


def candidate_object(request: dict, digest: str) -> str:
    return (f"matlab-candidates/{request['software']}/{request['source_ref']}/"
            f"{request['runtime_release'][1:]}/{digest}/{archive_name(request)}")


def verify_candidate(output: Path) -> dict:
    manifest = json.loads((output / "manifest.json").read_text())
    if not isinstance(manifest, dict):
        raise ValueError("candidate manifest must be an object")
    fields = ("software", "source_ref", "source_version", "spm_ref", "runtime_release")
    request = validate_request({key: manifest.get(key) for key in fields})
    digest = manifest.get("sha256")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("candidate manifest must contain a SHA256 digest")
    if (manifest.get("schema_version") != 1 or manifest.get("status") != "candidate"
            or manifest.get("source_repository") != REPOSITORIES[request["software"]]
            or manifest.get("archive") != archive_name(request)
            or manifest.get("swift_object") != candidate_object(request, digest)):
        raise ValueError("candidate provenance or publication destination disagrees with its request")
    archive = output / manifest["archive"]
    if not archive.is_file() or archive.is_symlink():
        raise ValueError("candidate archive must be a regular file")
    if manifest.get("size") != archive.stat().st_size or file_digest(archive) != digest:
        raise ValueError("candidate bytes disagree with the manifest")
    if (output / "SHA256SUMS").read_text() != f"{digest}  {manifest['archive']}\n":
        raise ValueError("candidate checksum file disagrees with the manifest")
    marker = MARKERS[request["software"]]
    if manifest.get("runtime_smoke") != {"passed": True, "marker": marker, "exit_code": 0}:
        raise ValueError("candidate has no successful compiled runtime smoke check")
    if marker not in (output / "runtime-smoke.log").read_text():
        raise ValueError("candidate runtime log lacks its success marker")
    return manifest


def package(request: dict, workspace: Path, output: Path) -> dict:
    compiled = output / "compiled"
    report = json.loads((output / "compiler-report.json").read_text())
    if report.get("runtime_release") != request["runtime_release"] or report.get("software") != request["software"]:
        raise ValueError("compiler report disagrees with the validated request")
    executable = validate_compiled(compiled, request)
    smoke = run_smoke(compiled, request, Path(report["matlab_root"]), output)
    _copy_licenses(workspace, compiled / "LICENSES", request["software"])
    name = archive_name(request)
    archive = output / name
    digest = write_archive(compiled, archive, executable)
    manifest = {"schema_version": 1, **request, "source_repository": REPOSITORIES[request["software"]],
                "archive": name, "sha256": digest, "size": archive.stat().st_size,
                "runtime_smoke": smoke, "status": "candidate",
                "compiler_adjustments": report.get("compiler_adjustments", []),
                "swift_object": candidate_object(request, digest)}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "SHA256SUMS").write_text(f"{digest}  {name}\n")
    return verify_candidate(output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["request", "prepare", "package", "verify"])
    parser.add_argument("--request", type=Path)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.action == "verify":
        manifest = verify_candidate(args.output)
        if destination := os.environ.get("GITHUB_OUTPUT"):
            with open(destination, "a") as handle:
                for key in ("archive", "swift_object"):
                    handle.write(f"{key}={manifest[key]}\n")
        return 0
    if args.request is None or args.workspace is None:
        parser.error("--request and --workspace are required for compilation actions")
    if args.action == "request":
        request = validate_request({key: os.environ.get("INPUT_" + key.upper(), "") for key in
                                    ["software", "source_ref", "source_version", "spm_ref", "runtime_release"]})
        args.request.write_text(json.dumps(request, indent=2) + "\n")
        args.output.mkdir(parents=True, exist_ok=True)
        if destination := os.environ.get("GITHUB_OUTPUT"):
            with open(destination, "a") as handle:
                handle.write(f"source_repository={REPOSITORIES[request['software']]}\n")
    else:
        request = read_request(args.request)
        if args.action == "prepare":
            prepare(request, args.workspace)
        else:
            manifest = package(request, args.workspace, args.output)
            print(json.dumps({"archive": manifest["archive"], "sha256": manifest["sha256"], "status": "candidate"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
