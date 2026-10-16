"""Run the real installer shell with network, package, and compiler boundaries stubbed."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

import pytest
import yaml


ACTION = Path(".github/actions/setup-apptainer").resolve()


@pytest.fixture
def installer(tmp_path):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    output = tmp_path / "output"
    log = tmp_path / "commands"
    package = tmp_path / "package.deb"
    package.write_bytes(b"verified package fixture")
    source = tmp_path / "source.tar.gz"
    with tarfile.open(source, "w:gz") as archive:
        for name, data in {
            "mconfig": b"#!/bin/bash\nmkdir builddir\n",
            "dist/debian/apparmor-userns": b"profile apptainer /usr/libexec/apptainer/bin/starter {}\n",
        }.items():
            member = tarfile.TarInfo(f"apptainer-1.4.3/{name}")
            member.size = len(data)
            member.mode = 0o755
            archive.addfile(member, io.BytesIO(data))
    metadata = tmp_path / "release.json"
    metadata.write_text(
        json.dumps(
            {
                "tag_name": "v1.4.3",
                "assets": [
                    {
                        "name": name,
                        "digest": f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}",
                    }
                    for name, path in [
                        ("apptainer_1.4.3_amd64.deb", package),
                        ("apptainer-1.4.3.tar.gz", source),
                    ]
                ],
            }
        )
    )
    stub = binaries / "stub"
    stub.write_text(
        f"#!{sys.executable}\n"
        + r"""
import json
import os
from pathlib import Path
import shutil
import sys

name = Path(sys.argv[0]).name
args = sys.argv[1:]
e = os.environ
with open(e["TEST_LOG"], "a") as stream:
    stream.write(json.dumps([name, *args]) + "\n")
if name == "uname":
    print(e["TEST_OS"] if args == ["-s"] else e["TEST_MACHINE"])
elif name == "dpkg":
    print(e["TEST_ARCH"])
elif name == "dpkg-deb":
    print({"Package": "apptainer", "Version": e["TEST_PACKAGE_VERSION"], "Architecture": e["TEST_PACKAGE_ARCH"]}[args[-1]])
elif name == "curl":
    dest = Path(args[args.index("-o") + 1])
    is_metadata = any("api.github.com/" in arg for arg in args)
    kind = "metadata" if is_metadata else "artifact"
    if e.get("TEST_DOWNLOAD_FAIL") == kind:
        dest.write_bytes(b"partial download")
        sys.exit(22)
    if is_metadata:
        shutil.copyfile(e["TEST_METADATA"], dest)
    elif e.get("TEST_BAD_DIGEST"):
        dest.write_bytes(b"corrupt artifact")
    else:
        shutil.copyfile(e["TEST_SOURCE"] if str(dest).endswith(".tar.gz") else e["TEST_PACKAGE"], dest)
elif name == "sudo":
    if e.get("TEST_INSTALL_FAIL"):
        sys.exit(100)
elif name == "make":
    sys.exit(int(e.get("TEST_BUILD_EXIT", "0")))
elif name == "go":
    print("linux" if args[-1] == "GOHOSTOS" else e["TEST_GO_ARCH"])
elif name == "apptainer":
    if e.get("TEST_VERSION_EXIT"):
        sys.exit(1)
    print("apptainer version " + e["TEST_INSTALLED_VERSION"])
elif name == "readelf":
    print("  Machine:                           " + e["TEST_ELF_MACHINE"])
"""
    )
    stub.chmod(0o755)
    for name in (
        "uname",
        "dpkg",
        "dpkg-deb",
        "sudo",
        "curl",
        "apptainer",
        "readelf",
        "make",
        "go",
    ):
        (binaries / name).symlink_to(stub)

    def run(arch="amd64", installed="1.4.3", **settings):
        output.unlink(missing_ok=True)
        log.unlink(missing_ok=True)
        action = yaml.safe_load((ACTION / "action.yml").read_text())
        step = next(
            step for step in action["runs"]["steps"] if step.get("id") == "install"
        )
        script = step["run"].replace("${{ inputs.apptainer-version }}", "1.4.3")
        script = script.replace("${{ github.action_path }}", str(ACTION))
        environment = {
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "RUNNER_TEMP": str(tmp_path),
            "GITHUB_OUTPUT": str(output),
            "APPTAINER_VERSION": "1.4.3",
            "GH_TOKEN": "",
            "TEST_OS": "Linux",
            "TEST_ARCH": arch,
            "TEST_MACHINE": "aarch64" if arch == "arm64" else "x86_64",
            "TEST_PACKAGE_ARCH": arch,
            "TEST_PACKAGE_VERSION": "1.4.3",
            "TEST_INSTALLED_VERSION": installed,
            "TEST_ELF_MACHINE": "AArch64"
            if arch == "arm64"
            else "Advanced Micro Devices X86-64",
            "TEST_GO_ARCH": "arm64",
            "TEST_LOG": str(log),
            "TEST_METADATA": str(metadata),
            "TEST_PACKAGE": str(package),
            "TEST_SOURCE": str(source),
            **settings,
        }
        result = subprocess.run(
            ["bash", "-e", "-c", script],
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
        )
        calls = (
            [json.loads(line) for line in log.read_text().splitlines()]
            if log.exists()
            else []
        )
        return result, output.read_text() if output.exists() else "", calls

    return run


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_installed_version_must_match_before_output(installer, arch):
    result, output, _ = installer(arch=arch, installed="1.5.4")
    assert result.returncode != 0, f"Accepted wrong installed version: {output!r}"
    assert "Expected Apptainer 1.4.3, got: apptainer version 1.5.4" in result.stderr
    assert output == ""


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_matching_version_output(installer, arch):
    result, output, calls = installer(arch=arch)
    assert result.returncode == 0, result.stderr
    assert output == "apptainer-version=1.4.3\n"
    assert ["apptainer", "--version"] in calls
    installs = [call for call in calls if call[:3] == ["sudo", "apt-get", "install"]]
    assert installs
    assert calls.index(["sudo", "apt-get", "update"]) < calls.index(installs[0])
    assert not any(
        "ppa:" in arg or arg == "add-apt-repository" for call in calls for arg in call
    )
    if arch == "arm64":
        assert ["make", "-C", "builddir", "-j2"] in calls
        assert ["sudo", "make", "-C", "builddir", "install"] in calls
    else:
        assert any(arg.endswith("apptainer_1.4.3_amd64.deb") for arg in installs[0])


@pytest.mark.parametrize(
    "arch, settings, message",
    [
        ("s390x", {}, "Unsupported native"),
        ("amd64", {"TEST_MACHINE": "aarch64"}, "Unsupported native"),
        ("amd64", {"TEST_OS": "Darwin"}, "only supported on Linux"),
        ("amd64", {"APPTAINER_VERSION": "../1.4.3"}, "Invalid Apptainer version"),
    ],
)
def test_rejects_invalid_platform_and_version_before_network(
    installer, arch, settings, message
):
    result, output, calls = installer(arch=arch, **settings)
    assert result.returncode != 0
    assert message in result.stderr
    assert output == ""
    assert not any(call[0] in {"curl", "sudo"} for call in calls)


@pytest.mark.parametrize(
    "arch, settings, message",
    [
        ("amd64", {"TEST_PACKAGE_VERSION": "1.5.4"}, "package metadata does not match"),
        ("amd64", {"TEST_PACKAGE_ARCH": "arm64"}, "package metadata does not match"),
        ("arm64", {"TEST_GO_ARCH": "amd64"}, "native Linux ARM64 Go toolchain"),
        ("arm64", {"TEST_BUILD_EXIT": "2"}, ""),
        ("amd64", {"TEST_INSTALL_FAIL": "1"}, ""),
        ("arm64", {"TEST_INSTALL_FAIL": "1"}, ""),
        ("amd64", {"TEST_VERSION_EXIT": "1"}, ""),
        (
            "arm64",
            {"TEST_ELF_MACHINE": "Advanced Micro Devices X86-64"},
            "Expected AArch64",
        ),
    ],
)
def test_failures_do_not_publish_output(installer, arch, settings, message):
    result, output, _ = installer(arch=arch, **settings)
    assert result.returncode != 0
    assert message in result.stderr
    assert output == ""


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
@pytest.mark.parametrize(
    "settings",
    [
        {"TEST_DOWNLOAD_FAIL": "metadata"},
        {"TEST_DOWNLOAD_FAIL": "artifact"},
        {"TEST_BAD_DIGEST": "1"},
    ],
)
def test_failed_download_never_enters_cache_or_installs(
    installer, tmp_path, arch, settings
):
    result, output, calls = installer(arch=arch, **settings)
    assert result.returncode != 0
    assert output == ""
    assert not any(call[0] in {"sudo", "make"} for call in calls)
    assert not list((tmp_path / "apptainer").rglob("*.deb"))
    assert not list((tmp_path / "apptainer").rglob("*.tar.gz"))
    assert not list((tmp_path / "apptainer").rglob("install.*"))
    result, output, _ = installer(arch=arch)
    assert result.returncode == 0, result.stderr
    assert output == "apptainer-version=1.4.3\n"


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_cache_is_reused_only_after_digest_verification(installer, tmp_path, arch):
    result, _, _ = installer(arch=arch)
    assert result.returncode == 0, result.stderr
    result, _, calls = installer(arch=arch)
    assert result.returncode == 0, result.stderr
    assert len([call for call in calls if call[0] == "curl"]) == 1
    cached = next(
        (tmp_path / "apptainer").rglob("*.deb" if arch == "amd64" else "*.tar.gz")
    )
    cached.write_bytes(b"partial stale cache")
    result, _, calls = installer(arch=arch)
    assert result.returncode == 0, result.stderr
    assert len([call for call in calls if call[0] == "curl"]) == 2
    assert cached.read_bytes() != b"partial stale cache"


@pytest.mark.parametrize("mutation", ["missing", "unsigned", "wrong_tag"])
def test_release_metadata_must_identify_verified_asset(installer, tmp_path, mutation):
    path = tmp_path / "release.json"
    release = json.loads(path.read_text())
    if mutation == "missing":
        release["assets"] = []
    elif mutation == "unsigned":
        release["assets"][0]["digest"] = None
    else:
        release["tag_name"] = "v1.5.4"
    path.write_text(json.dumps(release))
    result, output, calls = installer()
    assert result.returncode != 0
    assert output == ""
    assert not any(call[0] == "sudo" for call in calls)
