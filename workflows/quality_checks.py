"""Run the same code quality checks locally and on hosted CI runners."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import urllib.request


WORKFLOW_TOOLS = {
    "actionlint": (
        "1.7.12",
        "https://github.com/rhysd/actionlint/releases/download/v1.7.12/"
        "actionlint_1.7.12_linux_amd64.tar.gz",
        "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8",
    ),
    "shellcheck": (
        "0.11.0",
        "https://github.com/koalaman/shellcheck/releases/download/v0.11.0/"
        "shellcheck-v0.11.0.linux.x86_64.tar.xz",
        "8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198",
    ),
}
SHARED_INPUTS = {"requirements.txt", "pyproject.toml", "uv.lock", ".codespellrc"}
INTEGRATION_ROOTS = ("builder/", "workflows/", "tools/", "macros/", ".github/")
REQUIRED_JOBS = ("detect-changes", "python-quality", "test-builder", "test-go")
OPTIONAL_JOBS = {"buildkit": "buildkit", "recipes": "recipes"}


def run(command: list[str], **kwargs: object) -> None:
    print(f"+ {shlex.join(command)}", flush=True)
    subprocess.run(command, check=True, **kwargs)


def integration_selection(paths: list[str]) -> dict[str, str]:
    shared = any(
        path.startswith(INTEGRATION_ROOTS) or path in SHARED_INPUTS for path in paths
    )
    return {
        "buildkit": str(shared).lower(),
        "recipes": str(shared or any(p.startswith("recipes/") for p in paths)).lower(),
        "all-recipes": str(shared).lower(),
    }


def select(base: str | None, head: str, output: Path) -> None:
    if not base or set(base) == {"0"}:
        selection = dict.fromkeys(("buildkit", "recipes", "all-recipes"), "true")
        base = ""
    else:
        changed = subprocess.check_output(
            ["git", "diff", "--name-only", "-z", f"{base}...{head}"],
        )
        paths = [os.fsdecode(path) for path in changed.split(b"\0") if path]
        selection = integration_selection(paths)
    with output.open("a", encoding="utf-8") as stream:
        for name, value in {**selection, "base": base, "head": head}.items():
            stream.write(f"{name}={value}\n")


def gate_errors(needs: dict[str, dict]) -> list[str]:
    errors = []
    for name in REQUIRED_JOBS:
        if needs.get(name, {}).get("result") != "success":
            errors.append(f"{name} must succeed")
    outputs = needs.get("detect-changes", {}).get("outputs", {})
    for job, flag in OPTIONAL_JOBS.items():
        selected = outputs.get(flag)
        if selected not in {"true", "false"}:
            errors.append(f"Missing or invalid integration selection: {flag}")
            continue
        expected = "success" if selected == "true" else "skipped"
        if needs.get(job, {}).get("result") != expected:
            errors.append(f"{job} must be {expected} when {flag}={selected}")
    return errors


def install_workflow_tools(destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for name, (_, url, digest) in WORKFLOW_TOOLS.items():
        with urllib.request.urlopen(url, timeout=60) as response:
            archive = response.read()
        if hashlib.sha256(archive).hexdigest() != digest:
            raise ValueError(f"Checksum mismatch for {name}")
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            member = next(
                member
                for member in source.getmembers()
                if member.isfile() and Path(member.name).name == name
            )
            with source.extractfile(member) as binary:
                (destination / name).write_bytes(binary.read())
        (destination / name).chmod(0o755)


def python_checks() -> None:
    run([sys.executable, "-m", "ruff", "check", "builder", "workflows", "tools"])
    run(
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            "--check",
            "builder",
            "workflows",
            "tools",
        ]
    )
    run([sys.executable, "-m", "mypy"])
    run(
        [
            sys.executable,
            "-m",
            "codespell_lib",
            "builder",
            "workflows",
            "tools",
            "dashboard",
            ".github",
        ]
    )


def workflow_checks() -> None:
    for name, (version, _, _) in WORKFLOW_TOOLS.items():
        output = subprocess.check_output([name, "--version"], text=True)
        if version not in output.splitlines():
            if f"version: {version}" not in output.splitlines():
                raise ValueError(f"Install {name} {version} before running checks")
    environment = {**os.environ, "SHELLCHECK_OPTS": "--severity=warning"}
    run(["actionlint", "-shellcheck=shellcheck"], env=environment)
    scripts = sorted(
        str(path)
        for root in ("workflows", "tools", "dashboard", ".github")
        for path in Path(root).rglob("*.sh")
    )
    run(["shellcheck", "--severity=warning", *scripts])


def go_checks() -> None:
    sources = sorted(
        str(path)
        for root in ("builder", "dashboard")
        for path in Path(root).glob("*.go")
    )
    unformatted = subprocess.check_output(["gofmt", "-l", *sources], text=True)
    if unformatted:
        raise ValueError(f"Run gofmt -w on:\n{unformatted}")
    run(["go", "vet", "./..."], cwd="dashboard")
    run(["go", "test", "./..."], cwd="dashboard")
    files = ["docker-save-to-simg.go", "docker-save-to-simg_test.go"]
    run(["go", "vet", *files], cwd="builder")
    run(["go", "test", *files], cwd="builder")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("python", "workflows", "go", "gate"):
        commands.add_parser(name)
    installer = commands.add_parser("install-workflow-tools")
    installer.add_argument("destination", type=Path)
    selector = commands.add_parser("select")
    selector.add_argument("--base")
    selector.add_argument("--head", default="HEAD")
    selector.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "select":
        select(args.base, args.head, args.output)
    elif args.command == "install-workflow-tools":
        install_workflow_tools(args.destination)
    elif args.command == "python":
        python_checks()
    elif args.command == "workflows":
        workflow_checks()
    elif args.command == "go":
        go_checks()
    else:
        errors = gate_errors(json.loads(os.environ["NEEDS_JSON"]))
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        print("All required code quality checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
