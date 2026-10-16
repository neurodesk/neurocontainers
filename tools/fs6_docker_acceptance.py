"""Exercise a downloaded candidate through Docker, without rebuilding it."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import yaml

candidate, recipe, out = map(Path, sys.argv[1:4])
out.mkdir(parents=True, exist_ok=True)
manifest = json.loads((candidate / "manifest.json").read_text())
assert manifest["head_sha"] == os.environ["EXPECTED_HEAD"]
archive = candidate / manifest["docker_archive"]
h = hashlib.sha256()
with archive.open("rb") as stream:
    for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
        h.update(block)
assert h.hexdigest() == manifest["docker_sha256"]
subprocess.run(["docker", "load", "--input", str(archive)], check=True)
tag = manifest["candidate_tag"]
for filename, cmd in [
    ("docker-version.json", ["docker", "version", "--format", "{{json .}}"]),
    ("image-inspect.json", ["docker", "image", "inspect", tag]),
]:
    (out / filename).write_bytes(subprocess.check_output(cmd))
suite = yaml.safe_load(recipe.read_text())
tests = [
    dict(
        name="user-provided launch",
        command="command -v recon-all",
        expected_output_contains="/opt/freesurfer-6.0.0/bin/recon-all",
        shell="sh",
    )
] + suite["tests"]
results = []
for index, test in enumerate(tests):
    folder = out / ("test-%02d" % index)
    folder.mkdir()
    argv = [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        tag,
        test.get("shell", "bash"),
        "-lc",
        test["command"],
    ]
    (folder / "command.json").write_text(json.dumps(argv, indent=2))
    start = time.monotonic()
    result = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    (folder / "stdout.txt").write_text(result.stdout)
    (folder / "stderr.txt").write_text(result.stderr)
    contains = test.get("expected_output_contains")
    if contains:
        contains = contains.replace(
            "${upstream_version}", str(suite["upstream_version"])
        )
    passed = (
        result.returncode != test["expected_exit_code_not"]
        if "expected_exit_code_not" in test
        else result.returncode == test.get("expected_exit_code", 0)
    )
    passed &= contains is None or contains in result.stdout + result.stderr
    row = dict(
        name=test["name"],
        passed=passed,
        exit_code=result.returncode,
        seconds=round(time.monotonic() - start, 3),
    )
    results.append(row)
    print(json.dumps(row), flush=True)
    (out / "results.json").write_text(
        json.dumps(
            dict(head=manifest["head_sha"], docker_sha256=h.hexdigest(), tests=results),
            indent=2,
        )
    )
assert all(test["passed"] for test in results), (
    "Docker acceptance failure; inspect retained logs"
)
