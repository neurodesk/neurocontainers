from __future__ import annotations

import difflib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _recipe_dirs() -> list[Path]:
    recipes = _repo_root() / "recipes"
    return sorted(
        path
        for path in recipes.iterdir()
        if path.is_dir() and (path / "build.yaml").is_file()
    )


def _python_executable() -> str:
    system_python = Path("/usr/bin/python3")
    return str(system_python if system_python.exists() else (shutil.which("python3") or sys.executable))


def _dockerfile_text(build_dir: Path, suffix: str) -> str:
    dockerfiles = sorted(build_dir.glob(f"*{suffix}"))
    if not dockerfiles:
        raise AssertionError(f"no {suffix} file written under {build_dir}")
    if len(dockerfiles) != 1:
        raise AssertionError(f"expected one {suffix} file under {build_dir}, found {len(dockerfiles)}")
    return dockerfiles[0].read_text()


def _current_builder_dockerfile(recipe_dir: Path) -> str:
    repo_root = _repo_root()
    recipe_name = recipe_dir.name
    with tempfile.TemporaryDirectory() as tmp:
        env = os.environ.copy()
        env.setdefault("NEURODOCKER_AUTO_UPGRADE", "0")
        subprocess.check_call(
            [
                _python_executable(),
                str(repo_root / "builder" / "build.py"),
                "generate",
                recipe_name,
                "--output-directory",
                tmp,
                "--recreate",
                "--check-only",
                "--architecture",
                "x86_64",
                "--ignore-architectures",
            ],
            cwd=repo_root,
            env=env,
            stdout=subprocess.DEVNULL,
        )
        return _dockerfile_text(Path(tmp) / recipe_name, ".Dockerfile")


def _build3_dockerfile(recipe_dir: Path) -> str:
    repo_root = _repo_root()
    recipe_name = recipe_dir.name
    with tempfile.TemporaryDirectory() as tmp:
        env = os.environ.copy()
        env.setdefault("NEURODOCKER_AUTO_UPGRADE", "0")
        subprocess.check_call(
            [
                _python_executable(),
                "-m",
                "build3",
                "generate",
                recipe_name,
                "--output-root",
                tmp,
                "--recreate",
                "--architecture",
                "x86_64",
                "--ignore-architectures",
            ],
            cwd=repo_root,
            env=env,
            stdout=subprocess.DEVNULL,
        )
        return _dockerfile_text(Path(tmp) / recipe_name, ".dockerfile")


@pytest.mark.parity
def test_all_current_recipe_dockerfiles_match_existing_builder_one_to_one() -> None:
    if os.environ.get("BUILD3_STRICT_DOCKERFILE_PARITY") != "1":
        pytest.skip(
            "set BUILD3_STRICT_DOCKERFILE_PARITY=1 to compare every current recipe "
            "against builder/build.py output byte-for-byte"
        )

    failures: list[str] = []
    for recipe_dir in _recipe_dirs():
        try:
            expected = _current_builder_dockerfile(recipe_dir)
            actual = _build3_dockerfile(recipe_dir)
        except Exception as exc:  # noqa: BLE001 - aggregate per-recipe parity failures.
            failures.append(f"{recipe_dir.name}: generation failed: {exc}")
            continue

        if actual != expected:
            diff = "\n".join(
                difflib.unified_diff(
                    expected.splitlines(),
                    actual.splitlines(),
                    fromfile=f"builder/{recipe_dir.name}",
                    tofile=f"build3/{recipe_dir.name}",
                    lineterm="",
                    n=3,
                )
            )
            failures.append(f"{recipe_dir.name}: Dockerfile mismatch\n{diff[:4000]}")

    if failures:
        shown = failures[:10]
        remaining = len(failures) - len(shown)
        suffix = f"\n\n... {remaining} additional parity failures omitted" if remaining else ""
        pytest.fail("\n\n".join(shown) + suffix)
