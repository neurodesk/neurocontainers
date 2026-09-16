from __future__ import annotations

import pytest

from builder.variants import concrete_variant_specs
from tools.variant_matrix import build_matrix


def test_build_matrix_expands_named_variants(tmp_path) -> None:
    recipe_dir = tmp_path / "recipes" / "tool"
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "build.yaml").write_text(
        """name: tool
version: 1.0
architectures:
  - x86_64
  - aarch64
variants:
  gpu:
    architectures:
      - x86_64
      - aarch64
"""
    )

    assert build_matrix(tmp_path, ["tool"], '"x86-runner"', '"arm-runner"') == [
        {
            "application": "tool",
            "variant": "",
            "architecture": "x86_64",
            "runner": '"x86-runner"',
        },
        {
            "application": "tool",
            "variant": "arm64",
            "architecture": "aarch64",
            "runner": '"arm-runner"',
        },
        {
            "application": "tool",
            "variant": "gpu",
            "architecture": "x86_64",
            "runner": '"x86-runner"',
        },
        {
            "application": "tool",
            "variant": "gpu_arm64",
            "architecture": "aarch64",
            "runner": '"arm-runner"',
        },
    ]


def test_build_matrix_can_disable_default_builds(tmp_path) -> None:
    recipe_dir = tmp_path / "recipes" / "tool"
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "build.yaml").write_text(
        """name: tool
version: 1.0
architectures: [x86_64, aarch64]
build_default: false
variants:
  lite:
    architecture: x86_64
"""
    )
    assert build_matrix(tmp_path, ["tool"], '"x86-runner"', '"arm-runner"') == [
        {
            "application": "tool",
            "variant": "lite",
            "architecture": "x86_64",
            "runner": '"x86-runner"',
        }
    ]


def test_disabling_all_builds_is_rejected() -> None:
    with pytest.raises(ValueError, match="no enabled builds"):
        concrete_variant_specs({
            "name": "tool", "architectures": ["x86_64"], "build_default": False,
        })
