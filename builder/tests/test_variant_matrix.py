from __future__ import annotations

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
