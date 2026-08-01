from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from builder.config import default_config, resolve_recipe
from builder.recipe import RecipeFile, compile_recipe, load_recipe, load_recipe_file
from builder.dockerfile import render_dockerfile


def write_minimal_recipe(recipe_dir: Path, **metadata: object) -> None:
    recipe_dir.mkdir()
    recipe = {
        "name": "readme-test",
        "version": "1.2.3",
        "architectures": ["x86_64"],
        "categories": ["programming"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [],
        },
        **metadata,
    }
    (recipe_dir / "build.yaml").write_text(
        yaml.safe_dump(recipe, sort_keys=False),
        encoding="utf-8",
    )


def test_loads_existing_recipe() -> None:
    config = default_config()
    recipe_dir = resolve_recipe(config, "dcm2niix")
    recipe = load_recipe(recipe_dir)
    assert recipe["name"] == "dcm2niix"
    assert recipe["version"] == "v1.0.20240202"


def test_loads_typed_recipe_file() -> None:
    config = default_config()
    recipe_file = load_recipe_file(resolve_recipe(config, "dcm2niix"))
    assert isinstance(recipe_file, RecipeFile)
    assert recipe_file.name == "dcm2niix"
    assert recipe_file.build["kind"] == "neurodocker"


def test_compile_records_metadata() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    assert compiled.name == "dcm2niix"
    assert compiled.architecture == "x86_64"
    assert "dcm2niix/v1.0.20240202" in compiled.readme
    assert "downloaded_file" in compiled.staging_plan.files


def test_compile_renders_structured_readme(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(
        recipe_dir,
        structured_readme={
            "description": "Readme test {{ context.version }} description.",
            "example": "readme-test --version",
            "documentation": "https://example.com/docs",
            "citation": "Example citation.",
        },
    )

    compiled = compile_recipe(recipe_dir, architecture="x86_64")

    assert "## readme-test/1.2.3 ##" in compiled.readme
    assert "Readme test 1.2.3 description." in compiled.readme
    assert "Example:\n```\nreadme-test --version\n```" in compiled.readme
    assert (
        "More documentation can be found here: https://example.com/docs"
        in compiled.readme
    )
    assert "Citation:\n```\nExample citation.\n```" in compiled.readme
    assert "ml readme-test/1.2.3" in compiled.readme


def test_compile_preserves_top_level_readme_precedence(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(
        recipe_dir,
        readme="Top-level readme for {{ context.version }}.",
        structured_readme={
            "description": "Structured description.",
            "example": "readme-test --help",
        },
    )

    compiled = compile_recipe(recipe_dir, architecture="x86_64")

    assert compiled.readme == "Top-level readme for 1.2.3."


def test_compile_loads_readme_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(
        recipe_dir,
        readme_url="https://example.com/readme-{{ context.version }}.md",
    )
    requested_urls: list[str] = []

    def fake_read_readme_url(url: str) -> str:
        requested_urls.append(url)
        return "Remote readme."

    monkeypatch.setattr("builder.recipe._read_readme_url", fake_read_readme_url)

    compiled = compile_recipe(recipe_dir, architecture="x86_64")

    assert compiled.readme == "Remote readme."
    assert requested_urls == ["https://example.com/readme-1.2.3.md"]


def test_compile_rejects_empty_readme(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(recipe_dir, readme="   \n")

    with pytest.raises(ValueError, match="README.*cannot be empty"):
        compile_recipe(recipe_dir, architecture="x86_64")


def test_compile_named_variant_uses_concrete_container_identity() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "workshopdemo"),
        variant="arm64",
        include_dirs=config.include_dirs,
    )

    assert compiled.base_name == "workshopdemo"
    assert compiled.name == "workshopdemo_arm64"
    assert compiled.variant == "arm64"
    assert compiled.architecture == "aarch64"
    assert compiled.tag == "workshopdemo_arm64:1.0.0"


def test_compile_architecture_automatically_selects_arm64_variant() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "workshopdemo"),
        architecture="aarch64",
        include_dirs=config.include_dirs,
    )

    assert compiled.name == "workshopdemo_arm64"
    assert compiled.variant == "arm64"


def test_arbitrary_variant_can_span_architectures_and_enable_options(tmp_path) -> None:
    recipe_dir = tmp_path / "gpu-tool"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        """name: gpu-tool
version: 1.0
architectures: [x86_64, aarch64]
readme: gpu-tool {{ context.version }}
options:
  gpu:
    default: false
variants:
  gpu:
    architectures: [x86_64, aarch64]
    options:
      gpu: true
build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives:
    - condition: context.options.gpu
      run: echo gpu-enabled
deploy:
  bins: [gpu-tool]
categories: [workflows]
"""
    )

    compiled = compile_recipe(recipe_dir, variant="gpu", architecture="aarch64")

    assert compiled.name == "gpu-tool_gpu_arm64"
    assert compiled.variant == "gpu_arm64"
    assert "gpu-enabled" in render_dockerfile(compiled.definition)


def test_compile_rejects_variant_on_an_undeclared_architecture(tmp_path) -> None:
    recipe_dir = tmp_path / "gpu-tool"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        """name: gpu-tool
version: 1.0
architectures: [x86_64, aarch64]
variants:
  gpu:
    architecture: x86_64
build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives: []
deploy:
  bins: [gpu-tool]
categories: [workflows]
"""
    )

    with pytest.raises(ValueError, match="unknown variant/architecture"):
        compile_recipe(recipe_dir, variant="gpu", architecture="aarch64")


def write_single_architecture_recipe(recipe_dir: Path) -> None:
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        """name: x86only
version: 1.0
architectures: [x86_64]
readme: x86only {{ context.version }}
build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives: []
deploy:
  bins: [x86only]
categories: [workflows]
"""
    )


def test_ignore_architectures_still_builds_an_undeclared_architecture(tmp_path) -> None:
    recipe_dir = tmp_path / "x86only"
    write_single_architecture_recipe(recipe_dir)

    compiled = compile_recipe(
        recipe_dir,
        architecture="aarch64",
        ignore_architecture=True,
    )

    assert compiled.architecture == "aarch64"
    assert compiled.name == "x86only_arm64"
    assert compiled.variant == "arm64"


def test_undeclared_architecture_without_the_escape_hatch_is_rejected(tmp_path) -> None:
    recipe_dir = tmp_path / "x86only"
    write_single_architecture_recipe(recipe_dir)

    with pytest.raises(ValueError, match="unknown variant/architecture"):
        compile_recipe(recipe_dir, architecture="aarch64")


def test_ignore_architectures_preserves_variant_options(tmp_path) -> None:
    recipe_dir = tmp_path / "gpu-tool"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        """name: gpu-tool
version: 1.0
architectures: [x86_64]
readme: gpu-tool {{ context.version }}
options:
  gpu:
    default: false
variants:
  gpu:
    architecture: x86_64
    options:
      gpu: true
build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives:
    - condition: context.options.gpu
      run: echo gpu-enabled
deploy:
  bins: [gpu-tool]
categories: [workflows]
"""
    )

    compiled = compile_recipe(
        recipe_dir,
        variant="gpu",
        architecture="aarch64",
        ignore_architecture=True,
    )

    assert compiled.name == "gpu-tool_gpu_arm64"
    assert "gpu-enabled" in render_dockerfile(compiled.definition)
