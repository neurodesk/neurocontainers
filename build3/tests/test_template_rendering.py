from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from build3.template import RenderContext, TemplateError, TemplateRenderer
from build3.recipe import compile_recipe


def test_strict_context_rendering() -> None:
    renderer = TemplateRenderer()
    context = RenderContext(name="tool", version="1.2.3", arch="x86_64")
    assert renderer.render_string("{{ context.version }} {{ arch }}", context) == "1.2.3 x86_64"


def test_get_file_requires_declared_file() -> None:
    renderer = TemplateRenderer()
    context = RenderContext(name="tool", version="1.2.3", arch="x86_64")
    with pytest.raises(TemplateError):
        renderer.render_string('{{ get_file("missing") }}', context)


def test_get_local_tracks_requested_context() -> None:
    renderer = TemplateRenderer()
    context = RenderContext(
        name="tool",
        version="1.2.3",
        arch="x86_64",
        local_keys={"src"},
    )
    assert renderer.render_string('{{ get_local("src") }}', context) == "/.neurocontainer-local/src"
    assert context.requested_locals == ["src"]


def test_conditional_fixture_resolves_arch_variable() -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "conditional"
    compiled = compile_recipe(fixture, architecture="x86_64")
    assert any(
        getattr(item, "values", {}).get("SELECTED_PACKAGE") == "curl"
        for item in compiled.definition.directives
    )


def test_builtin_templates_are_native_directive_format() -> None:
    template_dir = Path(__file__).resolve().parents[1] / "src" / "build3" / "templates"
    assert sorted(path.stem for path in template_dir.glob("*.yaml")) == [
        "afni",
        "ants",
        "bids_validator",
        "convert3d",
        "dcm2niix",
        "freesurfer",
        "fsl",
        "matlabmcr",
        "minc",
        "miniconda",
        "mrtrix3",
        "spm12",
    ]
    for path in template_dir.glob("*.yaml"):
        data = yaml.safe_load(path.read_text())
        for method in ("binaries", "source"):
            method_data = data.get(method)
            if not isinstance(method_data, dict):
                continue
            assert "env" not in method_data, f"{path.name}:{method} still uses legacy env"
            assert "instructions" not in method_data, f"{path.name}:{method} still uses legacy instructions"
            assert isinstance(method_data.get("directives"), list), f"{path.name}:{method} has no directives"
