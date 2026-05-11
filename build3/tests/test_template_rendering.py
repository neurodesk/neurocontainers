from __future__ import annotations

from pathlib import Path

import pytest

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
