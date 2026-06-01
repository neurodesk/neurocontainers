from __future__ import annotations

from builder.config import default_config, resolve_recipe
from builder.recipe import compile_recipe


def test_prequal_uses_conda_python_for_synb0_runtime() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "prequal"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    directives = compiled.recipe["build"]["directives"]
    rendered = "\n".join(str(directive) for directive in directives)

    assert "python3.6" not in rendered
    assert "ppa:deadsnakes/ppa" not in rendered
    assert "/APPS/synb0/conda/envs/py37/bin/python -m venv pytorch" in rendered
    assert "sed -i 's/python3" in rendered
    assert "/python /g' /APPS/synb0/synb0.sh" in rendered
