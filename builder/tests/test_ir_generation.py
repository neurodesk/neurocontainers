from __future__ import annotations

from builder.config import default_config, resolve_recipe
from builder.ir import Env, Run, RunWithMounts, Workdir
from builder.recipe import compile_recipe


def test_dcm2niix_ir_contains_expected_directives() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    directives = compiled.definition.directives
    assert any(isinstance(item, Workdir) and item.path == "/opt/dcm2niix-v1.0.20240202" for item in directives)
    assert any(isinstance(item, RunWithMounts) and "dcm2niix_lnx.zip" in item.command for item in directives)
    assert any(isinstance(item, Env) and item.values.get("DEPLOY_PATH") == "/opt/dcm2niix-v1.0.20240202" for item in directives)


def test_ants_template_backend_generates_local_ir() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "ants"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    docker_env = [
        item.values
        for item in compiled.definition.directives
        if isinstance(item, Env)
    ]
    assert any(values.get("ANTSPATH") == "/opt/ants-2.6.5/bin" for values in docker_env)
    assert any(isinstance(item, Run) and "cd /tmp/ants/build" in item.command for item in compiled.definition.directives)
    assert any(
        isinstance(item, Env)
        and item.values.get("DEPLOY_PATH") == "/opt/ants-2.6.5/bin:/opt/ants-2.6.5/Scripts"
        for item in compiled.definition.directives
    )
