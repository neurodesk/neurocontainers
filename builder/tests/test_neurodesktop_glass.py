from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from types import ModuleType

from builder.config import default_config, resolve_recipe
from builder.dockerfile import render_dockerfile
from builder.recipe import compile_recipe


def _load_config_generator(script: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("generate_jupyter_config", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _server_proxy_names(config_source: str) -> set[str]:
    tree = ast.parse(config_source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if (
            isinstance(target, ast.Attribute)
            and target.attr == "servers"
            and isinstance(target.value, ast.Attribute)
            and target.value.attr == "ServerProxy"
            and isinstance(node.value, ast.Dict)
        ):
            return {
                key.value
                for key in node.value.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    raise AssertionError("generated config does not assign c.ServerProxy.servers")


def test_glass_image_uses_native_desktop_and_on_demand_jupyter() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "neurodesktop-lite"),
        architecture="aarch64",
        include_dirs=config.include_dirs,
        option_overrides={"glass": True},
    )
    dockerfile = render_dockerfile(compiled.definition)

    assert "neurodesktop-jupyter.service" in dockerfile
    assert "neurodesktop-start-jupyter" in dockerfile
    assert "jupyterlab.desktop" in dockerfile
    assert "--without-remote-desktop" in dockerfile
    assert 'DEPLOY_BINS="start-neurodesktop-jupyterlab"' in dockerfile
    assert "jovyan ALL=(ALL) NOPASSWD: ALL" in dockerfile

    assert "tigervnc" not in dockerfile
    assert "xrdp" not in dockerfile
    assert "guacamole-server" not in dockerfile
    assert "apache-tomcat" not in dockerfile
    assert "start-neurodesktop-guacamole" not in dockerfile


def test_jupyter_config_can_omit_the_nested_desktop(tmp_path: Path) -> None:
    config = default_config()
    recipe = resolve_recipe(config, "neurodesktop-lite")
    generator = _load_config_generator(
        recipe / "scripts" / "generate_jupyter_config.py"
    )
    webapps = tmp_path / "webapps.json"
    webapps.write_text('{"webapps": {}}', encoding="utf-8")
    output = tmp_path / "jupyter_notebook_config.py"

    generator.generate_config(
        webapps,
        recipe / "config" / "jupyter" / "jupyter_notebook_config.py.template",
        output,
        include_remote_desktop=False,
    )

    assert _server_proxy_names(output.read_text(encoding="utf-8")) == {"vscode"}


def test_standard_jupyter_config_retains_the_nested_desktop(tmp_path: Path) -> None:
    config = default_config()
    recipe = resolve_recipe(config, "neurodesktop-lite")
    generator = _load_config_generator(
        recipe / "scripts" / "generate_jupyter_config.py"
    )
    webapps = tmp_path / "webapps.json"
    webapps.write_text('{"webapps": {}}', encoding="utf-8")
    output = tmp_path / "jupyter_notebook_config.py"

    generator.generate_config(
        webapps,
        recipe / "config" / "jupyter" / "jupyter_notebook_config.py.template",
        output,
    )

    assert _server_proxy_names(output.read_text(encoding="utf-8")) == {
        "neurodesktop",
        "vscode",
    }
