from __future__ import annotations

import argparse
import json
import os
import subprocess
from types import SimpleNamespace

import pytest

from builder import cli


def test_cmd_login_returns_docker_exit_code_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = argparse.Namespace(dry_run=False, offline_mode=False)
    commands: list[list[str]] = []

    monkeypatch.setattr(cli, "cmd_build", lambda args: 0)
    monkeypatch.setattr(
        cli,
        "compile_from_args",
        lambda args: (
            object(),
            SimpleNamespace(
                tag="tool:1.0",
                architecture="x86_64",
                recipe_dir=cli.Path("/repo/recipes/tool"),
            ),
        ),
    )
    monkeypatch.setattr(
        subprocess,
        "call",
        lambda command: commands.append(command) or 130,
    )
    monkeypatch.setattr(
        subprocess,
        "check_call",
        lambda command: (_ for _ in ()).throw(
            AssertionError("cmd_login should not raise on docker run exit")
        ),
    )

    assert cli.cmd_login(args) == 130
    assert commands == [
        [
            "docker",
            "run",
            "--platform",
            "linux/amd64",
            "--rm",
            "-v",
            "/repo/recipes/tool:/buildhostdirectory",
            "-it",
            "tool:1.0",
        ]
    ]


def test_cmd_stage_can_download_declared_url_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    compiled = SimpleNamespace(
        name="tool",
        version="1.0",
        architecture="x86_64",
        variant="",
        staging_plan=SimpleNamespace(files={"archive": object()}),
    )
    config = SimpleNamespace(repo_root=object(), output_root=object())
    build_dir = SimpleNamespace()
    dockerfile_path = SimpleNamespace()

    def fake_write_build_files(*args, **kwargs):
        calls.append(kwargs)
        return build_dir, dockerfile_path

    monkeypatch.setattr(cli, "compile_from_args", lambda args: (config, compiled))
    monkeypatch.setattr(cli, "write_build_files", fake_write_build_files)

    args = argparse.Namespace(output_root=None, recreate=True, download=True)

    assert cli.cmd_stage(args) == 0
    assert calls == [{"recreate": True, "stage": True, "download": True}]


def test_write_build_files_stages_rendered_boutiques_descriptor(tmp_path):
    recipe_dir = tmp_path / "recipe"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        """
variables:
  descriptor_version: 9.8.7
name: boutique-test
version: 1.2.3
architectures: [aarch64]
categories: [programming]
build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives:
    - boutique:
        name: boutique-test
        description: Test descriptor staging.
        tool-version: '{{ context.descriptor_version }}'
        schema-version: '0.5'
        command-line: boutique-test [INPUT]
        inputs:
          - name: input
            id: input
            description: Test input.
            type: String
            optional: true
            value-key: '[INPUT]'
readme: Boutique staging test.
""".lstrip()
    )
    compiled = cli.compile_recipe(
        recipe_dir,
        architecture="aarch64",
    )

    build_dir, _ = cli.write_build_files(
        tmp_path,
        compiled,
        tmp_path / "build",
        recreate=True,
        stage=True,
    )

    descriptor = json.loads((build_dir / "boutique-test.json").read_text())
    assert descriptor["name"] == "boutique-test"
    assert descriptor["tool-version"] == "9.8.7"
    assert descriptor["command-line"] == "boutique-test [INPUT]"


def test_write_build_files_rejects_empty_readme(tmp_path: cli.Path) -> None:
    compiled = SimpleNamespace(name="tool", readme=" \n")

    with pytest.raises(ValueError, match="compiled README.*cannot be empty"):
        cli.write_build_files(tmp_path, compiled, tmp_path / "build")


def test_init_requires_user_to_configure_upstream(tmp_path, monkeypatch):
    import yaml
    from builder.audit_updates import validate_update_policy

    monkeypatch.setattr(
        cli, "default_config", lambda: SimpleNamespace(repo_root=tmp_path)
    )
    cli.cmd_init(argparse.Namespace(name="demo", version="1.0.0"))
    data = yaml.safe_load((tmp_path / "recipes/demo/build.yaml").read_text())
    assert data["auto_update"]["method"] == "github_release"
    with pytest.raises(ValueError):
        validate_update_policy(data)


def test_build_metadata_is_readable_with_restrictive_umask(tmp_path):
    from builder.ir import Definition, From

    recipe_dir = tmp_path / "recipe"
    recipe_dir.mkdir()
    source = recipe_dir / "build.yaml"
    source.write_text("name: tool\nversion: 1.0\n")
    source.chmod(0o600)
    compiled = SimpleNamespace(
        name="tool",
        version="1.0",
        readme="# Tool\n",
        recipe_dir=recipe_dir,
        definition=Definition([From("ubuntu:24.04")]),
        recipe={"build": {}},
        architecture="x86_64",
    )
    previous_umask = os.umask(0o077)
    try:
        build_dir, dockerfile = cli.write_build_files(tmp_path, compiled, tmp_path / "build")
    finally:
        os.umask(previous_umask)

    for artifact in (dockerfile, build_dir / "README.md", build_dir / "build.yaml"):
        assert artifact.stat().st_mode & 0o777 == 0o644
    assert (build_dir / "build.yaml").read_bytes() == source.read_bytes()
    assert (build_dir / "README.md").read_text() == "# Tool\n"
    assert source.stat().st_mode & 0o777 == 0o600
