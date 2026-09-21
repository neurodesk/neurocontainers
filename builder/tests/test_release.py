from __future__ import annotations

from builder.config import default_config, resolve_recipe
from builder.recipe import compile_recipe
from builder.release import release_data, release_version


def test_release_shape_matches_current_contract() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    data = release_data(compiled.name, compiled.version, compiled.recipe, "20260102")
    assert data["categories"] == ["data organisation"]
    app = data["apps"][f"{compiled.name} {compiled.version}"]
    assert app["version"] == "20260102"
    assert app["exec"] == ""


def test_release_renders_gui_app_exec_from_recipe_context() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "cat12"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    data = release_data(compiled.name, compiled.version, compiled.recipe, "20260519")
    exec_command = data["apps"][f"cat12GUI-{compiled.name} {compiled.version}"]["exec"]

    assert exec_command == "bash run_spm25.sh /opt/mcr/R2023b/"
    assert "{{" not in exec_command


def test_release_preserves_container_visibility_flags() -> None:
    data = release_data(
        "tool",
        "1.2.3",
        {
            "categories": ["workflows"],
            "show_in_menu": False,
            "show_in_applist": False,
        },
        "20260102",
    )

    assert data["show_in_menu"] is False
    assert data["show_in_applist"] is False


def test_arm64_release_shape_matches_current_contract() -> None:
    data = release_data(
        "tool",
        "1.2.3",
        {"categories": ["workflows"], "apptainer_args": ["--cleanenv"]},
        "20260102",
        "aarch64",
    )
    assert release_version("1.2.3", "aarch64") == "1.2.3-arm64"
    assert data["architecture"] == "aarch64"
    assert data["apps"]["tool 1.2.3 arm64"]["architecture"] == "aarch64"
    assert data["apps"]["tool 1.2.3 arm64"]["image"] == "tool_1.2.3_arm64"


def test_named_arm64_variant_is_a_normal_container_release() -> None:
    data = release_data(
        "tool_arm64",
        "1.2.3",
        {"categories": ["workflows"], "apptainer_args": ["--cleanenv"]},
        "20260102",
        "aarch64",
        "arm64",
        source_recipe="tool",
    )

    assert release_version("1.2.3", "aarch64", "arm64") == "1.2.3"
    assert data["variant"] == "arm64"
    assert data["architecture"] == "aarch64"
    assert data["recipe"] == "tool"
    assert data["apps"] == {
        "tool_arm64 1.2.3": {
            "version": "20260102",
            "exec": "",
            "apptainer_args": ["--cleanenv"],
        }
    }


def test_packaged_script_and_shared_input_changes_advance_build_date(tmp_path, monkeypatch):
    import os
    import subprocess
    import yaml
    from builder.release import build_date_for_recipe
    from tools import one_pr_release

    monkeypatch.delenv('BUILDDATE', raising=False)
    monkeypatch.setattr(one_pr_release, 'REPO_ROOT', tmp_path)
    recipe_dir = tmp_path / 'recipes' / 'demo'
    recipe_dir.mkdir(parents=True)
    shared_dir = tmp_path / 'macros' / 'shared'
    shared_dir.mkdir(parents=True)
    (recipe_dir / 'build.yaml').write_text(yaml.safe_dump({
        'name': 'demo', 'version': '7.1.0',
        'auto_update': {'method': 'sources', 'container_version': False, 'local': ['macros/shared']},
    }))
    script = recipe_dir / 'launcher.sh'
    script.write_text('echo first\n')
    shared = shared_dir / 'install.sh'
    shared.write_text('echo dependency\n')

    def git(*args, date=None):
        environment = dict(os.environ)
        if date:
            environment.update(GIT_AUTHOR_DATE=f'{date}T12:00:00+00:00',
                               GIT_COMMITTER_DATE=f'{date}T12:00:00+00:00')
        subprocess.run(['git', *args], cwd=tmp_path, env=environment,
                       check=True, capture_output=True)

    git('init')
    git('config', 'user.name', 'Test')
    git('config', 'user.email', 'test@example.org')
    git('add', '.')
    git('commit', '-m', 'Initial recipe', date='2026-09-19')
    for date, file in [('2026-09-20', script), ('2026-09-21', shared)]:
        file.write_text(file.read_text() + 'echo updated\n')
        git('add', '.')
        git('commit', '-m', 'Update packaged input', date=date)
        expected = date.replace('-', '')
        assert build_date_for_recipe(tmp_path, recipe_dir) == expected
        assert one_pr_release.build_date('demo') == expected
