from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from builder.ir import Env, Run
from builder.dockerfile import render_directive, render_dockerfile
from builder.template import RenderContext, TemplateError, TemplateRenderer
from builder.recipe import compile_recipe
from builder.template_backend import apply_builtin_template


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
    template_dir = Path(__file__).resolve().parents[1] / "templates"
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
            assert method_data.get("builder") == "neurodocker", f"{path.name}:{method} has no macro builder"
            assert "env" not in method_data, f"{path.name}:{method} still uses legacy env"
            assert "instructions" not in method_data, f"{path.name}:{method} still uses legacy instructions"
            assert isinstance(method_data.get("directives"), list), f"{path.name}:{method} has no directives"


def test_bids_validator_template_installs_setuptools_on_apt() -> None:
    path = Path(__file__).resolve().parents[1] / "templates" / "bids_validator.yaml"
    data = yaml.safe_load(path.read_text())
    apt_dependencies = data["binaries"]["dependencies"]["apt"]
    assert "python3-setuptools" in apt_dependencies


def test_miniconda_template_bootstraps_python_and_pip_for_pip_install() -> None:
    directives: list[Run] = []
    apply_builtin_template(
        "miniconda",
        {
            "version": "latest",
            "env_name": "testenv",
            "env_exists": "false",
            "pip_install": "examplepkg",
            "arch": "aarch64",
        },
        "apt",
        directives.append,
    )
    command = "\n".join(item.command for item in directives if isinstance(item, Run))
    assert 'bash -c "source activate testenv' in command
    assert "if ! python -m pip --version >/dev/null 2>&1; then" in command
    assert "conda install -y" in command
    assert "--name testenv python pip" in command


def test_miniconda_template_escapes_pip_packages_inside_bash_c() -> None:
    compiled = compile_recipe(
        Path(__file__).resolve().parent / "fixtures" / "miniconda_pip_install",
        architecture="x86_64",
    )
    dockerfile = render_dockerfile(compiled.definition)

    assert '\\"examplepkg==1.2.3\\"' in dockerfile
    assert '"examplepkg==1.2.3""' not in dockerfile


def test_miniconda_template_guards_conda_tos_for_older_installers() -> None:
    directives: list[Run] = []
    apply_builtin_template(
        "miniconda",
        {
            "version": "py37_4.12.0",
            "arch": "x86_64",
        },
        "apt",
        directives.append,
    )
    command = "\n".join(item.command for item in directives if isinstance(item, Run))
    assert "if conda tos --help >/dev/null 2>&1; then conda tos accept; fi;" in command
    assert "\nconda tos accept\n" not in command


def test_miniconda_template_latest_chains_update_after_tos_guard() -> None:
    directives: list[Run] = []
    apply_builtin_template(
        "miniconda",
        {
            "version": "latest",
            "arch": "x86_64",
        },
        "apt",
        directives.append,
    )
    command = "\n".join(item.command for item in directives if isinstance(item, Run))
    assert "if conda tos --help >/dev/null 2>&1; then conda tos accept; fi;" in command
    assert "\nconda update -yq -nbase conda" in command
    assert "\n&& conda update -yq -nbase conda" not in command
    dockerfile_run = "\n".join(render_directive(next(item for item in directives if isinstance(item, Run))))
    assert "then conda tos accept; fi; \\\n       conda update" in dockerfile_run


def test_miniconda_template_mamba_chains_after_tos_guard() -> None:
    directives: list[Run] = []
    apply_builtin_template(
        "miniconda",
        {
            "version": "py39_24.7.1-0",
            "mamba": "true",
            "arch": "x86_64",
        },
        "apt",
        directives.append,
    )
    command = "\n".join(item.command for item in directives if isinstance(item, Run))
    assert "\nconda install -yq -nbase conda-libmamba-solver" in command
    assert "\nconda config --set solver libmamba" in command
    assert "\n&& conda install -yq -nbase conda-libmamba-solver" not in command
    assert "\n&& conda config --set solver libmamba" not in command
    dockerfile_run = "\n".join(render_directive(next(item for item in directives if isinstance(item, Run))))
    assert "then conda tos accept; fi; \\\n       conda install" in dockerfile_run
    assert "\n    && conda config --set solver libmamba" in dockerfile_run


def test_miniconda_template_pinned_default_chains_after_tos_guard() -> None:
    directives: list[Run] = []
    apply_builtin_template(
        "miniconda",
        {
            "version": "py313_26.3.2-2",
            "conda_install": "python=3.12",
            "arch": "x86_64",
        },
        "apt",
        directives.append,
    )
    dockerfile_run = "\n".join(render_directive(next(item for item in directives if isinstance(item, Run))))
    assert not any(line.strip().startswith("#") for line in dockerfile_run.splitlines())

    shell_command = dockerfile_run.removeprefix("RUN ").replace("\\\n", " ")
    result = subprocess.run(
        ["/bin/sh", "-n", "-c", shell_command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_matlabmcr_template_uses_release_named_runtime_dirs_for_r2023b() -> None:
    directives = []
    apply_builtin_template(
        "matlabmcr",
        {"version": "2023b", "install_path": "/opt/mcr"},
        "apt",
        directives.append,
    )
    env = next(item.values for item in directives if isinstance(item, Env))
    assert "/opt/mcr/R2023b/runtime/glnxa64" in env["LD_LIBRARY_PATH"]
    assert env["MATLABCMD"].strip() == "/opt/mcr/R2023b/toolbox/matlab"
    assert env["MCRROOT"].strip() == "/opt/mcr/R2023b"
    assert env["XAPPLRESDIR"].strip() == "/opt/mcr/R2023b/x11/app-defaults"


def test_matlabmcr_template_allows_legacy_ncurses_package() -> None:
    directives = []
    apply_builtin_template(
        "matlabmcr",
        {"version": "2019b", "install_path": "/opt/mcr", "ncurses_package": "libncurses5"},
        "apt",
        directives.append,
    )
    command = "\n".join(item.command for item in directives if isinstance(item, Run))
    assert "libncurses5" in command
    assert "libncurses6" not in command
