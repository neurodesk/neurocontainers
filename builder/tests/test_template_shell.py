import re
import shlex
import subprocess

import pytest

from builder.dockerfile import render_dockerfile
from builder.ir import Definition, Run
from builder.recipe import _default_template_command
from builder.template_backend import apply_builtin_template


@pytest.mark.parametrize('manager', ['apt', 'rpm'])
def test_rendered_default_setup_is_valid_shell(manager):
    definition = Definition(pkg_manager=manager)
    definition.add(Run(_default_template_command(manager)))
    rendered = render_dockerfile(definition)
    command = re.search(r'(?ms)^RUN (.*?)(?=^[A-Z]+ |\Z)', rendered).group(1)
    subprocess.run(['sh', '-n'], input=command.replace('\\\n', ' '), text=True, check=True)


@pytest.mark.parametrize('installer', ['pip', 'conda'])
@pytest.mark.parametrize('packages', ['numpy "torch>=1.9"', "numpy 'pandas<3'", 'numpy pandas'])
def test_miniconda_passes_quoted_requirements_as_literal_arguments(packages, installer):
    directives = []
    apply_builtin_template('miniconda', {
        'version': 'py311_25.5.1-0', 'env_name': 'example',
        'env_exists': 'false', f'{installer}_install': packages,
    }, 'apt', directives.append)
    run = next(item.command for item in directives if isinstance(item, Run))
    words = shlex.split(run)
    inner = (
        next(words[i + 1] for i, word in enumerate(words) if word == '-c')
        if installer == 'pip' else re.search(r'(?ms)^conda install .*?(?=^sync)', run).group()
    )
    result = subprocess.run(
        ['bash', '-c', 'source() { :; }; python() { printf "%s\\n" "$@"; }; conda() { printf "%s\\n" "$@"; }; ' + inner],
        text=True, capture_output=True, check=True,
    )
    expected = ['-m', 'pip', 'install', '--no-cache-dir'] if installer == 'pip' else ['install', '-y', '--name', 'example']
    assert result.stdout.splitlines() == [*expected, *shlex.split(packages)]
