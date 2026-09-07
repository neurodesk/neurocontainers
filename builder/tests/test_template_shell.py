import json
import re
import shlex
import subprocess

import pytest

from builder.dockerfile import render_directive, render_dockerfile
from builder.ir import Definition, Run, RunWithMounts
from builder.recipe import _default_template_command
from builder.template_backend import apply_builtin_template


@pytest.mark.parametrize('with_mount', [False, True])
def test_blank_lines_do_not_change_rendered_shell_execution(with_mount, tmp_path):
    command = "\n\n  printf '%s\\n' first\n\n  printf '%s\\n' second\n\n"
    mount = '--mount=type=cache,target=/cache'
    directive = RunWithMounts((mount,), command) if with_mount else Run(command)
    rendered = render_directive(directive)[0].removeprefix('RUN ')
    if with_mount:
        assert rendered.startswith(mount + ' ')
        rendered = rendered.removeprefix(mount + ' ')
    result = subprocess.run(['sh', '-ec', rendered.replace('\\\n', ' ')],
                            text=True, capture_output=True, check=True)
    assert result.stdout == 'first\nsecond\n'
    definition = Definition(pkg_manager='apt')
    definition.add(directive)
    dockerfile = render_dockerfile(definition)
    save = dockerfile.split('# Save specification to JSON.\nRUN ', 1)[1]
    save = save.split('# End saving to specification to JSON.', 1)[0]
    spec_path = tmp_path / 'reproenv.json'
    save = save.replace('/.reproenv.json', shlex.quote(str(spec_path)))
    subprocess.run(['sh', '-ec', save.replace('\\\n', ' ')], check=True)
    recorded = json.loads(spec_path.read_text())['instructions'][0]['kwds']['command']
    expected = [mount, *shlex.split(command)] if with_mount else shlex.split(command)
    assert shlex.split(recorded) == expected


@pytest.mark.parametrize('version', ['2018a', '2019b', '2020a', '2023a', '2023b'])
def test_matlab_template_renders_valid_shell(version):
    directives = []
    apply_builtin_template('matlabmcr', {'version': version}, 'apt', directives.append)
    for directive in directives:
        if isinstance(directive, Run):
            rendered = render_directive(directive)[0].removeprefix('RUN ')
            subprocess.run(['sh', '-n'], input=rendered.replace('\\\n', ' '), text=True, check=True)


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
