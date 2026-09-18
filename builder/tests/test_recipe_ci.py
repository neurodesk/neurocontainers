from pathlib import Path

import pytest
import yaml

from workflows.recipe_ci import build_targets


def recipe(root, name, architectures, variants=None):
    directory = root / 'recipes' / name
    directory.mkdir(parents=True)
    data = {'name': name, 'version': '1.2.3', 'architectures': architectures}
    if variants:
        data['variants'] = variants
    (directory / 'build.yaml').write_text(yaml.safe_dump(data))


def test_explicit_selection_overrides_debug_and_expands_only_declared_targets(tmp_path):
    recipe(tmp_path, 'tool', ['x86_64', 'aarch64'], {'cuda': {'architecture': 'x86_64'}})
    recipe(tmp_path, 'other', ['x86_64'])
    result = build_targets(tmp_path, 'tool, tool', 'all', True)
    assert [(row['container'], row['variant'], row['architecture']) for row in result] == [
        ('tool', '', 'x86_64'), ('tool_arm64', 'arm64', 'aarch64'), ('tool_cuda', 'cuda', 'x86_64')]
    assert all(row['version'] == '1.2.3' for row in result)
    assert build_targets(tmp_path, 'tool', 'arm64', False) == [result[1]]


def test_full_selection_includes_builder_and_filters_unsupported_architectures(tmp_path):
    recipe(tmp_path, 'builder', ['x86_64'])
    recipe(tmp_path, 'tool', ['x86_64', 'aarch64'])
    assert [row['recipe'] for row in build_targets(tmp_path, '', 'x86_64', False)] == ['builder', 'tool']
    assert [row['recipe'] for row in build_targets(tmp_path, '', 'arm64', False)] == ['tool']


def test_debug_defaults_to_niimath(tmp_path):
    recipe(tmp_path, 'niimath', ['x86_64'])
    recipe(tmp_path, 'other', ['x86_64'])
    assert [row['recipe'] for row in build_targets(tmp_path, '', 'x86_64', True)] == ['niimath']


@pytest.mark.parametrize('requested', ['missing', '../tool', 'tool,missing'])
def test_missing_or_escaping_names_fail_before_any_build(tmp_path, requested):
    recipe(tmp_path, 'tool', ['x86_64'])
    with pytest.raises(ValueError, match='Unknown recipes'):
        build_targets(tmp_path, requested, 'all', False)


def test_explicit_unsupported_architecture_is_not_forced(tmp_path):
    recipe(tmp_path, 'tool', ['x86_64'])
    with pytest.raises(ValueError, match='No selected recipe'):
        build_targets(tmp_path, 'tool', 'arm64', False)


def test_recipe_ci_uses_fresh_candidates_and_propagates_fulltest_exit_status():
    workflow = yaml.load(Path('.github/workflows/recipes-ci.yml').read_text(), Loader=yaml.BaseLoader)
    assert workflow['permissions'] == {'contents': 'read'}
    steps = workflow['jobs']['verify']['steps']
    test_step = next(step for step in steps if '-m workflows.release_test_runner' in step.get('run', ''))
    assert '--candidate-container' in test_step['run']
    assert '--test-config "recipes/$RECIPE/fulltest.yaml"' in test_step['run']
    assert 'continue-on-error' not in test_step
    assert '|| true' not in test_step['run']
    assert not any('docker push' in step.get('run', '') for step in steps)
