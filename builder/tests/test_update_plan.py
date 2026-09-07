from pathlib import Path

import pytest
import yaml

from builder.update_observations import SourceObservation
from builder.update_plan import plan_sources, next_container_version, validate_sources_config, validate_target_bindings


def make_recipe(tmp_path):
    root = tmp_path / 'demo'
    root.mkdir()
    recipe = {
        'name': 'demo', 'version': '7.3.3',
        'variables': {'tool_version': '7.3', 'source_commit': 'a' * 40},
        'auto_update': {'method': 'sources', 'sources': [
            {'id': 'tool', 'method': 'pypi', 'package': 'example-tool',
             'target': {'variable': 'tool_version', 'fulltest_variable': 'tool_version'}},
            {'id': 'helper', 'method': 'github_commit', 'repo': 'example/helper', 'ref': 'main',
             'target': {'variable': 'source_commit'}},
        ]},
        'build': {'base-image': 'ubuntu:24.04', 'directives': [
            {'run': ['pip install example-tool=={{ context.tool_version }}']}]},
        'files': [{'name': 'helper', 'url': 'https://github.com/example/helper/archive/{{ context.source_commit }}.tar.gz'}],
    }
    path = root / 'build.yaml'
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    (root / 'fulltest.yaml').write_text(yaml.safe_dump({'name': 'demo', 'version': '7.3.3', 'tool_version': '7.3', 'tests': [{'name': 'version', 'command': 'example-tool --version', 'expected_output_contains': '${tool_version}'}]}, sort_keys=False))
    return path


def observations(tool='7.3', helper='a' * 40):
    return {'tool': SourceObservation(tool, 'https://pypi.org/project/example-tool', version=tool),
            'helper': SourceObservation(helper, 'https://github.com/example/helper')}


def test_independent_versions_change_real_inputs_together_and_replay(tmp_path):
    path = make_recipe(tmp_path)
    assert plan_sources(path, observations=observations()) is None
    found = observations('7.4', 'b' * 40)
    plan = plan_sources(path, observations=found)
    assert plan == plan_sources(path, observations=found)
    assert yaml.safe_load(path.read_text())['version'] == '7.3.3'
    assert plan.next_version == '7.3.3.post1'
    assert len(plan.changes) == 2
    plan.apply()
    plan.apply()
    changed = yaml.safe_load(path.read_text())
    assert changed['variables'] == {'tool_version': '7.4', 'source_commit': 'b' * 40}
    suite = yaml.safe_load(path.with_name('fulltest.yaml').read_text())
    assert suite['version'] == plan.next_version
    assert suite['tool_version'] == '7.4'
    assert plan_sources(path, observations=found) is None


def test_conflicting_file_prevents_all_writes(tmp_path):
    path = make_recipe(tmp_path)
    plan = plan_sources(path, observations=observations('7.4'))
    before = path.read_text()
    path.with_name('fulltest.yaml').write_text('changed independently\n')
    with pytest.raises(ValueError, match='conflicts'):
        plan.apply()
    assert path.read_text() == before


def test_failure_of_one_component_leaves_every_file_unchanged(tmp_path):
    path = make_recipe(tmp_path)
    before = path.read_text()
    with pytest.raises(KeyError):
        plan_sources(path, observations={'tool': observations('7.4')['tool']})
    assert path.read_text() == before


def test_versioned_inputs_do_not_downgrade(tmp_path):
    path = make_recipe(tmp_path)
    assert plan_sources(path, observations=observations('7.2')) is None


def test_duplicate_target_is_rejected(tmp_path):
    path = make_recipe(tmp_path)
    recipe = yaml.safe_load(path.read_text())
    config = recipe['auto_update']
    config['sources'][1]['target'] = config['sources'][0]['target']
    with pytest.raises(ValueError, match='multiple sources'):
        validate_sources_config(config)


def test_runtime_documentation_alone_is_not_a_source_binding(tmp_path):
    path = make_recipe(tmp_path)
    recipe = yaml.safe_load(path.read_text())
    recipe['build']['directives'] = []
    recipe['readme'] = '{{ context.tool_version }}'
    with pytest.raises(ValueError, match='acquisition'):
        validate_target_bindings(recipe)


@pytest.mark.parametrize(('old', 'new'), [('1.0', '1.0.post1'), ('1.0.post2', '1.0.post3'), ('4.0.1.sm75', '4.0.1.sm75.r1'), ('latest', 'latest.r1'), ('latest.r9', 'latest.r10')])
def test_container_revision_is_independent_of_upstream_version(old, new):
    assert next_container_version(old) == new


def test_query_parameters_are_not_yaml_anchors():
    from builder.update_plan import rewrite_scalar
    text = 'files:\n  - name: source\n    url: https://example.org/find?a=1&b=2\n'
    changed = rewrite_scalar(text, ('files', 0, 'url'), 'https://example.org/find?a=2&b=3')
    assert yaml.safe_load(changed)['files'][0]['url'].endswith('a=2&b=3')


def test_source_anchor_cannot_change_another_input_through_alias():
    from builder.update_plan import rewrite_scalar
    text = 'variables:\n  first: &source 1.2.3\n  second: *source\n'
    for variable in ('first', 'second'):
        with pytest.raises(ValueError, match='anchors'):
            rewrite_scalar(text, ('variables', variable), '2.0.0')
