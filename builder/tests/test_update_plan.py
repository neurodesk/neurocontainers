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


def make_apt_recipe(tmp_path):
    """A recipe whose label is the software version and whose pin is not."""
    root = tmp_path / 'apt-demo'
    root.mkdir()
    recipe = {
        'name': 'apt-demo', 'version': '2.10.36',
        'variables': {'package_version': '2.10.36-3ubuntu0.24.04.1', 'helper_commit': 'a' * 40},
        'auto_update': {'method': 'sources', 'container_version': 'tool', 'sources': [
            {'id': 'tool', 'method': 'apt', 'package': 'tool',
             'urls': ['https://archive.ubuntu.com/ubuntu/dists/noble/universe/binary-amd64/Packages.gz'],
             'target': {'variable': 'package_version', 'fulltest_variable': 'package_version'}},
            {'id': 'helper', 'method': 'github_commit', 'repo': 'example/helper', 'ref': 'main',
             'target': {'variable': 'helper_commit'}},
        ]},
        'build': {'base-image': 'ubuntu:24.04', 'directives': [
            {'install': ['tool={{ context.package_version }}']}]},
        'files': [{'name': 'helper', 'url': 'https://github.com/example/helper/archive/{{ context.helper_commit }}.tar.gz'}],
    }
    path = root / 'build.yaml'
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    (root / 'fulltest.yaml').write_text(yaml.safe_dump(
        {'name': 'apt-demo', 'version': '2.10.36', 'package_version': '2.10.36-3ubuntu0.24.04.1',
         'tests': [{'name': 'version', 'command': 'tool --version', 'expected_output_contains': '${version}'}]},
        sort_keys=False))
    return path


@pytest.fixture
def debian_ordering(monkeypatch):
    """Order apt versions without dpkg, which is absent on non-Debian hosts."""
    from builder import update_observations

    monkeypatch.setattr(
        update_observations, "_debian_newer", lambda candidate, current: candidate > current
    )


def apt_observation(package_version):
    from builder.update_observations import debian_upstream_version

    return SourceObservation(package_version, 'https://archive.ubuntu.com/ubuntu',
                             version=debian_upstream_version(package_version))


def test_nominated_source_moves_the_container_label_to_the_software_version(tmp_path, debian_ordering):
    # The apt pin carries a packaging revision that must not reach the label.
    path = make_apt_recipe(tmp_path)
    found = {'tool': apt_observation('2.10.42-1ubuntu2'),
             'helper': SourceObservation('a' * 40, 'https://github.com/example/helper')}

    plan = plan_sources(path, observations=found)

    assert plan.next_version == '2.10.42'
    plan.apply()
    changed = yaml.safe_load(path.read_text())
    assert changed['version'] == '2.10.42'
    assert changed['variables']['package_version'] == '2.10.42-1ubuntu2'
    suite = yaml.safe_load(path.with_name('fulltest.yaml').read_text())
    assert suite['version'] == '2.10.42'


def test_a_dependency_moving_alone_still_rebuilds_the_same_software(tmp_path):
    path = make_apt_recipe(tmp_path)
    found = {'tool': apt_observation('2.10.36-3ubuntu0.24.04.1'),
             'helper': SourceObservation('b' * 40, 'https://github.com/example/helper')}

    plan = plan_sources(path, observations=found)

    assert plan.next_version == '2.10.36.post1'


def test_repackaging_the_same_software_does_not_rename_the_container(tmp_path, debian_ordering):
    # A new Debian revision of the same upstream release is a rebuild, not a
    # new version of the software the label names.
    path = make_apt_recipe(tmp_path)
    found = {'tool': apt_observation('2.10.36-4ubuntu1'),
             'helper': SourceObservation('a' * 40, 'https://github.com/example/helper')}

    plan = plan_sources(path, observations=found)

    assert plan.next_version == '2.10.36.post1'
    plan.apply()
    assert yaml.safe_load(path.read_text())['variables']['package_version'] == '2.10.36-4ubuntu1'


def test_container_version_must_name_a_source_that_observes_a_version():
    policy = {'method': 'sources', 'container_version': 'helper', 'sources': [
        {'id': 'helper', 'method': 'github_commit', 'repo': 'example/helper', 'ref': 'main',
         'target': {'variable': 'helper_commit'}}]}

    with pytest.raises(ValueError, match='pins bytes'):
        validate_sources_config(policy)

    policy['container_version'] = 'absent'
    with pytest.raises(ValueError, match='must name one of'):
        validate_sources_config(policy)
