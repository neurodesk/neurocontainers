import pytest
import yaml

from builder.versioning import software_version, validate_container_version
from builder.update_plan import plan_sources
from builder.tests.test_update_plan import make_recipe, observations
from builder import check_version
from builder.template import RenderContext, TemplateRenderer


@pytest.mark.parametrize('upstream,label', [
    ('7.1', '7.1.0'), ('7.2', '7.2.0'), ('9.21.0', '9.21.0'),
    ('7.1.3', '7.1.3'), ('6.0.7.22', '6.0.7.22'),
    ('26.0.rc3', '26.0.0.rc3'), ('25.1.2.post1', '25.1.2.post1'),
    ('2025b', '2025b'), ('23a', '23a'), ('20260920', '20260920'),
    ('alpha119', 'alpha119'), ('11_3_3', '11.3.3'),
])
def test_software_version_preserves_upstream_components(upstream, label):
    assert software_version(upstream) == label


def test_manual_container_bump_is_rejected():
    recipe = {'version': '9.21.1', 'variables': {'upstream_version': '9.21.0'},
              'auto_update': {'container_version': {'variable': 'upstream_version'}}}
    with pytest.raises(ValueError, match='must match software version'):
        validate_container_version(recipe)
    recipe['version'] = '9.21.0'
    validate_container_version(recipe)


def test_drift_does_not_suppress_a_new_upstream_release(tmp_path):
    path = make_recipe(tmp_path)
    recipe = yaml.safe_load(path.read_text())
    recipe['version'] = '8.0.0'
    path.write_text(yaml.safe_dump(recipe))
    plan = plan_sources(path, observations=observations('7.4.2'))
    assert plan.next_version == '7.4.2'
    plan.apply()
    assert plan_sources(path, observations=observations('7.4.2')) is None


def test_dependency_rebuilds_have_distinct_plans_without_changing_software(tmp_path):
    path = make_recipe(tmp_path)
    first = plan_sources(path, observations=observations(helper='b' * 40))
    second = plan_sources(path, observations=observations(helper='c' * 40))
    assert first.next_version == second.next_version == '7.3.0'
    assert first.branch != second.branch
    first.apply()
    assert plan_sources(path, observations=observations(helper='b' * 40)) is None


def test_direct_updates_pad_labels_without_rewriting_upstream_urls(tmp_path):
    path = tmp_path / 'build.yaml'
    path.write_text('''name: demo
version: 7.0.0
auto_update:
  method: github_release
  repo: example/demo
files:
  - name: archive
    url: https://example.org/v{{ context.version }}/archive.tar.gz
''')
    path.with_name('fulltest.yaml').write_text('name: demo\nversion: 7.0.0\ntests:\n  - command: demo --version\n    expected_output_contains: "${version}"\n')
    label = check_version.tag_to_recipe_version('v7.1', '7.0.0')
    assert label == '7.1.0'
    updated, _ = check_version.prepare_bump(path, '7.0.0', label, 'example/demo', 'v7.1')
    recipe = yaml.safe_load(updated)
    context = RenderContext('demo', label, 'x86_64', values=recipe['variables'])
    assert TemplateRenderer().render_string(recipe['files'][0]['url'], context) == 'https://example.org/v7.1/archive.tar.gz'
    _, suite, _ = check_version.prepare_fulltest_bump(path, label, updated)
    assert yaml.safe_load(suite)['upstream_version'] == '7.1'
    assert yaml.safe_load(suite)['version'] == label


def test_missing_primary_version_policy_is_rejected():
    from builder.update_plan import validate_sources_config

    with pytest.raises(ValueError, match='requires container_version'):
        validate_sources_config({'method': 'sources', 'local': []})


def test_padded_label_does_not_overwrite_indirect_software_assertion(tmp_path):
    path = make_recipe(tmp_path)
    suite_path = path.with_name('fulltest.yaml')
    suite = yaml.safe_load(suite_path.read_text())
    suite['version'] = '${tool_version}'
    suite_path.write_text(yaml.safe_dump(suite))
    plan = plan_sources(path, observations=observations('7.4'))
    plan.apply()
    suite = yaml.safe_load(suite_path.read_text())
    assert suite['version'] == '7.4.0'
    assert suite['tool_version'] == '7.4'


def test_short_version_with_a_raw_tag_binding_remains_auditable(tmp_path):
    from builder.audit_updates import validate_update_policy

    path = tmp_path / 'build.yaml'
    path.write_text('''name: demo
version: 7.0.0
variables: {upstream_tag: v7.0.0}
auto_update: {method: github_tags, repo: example/demo, tag_variable: upstream_tag}
build:
  directives:
    - run: git clone --branch {{ context.upstream_tag }} https://github.com/example/demo
''')
    updated, _ = check_version.prepare_bump(path, '7.0.0', '7.1.0', 'example/demo', 'v7.1')
    recipe = yaml.safe_load(updated)
    validate_update_policy(recipe)
    assert recipe['variables']['upstream_tag'] == 'v7.1'
    assert recipe['variables']['upstream_version'] == '7.1'
    assert recipe['version'] == '7.1.0'
