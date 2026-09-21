import json

import pytest
import yaml

from tools.migrate_container_versions import migrate


@pytest.mark.parametrize('upstream,label', [('7.1', '7.1.0'), ('7.2', '7.2.0'), ('9.21.0', '9.21.0')])
def test_migration_preserves_inputs_and_release_records_and_replays(tmp_path, upstream, label):
    directory = tmp_path / 'recipes' / 'demo'
    directory.mkdir(parents=True)
    recipe = {
        'name': 'demo', 'version': '9.22.0',
        'variables': {'upstream_version': upstream},
        'auto_update': {'method': 'sources', 'sources': [{
            'id': 'demo', 'method': 'pypi', 'package': 'demo',
            'target': {'variable': 'upstream_version', 'fulltest_variable': 'upstream_version'},
        }]},
        'build': {'directives': [{'run': ['pip install demo=={{ context.upstream_version }}']}]},
    }
    path = directory / 'build.yaml'
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    suite = directory / 'fulltest.yaml'
    suite.write_text(yaml.safe_dump({'name': 'demo', 'version': '9.22.0', 'upstream_version': upstream, 'tests': []}))
    release = tmp_path / 'releases' / 'demo' / '9.22.0.json'
    release.parent.mkdir(parents=True)
    release.write_text(json.dumps({'apps': {'demo 9.22.0': {'version': '20260920'}}}))
    before = {p: p.read_bytes() for p in (path, suite, release)}
    assert migrate(tmp_path)['changes'][0]['new'] == label
    assert {p: p.read_bytes() for p in before} == before
    migrate(tmp_path, apply=True)
    changed = yaml.safe_load(path.read_text())
    assert changed['version'] == label
    assert changed['variables'] == recipe['variables']
    assert changed['build'] == recipe['build']
    assert yaml.safe_load(suite.read_text())['version'] == label
    assert release.read_bytes() == before[release]
    assert migrate(tmp_path, apply=True)['changes'] == []


def test_direct_short_version_keeps_exact_download_and_test_versions(tmp_path):
    from builder.template import RenderContext, TemplateRenderer

    directory = tmp_path / 'recipes' / 'demo'
    directory.mkdir(parents=True)
    path = directory / 'build.yaml'
    path.write_text('''name: demo
version: "7.1"
auto_update:
  method: github_release
  repo: example/demo
files:
  - name: archive
    url: https://example.org/v{{ context.version }}/archive.tar.gz
''')
    suite = directory / 'fulltest.yaml'
    suite.write_text('name: demo\nversion: "7.1"\ntests:\n  - command: demo --version\n    expected_output_contains: "${version}"\n')
    migrate(tmp_path, apply=True)
    recipe = yaml.safe_load(path.read_text())
    assert recipe['version'] == '7.1.0'
    context = RenderContext('demo', recipe['version'], 'x86_64', values=recipe['variables'])
    assert TemplateRenderer().render_string(recipe['files'][0]['url'], context) == 'https://example.org/v7.1/archive.tar.gz'
    tests = yaml.safe_load(suite.read_text())
    assert tests['upstream_version'] == '7.1'
    assert tests['tests'][0]['expected_output_contains'] == '${upstream_version}'
    assert migrate(tmp_path, apply=True)['changes'] == []
