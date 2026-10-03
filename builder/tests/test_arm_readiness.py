import json
import hashlib

from workflows.arm_readiness import readiness
from builder.tests.test_recipe_ci import recipe


def test_named_variants_and_current_versions_are_distinct_from_observations(tmp_path):
    recipe(tmp_path, 'tool', ['x86_64', 'aarch64'], {'gpu': {'architecture': 'aarch64'}})
    recipe(tmp_path, 'intel', ['x86_64'])
    releases = tmp_path / 'releases/tool_arm64'
    releases.mkdir(parents=True)
    (releases / '1.2.3.json').write_text(json.dumps({'architecture': 'aarch64', 'variant': 'arm64',
        'apps': {'tool_arm64 1.2.3': {'version': '20261001'}}}))
    report = readiness(tmp_path)
    assert report['summary'] == {'recipes': 2, 'declared_recipes': 1, 'arm_targets': 2,
        'current_release_records': 1, 'public_artifacts_observed': 0, 'native_passed': 0}
    targets = {row.get('container'): row for row in report['targets'] if row['declared']}
    assert targets['tool_arm64']['catalog_identity']
    assert not targets['tool_gpu_arm64']['release_record']


def test_wrong_platform_probe_is_reported_as_failure(tmp_path, monkeypatch):
    recipe(tmp_path, 'tool', ['aarch64'])
    releases = tmp_path / 'releases/tool_arm64'
    releases.mkdir(parents=True)
    (releases / '1.2.3.json').write_text(json.dumps({'architecture': 'aarch64', 'variant': 'arm64',
        'apps': {'tool_arm64 1.2.3': {'version': '20261001'}}}))
    def wrong_platform(*args):
        raise ValueError('Published image is not linux/arm64')
    monkeypatch.setattr('workflows.arm_readiness.probe_public_release', wrong_platform)
    row = readiness(tmp_path, probe=True)['targets'][0]
    assert row['published']['status'] == 'error'
    assert row['native_runtime']['status'] == 'unobserved'


def test_native_observation_requires_matching_identity_and_machine(tmp_path):
    recipe(tmp_path, 'tool', ['aarch64'])
    results = tmp_path / 'results/tool_arm64'
    results.mkdir(parents=True)
    data = {'container': 'tool_arm64', 'version': '1.2.3', 'machine': 'x86_64', 'status': 'passed',
            'recipe_sha256': hashlib.sha256((tmp_path / 'recipes/tool/build.yaml').read_bytes()).hexdigest(),
            'image_id': 'sha256:' + 'a' * 64, 'candidate_sha256': 'b' * 64}
    (results / 'native-result.json').write_text(json.dumps(data))
    assert readiness(tmp_path, results=results.parent)['summary']['native_passed'] == 0
    data['machine'] = 'aarch64'
    (results / 'native-result.json').write_text(json.dumps(data))
    assert readiness(tmp_path, results=results.parent)['summary']['native_passed'] == 1


def test_index_selects_arm_and_keeps_index_identity_for_referrers(monkeypatch):
    from builder.image_fingerprint import HttpResponse
    from workflows.arm_readiness import probe_public_release
    index = {'manifests': [
        {'digest': 'sha256:unknown', 'platform': {'os': 'unknown', 'architecture': 'unknown'}},
        {'digest': 'sha256:intel', 'platform': {'os': 'linux', 'architecture': 'amd64'}},
        {'digest': 'sha256:arm', 'platform': {'os': 'linux', 'architecture': 'arm64'}}]}
    body = json.dumps(index).encode()
    index_digest = 'sha256:' + hashlib.sha256(body).hexdigest()
    paths = []
    class Client:
        def __init__(self, *args):
            pass
        def get(self, path, accept):
            paths.append(path)
            if '/referrers/' in path:
                return HttpResponse(200, body=json.dumps({'manifests': [{'digest': 'sha256:sif',
                    'artifactType': 'application/vnd.sylabs.sif.layer.v1.sif'}]}).encode())
            return HttpResponse(200, body=body)
        def get_manifest(self, repository, digest):
            assert digest == 'sha256:arm'
            return {'config': {'digest': 'sha256:config'}}
        def get_config_blob(self, repository, digest):
            return {'architecture': 'arm64', 'os': 'linux'}
    monkeypatch.setattr('workflows.arm_readiness.RegistryClient', Client)
    result = probe_public_release('tool_arm64', '1.2.3', '20261003')
    assert result['image_digest'] == index_digest
    assert result['platform_manifest_digest'] == 'sha256:arm'
    assert paths[-1].endswith('/referrers/' + index_digest)


def test_passed_observation_without_provenance_is_not_current(tmp_path):
    recipe(tmp_path, 'tool', ['aarch64'])
    results = tmp_path / 'results/tool_arm64'
    results.mkdir(parents=True)
    (results / 'native-result.json').write_text(json.dumps({'container': 'tool_arm64',
        'version': '1.2.3', 'machine': 'aarch64', 'status': 'passed'}))
    report = readiness(tmp_path, results=results.parent)
    assert report['summary']['native_passed'] == 0
    assert report['targets'][0]['native_runtime']['status'] == 'error'
