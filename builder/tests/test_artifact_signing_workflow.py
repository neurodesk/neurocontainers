import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


@pytest.fixture
def workflow():
    return yaml.safe_load(Path('.github/workflows/promote-container-candidate.yml').read_text())


@pytest.mark.parametrize('setting, expected', [
    ('', 'false'), ('false', 'false'), ('true', 'true'),
    ('TRUE', 'false'), ('1', 'false'), (' true ', 'false'),
])
def test_signing_policy_is_explicit_and_shared(workflow, tmp_path, setting, expected):
    resolve = workflow['jobs']['resolve']
    policy = next(step for step in resolve['steps'] if step.get('id') == 'signing')
    output = tmp_path / 'output'
    subprocess.run(['bash', '-e', '-c', policy['run']], check=True, env={
        **os.environ, 'ARTIFACT_SIGNING_ENABLED': setting, 'GITHUB_OUTPUT': str(output),
    })
    assert output.read_text() == f'sign_artifacts={expected}\n'
    assert policy['env']['ARTIFACT_SIGNING_ENABLED'] == '${{ vars.ARTIFACT_SIGNING_ENABLED }}'
    assert resolve['outputs']['sign_artifacts'] == '${{ steps.signing.outputs.sign_artifacts }}'
    publish = workflow['jobs']['publish']
    for name in ('Install pinned Cosign', 'Sign and verify accepted artifacts'):
        step = next(step for step in publish['steps'] if step.get('name') == name)
        assert step['if'] == "needs.resolve.outputs.sign_artifacts == 'true'"
        assert not step.get('continue-on-error', False)
    finalize = workflow['jobs']['finalize']
    assert 'publish' in finalize['needs']
    copy = next(step for step in finalize['steps']
                if step.get('name') == 'Finalize public SIF object names with server-side copies')
    assert copy['env']['SIGN_ARTIFACTS'] == '${{ needs.resolve.outputs.sign_artifacts }}'
    assert not copy.get('continue-on-error', False)


def test_acceptance_cannot_finalize_or_report_a_production_release(workflow):
    jobs = workflow['jobs']
    for name in ('publish', 'finalize', 'report_promotion'):
        assert '!inputs.signing_acceptance' in jobs[name]['if']
    assert 'finalize' in jobs['sync_openrecon']['needs']
    assert 'always()' not in jobs['sync_openrecon']['if']
    acceptance = jobs['signing_acceptance']
    assert "github.event_name == 'workflow_dispatch'" in acceptance['if']
    assert 'inputs.signing_acceptance' in acceptance['if']
    checkout = next(step for step in acceptance['steps'] if 'checkout@' in step.get('uses', ''))
    assert checkout['with']['ref'] == '${{ github.workflow_sha }}'
    assert acceptance['permissions']['contents'] == 'read'
    assert acceptance['permissions']['id-token'] == 'write'


@pytest.mark.parametrize('failure', ['', 'upload', 'download', 'verify', 'accept-tampered', 'accept-identity'])
def test_acceptance_roundtrip_stays_in_staging_and_fails_closed(workflow, tmp_path, failure):
    step = next(step for step in workflow['jobs']['signing_acceptance']['steps']
                if step.get('name') == 'Stage, sign and verify a registry and S3 round trip')
    script = step['run'][step['run'].index('manifest='):]
    candidate = tmp_path / 'promotion-bundle/demo'
    candidate.mkdir(parents=True)
    data = b'tested SIF bytes'
    (candidate / 'demo.simg').write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    (tmp_path / 'verified-manifests.json').write_text(json.dumps([{
        'sif': 'demo.simg', 'sif_sha256': digest,
        'docker_archive': 'docker.tar', 'candidate_tag': 'candidate:test',
    }]))
    log = tmp_path / 'calls.jsonl'
    stub_code = f'#!{sys.executable}\n' + '''import json, os, pathlib, shutil, sys
command = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['CALL_LOG'], 'a') as stream:
    stream.write(json.dumps([command, *args]) + '\\n')
failure = os.environ['FAILURE']
if command == 'oras' and args[0] == 'resolve':
    print('sha256:' + 'a' * 64)
elif command == 'python':
    if args[1] == 'tools.sign_promotion':
        for suffix in ('.sigstore.json', '.promotion.json', '.promotion.sigstore.json'):
            pathlib.Path('promotion-bundle/demo/demo.simg' + suffix).write_text('{}')
    else:
        print('oras://ghcr.io/neurodesk/signing-acceptance-demo@sha256:' + 'b' * 64)
elif command == 'aws':
    source, destination = args[2:4]
    remote = pathlib.Path('remote')
    remote.mkdir(exist_ok=True)
    if source.startswith('s3://'):
        if failure == 'download':
            raise SystemExit(17)
        shutil.copyfile(remote / pathlib.Path(source).name, destination)
    else:
        if failure == 'upload':
            raise SystemExit(17)
        shutil.copyfile(source, remote / pathlib.Path(destination).name)
elif command == 'cosign':
    if args[0] == 'verify-blob':
        pathlib.Path(args[args.index('--bundle') + 1]).read_bytes()
    if failure == 'verify':
        raise SystemExit(17)
    if pathlib.Path(args[-1]).name == 'tampered.simg':
        raise SystemExit(0 if failure == 'accept-tampered' else 1)
    identity = args[args.index('--certificate-identity') + 1]
    if 'untrusted' in identity:
        raise SystemExit(0 if failure == 'accept-identity' else 1)
'''
    for name in ('docker', 'oras', 'python', 'aws', 'cosign'):
        stub = tmp_path / name
        stub.write_text(stub_code)
        stub.chmod(0o755)
    result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', script], cwd=tmp_path,
                            capture_output=True, text=True, env={
        **os.environ, 'PATH': f'{tmp_path}:{os.environ["PATH"]}',
        'CONTAINER': 'demo', 'MERGE_SHA': 'c' * 40,
        'RUNNER_TEMP': str(tmp_path), 'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2',
        'GITHUB_WORKFLOW_SHA': 'd' * 40, 'GITHUB_STEP_SUMMARY': str(tmp_path / 'summary'),
        'CALL_LOG': str(log), 'FAILURE': failure,
    })
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    remote_paths = [argument for call in calls for argument in call if argument.startswith('s3://')]
    assert remote_paths
    assert all(path.startswith('s3://neurocontainers/signing-acceptance/123/2/demo/') for path in remote_paths)
    assert ['docker', 'push', 'ghcr.io/neurodesk/signing-acceptance-demo:run-123-2'] in calls
    evidence = tmp_path / 'signing-evidence/demo/acceptance.json'
    if failure:
        assert result.returncode != 0
    else:
        assert result.returncode == 0, result.stderr
    assert evidence.exists() == (failure == '')
    if not failure:
        assert json.loads(evidence.read_text())['sif_sha256'] == digest


@pytest.mark.parametrize('enabled, fail_operation', [
    ('', ''), ('false', ''), ('false', 'cp'), ('true', ''),
    ('true', 'cp'), ('true', 'head-object'),
])
def test_public_sif_copy_requires_sidecars_only_when_enabled(
    workflow, tmp_path, enabled, fail_operation
):
    step = next(step for step in workflow['jobs']['finalize']['steps']
                if step.get('name') == 'Finalize public SIF object names with server-side copies')
    (tmp_path / 'verified-manifests.json').write_text(json.dumps([
        {'container': 'demo', 'sif': 'demo.simg'},
    ]))
    log = tmp_path / 'calls.jsonl'
    for command in ('aws', 'python'):
        stub = tmp_path / command
        stub.write_text(f'#!{sys.executable}\n' + '''import json, os, pathlib, sys
with open(os.environ['CALL_LOG'], 'a') as stream:
    stream.write(json.dumps([pathlib.Path(sys.argv[0]).name, *sys.argv[1:]]) + '\\n')
if os.environ['FAIL_OPERATION'] in sys.argv[1:3] and sys.argv[-1].endswith('.sigstore.json'):
    raise SystemExit(23)
''')
        stub.chmod(0o755)
    result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', step['run']],
                            cwd=tmp_path, capture_output=True, text=True, env={
        **os.environ, 'PATH': f'{tmp_path}:{os.environ["PATH"]}',
        'HEAD_SHA': 'a' * 40, 'SIGN_ARTIFACTS': enabled,
        'CALL_LOG': str(log), 'FAIL_OPERATION': fail_operation,
    })
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    copies = [call[-1] for call in calls if call[:3] == ['aws', 's3', 'cp']]
    should_fail = enabled == 'true' and bool(fail_operation)
    assert result.returncode == (23 if should_fail else 0), result.stderr
    assert copies[0] == 's3://neurocontainers/demo.simg'
    if should_fail:
        assert not any(call[0] == 'python' for call in calls)
    else:
        suffixes = ['', '.sigstore.json', '.promotion.json', '.promotion.sigstore.json']
        if enabled != 'true':
            suffixes = ['']
        assert copies == ['s3://neurocontainers/demo.simg' + suffix for suffix in suffixes]
        assert calls[-1] == [
            'python', '-m', 'tools.copy_swift_object', '--source-sha', 'a' * 40,
            '--container', 'demo', '--sif', 'demo.simg',
        ]
