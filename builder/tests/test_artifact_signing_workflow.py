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
