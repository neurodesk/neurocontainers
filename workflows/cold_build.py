"""Select and execute bounded cold builds without publishing releases."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml

from builder.variants import concrete_variant_specs


def plan(root: Path, policy: dict, state: dict, today: date) -> dict:
    eligible, excluded = [], []
    for path in sorted((root / 'recipes').glob('*/build.yaml')):
        recipe = yaml.safe_load(path.read_text())
        base = str(recipe['build']['base-image']).split('@', 1)[0]
        support = policy['bases'].get(base)
        reason = None
        if not support:
            reason = 'base lifecycle is not in the supported policy'
        elif today >= date.fromisoformat(support['expires']):
            reason = 'base standard support policy has expired'
        elif not (path.parent / 'fulltest.yaml').is_file():
            reason = 'fulltest.yaml is missing'
        for spec in concrete_variant_specs(recipe):
            target = {'recipe': path.parent.name, 'container': spec['name'], 'variant': spec['variant'],
                      'architecture': spec['architecture'], 'version': str(recipe['version']),
                      'base': base, 'recipe_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
            if reason:
                excluded.append({**target, 'reason': reason})
            else:
                eligible.append(target)
    due, overdue = [], []
    for target in eligible:
        prior = state.get(target['container'], {})
        attempt = date.fromisoformat(prior.get('attempted_at', '1970-01-01')[:10])
        success = date.fromisoformat(prior.get('succeeded_at', '1970-01-01')[:10])
        changed = prior.get('recipe_sha256') != target['recipe_sha256']
        if changed or prior.get('succeeded_recipe_sha256') != target['recipe_sha256'] or today - success >= timedelta(days=policy['coverage_days']):
            overdue.append(target['container'])
        interval = policy['retry_days'] if prior.get('status') == 'failed' else policy['cadence_days']
        if changed or today - attempt >= timedelta(days=interval):
            due.append((attempt, target))
    due.sort(key=lambda item: (item[0], item[1]['container']))
    return {'date': today.isoformat(), 'targets': [target for _, target in due[:policy['batch_size']]],
            'eligible': len(eligible), 'due': len(due), 'overdue': overdue, 'excluded': excluded,
            'capacity_warning': len(eligible) > policy['batch_size'] * policy['coverage_days']}


def execute(root: Path, target: dict, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    observation = {**target, 'attempted_at': datetime.now(timezone.utc).isoformat(),
                   'machine': platform.machine(), 'status': 'failed'}
    image = f'neurocontainer-cold/{target["container"]}:validation'
    log = output / 'cold-build.log'
    def run(command: list[str], *, capture: bool = False) -> subprocess.CompletedProcess:
        with log.open('a') as stream:
            stream.write(' '.join(command) + '\n')
            stream.flush()
            return subprocess.run(command, cwd=root, check=True,
                                  stdout=subprocess.PIPE if capture else stream,
                                  stderr=stream)
    try:
        expected_machine = 'aarch64' if target['architecture'] == 'aarch64' else 'x86_64'
        if platform.machine() != expected_machine:
            raise ValueError('Cold validation requires a native architecture runner')
        observation['commit'] = run(['git', 'rev-parse', 'HEAD'], capture=True).stdout.decode().strip()
        run([sys.executable, '-m', 'builder', 'stage', target['recipe'], '--recreate', '--download',
             '--architecture', target['architecture'], '--variant', target['variant']])
        build = root / 'build' / target['container']
        dockerfile = next(build.glob('*.Dockerfile'))
        observation['dockerfile_sha256'] = hashlib.sha256(dockerfile.read_bytes()).hexdigest()
        observation['resolved_inputs'] = {}
        for staged_file in sorted(build.rglob('*')):
            if staged_file.is_file():
                digest = hashlib.sha256()
                with staged_file.open('rb') as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                        digest.update(chunk)
                observation['resolved_inputs'][str(staged_file.relative_to(build))] = digest.hexdigest()
        context = run([sys.executable, '-m', 'builder', 'staged-context-args', str(build)], capture=True)
        contexts = [arg.decode() for arg in context.stdout.split(b'\0') if arg]
        run(['docker', 'buildx', 'build', str(build), '--load', '--no-cache', '--pull',
             '--file', str(dockerfile), '--tag', image,
             '--build-context', f'neurocontainer-cache={build / "cache"}', *contexts])
        inspection = run(['docker', 'image', 'inspect', image], capture=True)
        (output / 'image-inspect.json').write_bytes(inspection.stdout)
        observation['image_id'] = json.loads(inspection.stdout)[0]['Id']
        converter = output / 'docker-save-to-simg'
        run(['go', 'build', '-o', str(converter), 'builder/docker-save-to-simg.go'])
        archive, candidate = output / 'candidate.tar', output / 'candidate.simg'
        run(['docker', 'save', '--output', str(archive), image])
        run([str(converter), str(archive), str(candidate)])
        archive.unlink()
        run([sys.executable, '-m', 'workflows.release_test_runner', '--recipe', target['recipe'],
             '--version', target['version'], '--release-file', str(output / 'unused-release.json'),
             '--candidate-container', str(candidate), '--test-config', f'recipes/{target["recipe"]}/fulltest.yaml',
             '--results-path', str(output / 'test-results.json'), '--output-dir', str(output / 'test-output'),
             '--repo-root', str(root), '--verbose'])
        observation['status'] = 'passed'
        observation['succeeded_recipe_sha256'] = target['recipe_sha256']
        observation['succeeded_at'] = datetime.now(timezone.utc).isoformat()
    except (OSError, ValueError, StopIteration, subprocess.CalledProcessError) as exc:
        observation['error'] = str(exc)
    (output / 'observation.json').write_text(json.dumps(observation, indent=2) + '\n')
    return observation


def merge(state: dict, observations: list[dict]) -> dict:
    merged = dict(state)
    for observation in observations:
        prior = merged.get(observation['container'], {})
        merged[observation['container']] = {**prior, **observation}
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['plan', 'run', 'merge'])
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--policy', type=Path, default=Path('workflows/cold-build-policy.json'))
    parser.add_argument('--state', type=Path, default=Path('cold-state.json'))
    parser.add_argument('--today', type=date.fromisoformat, default=date.today())
    parser.add_argument('--target', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--observations', type=Path)
    parser.add_argument('--plan', type=Path)
    args = parser.parse_args()
    state = json.loads(args.state.read_text()) if args.state.is_file() else {}
    if args.command == 'run':
        result = execute(args.root.resolve(), json.loads(args.target.read_text()), args.output.resolve())
        raise SystemExit(int(result['status'] != 'passed'))
    if args.command == 'plan':
        result = plan(args.root, json.loads(args.policy.read_text()), state, args.today)
        print(json.dumps(result['targets']))
    else:
        observations = [json.loads(path.read_text()) for path in args.observations.rglob('observation.json')]
        if args.plan:
            planned = json.loads(args.plan.read_text())
            observed = {row['container'] for row in observations}
            observations.extend({**target, 'attempted_at': planned['date'], 'status': 'failed',
                                 'error': 'Runner did not retain a build observation'}
                                for target in planned['targets'] if target['container'] not in observed)
        result = merge(state, observations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
