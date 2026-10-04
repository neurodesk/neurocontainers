from datetime import date, timedelta

import yaml

from workflows.cold_build import merge, plan


def create(root, name, base='ubuntu:24.04', fulltest=True):
    directory = root / 'recipes' / name
    directory.mkdir(parents=True)
    data = {'name': name, 'version': '1', 'architectures': ['x86_64', 'aarch64'],
            'build': {'base-image': base}}
    (directory / 'build.yaml').write_text(yaml.safe_dump(data))
    if fulltest:
        (directory / 'fulltest.yaml').write_text('tests: []')


def policy():
    return {'bases': {'ubuntu:24.04': {'expires': '2029-05-01'}},
            'cadence_days': 45, 'coverage_days': 60, 'retry_days': 7, 'batch_size': 2}


def test_expired_unknown_and_missing_tests_are_explicit(tmp_path):
    create(tmp_path, 'supported')
    create(tmp_path, 'unknown', 'debian:bookworm')
    create(tmp_path, 'missing', fulltest=False)
    report = plan(tmp_path, policy(), {}, date(2026, 10, 3))
    assert report['eligible'] == 2
    assert len(report['excluded']) == 4
    assert {row['reason'] for row in report['excluded']} == {
        'base lifecycle is not in the supported policy', 'fulltest.yaml is missing'}
    assert plan(tmp_path, policy(), {}, date(2029, 5, 1))['eligible'] == 0


def test_failed_retries_do_not_starve_untested_targets(tmp_path):
    for name in ('a', 'b', 'c'):
        create(tmp_path, name)
    state, visited = {}, set()
    today = date(2026, 10, 3)
    for offset in range(60):
        now = today + timedelta(days=offset)
        report = plan(tmp_path, policy(), state, now)
        assert len(report['targets']) <= 2
        visited.update(row['container'] for row in report['targets'])
        state = merge(state, [{**target, 'attempted_at': now.isoformat(), 'status': 'failed'}
                              for target in report['targets']])
    assert len(visited) == 6
    assert len(report['overdue']) == 6


def test_successful_coverage_and_change_rescheduling(tmp_path):
    create(tmp_path, 'tool')
    today = date(2026, 10, 3)
    targets = plan(tmp_path, policy(), {}, today)['targets']
    state = merge({}, [{**row, 'attempted_at': today.isoformat(), 'succeeded_at': today.isoformat(),
                        'succeeded_recipe_sha256': row['recipe_sha256'], 'status': 'passed'} for row in targets])
    assert not plan(tmp_path, policy(), state, today + timedelta(days=44))['targets']
    assert not plan(tmp_path, policy(), state, today + timedelta(days=59))['overdue']
    assert len(plan(tmp_path, policy(), state, today + timedelta(days=60))['overdue']) == 2
    path = tmp_path / 'recipes/tool/build.yaml'
    path.write_text(path.read_text() + '\n# changed input\n')
    assert len(plan(tmp_path, policy(), state, today)['targets']) == 2


def test_failed_attempt_preserves_previous_success_for_coverage(tmp_path):
    create(tmp_path, 'tool')
    today = date(2026, 10, 3)
    row = plan(tmp_path, policy(), {}, today)['targets'][0]
    state = merge({}, [{**row, 'status': 'passed', 'succeeded_at': today.isoformat()}])
    state = merge(state, [{**row, 'status': 'failed', 'attempted_at': today.isoformat()}])
    assert state[row['container']]['succeeded_at'] == today.isoformat()
    assert state[row['container']]['status'] == 'failed'


def test_sustained_batch_covers_all_targets_before_sixty_days(tmp_path):
    for number in range(10):
        create(tmp_path, f'tool{number}')
    state = {}
    today = date(2026, 10, 3)
    for offset in range(120):
        now = today + timedelta(days=offset)
        report = plan(tmp_path, policy(), state, now)
        state = merge(state, [{**row, 'attempted_at': now.isoformat(), 'succeeded_at': now.isoformat(),
                    'succeeded_recipe_sha256': row['recipe_sha256'], 'status': 'passed'} for row in report['targets']])
        if offset >= 10:
            assert not report['overdue']
    assert len(state) == 20
