import json
import subprocess

import pytest

from workflows.cvmfs_scan import inventory_entries, scan


def inventory():
    return {'revision': 'cvmfs:123', 'containers': [
        {'identity': 'tool-1', 'path': 'tool', 'kind': 'directory', 'enabled': True},
        {'identity': 'retired', 'path': 'retired', 'kind': 'directory', 'enabled': False}]}


def test_reports_every_entry_without_scanners_in_dry_run(tmp_path):
    report = scan(tmp_path, inventory(), tmp_path / 'reports', dry_run=True,
                  syft_version='1', grype_version='2')
    assert report['coverage']['planned'] == report['coverage']['disabled'] == 1
    assert json.loads((tmp_path / 'reports/coverage.json').read_text()) == report


@pytest.mark.parametrize('failure', [False, True])
def test_scanner_failure_is_never_clean(tmp_path, monkeypatch, failure):
    (tmp_path / 'tool').mkdir()
    def command(args, output=None):
        if args[1] == 'version':
            return {'version': '1' if args[0] == 'syft' else '2'}
        if args[1] == 'db':
            return {'built': '2026-10-01'}
        if failure:
            raise subprocess.CalledProcessError(2, args, stderr='scanner crashed')
        if output:
            output.write_text('{}')
        return {'matches': [{'vulnerability': {'id': 'CVE-example'}}]}
    monkeypatch.setattr('workflows.cvmfs_scan.run_json', command)
    monkeypatch.setattr('workflows.cvmfs_scan.subprocess.run', lambda *args, **kwargs: None)
    report = scan(tmp_path, inventory(), tmp_path / 'reports', dry_run=False,
                  syft_version='1', grype_version='2')
    assert report['coverage']['clean'] == 0
    assert report['coverage']['error' if failure else 'findings'] == 1


def test_rejects_path_escape_and_duplicate_identity(tmp_path):
    data = inventory()
    data['containers'][0]['path'] = '../outside'
    with pytest.raises(ValueError, match='escapes'):
        inventory_entries(tmp_path, data)
    data = inventory()
    data['containers'].append(data['containers'][0])
    with pytest.raises(ValueError, match='Duplicate'):
        inventory_entries(tmp_path, data)


def test_version_mismatch_accounts_for_enabled_entries_as_errors(tmp_path, monkeypatch):
    monkeypatch.setattr('workflows.cvmfs_scan.run_json', lambda *args: {'version': 'wrong'})
    report = scan(tmp_path, inventory(), tmp_path / 'reports', dry_run=False,
                  syft_version='1', grype_version='2')
    assert report['coverage']['error'] == 1
    assert report['coverage']['disabled'] == 1


def test_failed_rerun_removes_prior_success_reports(tmp_path, monkeypatch):
    (tmp_path / 'reports/tool-1').mkdir(parents=True)
    (tmp_path / 'reports/tool-1/sbom.json').write_text('old SBOM')
    (tmp_path / 'reports/tool-1/vulnerabilities.json').write_text('old clean report')
    monkeypatch.setattr('workflows.cvmfs_scan.run_json', lambda *args: {'version': 'wrong'})
    report = scan(tmp_path, inventory(), tmp_path / 'reports', dry_run=False,
                  syft_version='1', grype_version='2')
    assert report['coverage']['error'] == 1
    assert not (tmp_path / 'reports/tool-1/sbom.json').exists()
    assert not (tmp_path / 'reports/tool-1/vulnerabilities.json').exists()
