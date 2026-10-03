"""Scan an explicit, enabled CVMFS inventory without modifying its payloads."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def run_json(command: list[str], output: Path | None = None) -> dict:
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    if output is not None:
        output.write_text(result.stdout)
    return json.loads(result.stdout)


def inventory_entries(root: Path, inventory: dict) -> list[dict]:
    if not inventory.get('revision'):
        raise ValueError('Inventory must identify its CVMFS revision')
    entries = []
    identities = set()
    for entry in inventory['containers']:
        identity = entry['identity']
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', identity):
            raise ValueError(f'Unsafe container identity: {identity}')
        if identity in identities:
            raise ValueError(f'Duplicate container identity: {identity}')
        identities.add(identity)
        path = (root / entry['path']).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError(f'Inventory path escapes CVMFS root: {identity}')
        if entry['kind'] not in ('directory', 'sif'):
            raise ValueError(f'Unknown payload kind: {identity}')
        entries.append({**entry, 'resolved_path': str(path)})
    return entries


def scan(root: Path, inventory: dict, output: Path, *, dry_run: bool,
         syft_version: str, grype_version: str) -> dict:
    entries = inventory_entries(root, inventory)
    output.mkdir(parents=True, exist_ok=True)
    report = {'revision': inventory['revision'], 'started_at': datetime.now(timezone.utc).isoformat(),
              'mode': 'dry-run' if dry_run else 'scan', 'containers': []}
    scanner_error = None
    if not dry_run:
        try:
            report['syft'] = run_json(['syft', 'version', '-o', 'json'])
            report['grype'] = run_json(['grype', 'version', '-o', 'json'])
            if report['syft']['version'] != syft_version or report['grype']['version'] != grype_version:
                raise ValueError('Scanner versions do not match the configured pins')
            subprocess.run(['grype', 'db', 'update'], check=True, capture_output=True, text=True)
            report['database'] = run_json(['grype', 'db', 'status', '-o', 'json'])
        except (OSError, subprocess.CalledProcessError, ValueError, KeyError) as exc:
            scanner_error = str(exc)
    for entry in entries:
        row = {k: v for k, v in entry.items() if k != 'resolved_path'}
        report['containers'].append(row)
        if not entry.get('enabled', False):
            row['status'] = 'disabled'
            continue
        if dry_run:
            row['status'] = 'planned'
            continue
        directory = output / entry['identity']
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir()
        try:
            if scanner_error:
                raise ValueError(scanner_error)
            source = Path(entry['resolved_path'])
            if entry['kind'] == 'directory' and not source.is_dir():
                raise ValueError('Directory payload is missing')
            with tempfile.TemporaryDirectory(prefix='cvmfs-scan-') as temporary:
                if entry['kind'] == 'sif':
                    digest = hashlib.sha256()
                    with source.open('rb') as stream:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                            digest.update(chunk)
                    row['sha256'] = digest.hexdigest()
                    sandbox = Path(temporary) / 'rootfs'
                    subprocess.run(['apptainer', 'build', '--sandbox', str(sandbox), str(source)],
                                   check=True, capture_output=True, text=True)
                    source = sandbox
                run_json(['syft', f'dir:{source}', '-o', 'syft-json'], directory / 'sbom.json')
                findings = run_json(['grype', f'sbom:{directory / "sbom.json"}', '-o', 'json'],
                                    directory / 'vulnerabilities.json')
                row['matches'] = len(findings['matches'])
                row['status'] = 'findings' if row['matches'] else 'clean'
        except (OSError, subprocess.CalledProcessError, ValueError, KeyError) as exc:
            row['status'] = 'error'
            row['error'] = str(exc)
            if isinstance(exc, subprocess.CalledProcessError):
                (directory / 'scanner-error.log').write_text(exc.stderr or '')
    report['coverage'] = {status: sum(row['status'] == status for row in report['containers'])
                          for status in ('disabled', 'planned', 'clean', 'findings', 'error')}
    (output / 'coverage.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--syft-version', required=True)
    parser.add_argument('--grype-version', required=True)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.root.resolve()):
        parser.error('Reports must be outside the read-only CVMFS root')
    report = scan(args.root, json.loads(args.inventory.read_text()), args.output,
                  dry_run=args.dry_run, syft_version=args.syft_version, grype_version=args.grype_version)
    print(json.dumps(report['coverage']))
    raise SystemExit(1 if report['coverage']['error'] else 0)


if __name__ == '__main__':
    main()
