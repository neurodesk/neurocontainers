#!/usr/bin/env python3
"""Read retirement protection from the committed scigetorg/apps.json catalog."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cvmfs.check_publication import PublicationUnavailable, inventory_ids  # noqa: E402
from cvmfs.generate_metadata import log_lines  # noqa: E402


def retained_ids(repo: Path, revision: str) -> frozenset[str]:
    def committed(path):
        return subprocess.run(
            ["git", "-C", str(repo), "show", f"{revision}:{path}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    domains = json.loads(committed("domains.json")).get("domains", {})
    if not domains:
        raise PublicationUnavailable("No domains in domains.json")

    all_lines = []
    for domain in sorted(domains):
        all_lines.extend(log_lines(json.loads(committed(f"{domain}/apps.json"))))

    return inventory_ids("\n".join(all_lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    try:
        retained = retained_ids(args.repo, args.revision)
    except (
        PublicationUnavailable,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as error:
        print(
            f"[ERROR] Cannot establish retirement protection: {error}", file=sys.stderr
        )
        return 2
    print("\n".join(sorted(retained)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
