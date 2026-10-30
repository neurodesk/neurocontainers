"""Merge every configured domain into a flat inventory for upload consumers."""

import json
import sys
from pathlib import Path


def merge_catalogs(root: Path) -> dict:
    domains = json.loads((root / "domains.json").read_text())["domains"]
    if not domains:
        raise ValueError("No configured domains")
    merged = {}
    for domain in sorted(domains):
        if not domain or domain in {".", ".."} or "/" in domain or "\\" in domain:
            raise ValueError(f"Invalid domain: {domain}")
        catalog = json.loads((root / domain / "apps.json").read_text())
        for name, entry in catalog.items():
            if name in merged and merged[name] != entry:
                raise ValueError(f"Conflicting app group across domains: {name}")
            merged[name] = entry
    if not merged:
        raise ValueError("Empty domain inventories")
    return merged


if __name__ == "__main__":
    root = Path(sys.argv[1])
    (root / "apps.json").write_text(json.dumps(merge_catalogs(root)))
