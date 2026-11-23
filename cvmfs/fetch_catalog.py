#!/usr/bin/env python3
"""Fetch domain apps catalogs and generate the CVMFS sync inventory."""

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cvmfs.check_publication import PublicationUnavailable, inventory_ids  # noqa: E402
from cvmfs.generate_metadata import log_lines  # noqa: E402

DOMAIN_RE = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
IMAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]{0,254}$")
USER_AGENT = "neurodesk-cvmfs-sync"
DEFAULT_TIMEOUT = 30


def _fetch_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> object:
    # Cloudflare rejects the default Python-urllib user agent (error 1010).
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _validate_domain(domain: str) -> str:
    if not isinstance(domain, str) or not DOMAIN_RE.fullmatch(domain):
        raise ValueError(f"Invalid catalog domain: {domain!r}")
    return domain


def resolve_domains(
    base_url: str,
    domains_override: Optional[str] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[str]:
    """Return sorted domain names from override or remote domains.json."""
    if domains_override and domains_override.strip():
        seen = []
        for raw in domains_override.split():
            domain = _validate_domain(raw)
            if domain not in seen:
                seen.append(domain)
        return seen

    registry = _fetch_json(f"{base_url.rstrip('/')}/domains.json", timeout=timeout)
    if not isinstance(registry, dict):
        raise ValueError("Invalid domains.json: expected a JSON object")
    domains = registry.get("domains")
    if not isinstance(domains, dict) or not domains:
        raise ValueError("Invalid domains.json: no configured domains")
    return sorted(_validate_domain(domain) for domain in domains)


def fetch_merged_catalog(
    base_url: str,
    domains_override: Optional[str] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, dict]:
    """Fetch and merge every selected domain's apps.json into a flat dict."""
    domains = resolve_domains(base_url, domains_override, timeout=timeout)
    merged: Dict[str, dict] = {}
    root = base_url.rstrip("/")

    for domain in domains:
        catalog = _fetch_json(f"{root}/{domain}/apps.json", timeout=timeout)
        if not isinstance(catalog, dict):
            raise ValueError(
                f"Invalid apps catalog for domain {domain!r}: expected an object"
            )
        for name, entry in catalog.items():
            if name in merged and merged[name] != entry:
                raise ValueError(f"Conflicting app group across domains: {name}")
            merged[name] = entry

    if not merged:
        raise ValueError("Merged domain catalogs are empty")
    return merged


def write_catalog_and_log(
    base_url: str,
    catalog_out: Path,
    log_out: Path,
    domains_override: Optional[str] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[str]:
    merged = fetch_merged_catalog(base_url, domains_override, timeout=timeout)

    lines: List[str] = []
    seen_ids = set()
    for line in log_lines(merged):
        parts = line.split(" ", 1)
        if (
            len(parts) != 2
            or not IMAGE_ID_RE.fullmatch(parts[0])
            or not parts[1].startswith("categories:")
            or "\n" in line
            or "\r" in line
        ):
            raise PublicationUnavailable(f"Invalid inventory entry: {line[:256]!r}")
        image_id = parts[0]
        if image_id in seen_ids:
            continue
        seen_ids.add(image_id)
        lines.append(line)

    if not lines:
        raise ValueError("Generated CVMFS inventory is empty")
    log_text = "".join(f"{line}\n" for line in lines)
    inventory_ids(log_text)
    catalog_out.write_text(json.dumps(merged), encoding="utf-8")
    log_out.write_text(log_text, encoding="utf-8")
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="Apps catalog base URL")
    parser.add_argument(
        "--domains",
        default="",
        help="Optional whitespace-separated domain override; defaults to all domains in domains.json",
    )
    parser.add_argument(
        "--catalog-out", type=Path, required=True, help="Path to write merged apps.json"
    )
    parser.add_argument(
        "--log-out", type=Path, required=True, help="Path to write log-format inventory"
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    write_catalog_and_log(
        args.base_url,
        args.catalog_out,
        args.log_out,
        domains_override=args.domains,
        timeout=args.timeout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
