"""Report ARM declarations, current release records and observed native results."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml

from builder.variants import concrete_variant_specs
from builder.image_fingerprint import (
    MANIFEST_ACCEPT,
    RegistryClient,
    RegistryError,
    select_platform_manifest,
)


def probe_public_release(container: str, version: str, date: str) -> dict:
    client = RegistryClient("quay.io")
    repository = f"neurodesk/{container}"
    response = client.get(
        f"/v2/{repository}/manifests/{version}_{date}", MANIFEST_ACCEPT
    )
    if response.status_code != 200:
        raise RegistryError(
            f"Anonymous manifest request returned HTTP {response.status_code}"
        )
    digest = "sha256:" + hashlib.sha256(response.body).hexdigest()
    manifest = response.json()
    platform_digest = digest
    if manifest.get("manifests") is not None:
        platform_digest = select_platform_manifest(manifest["manifests"], "arm64")
        manifest = client.get_manifest(repository, platform_digest)
    config = client.get_config_blob(repository, manifest["config"]["digest"])
    if config["architecture"] != "arm64" or config["os"] != "linux":
        raise ValueError("Published image is not linux/arm64")
    response = client.get(
        f"/v2/{repository}/referrers/{digest}",
        "application/vnd.oci.image.index.v1+json",
    )
    if response.status_code != 200:
        raise RegistryError(
            f"Anonymous SIF referrer request returned HTTP {response.status_code}"
        )
    sifs = [
        item["digest"]
        for item in response.json().get("manifests", [])
        if item.get("artifactType") == "application/vnd.sylabs.sif.layer.v1.sif"
    ]
    if not sifs:
        raise ValueError("Published image has no SIF referrer")
    return {
        "status": "observed",
        "image_digest": digest,
        "platform_manifest_digest": platform_digest,
        "sif_manifest_digests": sifs,
        "platform": "linux/arm64",
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }


def readiness(
    root: Path, *, probe: bool = False, recipes: str = "", results: Path | None = None
) -> dict:
    selected = {name.strip() for name in recipes.split(",") if name.strip()}
    rows = []
    for path in sorted((root / "recipes").glob("*/build.yaml")):
        recipe = yaml.safe_load(path.read_text())
        name = path.parent.name
        if selected and name not in selected:
            continue
        arm_specs = [
            spec
            for spec in concrete_variant_specs(recipe)
            if spec["architecture"] == "aarch64"
        ]
        if not arm_specs:
            rows.append(
                {
                    "recipe": name,
                    "declared": False,
                    "blocker": "ARM support is not declared; upstream feasibility needs review",
                }
            )
        for spec in arm_specs:
            version = str(recipe["version"])
            container = spec["name"]
            release_file = root / "releases" / container / f"{version}.json"
            row = {
                "recipe": name,
                "container": container,
                "variant": spec["variant"],
                "architecture": "aarch64",
                "version": version,
                "declared": True,
                "recipe_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "fulltest": (path.parent / "fulltest.yaml").is_file(),
                "release_record": release_file.is_file(),
                "published": {"status": "unobserved"},
                "native_runtime": {"status": "unobserved"},
                "module_client": {"status": "unobserved"},
            }
            if release_file.is_file():
                release = json.loads(release_file.read_text())
                entry = release.get("apps", {}).get(f"{container} {version}")
                row["catalog_identity"] = bool(
                    entry
                    and release.get("variant") == spec["variant"]
                    and release.get("architecture") == "aarch64"
                )
                if probe:
                    try:
                        if not row["catalog_identity"]:
                            raise ValueError(
                                "Release record does not match the named ARM identity"
                            )
                        row["published"] = probe_public_release(
                            container, version, str(entry["version"])
                        )
                    except (OSError, ValueError, KeyError, RegistryError) as exc:
                        row["published"] = {"status": "error", "error": str(exc)}
            if results:
                result_file = results / container / "native-result.json"
                if result_file.is_file():
                    result = json.loads(result_file.read_text())
                    artifact_known = result.get("status") != "passed" or (
                        re.fullmatch(r"sha256:[0-9a-f]{64}", result.get("image_id", ""))
                        and re.fullmatch(
                            r"[0-9a-f]{64}", result.get("candidate_sha256", "")
                        )
                    )
                    if (
                        result.get("container") == container
                        and result.get("version") == version
                        and result.get("machine") in ("aarch64", "arm64")
                        and result.get("recipe_sha256") == row["recipe_sha256"]
                        and artifact_known
                    ):
                        row["native_runtime"] = result
                    else:
                        row["native_runtime"] = {
                            "status": "error",
                            "error": "Native result identity, recipe hash, machine or artifact provenance mismatch",
                        }
            rows.append(row)
    unknown = selected - {row["recipe"] for row in rows}
    if unknown:
        raise ValueError(f"Unknown recipes: {sorted(unknown)}")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "recipes": len({row["recipe"] for row in rows}),
            "declared_recipes": len({row["recipe"] for row in rows if row["declared"]}),
            "arm_targets": sum(row["declared"] for row in rows),
            "current_release_records": sum(
                row.get("release_record", False) for row in rows
            ),
            "public_artifacts_observed": sum(
                row.get("published", {}).get("status") == "observed" for row in rows
            ),
            "native_passed": sum(
                row.get("native_runtime", {}).get("status") == "passed" for row in rows
            ),
        },
        "targets": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--recipes", default="")
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    report = readiness(
        args.root, probe=args.probe, recipes=args.recipes, results=args.results
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"]))
    raise SystemExit(
        int(
            any(
                row.get(field, {}).get("status") == "error"
                for row in report["targets"]
                for field in ("published", "native_runtime")
            )
        )
    )


if __name__ == "__main__":
    main()
