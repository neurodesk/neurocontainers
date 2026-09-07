#!/usr/bin/env python3
"""Replay source-policy transitions and inspect their generated acquisition inputs."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from builder.audit_updates import validate_update_policy
from builder.config import default_config
from builder.dockerfile import render_dockerfile
from builder.recipe import compile_recipe, load_recipe
from builder.update_observations import SourceObservation
from builder.update_plan import plan_sources
from builder.variants import concrete_variant_specs


def acquisition_signature(compiled) -> tuple:
    return (
        render_dockerfile(compiled.definition),
        tuple((name, file.url, file.sha256, file.contents) for name, file in compiled.staging_plan.files.items()),
    )


def verify(recipe_dir: Path, source_prefix: str = "", next_version: str = "9999.0.0") -> dict:
    recipe = load_recipe(recipe_dir)
    validate_update_policy(recipe, recipe_path=recipe_dir / "build.yaml")
    sources = recipe["auto_update"]["sources"]
    observations = {}
    for source in sources:
        target = source["target"]
        metadata = {field: recipe["variables"][name] for name, field in target.get("variables", {}).items()}
        if "variable" in target:
            value = str(recipe["variables"][target["variable"]])
            observations[source["id"]] = SourceObservation(value, "https://example.com/recorded-source", version=value, tag=value, metadata=metadata)
        else:
            file = next(file for file in recipe["files"] if file["name"] == target["file"])
            value = file["sha256"] if source["method"] == "http_digest" else file["url"]
            observations[source["id"]] = SourceObservation(value, file["url"], metadata={"sha256": file["sha256"], **metadata})
    assert plan_sources(recipe_dir / "build.yaml", observations=observations) is None
    specs = concrete_variant_specs(recipe)
    config = default_config()
    current = [compile_recipe(recipe_dir, architecture=s["architecture"], variant=s["variant"] or None,
                              include_dirs=config.include_dirs) for s in specs]
    with tempfile.TemporaryDirectory(prefix="verify-source-policy-") as directory:
        candidate = Path(directory) / recipe_dir.name
        shutil.copytree(recipe_dir, candidate)
        (candidate / "README.md").write_text(current[0].readme)
        selected_sources = [source for source in sources if source["id"].startswith(source_prefix)]
        for source in selected_sources:
            advanced = dict(observations)
            metadata = dict(observations[source["id"]].metadata)
            for field in source["target"].get("variables", {}).values():
                metadata[field] = next_version
            if source["method"] in {"github_commit", "git_commit"}:
                value = "a" * 40 if observations[source["id"]].value != "a" * 40 else "b" * 40
                advanced[source["id"]] = SourceObservation(
                    value,
                    "https://example.com/next-commit",
                    metadata=metadata,
                )
            elif source["method"] in {"http_digest", "oci_digest"}:
                value = "a" * 64 if observations[source["id"]].value.removeprefix("sha256:") != "a" * 64 else "b" * 64
                value = "sha256:" + value if source["method"] == "oci_digest" else value
                metadata["sha256"] = value.removeprefix("sha256:")
                advanced[source["id"]] = SourceObservation(
                    value,
                    "https://example.com/next-digest",
                    metadata=metadata,
                )
            elif "file" in source["target"]:
                value = "https://example.com/next-artifact"
                metadata["sha256"] = "b" * 64
                advanced[source["id"]] = SourceObservation(
                    value,
                    "https://example.com/next-release",
                    version=next_version,
                    tag="artifact-" + next_version,
                    metadata=metadata,
                )
            else:
                value = "99999999" if source["method"] == "zenodo" else next_version
                advanced[source["id"]] = SourceObservation(
                    value,
                    "https://example.com/next-release",
                    version=(
                        None
                        if source["method"] in {"apt", "zenodo"}
                        else value
                    ),
                    tag=(
                        None
                        if source["method"] in {"apt", "zenodo"}
                        else "v" + value
                    ),
                    metadata=metadata,
                )
            plan = plan_sources(candidate / "build.yaml", observations=advanced)
            assert plan is not None, source["id"]
            assert plan.fingerprint == plan_sources(candidate / "build.yaml", observations=advanced).fingerprint
            plan.apply()
            changed = [compile_recipe(candidate, architecture=s["architecture"], variant=s["variant"] or None,
                                      include_dirs=config.include_dirs) for s in specs]
            selected = getattr(advanced[source["id"]], source["target"].get("value", "value"))
            assert any(selected in repr(acquisition_signature(item)) for item in changed), source["id"]
            assert any(acquisition_signature(a) != acquisition_signature(b)
                       for a, b in zip(current, changed)), source["id"]
            suite = yaml.safe_load((candidate / "fulltest.yaml").read_text())
            assert str(suite["version"]) == plan.next_version
            if variable := source["target"].get("fulltest_variable"):
                assert str(suite[variable]) == selected
            updated = yaml.safe_load((candidate / "build.yaml").read_text())
            for other in sources:
                variable = other["target"].get("variable")
                if other["id"] != source["id"] and variable:
                    assert updated["variables"][variable] == recipe["variables"][variable]
            assert plan_sources(candidate / "build.yaml", observations=advanced) is None
            for patch in plan.patches:
                patch.path.write_text(patch.before)
    return {"recipe": recipe_dir.name, "sources": len(selected_sources), "variants": len(specs),
            "current_noop": True, "advanced_inputs_change": True,
            "deterministic_plan": True, "replay_noop": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recipes", nargs="*")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--source-prefix", default="")
    parser.add_argument("--next-version", default="9999.0.0")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    names = args.recipes or json.loads(Path(__file__).with_name("source_policy_migration.json").read_text())
    rows = []
    for name in names:
        row = verify(root / "recipes" / name, args.source_prefix, args.next_version)
        rows.append(row)
        print(f"{name}: {row['sources']} sources, {row['variants']} variants, replay passed", flush=True)
    if args.report:
        args.report.write_text(json.dumps(rows, indent=2) + "\n")
    print(f"Verified {len(rows)} recipes and {sum(r['sources'] for r in rows)} source transitions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
