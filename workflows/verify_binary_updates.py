#!/usr/bin/env python3
"""Verify binary update bindings and replay plans in disposable recipe copies."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from builder.dockerfile import render_dockerfile
from builder.recipe import compile_recipe
from builder.update_observations import SourceObservation
from builder.update_plan import plan_sources

RECIPES = "bcbtoolkit brainnetviewer brainstorm conn eeglab fieldtrip physio samsrfx mipav noddi spm12 spm12bi tgvqsm trackvis diffusiontoolkit mfcsc lcmodel startrack gingerale mgltools mitkdiffusion terastitcher ashs convert3d minc slicer slicersalt synthseg".split()


def current_observations(recipe):
    observations = {}
    for source in recipe["auto_update"].get("sources", []):
        target = source["target"]
        if "variable" in target:
            value = str(recipe["variables"][target["variable"]])
            observations[source["id"]] = SourceObservation(value, "https://example.org/verification", version=value, tag=value)
        else:
            file = next(file for file in recipe["files"] if file["name"] == target["file"])
            metadata = {field: str(recipe["variables"][variable]) for variable, field in target.get("variables", {}).items()}
            metadata["sha256"] = file["sha256"]
            value = file["sha256"] if source["method"] == "http_digest" else file["url"]
            observations[source["id"]] = SourceObservation(value, file["url"], metadata=metadata)
    return observations


def verify(name, temporary):
    source_dir = ROOT / "recipes" / name
    recipe_dir = temporary / name
    shutil.copytree(source_dir, recipe_dir)
    path = recipe_dir / "build.yaml"
    recipe = yaml.safe_load(path.read_text())
    observations = current_observations(recipe)
    assert plan_sources(path, observations=observations) is None, "current observation created an update"
    before = {}
    for architecture in recipe["architectures"]:
        compiled = compile_recipe(recipe_dir, architecture=architecture, include_dirs=(ROOT,))
        before[architecture] = (render_dockerfile(compiled.definition), dataclasses.asdict(compiled.staging_plan))
    first = recipe["auto_update"]["sources"][0]
    observed = observations[first["id"]]
    fixture_digest = hashlib.sha256(b"verification fixture; never written to real recipes").hexdigest()
    if "file" in first["target"]:
        metadata = {**observed.metadata, "sha256": fixture_digest}
        value = fixture_digest if first["method"] == "http_digest" else observed.value + ("&" if "?" in observed.value else "?") + "verification=changed"
        observations[first["id"]] = dataclasses.replace(observed, value=value, metadata=metadata)
    else:
        observations[first["id"]] = dataclasses.replace(observed, value="b" * 40, tag="b" * 40, version=None)
    plan = plan_sources(path, observations=observations)
    assert plan is not None, "changed artifact produced no plan"
    assert plan == plan_sources(path, observations=observations), "plan is not deterministic"
    plan.apply()
    plan.apply()
    assert plan_sources(path, observations=observations) is None, "applied update is not a no-op"
    after_recipe = yaml.safe_load(path.read_text())
    suite = yaml.safe_load(path.with_name("fulltest.yaml").read_text())
    assert str(after_recipe["version"]) == str(suite["version"]), "container and fulltest diverged"
    results = []
    for architecture in recipe["architectures"]:
        compiled = compile_recipe(recipe_dir, architecture=architecture, include_dirs=(ROOT,))
        after = (render_dockerfile(compiled.definition), dataclasses.asdict(compiled.staging_plan))
        assert after != before[architecture], "update did not change generated acquisition inputs"
        results.append({"architecture": architecture, "dockerfile_changed": after[0] != before[architecture][0], "staged_inputs_changed": after[1] != before[architecture][1]})
    return {"recipe": name, "current_noop": True, "deterministic_plan": True, "replay_noop": True, "next_version": plan.next_version, "architectures": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("/tmp/neuro-binary-verification.json"))
    args = parser.parse_args()
    results = []
    with tempfile.TemporaryDirectory(prefix="neuro-binary-verify-") as directory:
        for name in RECIPES:
            try:
                result = verify(name, Path(directory))
                print(name + ": passed", flush=True)
            except Exception as error:
                result = {"recipe": name, "error": str(error)}
                print(name + ": " + str(error), flush=True)
            results.append(result)
    args.report.write_text(json.dumps(results, indent=2) + "\n")
    return int(any("error" in result for result in results))


if __name__ == "__main__":
    raise SystemExit(main())
