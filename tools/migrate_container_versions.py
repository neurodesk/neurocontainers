"""Align container labels with pinned software; preview by default, --apply to write.

Installed inputs and published release records are preserved. Build dates identify
new builds of an existing software version. The migration requires no network.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from builder.update_plan import VERSIONLESS_METHODS, rewrite_scalar
from builder.versioning import bind_upstream_version, container_version, software_version
from builder.yaml_edit import set_scalar

# Bundles and locally maintained software have their own release identity.
# These inputs are dependencies, not a primary application's version.
BUNDLES = {"bidstools", "brainlesion", "code", "dicomtools", "esilpd", "flames",
           "mneextended", "neurodesktop-lite", "syncro", "musclemap", "topofit"}
PRIMARY_SOURCES = {
    "bidsappaa": "upstream", "bidsapphcppipelines": "base_image_tag",
    "connectomeworkbench": "connectome-workbench", "cpac": "base_image_tag",
    "hnncore": "hnn_core", "irkernel": "r-irkernel", "mipav": "standalone",
    "mitkdiffusion": "standalone", "mricron": "mricron-amd64", "nftsim": "base_image_tag",
    "sigviewer": "sigviewer", "sovabids": "sovabids", "spinalcordtoolbox": "sct",
    "vesselboost": "vesselboost",
}


def primary_version(recipe: dict) -> str | dict | bool:
    config = recipe["auto_update"]
    if "container_version" in config:
        return config["container_version"]
    name = recipe["name"]
    if name in BUNDLES:
        return False
    if name == "palm":
        return {"variable": "palm_version", "prefix": "alpha"}
    if name in PRIMARY_SOURCES:
        return PRIMARY_SOURCES[name]
    sources = config.get("sources", [])
    variables = recipe.get("variables", {})
    for variable in ("upstream_version", "software_version", "install_version"):
        if variable not in variables:
            continue
        for source in sources:
            if source["method"] in VERSIONLESS_METHODS and not source.get("version_file"):
                continue
            target = source["target"]
            if target.get("variable") == variable or target.get("variables", {}).get(variable) == "version":
                return source["id"]
        return {"variable": variable}
    for source in sources:
        if source["id"] == name and source["method"] not in VERSIONLESS_METHODS:
            return source["id"]
    return False


def migrate(root: Path, *, apply: bool = False) -> dict:
    writes = {}
    changes = []
    for path in sorted((root / "recipes").glob("*/build.yaml")):
        original = path.read_text()
        recipe = yaml.safe_load(original)
        current = str(recipe["version"])
        config = recipe["auto_update"]
        updated = original
        if config["method"] == "sources":
            driver = primary_version(recipe)
            if "container_version" not in config:
                # Insert without reformatting source definitions or comments.
                node = yaml.compose(original)
                policy = next(v for k, v in node.value if k.value == "auto_update")
                at = original.rfind("\n", 0, policy.start_mark.index) + 1
                line = " " * policy.start_mark.column + "container_version: " + json.dumps(driver) + "\n"
                updated = original[:at] + line + original[at:]
        elif software_version(current) != current and not config.get("version_variable"):
            updated = bind_upstream_version(updated, current)
        version = container_version(yaml.safe_load(updated))
        if current != version:
            updated = rewrite_scalar(updated, ("version",), version)
        suite_path = path.with_name("fulltest.yaml")
        suite_original = suite_path.read_text()
        suite_updated = suite_original
        suite = yaml.safe_load(suite_original)
        if config["method"] != "sources" and (variable := yaml.safe_load(updated)["auto_update"].get("version_variable")):
            if variable not in suite:
                suite_updated = suite_updated.replace("${version}", "${" + variable + "}")
                suite_updated = set_scalar(suite_updated, None, variable, current)
        if current != version:
            # Preserve software assertions even if the suite previously reused
            # its container identity as an installed-version scalar.
            if re.fullmatch(r"\$\{\w+\}", str(suite["version"])):
                suite_updated = set_scalar(suite_updated, None, "version", version)
            else:
                suite_updated = rewrite_scalar(suite_updated, ("version",), version)
        if updated != original or suite_updated != suite_original:
            writes[path] = updated
            writes[suite_path] = suite_updated
            changes.append({"recipe": path.parent.name, "old": current, "new": version,
                            "driver": yaml.safe_load(updated)["auto_update"].get("container_version")})
    if apply:
        for path, contents in writes.items():
            path.write_text(contents)
    return {"changes": changes, "applied": apply}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(migrate(args.root, apply=args.apply), indent=2))


if __name__ == "__main__":
    main()
