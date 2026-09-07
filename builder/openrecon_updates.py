"""Require update ownership for dependencies installed by the OpenRecon macro."""

from pathlib import Path

import yaml


OPENRECON_PINS = {
    "openrecon_ismrmrd_version": "1.14.2",
    "openrecon_siemens_commit": "055eefc09dfa77fbf11fff85979dea9ef2879ba5",
    "openrecon_python_tools_commit": "17fe6fbf1bb645112e2ad0023eb4f42afb36421e",
    "openrecon_server_commit": "33362f2139701fbf5ea855808a325eb59b7db2ae",
}

OPENRECON_SOURCES = (
    {
        "id": "openrecon_ismrmrd",
        "method": "github_release",
        "repo": "ismrmrd/ismrmrd",
        "target": {"variable": "openrecon_ismrmrd_version", "value": "version"},
    },
    {
        "id": "openrecon_siemens",
        "method": "github_commit",
        "repo": "ismrmrd/siemens_to_ismrmrd",
        "ref": "HEAD",
        "target": {"variable": "openrecon_siemens_commit"},
    },
    {
        "id": "openrecon_python_tools",
        "method": "github_commit",
        "repo": "ismrmrd/ismrmrd-python-tools",
        "ref": "HEAD",
        "target": {"variable": "openrecon_python_tools_commit"},
    },
    {
        "id": "openrecon_server",
        "method": "github_commit",
        "repo": "astewartau/python-ismrmrd-server",
        "ref": "fix/ismrmrd-compatibility",
        "target": {"variable": "openrecon_server_commit"},
    },
)


def uses_openrecon(recipe: dict) -> bool:
    root = Path(__file__).resolve().parents[1] / "macros"
    visited = set()

    def walk(directives: list) -> bool:
        for directive in directives:
            if not isinstance(directive, dict):
                continue
            if walk(directive.get("group", [])):
                return True
            include = directive.get("include")
            if not isinstance(include, str):
                continue
            path = (root / include.removeprefix("macros/")).resolve()
            if not path.is_relative_to(root) or path in visited:
                continue
            if path == root / "openrecon/neurodocker.yaml":
                return True
            visited.add(path)
            if path.is_file():
                macro = yaml.safe_load(path.read_text())
                if walk(macro.get("directives", [])):
                    return True
        return False

    return walk(recipe.get("build", {}).get("directives", []))


def validate_openrecon_policy(recipe: dict) -> None:
    if not uses_openrecon(recipe):
        return
    policy = recipe.get("auto_update") or {}
    if policy.get("method") != "sources":
        raise ValueError("OpenRecon requires explicit shared dependency sources")
    for required in OPENRECON_SOURCES:
        variable = required["target"]["variable"]
        sources = [source for source in policy.get("sources", [])
                   if source.get("target", {}).get("variable") == variable]
        if len(sources) != 1:
            raise ValueError(f"OpenRecon requires one source targeting {variable}")
        source = sources[0]
        for key in ("method", "repo", "ref"):
            if source.get(key) != required.get(key):
                raise ValueError(f"OpenRecon {variable} requires {key}={required.get(key)}")
        if source["target"].get("value", "value") != required["target"].get("value", "value"):
            raise ValueError(f"OpenRecon {variable} selects the wrong observation field")
