"""Keep software versions separate from dated container builds."""

from __future__ import annotations

import re

import yaml

from .yaml_edit import mapping_nodes, set_scalar


def software_version(value: str) -> str:
    """Pad short numeric releases without changing upstream release components."""
    value = str(value)
    if re.fullmatch(r"\d+(?:_\d+)+", value):
        value = value.replace("_", ".")
    match = re.fullmatch(r"([vV]?)(\d+(?:\.\d+)*)(.*)", value)
    if not match:
        return value
    prefix, release, suffix = match.groups()
    # Date labels and vendor releases (2025b, 23a) are not semantic versions.
    if "." not in release and (len(release) >= 6 or re.fullmatch(r"[ab]", suffix)):
        return value
    parts = release.split(".")
    return prefix + ".".join(parts + ["0"] * max(0, 3 - len(parts))) + suffix


def source_version(recipe: dict, source: dict) -> str:
    """Read the installed version from the source's local pins, without networking."""
    from .update_sources import parse_release_tag

    target = source["target"]
    variables = recipe.get("variables", {})
    for variable, field in target.get("variables", {}).items():
        if field == "version":
            return str(variables[variable])
    if "variable" in target:
        value = str(variables[target["variable"]])
        if source["method"] == "apt":
            if target.get("value", "value") != "value":
                raise ValueError(
                    f"{source['id']}: apt sources must store the raw package pin (value: value)"
                )
            from .update_observations import debian_upstream_version

            return debian_upstream_version(value)
        if target.get("value") != "tag":
            return value
        release = parse_release_tag(
            value,
            "",
            re.compile(source["version_regex"]) if "version_regex" in source else None,
            source.get("version_scheme", "numeric"),
            source.get("include_prereleases", False),
        )
        if release is not None:
            return release.version
    else:
        file = next(file for file in recipe["files"] if file["name"] == target["file"])
        if pattern := source.get("version_regex"):
            if match := re.search(pattern, file["url"]):
                return match.group("version")
    raise ValueError(
        f"{source['id']}: container_version requires a locally recorded software version"
    )


def container_version(recipe: dict) -> str:
    """Resolve the label from the primary source, or retain a local bundle's label."""
    config = recipe.get("auto_update", {})
    driver = config.get("container_version")
    if isinstance(driver, dict):
        var_name = driver["variable"]
        value = str(recipe.get("variables", {}).get(var_name, ""))
        if not value:
            raise ValueError(f"container_version requires variables.{var_name}")
        apt_driver = _resolve_apt_driver_source(config)
        if apt_driver is not None:
            target = apt_driver.get("target") or {}
            if target.get("value", "value") != "value":
                raise ValueError(
                    f"{apt_driver['id']}: apt sources must store the raw package pin (value: value)"
                )
            is_raw_pin = (
                target.get("variable") == var_name
                or (target.get("variables") or {}).get(var_name) == "value"
            )
            if is_raw_pin:
                from .update_observations import debian_upstream_version

                value = debian_upstream_version(value)
        value = driver.get("prefix", "") + value
    elif driver:
        source = next(source for source in config["sources"] if source["id"] == driver)
        value = source_version(recipe, source)
    elif config.get("version_variable"):
        value = str(recipe["variables"][config["version_variable"]])
    else:
        value = str(recipe["version"])
    return software_version(value)


def _resolve_apt_driver_source(config: dict) -> dict | None:
    """Find the APT source driving container_version, whether named by id or variable."""
    sources = config.get("sources") or []
    driver = config.get("container_version")
    if isinstance(driver, str):
        source = next((s for s in sources if s.get("id") == driver), None)
        return source if source and source.get("method") == "apt" else None
    var_name: str | None = None
    if isinstance(driver, dict) and isinstance(driver.get("variable"), str):
        var_name = driver["variable"]
    elif isinstance(config.get("version_variable"), str):
        var_name = config["version_variable"]
    if not var_name:
        return None
    for source in sources:
        if source.get("method") != "apt":
            continue
        target = source.get("target") or {}
        if target.get("variable") == var_name or var_name in (
            target.get("variables") or {}
        ):
            return source
    return None


def _apt_peer_upstream_identity(variables: dict, peer: dict) -> str:
    from .update_observations import debian_upstream_source_version

    target = peer.get("target") or {}
    if "variable" in target:
        if target.get("value", "value") != "value":
            raise ValueError(
                f"{peer['id']}: apt sources must store the raw package pin (value: value)"
            )
        return debian_upstream_source_version(str(variables[target["variable"]]))
    target_vars = target.get("variables") or {}
    for var_name, field in target_vars.items():
        if field == "value":
            return debian_upstream_source_version(str(variables[var_name]))
    raise ValueError(f"{peer['id']}: APT peer source requires a raw package pin target")


def primary_apt_peer_versions(recipe: dict) -> dict[str, str]:
    """Return upstream source identities for peer primary APT sources of the same package."""
    config = recipe.get("auto_update", {})
    driver = _resolve_apt_driver_source(config)
    if not driver:
        return {}
    sources = config.get("sources") or []
    package = driver.get("package")
    peers = [
        s
        for s in sources
        if s.get("method") == "apt"
        and s.get("package") == package
        and not s.get("dependency")
    ]
    if len(peers) < 2:
        return {}
    variables = recipe.get("variables", {})
    return {peer["id"]: _apt_peer_upstream_identity(variables, peer) for peer in peers}


def apt_peer_versions_agree(peers: dict[str, str]) -> bool:
    """Return True when all peer APT source identities are Debian-version equivalent."""
    if len(peers) < 2:
        return True
    from .update_observations import debian_upstream_equivalence_key

    return len({debian_upstream_equivalence_key(ver) for ver in peers.values()}) <= 1


def validate_container_version(recipe: dict) -> None:
    peers = primary_apt_peer_versions(recipe)
    if peers and not apt_peer_versions_agree(peers):
        details = ", ".join(f"{sid}={ver}" for sid, ver in peers.items())
        raise ValueError(
            "multi-architecture APT sources for the primary package must share "
            f"the same software version ({details})"
        )
    expected = container_version(recipe)
    if str(recipe["version"]) != expected:
        raise ValueError(
            f"container version {recipe['version']!r} must match software version {expected!r}; "
            "use the build date to distinguish rebuilds, not a version bump"
        )


def bind_upstream_version(text: str, value: str) -> str:
    """Decouple a direct policy's acquisition strings before padding its label."""
    config = yaml.safe_load(text)["auto_update"]
    variable = config.get("version_variable", "upstream_version")
    if "version_variable" not in config:
        variables = yaml.safe_load(text).get("variables", {})
        if variable in variables:
            raise ValueError(
                f"variables.{variable} already exists; bind auto_update.version_variable explicitly"
            )
        nodes = mapping_nodes(text)
        spans = [
            nodes[key][1]
            for key in ("build", "variables", "files", "deploy")
            if key in nodes
        ]
        for node in sorted(spans, key=lambda node: node.start_mark.index, reverse=True):
            start, end = node.start_mark.index, node.end_mark.index
            content = re.sub(
                r"context\.(?:original_)?version\b",
                "context." + variable,
                text[start:end],
            )
            text = text[:start] + content + text[end:]
        text = set_scalar(text, "auto_update", "version_variable", variable)
    return set_scalar(text, "variables", variable, value)
