"""Plan atomic updates of independently versioned installed inputs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping

import yaml
from packaging.version import InvalidVersion, Version


def source_config(source: dict) -> dict:
    return {key: value for key, value in source.items() if key not in {"id", "target"}}


def validate_sources_config(config: dict) -> None:
    from .update_observations import validate_source

    if set(config) - {"method", "sources", "local"}:
        raise ValueError("sources policy accepts only method, sources and local")
    sources = config.get("sources", [])
    if not isinstance(sources, list) or (not sources and "local" not in config):
        raise ValueError("sources policy requires installed sources or local paths")
    if "local" in config:
        paths = config["local"]
        if not isinstance(paths, list) or any(
            not isinstance(path, str) or not path or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts or path.startswith(".git")
            for path in paths
        ):
            raise ValueError("auto_update.local must contain repository-relative paths")
    ids = set()
    destinations = set()
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("each update source must be a mapping")
        name = source.get("id", "")
        if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError("update source requires a stable id")
        if name in ids:
            raise ValueError(f"duplicate update source id: {name}")
        ids.add(name)
        validate_source(source_config(source))
        target = source.get("target")
        if not isinstance(target, dict) or len(set(target) & {"variable", "file"}) != 1:
            raise ValueError(f"{name}: target requires exactly one variable or file")
        allowed = {"variable", "value", "fulltest_variable", "variables"} if "variable" in target else {"file", "variables"}
        if set(target) - allowed:
            raise ValueError(f"{name}: unsupported update target fields")
        kind = "variable" if "variable" in target else "file"
        key = target[kind]
        pattern = r"[A-Za-z_][A-Za-z_0-9]*" if kind == "variable" else r"[A-Za-z0-9_][A-Za-z_0-9.-]*"
        if not isinstance(key, str) or not re.fullmatch(pattern, key):
            raise ValueError(f"{name}: invalid target name")
        destination = (kind, key)
        if destination in destinations:
            raise ValueError(f"multiple sources write {destination}")
        destinations.add(destination)
        if target.get("value", "value") not in {"value", "version", "tag"}:
            raise ValueError(f"{name}: value must select value, version or tag")
        if "variable" in target and key in {"version", "original_version"}:
            raise ValueError("independent sources cannot target the container version")
        if "file" in target and source["method"] not in {"http_digest", "artifact_listing", "slicer_release", "freesurfer_release"}:
            raise ValueError(f"{name}: a file target requires an artifact or digest source")
        if "variables" in target:
            if not isinstance(target["variables"], dict) or not all(
                isinstance(k, str) and isinstance(v, str)
                and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", k)
                and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", v)
                for k, v in target["variables"].items()
            ):
                raise ValueError(f"{name}: variables must map recipe names to artifact fields")
            for variable in target["variables"]:
                destination = ("variable", variable)
                if destination in destinations:
                    raise ValueError(f"multiple sources write {destination}")
                destinations.add(destination)
        if "fulltest_variable" in target and not re.fullmatch(
            r"[A-Za-z_][A-Za-z_0-9]*", str(target["fulltest_variable"])
        ):
            raise ValueError(f"{name}: invalid fulltest variable")


def _acquisition_text(recipe: dict, variable: str) -> str:
    """Expand variables while retaining the selected input as a marker."""
    variables = dict(recipe.get("variables") or {})
    variables[variable] = "__UPDATE_INPUT__"
    parts = []
    included = set()
    build = recipe.get("build", {})
    parts.append(str(build.get("base-image", "")))
    for file in recipe.get("files", []):
        parts.extend(str(file.get(key, "")) for key in ("url", "sha256", "contents"))

    def walk(items):
        for item in items or []:
            if not isinstance(item, dict):
                continue
            if "variables" in item:
                variables.update(item["variables"])
            if "include" in item:
                relative = item["include"]
                relative = relative if relative.startswith("macros/") else "macros/" + relative
                root = Path(__file__).resolve().parents[1]
                path = (root / relative).resolve()
                if not path.is_relative_to(root / "macros"):
                    raise ValueError("shared update inputs must remain under macros/")
                if path not in included:
                    included.add(path)
                    macro = yaml.safe_load(path.read_text())
                    walk(macro.get("directives", []))
            for key in ("install", "template", "file"):
                if key in item:
                    parts.append(json.dumps(item[key]))
            commands = item.get("run", [])
            for command in [commands] if isinstance(commands, str) else commands:
                package_command = re.search(
                    r"\b(?:git|pip\d*|python[\d.]*|conda|mamba|npm|apt-get|Rscript|R|cmake|make)\b",
                    command,
                )
                installer_command = re.search(
                    r"\b(?:bash|sh)\s+(?:{{\s*get_file\(|(?:/[\w.-]+)+\.sh\b)",
                    command,
                )
                if package_command or installer_command:
                    parts.append(command)
            walk(item.get("group"))

    walk(build.get("directives"))
    text = "\n".join(parts)
    text = re.sub(r"{{\s*(?:context|local)\." + re.escape(variable) + r"\b[^{}]*}}", "__UPDATE_INPUT__", text)
    for _ in range(20):
        updated = re.sub(
            r"{{\s*(?:context|local)\.([A-Za-z_][A-Za-z_0-9]*)\s*}}",
            lambda m: str(variables.get(m[1], m[0])), text,
        )
        if updated == text:
            break
        text = updated
    return text


def validate_target_bindings(recipe: dict, recipe_path: Path | None = None) -> None:
    config = recipe["auto_update"]
    variables = recipe.get("variables") or {}
    for source in config.get("sources", []):
        target = source["target"]
        if "variable" in target:
            variable = target["variable"]
            value = variables.get(variable)
            if not isinstance(value, (str, int, float)) or not str(value):
                raise ValueError(f"{source['id']}: variables.{variable} requires a current pin")
            if "__UPDATE_INPUT__" not in _acquisition_text(recipe, variable):
                raise ValueError(f"{source['id']}: {variable} is not used in an acquisition input")
            if source["method"] in {"github_commit", "git_commit"} and not re.fullmatch(r"[a-f0-9]{40}", str(value)):
                raise ValueError(f"{source['id']}: source commits must contain all 40 hex digits")
            if source["method"] == "oci_digest" and not re.fullmatch(r"sha256:[a-f0-9]{64}", str(value)):
                raise ValueError(f"{source['id']}: image must be pinned to a SHA-256 digest")
            if source["method"] == "http_digest" and not re.fullmatch(r"[a-f0-9]{64}", str(value)):
                raise ValueError(f"{source['id']}: file digest requires 64 hexadecimal digits")
        else:
            matches = [file for file in recipe.get("files", []) if file.get("name") == target["file"]]
            if len(matches) != 1 or not matches[0].get("url"):
                raise ValueError(f"{source['id']}: target requires one declared URL file")
            if not re.fullmatch(r"[a-f0-9]{64}", str(matches[0].get("sha256", ""))):
                raise ValueError(f"{source['id']}: file requires a verified SHA-256 baseline")
            for variable in target.get("variables", {}):
                if variable not in variables or not isinstance(variables[variable], (str, int, float)):
                    raise ValueError(f"{source['id']}: artifact variable {variable} requires a current scalar")
    if recipe_path is not None:
        suite = recipe_path.with_name("fulltest.yaml")
        if not suite.is_file():
            raise ValueError("automatic updates require a sibling fulltest.yaml runtime test")
        fulltest = yaml.safe_load(suite.read_text())
        for source in config.get("sources", []):
            target = source["target"]
            if key := target.get("fulltest_variable"):
                if str(fulltest.get(key, "")) != str(variables[target["variable"]]):
                    raise ValueError(f"fulltest.{key} must match variables.{target['variable']}")
            for variable in target.get("variables", {}):
                if variable in fulltest and str(fulltest[variable]) != str(variables[variable]):
                    raise ValueError(f"fulltest.{variable} must match variables.{variable}")


def _is_older(observed: str, current: str) -> bool:
    try:
        return Version(observed.lstrip("vVrR").replace("_", ".")) < Version(current.lstrip("vVrR").replace("_", "."))
    except InvalidVersion:
        return False


def _current_artifact_version(source: dict, recipe: dict, file: dict) -> str | None:
    for variable, field in source["target"].get("variables", {}).items():
        if field == "version":
            return str(recipe["variables"][variable])
    if pattern := source.get("version_regex"):
        match = re.search(pattern, file["url"])
        if match:
            return match.group("version")
    return None


def rewrite_scalar(text: str, path: tuple[str | int, ...], value: str) -> str:
    """Replace a unique YAML scalar without reformatting unrelated recipe content."""
    node = yaml.compose(text)
    parent = None
    for key in path:
        parent = node
        if isinstance(node, yaml.MappingNode):
            matches = [v for k, v in node.value if k.value == key]
            if len(matches) != 1:
                raise ValueError(f"expected one YAML value at {path}")
            node = matches[0]
        elif isinstance(node, yaml.SequenceNode) and isinstance(key, int):
            node = node.value[key]
        else:
            raise ValueError(f"invalid YAML path {path}")
    if not isinstance(node, yaml.ScalarNode) or node.style in {"|", ">"}:
        raise ValueError(f"update target must be a scalar: {path}")
    anchored = any(isinstance(token, (yaml.tokens.AnchorToken, yaml.tokens.AliasToken))
                   and node.start_mark.index <= token.start_mark.index < node.end_mark.index
                   for token in yaml.scan(text))
    if anchored or (parent is not None and node.start_mark.index < parent.start_mark.index):
        raise ValueError(f"update target cannot use YAML anchors or aliases: {path}")
    return text[:node.start_mark.index] + json.dumps(str(value)) + text[node.end_mark.index:]


def next_container_version(current: str) -> str:
    match = re.fullmatch(r"(.*)\.(post|r)(\d+)", current)
    if match:
        return f"{match[1]}.{match[2]}{int(match[3]) + 1}"
    try:
        Version(current)
    except InvalidVersion:
        return current + ".r1"
    return current + ".post1"


@dataclass(frozen=True)
class FilePatch:
    path: Path
    before: str
    after: str


@dataclass(frozen=True)
class UpdatePlan:
    recipe: str
    current_version: str
    next_version: str
    patches: tuple[FilePatch, ...]
    changes: tuple[str, ...]
    fingerprint: str
    upstream_urls: tuple[str, ...]

    @property
    def branch(self) -> str:
        return f"auto-update/{self.recipe}-{self.next_version}-{self.fingerprint[:10]}"

    def apply(self) -> None:
        for patch in self.patches:
            if patch.path.read_text() not in {patch.before, patch.after}:
                raise ValueError(f"update plan conflicts with changed file: {patch.path}")
        for patch in self.patches:
            if patch.path.read_text() != patch.after:
                patch.path.write_text(patch.after)


def plan_sources(recipe_path: Path, github_session=None, *, observations: Mapping | None = None) -> UpdatePlan | None:
    """Resolve all components before returning any edits; failures leave files untouched."""
    from .update_observations import observe_source

    original = recipe_path.read_text()
    recipe = yaml.safe_load(original)
    validate_sources_config(recipe["auto_update"])
    validate_target_bindings(recipe, recipe_path)
    suite_path = recipe_path.with_name("fulltest.yaml")
    suite_original = suite_path.read_text()
    updated, suite_updated = original, suite_original
    changes, urls = [], []
    for source in recipe["auto_update"].get("sources", []):
        target = source["target"]
        current = str(recipe["variables"][target["variable"]]) if "variable" in target else None
        observation = observations[source["id"]] if observations is not None else observe_source(
            source_config(source), github_session, current=current,
        )
        if observation is None:
            raise ValueError(f"{source['id']}: upstream supplied no installable input")
        edits = []
        metadata_downgrade = False
        for variable, field in target.get("variables", {}).items():
            if field != "version":
                continue
            observed_version = observation.metadata.get(field)
            if not isinstance(observed_version, str) or not observed_version:
                raise ValueError(
                    f"{source['id']}: observation lacks metadata version"
                )
            if _is_older(observed_version, str(recipe["variables"][variable])):
                metadata_downgrade = True
                break
        if metadata_downgrade:
            continue
        if "variable" in target:
            selected = getattr(observation, target.get("value", "value"))
            if not isinstance(selected, str) or not selected:
                raise ValueError(f"{source['id']}: observation lacks selected value")
            if selected == current:
                continue
            if source["method"] == "apt":
                from .update_observations import _debian_newer

                if not _debian_newer(selected, current):
                    continue
            if observation.version and target.get("value") == "tag":
                from .update_sources import parse_release_tag

                previous = parse_release_tag(current, "", re.compile(source["version_regex"]) if "version_regex" in source else None,
                                             source.get("version_scheme", "numeric"), source.get("include_prereleases", False))
                if previous and _is_older(observation.version, previous.version):
                    continue
            if observation.version and target.get("value", "value") != "tag":
                try:
                    if Version(selected) <= Version(current):
                        continue
                except InvalidVersion:
                    pass
            edits.append((("variables", target["variable"]), selected))
            if variable := target.get("fulltest_variable"):
                suite_updated = rewrite_scalar(suite_updated, (variable,), selected)
        else:
            index, file = next((i, f) for i, f in enumerate(recipe["files"]) if f["name"] == target["file"])
            current_version = _current_artifact_version(source, recipe, file)
            if observation.version and current_version and _is_older(observation.version, current_version):
                continue
            digest = observation.value if source["method"] == "http_digest" else observation.metadata["sha256"]
            if not re.fullmatch(r"[a-f0-9]{64}", digest):
                raise ValueError(f"{source['id']}: invalid observed SHA-256")
            edits.append((("files", index, "sha256"), digest))
            if source["method"] != "http_digest":
                edits.append((("files", index, "url"), observation.value))
        for variable, field in target.get("variables", {}).items():
            selected = str(observation.metadata[field])
            edits.append((("variables", variable), selected))
            if variable in yaml.safe_load(suite_updated):
                suite_updated = rewrite_scalar(suite_updated, (variable,), selected)
        previous = updated
        for path, value in edits:
            node = yaml.safe_load(updated)
            for part in path:
                node = node[part]
            if str(node) != value:
                updated = rewrite_scalar(updated, path, value)
                changes.append(f"`{'.'.join(map(str, path))}`: `{node}` → `{value}`")
        if updated != previous:
            urls.append(observation.url)
    if updated == original:
        return None
    current_version = str(recipe["version"])
    next_version = next_container_version(current_version)
    updated = rewrite_scalar(updated, ("version",), next_version)
    suite = yaml.safe_load(suite_updated)
    indirect = re.fullmatch(r"\$\{(\w+)\}", str(suite["version"]))
    suite_updated = rewrite_scalar(suite_updated, (indirect[1] if indirect else "version",), next_version)
    patches = (FilePatch(recipe_path, original, updated), FilePatch(suite_path, suite_original, suite_updated))
    identity = json.dumps([(p.path.name, p.before, p.after) for p in patches], separators=(",", ":"))
    fingerprint = hashlib.sha256(identity.encode()).hexdigest()
    return UpdatePlan(recipe_path.parent.name, current_version, next_version, patches,
                      tuple(changes), fingerprint, tuple(sorted(set(urls))))
