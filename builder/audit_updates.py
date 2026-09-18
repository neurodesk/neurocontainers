"""Check update coverage without contacting upstream services."""

from __future__ import annotations

import argparse
import json
import re
import shlex
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from packaging.version import Version

from .update_sources import SAFE_SOURCE_REF, parse_release_tag, validate_update_config


def versioned_source_inputs(recipe: dict) -> list[tuple[str, str]]:
    """Read acquisition inputs, excluding runtime paths, tests and documentation."""
    variables = dict(recipe.get("variables") or {})
    if tag_variable := recipe.get("auto_update", {}).get("tag_variable"):
        variables[tag_variable] = "{{ context." + tag_variable + " }}"
    inputs = []
    consumed_files = set()
    copied_files = {}

    def expand(value, scope):
        text = str(value)
        for _ in range(20):
            updated = re.sub(
                r"{{\s*(?:context|local)\.([A-Za-z_][A-Za-z_0-9]*)\s*}}",
                lambda match: str(scope.get(match.group(1), match.group(0))),
                text,
            )
            if updated == text:
                break
            text = updated
        return text

    def file_input(file, scope):
        if file.get("url"):
            inputs.append(("url", expand(file["url"], scope)))
        if file.get("contents"):
            inputs.append(
                ("requirements:" + str(file["name"]), expand(file["contents"], scope))
            )

    def directives(items, scope):
        scope = dict(scope)
        for directive in items or []:
            if not isinstance(directive, dict):
                continue
            if "variables" in directive:
                scope.update(directive["variables"])
            if "group" in directive:
                directives(directive["group"], scope)
            if "file" in directive:
                file_input(directive["file"], scope)
            if "copy" in directive:
                copy = directive["copy"]
                parts = shlex.split(copy) if isinstance(copy, str) else copy
                if len(parts) == 2:
                    destination = parts[1]
                    if destination.endswith("/"):
                        destination += parts[0]
                    copied_files[destination] = parts[0]
            if "template" in directive:
                template = directive["template"]
                if template.get("yaml_file") in copied_files:
                    consumed_files.add(copied_files[template["yaml_file"]])
                if "version" in template:
                    inputs.append(
                        (
                            "template",
                            str(template["name"])
                            + " "
                            + expand(template["version"], scope),
                        )
                    )
                for key in ("pip_install", "conda_install"):
                    if key in template:
                        inputs.append(("package", expand(template[key], scope)))
            commands = directive.get("run", [])
            if isinstance(commands, str):
                commands = [commands]
            for command in commands:
                expanded = expand(command, scope)
                if re.search(
                    r"\b(?:julia|python[0-9.]*|pip[0-9.]*|conda|mamba)\b", expanded
                ):
                    consumed_files.update(
                        name
                        for destination, name in copied_files.items()
                        if destination in expanded
                    )
                if re.search(
                    r"\b(?:pip[0-9.]*|conda|mamba|micromamba|npm)\b.*\binstall\b|\binstall_(?:github|version)\(",
                    expanded,
                ):
                    consumed_files.update(
                        re.findall(r'get_file\(["\']([^"\']+)["\']\)', expanded)
                    )
                    inputs.append(("package", expanded))
                for clone in re.finditer(r"\bgit\s+clone\b[^;&\n]*", expanded):
                    inputs.append(("git", clone.group()))

    build = recipe.get("build") or {}
    inputs.append(("image", expand(build.get("base-image", ""), variables)))
    for file in recipe.get("files") or []:
        file_input(file, variables)
    directives(build.get("directives"), variables)
    return [
        ("requirements" if kind.startswith("requirements:") else kind, text)
        for kind, text in inputs
        if not kind.startswith("requirements:")
        or kind.split(":", 1)[1] in consumed_files
    ]


def _git_clone_tracks(text: str, repo: str, tag_variable: str | None = None) -> bool:
    variable_names = (
        re.escape(tag_variable) if tag_variable else "(?:original_)?version"
    )
    text = re.sub(
        rf"{{{{\s*context\.(?:{variable_names})\s*}}}}", "__UPDATE_VERSION__", text
    )
    try:
        arguments = shlex.split(text)
    except ValueError:
        return False
    sources = {f"https://github.com/{repo}", f"https://github.com/{repo}.git"}
    if not sources.intersection(argument.lower() for argument in arguments):
        return False
    for index, argument in enumerate(arguments):
        if argument.startswith("--branch=") and "__UPDATE_VERSION__" in argument:
            return True
        if argument in {"--branch", "-b"} and index + 1 < len(arguments):
            if "__UPDATE_VERSION__" in arguments[index + 1]:
                return True
    return False


def validate_update_policy(recipe: dict, *, recipe_path: Path | None = None) -> None:
    config = recipe.get("auto_update")
    if not isinstance(config, dict):
        raise ValueError(
            "auto_update is required; bind an automatic policy to installed sources or local repository inputs"
        )
    if config.get("method") == "manual" or config.get("mode") == "notify":
        raise ValueError(
            "every recipe requires automatic updates; bind installed sources or use a sources policy with local repository inputs"
        )
    validate_update_config(config)
    from .openrecon_updates import validate_openrecon_policy

    validate_openrecon_policy(recipe)
    if config["method"] == "sources":
        from .update_plan import validate_target_bindings

        validate_target_bindings(recipe, recipe_path)
        return
    if tag_variable := config.get("tag_variable"):
        tag = (recipe.get("variables") or {}).get(tag_variable)
        if not isinstance(tag, str) or not SAFE_SOURCE_REF.fullmatch(tag):
            raise ValueError(
                f"auto_update.tag_variable requires variables.{tag_variable} to contain the current source ref"
            )
        release = parse_release_tag(
            tag,
            "",
            re.compile(config["version_regex"]) if "version_regex" in config else None,
            config.get("version_scheme", "numeric"),
            config.get("include_prereleases", False),
        )
        if release is None or Version(release.version) != Version(
            str(recipe.get("version", ""))
        ):
            raise ValueError(
                f"variables.{tag_variable} must select the current recipe version"
            )
    method = config["method"]
    if recipe_path is not None and not recipe_path.with_name("fulltest.yaml").is_file():
        raise ValueError(
            "automatic updates require a sibling fulltest.yaml runtime test"
        )
    for name in config.get("assets", {}):
        files = [file for file in recipe.get("files", []) if file.get("name") == name]
        if len(files) != 1 or not files[0].get("url"):
            raise ValueError(
                f"auto_update.assets requires one declared URL file named {name}"
            )
        if "sha256" in files[0] or "checksum" in files[0]:
            raise ValueError(f"{name} pins a checksum that needs update handling")
    inputs = versioned_source_inputs(recipe)
    bindings = (
        ("context." + config["tag_variable"],)
        if config.get("tag_variable")
        else ("context.version", "context.original_version")
    )
    versioned = [
        (kind, text)
        for kind, text in inputs
        if any(binding in text for binding in bindings)
    ]
    if method == "dockerhub":
        bound = any(
            kind == "image" and config["repo"] + ":" in text for kind, text in versioned
        )
    elif method == "oci":
        source = urlsplit(config["url"])
        image = (
            source.netloc
            + "/"
            + source.path.removeprefix("/v2/").removesuffix("/tags/list")
        )
        bound = any(kind == "image" and image + ":" in text for kind, text in versioned)
    elif method in {"pypi", "npm"}:
        package = "[-_]".join(
            re.escape(part) for part in re.split("[-_]", config["package"])
        )
        pattern = re.compile(
            rf"(?<![A-Za-z0-9_-]){package}(?:\[[^]]+\])?\s*(?:==?|@)\s*['\"]?{{{{\s*context\.(?:original_)?version",
            re.I,
        )
        bound = any(
            kind in {"package", "requirements"} and pattern.search(text)
            for kind, text in versioned
        )
    elif method.startswith("github_"):
        repo = config["repo"].lower()
        name = recipe.get("name", "").lower()
        package_pin = re.compile(
            rf"(?<![A-Za-z0-9_-]){re.escape(name)}(?:\[[^]]+\])?\s*==?\s*{{{{\s*context\.(?:original_)?version",
            re.I,
        )
        julia_pin = re.compile(
            rf'PackageSpec\(name="{re.escape(name)}",\s*version="{{{{\s*context\.(?:original_)?version',
            re.I,
        )
        bound = any(
            (
                kind == "url"
                and (repo in text.lower() or (name and name in text.lower()))
            )
            or (
                kind == "package" and (repo in text.lower() or package_pin.search(text))
            )
            or (kind == "requirements" and julia_pin.search(text))
            or (
                kind == "git"
                and _git_clone_tracks(text, repo, config.get("tag_variable"))
            )
            or (kind == "image" and repo in text.lower())
            or (kind == "template" and name.startswith(text.split()[0].lower()))
            for kind, text in versioned
        )
    else:
        github_feed = re.match(
            r"https://api\.github\.com/repos/([^/]+/[^/]+)/",
            config.get("url", "").lower(),
        )
        bound = any(
            kind in {"url", "template", "package", "image"}
            or (
                kind == "git"
                and github_feed
                and _git_clone_tracks(text, github_feed.group(1))
            )
            for kind, text in versioned
        )
    if not bound:
        raise ValueError(
            "automatic updates require context.version in a source download, package pin, source checkout or matching base image; runtime paths and tests do not bind the installed version"
        )


def audit(root: Path) -> list[dict]:
    rows = []
    for path in sorted(root.glob("*/build.y*ml")):
        row = {"recipe": path.parent.name, "path": str(path)}
        try:
            recipe = yaml.safe_load(path.read_text())
            if not isinstance(recipe, dict):
                raise ValueError("recipe must be a YAML mapping")
            validate_update_policy(recipe, recipe_path=path)
            config = recipe["auto_update"]
            row.update(
                status="automatic",
                method=config["method"],
                source=config.get("repo")
                or config.get("package")
                or config.get("url", ""),
            )
        except (ValueError, yaml.YAMLError) as exc:
            row.update(status="error", reason=str(exc))
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recipes", type=Path, default=Path("recipes"))
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    rows = audit(args.recipes)
    if args.json:
        args.json.write_text(json.dumps(rows, indent=2) + "\n")
    for row in rows:
        print(
            f"{row['recipe']}: {row['status']} {row.get('source', '')} {row.get('reason', '')}"
        )
    print(dict(Counter(row["status"] for row in rows)))
    return int(not rows or any(row["status"] == "error" for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
