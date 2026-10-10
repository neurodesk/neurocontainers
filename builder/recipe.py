from __future__ import annotations

import hashlib
import json
import os
import platform
import shlex
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from .ir import Copy, Definition, Entrypoint, Env, From, Install, Run, RunWithMounts, User, Workdir
from .staging import CopySource, DeclaredFile, StagingPlan, declared_file_from_mapping
from .template import RenderContext, TemplateRenderer
from .template_backend import apply_builtin_template
from .validation import validate_recipe_dict
from .cache import DEFAULT_TIMEOUT_SECONDS, DEFAULT_USER_AGENT
from .config import ARCHITECTURE_ALIASES as ARCHITECTURE_ALIASES, canonical_architecture
from .variants import select_concrete_variant, variant_specs as variant_specs


GLOBAL_MOUNT_POINTS = [
    "/afm01",
    "/afm02",
    "/cvmfs",
    "/90days",
    "/30days",
    "/QRISdata",
    "/RDS",
    "/data",
    "/short",
    "/proc_temp",
    "/TMPDIR",
    "/nvme",
    "/neurodesktop-storage",
    "/local",
    "/gpfs1",
    "/working",
    "/winmounts",
    "/state",
    "/tmp",
    "/autofs",
    "/cluster",
    "/local_mount",
    "/scratch",
    "/clusterdata",
    "/nvmescratch",
]


def _check_docker_image(image: str) -> str:
    if image == "":
        raise ValueError("Docker image cannot be empty")
    if ":" not in image:
        return image + ":latest"
    return image


def _hash_obj(value: Any) -> str:
    if isinstance(value, str):
        data = value.encode("utf-8")
    elif isinstance(value, (dict, list)):
        data = yaml.dump(value).encode("utf-8")
    else:
        raise ValueError(f"object type not supported for hashing: {type(value)}")
    return hashlib.sha256(data).hexdigest()


def _render_release_recipe(
    recipe: dict[str, Any],
    renderer: TemplateRenderer,
    context: RenderContext,
) -> dict[str, Any]:
    rendered_recipe = dict(recipe)
    for key in ("categories", "apptainer_args", "show_in_menu", "show_in_applist", "gui_apps"):
        if key in rendered_recipe:
            rendered_recipe[key] = renderer.render_value(rendered_recipe[key], context)
    return rendered_recipe


def _render_structured_readme(
    recipe: dict[str, Any],
    renderer: TemplateRenderer,
    context: RenderContext,
) -> str:
    structured = recipe.get("structured_readme")
    if not isinstance(structured, dict):
        return ""

    rendered = renderer.render_value(structured, context)
    fields = {
        key: str(rendered.get(key) or "").strip()
        for key in ("description", "example", "documentation", "citation")
    }
    if not any(fields.values()):
        return ""

    sections = [
        "----------------------------------",
        f"## {context.name}/{context.version} ##",
    ]
    if fields["description"]:
        sections.append(fields["description"])
    if fields["example"]:
        sections.append(f'Example:\n```\n{fields["example"]}\n```')
    if fields["documentation"]:
        sections.append(
            f'More documentation can be found here: {fields["documentation"]}'
        )
    if fields["citation"]:
        sections.append(f'Citation:\n```\n{fields["citation"]}\n```')
    sections.extend(
        [
            f"To run container outside of this environment: ml {context.name}/{context.version}",
            "----------------------------------",
        ]
    )
    return "\n\n".join(sections)


def _read_readme_url(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        ) as response:
            return response.read().decode("utf-8")
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        OSError,
        UnicodeDecodeError,
    ) as exc:
        raise ValueError(f"failed to load readme_url {url}: {exc}") from exc


@dataclass
class RecipeFile:
    path: Path
    data: dict[str, Any]

    @property
    def name(self) -> str:
        return str(self.data["name"])

    @property
    def version(self) -> str:
        return str(self.data["version"])

    @property
    def build(self) -> dict[str, Any]:
        return self.data["build"]


@dataclass
class CompiledRecipe:
    recipe: dict[str, Any]
    recipe_dir: Path
    name: str
    base_name: str
    variant: str
    version: str
    architecture: str
    readme: str
    definition: Definition
    staging_plan: StagingPlan
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def tag(self) -> str:
        return f"{self.name}:{self.version}".lower()


def load_recipe(recipe_dir: Path) -> dict[str, Any]:
    path = recipe_dir / "build.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"recipe file not found: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"recipe file must contain a mapping: {path}")
    if "name" in data and data["name"] is not None:
        data["name"] = str(data["name"])
    if "version" in data and data["version"] is not None:
        data["version"] = str(data["version"])
    validate_recipe_dict(data, strict_metadata=False)
    return data


def load_recipe_file(recipe_dir: Path) -> RecipeFile:
    return RecipeFile(path=recipe_dir / "build.yaml", data=load_recipe(recipe_dir))


def normalize_architecture(value: str | None) -> str:
    return canonical_architecture(value or platform.machine())


def _split_install(value: Any) -> list[str]:
    if isinstance(value, str):
        return shlex.split(value)
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    raise ValueError("install directive must be a string or list")


def _copy_parts(value: Any) -> list[str]:
    if isinstance(value, str):
        return shlex.split(value)
    if isinstance(value, list):
        return [str(item) for item in value]
    raise ValueError("copy directive must be a string or list")


def _default_directives(definition: Definition, build: dict[str, Any], pkg_manager: str) -> None:
    definition.add(From(str(build["base-image"])))
    definition.add(User("root"))
    add_default = bool(build.get("add-default-template", True))
    if add_default:
        definition.add(
            Env(
                {
                    "LANG": "en_US.UTF-8",
                    "LC_ALL": "en_US.UTF-8",
                    "ND_ENTRYPOINT": "/neurodocker/startup.sh",
                }
            )
        )
        definition.add(
            Run(_default_template_command(pkg_manager))
        )
    definition.add(Run("printf '#!/bin/bash\\nls -la' > /usr/bin/ll"))
    definition.add(Run("chmod +x /usr/bin/ll"))
    definition.add(Run("mkdir -p " + " ".join(GLOBAL_MOUNT_POINTS)))
    if pkg_manager == "apt" and bool(build.get("add-tzdata", add_default)):
        definition.add(Env({"DEBIAN_FRONTEND": "noninteractive"}))
        definition.add(Env({"TZ": "UTC"}))
        definition.add(Install(("tzdata",)))
        definition.add(Run("ln -snf /usr/share/zoneinfo/UTC /etc/localtime && echo UTC > /etc/timezone"))


def _default_template_command(pkg_manager: str) -> str:
    if pkg_manager == "apt":
        return (
            'export ND_ENTRYPOINT="/neurodocker/startup.sh"\n'
            "apt-get update -qq\n"
            "apt-get install -y -q --no-install-recommends \\\n"
            "    apt-utils \\\n"
            "    bzip2 \\\n"
            "    ca-certificates \\\n"
            "    curl \\\n"
            "    locales \\\n"
            "    unzip\n"
            "rm -rf /var/lib/apt/lists/*\n"
            "if ! grep -qx 'en_US.UTF-8 UTF-8' /etc/locale.gen 2>/dev/null; then\n"
            "  printf 'en_US.UTF-8 UTF-8\\n' >> /etc/locale.gen;\n"
            "fi\n"
            "locale-gen en_US.UTF-8\n"
            'update-locale LANG="en_US.UTF-8"\n'
            "chmod 777 /opt && chmod a+s /opt\n"
            "mkdir -p /neurodocker\n"
            'if [ ! -f "$ND_ENTRYPOINT" ]; then\n'
            "  echo '#!/usr/bin/env bash' >> \"$ND_ENTRYPOINT\"\n"
            "  echo 'set -e' >> \"$ND_ENTRYPOINT\"\n"
            "  echo 'export USER=\"${USER:=`whoami`}\"' >> \"$ND_ENTRYPOINT\"\n"
            "  echo 'if [ -n \"$1\" ]; then \"$@\"; else /usr/bin/env bash; fi' >> \"$ND_ENTRYPOINT\";\n"
            "fi\n"
            "chmod -R 777 /neurodocker && chmod a+s /neurodocker"
        )
    if pkg_manager in {"yum", "rpm"}:
        return (
            'export ND_ENTRYPOINT="/neurodocker/startup.sh"\n'
            "if ls /etc/yum.repos.d/CentOS-* >/dev/null 2>&1; then\n"
            "    sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*\n"
            "    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*;\n"
            "fi\n"
            "yum install -y -q \\\n"
            "    bzip2 \\\n"
            "    ca-certificates \\\n"
            "    glibc-langpack-en \\\n"
            "    glibc-locale-source \\\n"
            "    unzip\n"
            "yum clean all\n"
            "rm -rf /var/cache/yum/*\n"
            "localedef -i en_US -f UTF-8 en_US.UTF-8\n"
            "chmod 777 /opt && chmod a+s /opt\n"
            "mkdir -p /neurodocker\n"
            'if [ ! -f "$ND_ENTRYPOINT" ]; then\n'
            "  echo '#!/usr/bin/env bash' >> \"$ND_ENTRYPOINT\"\n"
            "  echo 'set -e' >> \"$ND_ENTRYPOINT\"\n"
            "  echo 'export USER=\"${USER:=`whoami`}\"' >> \"$ND_ENTRYPOINT\"\n"
            "  echo 'if [ -n \"$1\" ]; then \"$@\"; else /usr/bin/env bash; fi' >> \"$ND_ENTRYPOINT\";\n"
            "fi\n"
            "chmod -R 777 /neurodocker && chmod a+s /neurodocker"
        )
    return (
        'export ND_ENTRYPOINT="/neurodocker/startup.sh"\n'
        "mkdir -p /neurodocker\n"
        'if [ ! -f "$ND_ENTRYPOINT" ]; then\n'
        "  echo '#!/usr/bin/env bash' >> \"$ND_ENTRYPOINT\"\n"
        "  echo 'set -e' >> \"$ND_ENTRYPOINT\"\n"
        "  echo 'export USER=\"${USER:=`whoami`}\"' >> \"$ND_ENTRYPOINT\"\n"
        "  echo 'if [ -n \"$1\" ]; then \"$@\"; else /usr/bin/env bash; fi' >> \"$ND_ENTRYPOINT\";\n"
        "fi\n"
        "chmod -R 777 /neurodocker && chmod a+s /neurodocker"
    )


def compile_recipe(
    recipe_dir: Path,
    *,
    architecture: str | None = None,
    variant: str | None = None,
    ignore_architecture: bool = False,
    local_keys: set[str] | None = None,
    include_dirs: tuple[Path, ...] = (),
    parallel_jobs: int | None = None,
    option_overrides: dict[str, bool] | None = None,
    resolve_readme_url: bool = True,
) -> CompiledRecipe:
    recipe_file = load_recipe_file(recipe_dir)
    recipe = recipe_file.data
    selected_variant = select_concrete_variant(
        recipe,
        architecture=architecture,
        variant=variant,
        ignore_architecture=ignore_architecture,
        host_architecture=platform.machine(),
        host_platform=platform.system(),
    )
    variant_name = str(selected_variant["variant"])
    arch = str(selected_variant["architecture"])

    renderer = TemplateRenderer()
    option_values = {
        str(key): bool((value or {}).get("default", False))
        for key, value in (recipe.get("options") or {}).items()
        if isinstance(value, dict)
    }
    option_values.update(selected_variant["options"])
    option_values.update(option_overrides or {})
    version = str(recipe["version"])
    for key, value in (recipe.get("options") or {}).items():
        if option_values.get(str(key)) and isinstance(value, dict):
            version += str(value.get("version_suffix") or "")
    plan = StagingPlan(recipe_dir=recipe_dir)
    context = RenderContext(
        name=str(selected_variant["name"]),
        version=version,
        original_version=recipe["version"],
        arch=arch,
        variant=variant_name,
        parallel_jobs=parallel_jobs or (os.cpu_count() or 1),
        local_keys=local_keys or set(),
        options=SimpleNamespace(**option_values),
        staging_plan=plan,
    )
    definition = Definition()
    deploy_bins: list[Any] = []
    deploy_path: list[Any] = []

    # Relative filenames resolve against the directory of the recipe or of the
    # macro declaring them, so a shared macro can ship its own files.
    file_dirs = [recipe_dir]

    def register_file(mapping: dict[str, Any]) -> None:
        if "condition" in mapping and not renderer.render_condition(str(mapping["condition"]), context):
            return
        name = renderer.render_string(str(mapping["name"]), context)
        rendered = dict(mapping)
        rendered["name"] = name
        for key in ("filename", "url", "contents", "sha256"):
            if key in rendered and rendered[key] is not None:
                rendered[key] = renderer.render_value(rendered[key], context)
        file = declared_file_from_mapping(name, rendered)
        plan.add_file(file, relative_to=file_dirs[-1])

    for key, value in (recipe.get("variables") or {}).items():
        context.values[key] = renderer.render_value(value, context)

    for mapping in recipe.get("files", []) or []:
        register_file(mapping)

    readme = str(recipe.get("readme") or "")
    if not readme.strip():
        readme_path = recipe_dir / "README.md"
        if readme_path.exists():
            readme = readme_path.read_text()
    readme = renderer.render_string(readme, context)
    if not readme.strip():
        readme = _render_structured_readme(recipe, renderer, context)
    if not readme.strip() and recipe.get("readme_url"):
        readme_url = renderer.render_string(str(recipe["readme_url"]), context)
        readme = (
            _read_readme_url(readme_url)
            if resolve_readme_url
            else f"README source: {readme_url}"
        )
    if not readme.strip():
        raise ValueError(
            f"{recipe['name']}: README content cannot be empty; set readme, "
            "add README.md, provide structured_readme, or set readme_url"
        )

    build = dict(recipe["build"])
    build["base-image"] = _check_docker_image(str(renderer.render_value(build["base-image"], context)))
    build["pkg-manager"] = renderer.render_value(build["pkg-manager"], context)
    pkg_manager = str(build["pkg-manager"])
    definition.pkg_manager = pkg_manager
    definition.fix_locale_def = bool(build.get("fix-locale-def", False))
    _default_directives(definition, build, pkg_manager)

    def apply_directive(directive: dict[str, Any], local_values: dict[str, Any] | None = None) -> None:
        if "condition" in directive and not renderer.render_condition(str(directive["condition"]), context):
            return
        if local_values:
            old_values = dict(context.values)
            context.values.update(local_values)
        else:
            old_values = None
        try:
            if "install" in directive:
                rendered = renderer.render_value(directive["install"], context)
                definition.add(Install(tuple(_split_install(rendered))))
            elif "run" in directive:
                with context.mount_scope("h" + _hash_obj(directive)[:8]) as scope:
                    rendered = renderer.render_value(directive["run"], context)
                if isinstance(rendered, str):
                    rendered = [rendered]
                elif not isinstance(rendered, list):
                    raise ValueError("run directive must render to a string or list")
                commands = [str(item) for item in rendered if item is not None and str(item) != ""]
                command = " " + " \\\n && ".join(commands)
                if scope.mounts:
                    definition.add(RunWithMounts(scope.mounts, command))
                else:
                    definition.add(Run(command))
            elif "workdir" in directive:
                definition.add(Workdir(str(renderer.render_value(directive["workdir"], context))))
            elif "user" in directive:
                definition.add(User(str(renderer.render_value(directive["user"], context))))
            elif "entrypoint" in directive:
                definition.add(Entrypoint(str(renderer.render_value(directive["entrypoint"], context))))
            elif "environment" in directive:
                env = renderer.render_value(directive["environment"], context)
                if not isinstance(env, dict):
                    raise ValueError("environment directive must render to a mapping")
                for key, value in env.items():
                    definition.add(Env({str(key): str(value)}))
            elif "copy" in directive:
                parts = _copy_parts(renderer.render_value(directive["copy"], context))
                if len(parts) < 2:
                    raise ValueError("copy directive requires source and destination")
                resolved_sources = [plan.add_copy_source(source) for source in parts[:-1]]
                definition.add(Copy(tuple(resolved_sources), parts[-1]))
            elif "variables" in directive:
                values = directive["variables"]
                if not isinstance(values, dict):
                    raise ValueError("variables directive must be a mapping")
                for key, value in values.items():
                    context.values[str(key)] = renderer.render_value(value, context)
            elif "group" in directive:
                with_values: dict[str, Any] = {}
                for key, value in (directive.get("with") or {}).items():
                    with_values[str(key)] = renderer.render_value(value, context)
                for child in directive["group"]:
                    apply_directive(child, with_values)
            elif "include" in directive:
                include_name = str(renderer.render_value(directive["include"], context))
                include_path = None
                for include_dir in include_dirs:
                    candidate = include_dir / include_name
                    if candidate.exists():
                        include_path = candidate
                        break
                if include_path is None:
                    raise FileNotFoundError(f"include not found: {include_name}")
                include_data = yaml.safe_load(include_path.read_text())
                file_dirs.append(include_path.parent.resolve())
                try:
                    for child in include_data.get("directives", []):
                        apply_directive(child)
                finally:
                    file_dirs.pop()
            elif "file" in directive:
                file_mapping = directive["file"]
                if not isinstance(file_mapping, dict):
                    raise ValueError("file directive must be a mapping")
                register_file(file_mapping)
            elif "deploy" in directive:
                deploy = renderer.render_value(directive["deploy"], context)
                if isinstance(deploy, dict):
                    if "bins" in deploy:
                        bins = deploy["bins"]
                        if not isinstance(bins, list):
                            raise ValueError("Deploy bins must be a list")
                        deploy_bins.extend(bins)
                    if "path" in deploy:
                        path = deploy["path"]
                        if not isinstance(path, list):
                            raise ValueError("Deploy path must be a list")
                        deploy_path.extend(path)
            elif "template" in directive:
                template = directive["template"]
                if not isinstance(template, dict):
                    raise ValueError("template directive must be a mapping")
                with context.mount_scope("h" + _hash_obj(directive)[:8]) as scope:
                    name = str(renderer.render_value(template.get("name", ""), context))
                    params = {
                        str(key): renderer.render_value(value, context)
                        for key, value in template.items()
                        if key != "name"
                    }
                params.setdefault("arch", "x86_64" if context.arch == "x86_64" else "aarch64")
                template_directives = []
                apply_builtin_template(name, params, pkg_manager, template_directives.append)
                for item in template_directives:
                    if scope.mounts and isinstance(item, Run):
                        item = RunWithMounts(scope.mounts, item.command)
                    definition.add(item)
            elif "boutique" in directive:
                boutique_data = renderer.render_value(directive["boutique"], context)
                if not isinstance(boutique_data, dict):
                    raise ValueError("Boutique directive must be a mapping")
                filename = f"{boutique_data.get('name', 'tool')}.json"
                plan.add_file(
                    DeclaredFile(
                        name=filename,
                        contents=json.dumps(boutique_data, indent=2) + "\n",
                    ),
                    renderable=False,
                )
                plan.copy_sources.append(
                    CopySource(source=filename, declared_name=filename)
                )
                definition.add(Run("mkdir -p /boutique"))
                definition.add(Copy((filename,), f"/boutique/{filename}"))
            elif "test" in directive:
                return
            else:
                raise ValueError(f"unsupported directive: {directive}")
        finally:
            if old_values is not None:
                context.values = old_values

    for directive in build.get("directives", []) or []:
        apply_directive(directive)

    top_level_deploy = renderer.render_value(recipe.get("deploy") or {}, context)
    if not deploy_bins and isinstance(top_level_deploy, dict):
        bins = top_level_deploy.get("bins", [])
        if isinstance(bins, list):
            deploy_bins = bins
    if not deploy_path and isinstance(top_level_deploy, dict):
        path = top_level_deploy.get("path", [])
        if isinstance(path, list):
            deploy_path = path
    definition.add(Env({"DEPLOY_PATH": ":".join(str(item) for item in deploy_path)}))
    definition.add(Env({"DEPLOY_BINS": ":".join(str(item) for item in deploy_bins)}))
    definition.add(Copy(("README.md",), "/README.md"))
    definition.add(Copy(("build.yaml",), "/build.yaml"))
    has_entrypoint = any(isinstance(item, Entrypoint) for item in definition.directives)
    if bool(build.get("add-default-template", True)) and not has_entrypoint:
        definition.add(Entrypoint("/neurodocker/startup.sh"))

    return CompiledRecipe(
        recipe=_render_release_recipe(recipe, renderer, context),
        recipe_dir=recipe_dir,
        name=context.name,
        base_name=recipe["name"],
        variant=variant_name,
        version=version,
        architecture=arch,
        readme=readme,
        definition=definition,
        staging_plan=plan,
        metadata=renderer.render_value({
            "copyright": [{"license": item.get("license")} for item in recipe.get("copyright") or []],
            "structured_readme": {"description": (recipe.get("structured_readme") or {}).get("description", "")},
        }, context),
    )
