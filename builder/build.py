#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_BUILD3_SRC = _REPO_ROOT / "build3" / "src"
if str(_BUILD3_SRC) not in sys.path:
    sys.path.insert(0, str(_BUILD3_SRC))

from build3.adapters import BuildInputs, BuildKitAdapter, SifAdapter, platform_for_architecture
from build3.cli import dockerfile_name, write_build_files
from build3.config import default_config, resolve_recipe
from build3.recipe import compile_recipe
from build3.release import build_date_for_recipe, release_data, write_release_file


NEUROCONTAINER_CACHE_CONTEXT_NAME = "neurocontainer-cache"
CONTAINER_TESTER_IMAGE = "neurocontainers/container-tester:latest"
CONTAINER_TESTER_BINARY_NAME = "container-tester"


def get_repo_path() -> str:
    return str(_REPO_ROOT)


def get_recipe_directory(repo_path: str, name: str) -> str:
    path = Path(name)
    if path.is_absolute() or "/" in name:
        return str(path.resolve())
    return str(Path(repo_path) / "recipes" / name)


def autodetect_recipe_path(repo_path: str, cwd: str) -> str | None:
    current = Path(cwd).resolve()
    repo = Path(repo_path).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "build.yaml").is_file():
            return str(candidate)
        if candidate == repo:
            break
    return None


def load_description_file(recipe_dir: str) -> Any:
    with open(Path(recipe_dir) / "build.yaml", "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_cache_dir() -> str:
    return os.environ.get(
        "NEUROCONTAINERS_CACHE_DIR",
        os.path.join(os.path.expanduser("~"), ".cache", "neurocontainers"),
    )


def get_docker_buildx_cache_dir(tag: str, architecture: str) -> str:
    safe = tag.replace("/", "_").replace(":", "_")
    return os.path.join(get_cache_dir(), "docker-buildx", architecture, safe)


def get_build_context_cache_dir() -> str:
    path = os.path.join(get_cache_dir(), "build-context")
    os.makedirs(path, exist_ok=True)
    return path


def get_cached_download_path(url: str) -> str:
    cache_dir = get_cache_dir()
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, hashlib.sha256(url.encode("utf-8")).hexdigest())


def get_guest_filename(name: str, url: str | None = None) -> str:
    if url is not None:
        parsed = urllib.parse.urlparse(url)
        basename = os.path.basename(urllib.parse.unquote(parsed.path))
        if basename not in {"", ".", ".."}:
            return basename
    return name


def link_or_copy_file(source: str, destination: str) -> None:
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        try:
            if Path(source).samefile(target):
                return
        except OSError:
            pass
        target.unlink()
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


def download_with_cache(
    url: str,
    check_only: bool = False,
    insecure: bool = False,
    retry: int = 1,
    curl_options: str = "",
    force_download: bool = False,
) -> str:
    output_filename = get_cached_download_path(url)
    temp_filename = output_filename + ".tmp"
    if force_download:
        for filename in (output_filename, temp_filename):
            if os.path.exists(filename):
                os.remove(filename)
    if os.path.exists(output_filename) and os.path.getsize(output_filename) > 0:
        return output_filename
    if check_only:
        Path(output_filename).touch()
        return output_filename
    if shutil.which("curl"):
        command = ["curl", "--location", "--fail", "--show-error", "--output", temp_filename]
        if insecure:
            command.append("--insecure")
        if retry:
            command.extend(["--retry", str(retry)])
        if curl_options:
            command.extend(curl_options.split())
        command.append(url)
        subprocess.check_call(command)
        shutil.move(temp_filename, output_filename)
    else:
        with urllib.request.urlopen(url) as response, open(temp_filename, "wb") as handle:
            shutil.copyfileobj(response, handle)
        shutil.move(temp_filename, output_filename)
    return output_filename


def cleanup_cached_file(url: str) -> bool:
    success = True
    for filename in (get_cached_download_path(url), get_cached_download_path(url) + ".tmp"):
        if os.path.exists(filename):
            try:
                os.remove(filename)
            except OSError:
                success = False
    return success


def cleanup_temp_files() -> int:
    cache_dir = Path(get_cache_dir())
    if not cache_dir.exists():
        return 0
    cleaned = 0
    for path in cache_dir.glob("*.tmp"):
        path.unlink()
        cleaned += 1
    return cleaned


def get_build_platform(arch: str) -> str:
    return platform_for_architecture(_normalize_architecture(arch))


def _normalize_architecture(arch: str | None) -> str:
    aliases = {"amd64": "x86_64", "AMD64": "x86_64", "arm64": "aarch64", "ARM64": "aarch64"}
    return aliases.get(arch or platform.machine(), arch or platform.machine())


def _parse_options(options: list[str] | None) -> dict[str, bool]:
    parsed: dict[str, bool] = {}
    for option in options or []:
        key, separator, value = option.partition("=")
        if not key or separator != "=":
            raise ValueError("--option must use key=value syntax")
        parsed[key] = value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return parsed


def _parse_local_context(local_context: str | None) -> tuple[set[str], tuple[tuple[str, Path], ...]]:
    if not local_context:
        return set(), ()
    key, separator, value = local_context.partition("=")
    if not key or separator != "=":
        raise ValueError("--local must use key=path syntax")
    if key == NEUROCONTAINER_CACHE_CONTEXT_NAME:
        raise ValueError(f"Local context name {key!r} is reserved")
    return {key}, ((key, Path(value).resolve()),)


@dataclass
class BuildContext:
    base_path: str
    recipe_path: str
    name: str
    version: str
    arch: str
    check_only: bool = False
    build_directory: str | None = None
    dockerfile_name: str | None = None
    tag: str | None = None
    files: dict[str, dict[str, str]] = field(default_factory=dict)
    local_contexts: tuple[tuple[str, Path], ...] = ()

    def set_max_parallel_jobs(self, value: int | None) -> None:
        self.max_parallel_jobs = value


class LocalBuildContext:
    def __init__(self, context: BuildContext, cache_id: str):
        self.context = context
        self.cache_id = cache_id

    def get_file(self, name: str) -> str:
        info = self.context.files[name]
        source = info["cached_path"]
        guest = info.get("guest_filename") or name
        target_dir = Path(get_cache_dir()) / "build-context" / self.cache_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / guest
        if target.exists() and Path(source).exists() and target.read_bytes() != Path(source).read_bytes():
            stem = target.stem
            suffix = target.suffix
            guest = f"{stem}_{hashlib.sha256(source.encode('utf-8')).hexdigest()[:12]}{suffix}"
            target = target_dir / guest
        link_or_copy_file(source, str(target))
        return f"/.neurocontainer-cache/{self.cache_id}/{guest}"


def _compile_recipe(
    recipe_path: str,
    architecture: str | None,
    ignore_architecture: bool,
    local_context: str | None,
    options: list[str] | None,
    max_parallel_jobs: int | None,
):
    config = default_config(_REPO_ROOT)
    local_keys, local_contexts = _parse_local_context(local_context)
    compiled = compile_recipe(
        Path(recipe_path),
        architecture=architecture,
        ignore_architecture=ignore_architecture,
        local_keys=local_keys,
        include_dirs=config.include_dirs,
        parallel_jobs=max_parallel_jobs,
        option_overrides=_parse_options(options),
    )
    return config, compiled, local_contexts


def generate_from_description(
    repo_path: str,
    recipe_path: str,
    description_file: Any,
    output_directory: str,
    architecture: str | None = None,
    ignore_architecture: bool | None = False,
    auto_build: bool = False,
    max_parallel_jobs: int | None = None,
    options: list[str] | None = None,
    recreate_output_dir: bool = False,
    check_only: bool = False,
    gpu: bool = False,
    local_context: str | None = None,
    skip_file_population: bool = False,
) -> BuildContext | None:
    if description_file.get("draft") and auto_build:
        print("WARN: This is a draft recipe. Auto build is enabled. Skipping build.")
        return None
    config, compiled, local_contexts = _compile_recipe(
        recipe_path,
        architecture,
        bool(ignore_architecture),
        local_context,
        options,
        max_parallel_jobs,
    )
    build_dir, dockerfile_path = write_build_files(
        Path(repo_path),
        compiled,
        Path(output_directory),
        recreate=recreate_output_dir,
        stage=not skip_file_population,
        download=not check_only and not skip_file_population,
    )
    ctx = BuildContext(
        base_path=repo_path,
        recipe_path=recipe_path,
        name=compiled.name,
        version=compiled.version,
        arch=compiled.architecture,
        check_only=check_only,
        build_directory=str(build_dir),
        dockerfile_name=dockerfile_path.name,
        tag=compiled.tag,
        local_contexts=local_contexts,
    )
    ctx.files = {
        name: {
            "guest_filename": file.guest_filename or name,
            "cached_path": str((build_dir / "cache" / (file.guest_filename or name)).resolve()),
            "url": file.url or "",
        }
        for name, file in compiled.staging_plan.files.items()
    }
    if check_only:
        print("Dockerfile generated successfully at", ctx.dockerfile_name)
    return ctx


def generate_release_file(name: str, version: str, recipe: dict[str, Any], recipe_path: str) -> Path:
    config = default_config(_REPO_ROOT)
    date = build_date_for_recipe(config.repo_root, Path(recipe_path))
    data = release_data(name, version, recipe, date)
    return write_release_file(config.repo_root, name, version, data)


def should_generate_release_file(generate_release: bool) -> bool:
    return bool(generate_release)


def _docker_buildx_available() -> bool:
    return shutil.which("docker") is not None


def _dockerfile_requires_buildkit(dockerfile_path: str) -> bool:
    return "--mount=type=bind" in Path(dockerfile_path).read_text(errors="ignore")


def _dockerfile_uses_file_cache_context(dockerfile_path: str) -> bool:
    return f"from={NEUROCONTAINER_CACHE_CONTEXT_NAME}" in Path(dockerfile_path).read_text(errors="ignore")


def build_and_run_container(
    dockerfile_name: str,
    name: str,
    version: str,
    tag: str,
    architecture: str,
    recipe_path: str,
    build_directory: str,
    login: bool = False,
    build_sif: bool = False,
    generate_release: bool = False,
    gpu: bool = False,
    local_context: str | None = None,
    mount: str | None = None,
    use_buildkit: bool = False,
    use_podman: bool = False,
    load_into_docker: bool = False,
    offline_mode: bool = False,
) -> None:
    local_keys, local_contexts = _parse_local_context(local_context)
    inputs = BuildInputs(
        name=name,
        version=version,
        tag=tag,
        architecture=_normalize_architecture(architecture),
        build_dir=Path(build_directory),
        dockerfile_path=Path(build_directory) / dockerfile_name,
        local_contexts=local_contexts,
    )
    if build_sif:
        archive = Path(build_directory) / f"{name}_{version}.docker.tar"
        if use_buildkit:
            BuildKitAdapter().run(inputs, archive)
        else:
            subprocess.check_call(DockerAdapterCompat(use_podman=use_podman).command(inputs))
            subprocess.check_call(["docker", "save", "-o", str(archive), tag])
        sif = Path(get_repo_path()) / "sifs" / f"{name}_{version}.sif"
        SifAdapter().run(archive, sif)
        return
    if use_buildkit:
        archive = Path(build_directory) / f"{name}_{version}.docker.tar"
        BuildKitAdapter().run(inputs, archive)
        if load_into_docker and shutil.which("docker"):
            subprocess.check_call(["docker", "load", "-i", str(archive)])
    else:
        subprocess.check_call(DockerAdapterCompat(use_podman=use_podman).command(inputs))
    if login:
        command = ["docker", "run", "--rm", "-it"]
        if gpu:
            command.extend(["--gpus", "all"])
        if offline_mode:
            command.extend(["--network", "none"])
        if mount:
            command.extend(["-v", mount])
        command.append(tag)
        subprocess.check_call(command)


class DockerAdapterCompat:
    def __init__(self, use_podman: bool = False):
        self.use_podman = use_podman

    def command(self, inputs: BuildInputs) -> list[str]:
        cache_dir = get_docker_buildx_cache_dir(inputs.tag, inputs.architecture)
        command = [
            "podman" if self.use_podman else "docker",
            "buildx" if not self.use_podman else "build",
        ]
        if not self.use_podman:
            command.extend(["build", "--load"])
        command.extend(
            [
                "--platform",
                platform_for_architecture(inputs.architecture),
                "-f",
                inputs.dockerfile_path.name,
                "-t",
                inputs.tag,
            ]
        )
        if not self.use_podman:
            command.extend(
                [
                    "--cache-from",
                    f"type=local,src={cache_dir}",
                    "--cache-to",
                    f"type=local,dest={cache_dir},mode=max",
                ]
            )
        staged_cache = inputs.build_dir / "cache"
        cache_context = staged_cache if staged_cache.exists() else Path(get_build_context_cache_dir())
        command.extend(["--build-context", f"{NEUROCONTAINER_CACHE_CONTEXT_NAME}={cache_context}"])
        for key, path in inputs.local_contexts:
            command.extend(["--build-context", f"{key}={path}"])
        command.append(".")
        return command


def run_tests(recipe_path: str, gpu: bool = False, offline_mode: bool = False) -> None:
    from workflows.test_runner import ContainerTestRunner, TestRequest

    recipe = Path(recipe_path).name
    request = TestRequest(recipe=recipe, location="docker", runtime="docker", gpu=gpu, allow_missing_tests=True)
    ContainerTestRunner().run(request)


def run_container_tester(*args: Any, **kwargs: Any) -> None:
    return None


def init_new_recipe(repo_path: str, name: str, version: str) -> None:
    if not name or not version:
        raise ValueError("Name and version cannot be empty.")
    recipe_path = Path(get_recipe_directory(repo_path, name))
    recipe_path.mkdir(parents=True, exist_ok=True)
    description_file = recipe_path / "build.yaml"
    if description_file.exists():
        raise ValueError(f"Description file {description_file} already exists.")
    description_file.write_text(
        yaml.safe_dump(
            {
                "name": name,
                "version": version,
                "architectures": ["x86_64"],
                "copyright": [{"license": "TODO", "url": "TODO"}],
                "build": {
                    "kind": "neurodocker",
                    "base-image": "ubuntu:24.04",
                    "pkg-manager": "apt",
                    "directives": [
                        {"file": {"name": "hello.txt", "contents": "Hello, world!"}},
                        {"run": ['cat {{ get_file("hello.txt") }}']},
                        {"deploy": {"bins": ["TODO"]}},
                    ],
                },
                "readme": "TODO",
            },
            sort_keys=False,
            default_flow_style=False,
        )
    )


def _recipe_path_from_optional_name(name: str | None) -> str:
    repo_path = get_repo_path()
    if name is None:
        detected = autodetect_recipe_path(repo_path, os.getcwd())
        if detected is None:
            raise SystemExit("No recipe found in current directory.")
        return detected
    return get_recipe_directory(repo_path, name)


def generate_dockerfile(repo_path: str, recipe_path: str, **kwargs: Any) -> BuildContext | None:
    recipe = load_description_file(recipe_path)
    return generate_from_description(
        repo_path,
        recipe_path,
        recipe,
        kwargs.pop("output_directory", str(Path(repo_path) / "build")),
        recreate_output_dir=True,
        **kwargs,
    )


def generate_and_build(
    repo_path: str,
    recipe_path: str,
    login: bool = False,
    architecture: str | None = None,
    ignore_architecture: bool = False,
    generate_release: bool = False,
    gpu: bool = False,
    local_context: str | None = None,
    mount: str | None = None,
    use_buildkit: bool = False,
    use_podman: bool = False,
    load_into_docker: bool = False,
    offline_mode: bool = False,
) -> None:
    ctx = generate_dockerfile(
        repo_path,
        recipe_path,
        architecture=architecture,
        ignore_architecture=ignore_architecture,
        local_context=local_context,
    )
    if ctx is None or ctx.dockerfile_name is None or ctx.build_directory is None or ctx.tag is None:
        raise RuntimeError("Recipe generation failed")
    build_and_run_container(
        ctx.dockerfile_name,
        ctx.name,
        ctx.version,
        ctx.tag,
        ctx.arch,
        recipe_path,
        ctx.build_directory,
        login=login,
        generate_release=generate_release,
        gpu=gpu,
        local_context=local_context,
        mount=mount,
        use_buildkit=use_buildkit,
        use_podman=use_podman,
        load_into_docker=load_into_docker,
        offline_mode=offline_mode,
    )
    if generate_release:
        generate_release_file(ctx.name, ctx.version, load_description_file(recipe_path), recipe_path)


def add_offline_mode_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--offline-mode", "--offline_mode", dest="offline_mode", action="store_true")


def generate_main() -> None:
    parser = argparse.ArgumentParser(description="NeuroContainer Builder - Generate Dockerfiles")
    parser.add_argument("name", nargs="?")
    args = parser.parse_args()
    generate_dockerfile(get_repo_path(), _recipe_path_from_optional_name(args.name))


def build_main(login: bool = False) -> None:
    parser = argparse.ArgumentParser(description="NeuroContainer Builder - Build Docker images")
    parser.add_argument("name", nargs="?")
    parser.add_argument("--architecture", default=platform.machine())
    parser.add_argument("--ignore-architectures", action="store_true")
    parser.add_argument("--generate-release", action="store_true")
    parser.add_argument("--gpu", action="store_true")
    parser.add_argument("--local")
    parser.add_argument("--mount")
    parser.add_argument("--use-buildkit", action="store_true")
    parser.add_argument("--use-podman", action="store_true")
    parser.add_argument("--load-into-docker", action="store_true")
    add_offline_mode_argument(parser)
    args = parser.parse_args()
    generate_and_build(
        get_repo_path(),
        _recipe_path_from_optional_name(args.name),
        login=login,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        generate_release=args.generate_release,
        gpu=args.gpu,
        local_context=args.local,
        mount=args.mount,
        use_buildkit=args.use_buildkit,
        use_podman=args.use_podman,
        load_into_docker=args.load_into_docker,
        offline_mode=args.offline_mode,
    )


def login_main() -> None:
    build_main(login=True)


def sf_make_main() -> None:
    parser = argparse.ArgumentParser(description="Build a recipe into a SIF")
    parser.add_argument("name", nargs="?")
    parser.add_argument("--architecture", default=platform.machine())
    parser.add_argument("--ignore-architectures", action="store_true")
    parser.add_argument("--local")
    parser.add_argument("--mount")
    parser.add_argument("--use-docker", action="store_true")
    args = parser.parse_args()
    ctx = generate_dockerfile(
        get_repo_path(),
        _recipe_path_from_optional_name(args.name),
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        local_context=args.local,
    )
    if ctx is None or ctx.dockerfile_name is None or ctx.build_directory is None or ctx.tag is None:
        raise RuntimeError("Recipe generation failed")
    build_and_run_container(
        ctx.dockerfile_name,
        ctx.name,
        ctx.version,
        ctx.tag,
        ctx.arch,
        ctx.recipe_path,
        ctx.build_directory,
        build_sif=True,
        local_context=args.local,
        mount=args.mount,
        use_buildkit=not args.use_docker,
    )


def test_main() -> None:
    parser = argparse.ArgumentParser(description="NeuroContainer Builder - Run tests")
    parser.add_argument("name", nargs="?")
    parser.add_argument("--architecture", default=platform.machine())
    parser.add_argument("--ignore-architectures", action="store_true")
    parser.add_argument("--gpu", action="store_true")
    add_offline_mode_argument(parser)
    args = parser.parse_args()
    recipe_path = _recipe_path_from_optional_name(args.name)
    generate_and_build(
        get_repo_path(),
        recipe_path,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        gpu=args.gpu,
        offline_mode=args.offline_mode,
    )
    run_tests(recipe_path, gpu=args.gpu, offline_mode=args.offline_mode)


def test_remote_main() -> None:
    from workflows.test_runner import ContainerTestRunner, TestRequest

    parser = argparse.ArgumentParser(description="Run release container tests")
    parser.add_argument("recipe")
    parser.add_argument("--version")
    parser.add_argument("--release-file")
    parser.add_argument("--runtime", choices=["docker", "apptainer", "singularity"])
    parser.add_argument("--location", choices=["auto", "cvmfs", "local", "release", "docker"], default="auto")
    parser.add_argument("--test-config")
    parser.add_argument("-o", "--output")
    parser.add_argument("--gpu", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--auto-cleanup", action="store_true")
    parser.add_argument("--docker-to-simg", action="store_true")
    parser.add_argument("--docker-registry", default="neurodesk")
    parser.add_argument("--docker-save-to-simg", default="builder/docker-save-to-simg.go")
    parser.add_argument("--cleanup-all", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    runner = ContainerTestRunner()
    if args.cleanup_all:
        print(f"Cleaned up {runner.cleanup_all(verbose=args.verbose)} cached container file(s)")
        return
    output_path = Path(args.output).resolve() if args.output else None
    request = TestRequest(
        recipe=args.recipe,
        version=args.version,
        release_file=args.release_file,
        test_config=args.test_config,
        runtime=args.runtime,
        location=args.location,
        gpu=args.gpu,
        cleanup=args.cleanup,
        auto_cleanup=args.auto_cleanup,
        docker_to_simg=args.docker_to_simg,
        docker_registry=args.docker_registry,
        docker_save_to_simg=args.docker_save_to_simg,
        verbose=args.verbose,
        allow_missing_tests=False,
        output_dir=output_path.parent if output_path else None,
        results_path=output_path,
    )
    outcome = runner.run(request)
    print(f"\nTest results written to {outcome.results_path}")
    raise SystemExit(0 if outcome.status == "passed" else 1)


def init_main() -> None:
    parser = argparse.ArgumentParser(description="Initialize a new recipe")
    parser.add_argument("name")
    parser.add_argument("version")
    args = parser.parse_args()
    init_new_recipe(get_repo_path(), args.name, args.version)


def cache_main() -> None:
    parser = argparse.ArgumentParser(description="Inspect or clean cached downloads")
    parser.add_argument("recipe", nargs="?")
    parser.add_argument("--url")
    parser.add_argument("--temp-files", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if args.url:
        raise SystemExit(0 if cleanup_cached_file(args.url) else 1)
    if args.all:
        shutil.rmtree(get_cache_dir(), ignore_errors=True)
        return
    print(f"Cleaned up {cleanup_temp_files()} temporary files")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="NeuroContainer Builder")
    subparsers = parser.add_subparsers(dest="command")
    generate = subparsers.add_parser("generate")
    generate.add_argument("name")
    generate.add_argument("--output-directory", default=str(Path.cwd() / "build"))
    generate.add_argument("--recreate", action="store_true")
    generate.add_argument("--build", action="store_true")
    generate.add_argument("--build-sif", action="store_true")
    generate.add_argument("--build-tinyrange", action="store_true")
    generate.add_argument("--tinyrange-path", default="tinyrange")
    generate.add_argument("--max-parallel-jobs", type=int, default=os.cpu_count())
    generate.add_argument("--test", action="store_true")
    generate.add_argument("--architecture", default=platform.machine())
    generate.add_argument("--ignore-architectures", action="store_true")
    generate.add_argument("--option", action="append")
    generate.add_argument("--login", action="store_true")
    generate.add_argument("--check-only", action="store_true")
    generate.add_argument("--auto-build", action="store_true")
    generate.add_argument("--generate-release", action="store_true")
    generate.add_argument("--gpu", action="store_true")
    generate.add_argument("--use-buildkit", action="store_true")
    generate.add_argument("--use-podman", action="store_true")
    generate.add_argument("--load-into-docker", action="store_true")
    add_offline_mode_argument(generate)
    init = subparsers.add_parser("init")
    init.add_argument("name")
    init.add_argument("version")
    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--url")
    cleanup.add_argument("--temp-files", action="store_true")
    cleanup.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)

    if args.command == "init":
        init_new_recipe(get_repo_path(), args.name, args.version)
        return
    if args.command == "cleanup":
        if args.url:
            raise SystemExit(0 if cleanup_cached_file(args.url) else 1)
        if args.all:
            shutil.rmtree(get_cache_dir(), ignore_errors=True)
        else:
            print(f"Cleaned up {cleanup_temp_files()} temporary files")
        return
    if args.command == "generate":
        recipe_path = get_recipe_directory(get_repo_path(), args.name)
        if args.build_tinyrange:
            raise NotImplementedError("TinyRange builds are not supported by the build3 backend")
        recipe = load_description_file(recipe_path)
        ctx = generate_from_description(
            get_repo_path(),
            recipe_path,
            recipe,
            args.output_directory,
            architecture=args.architecture,
            ignore_architecture=args.ignore_architectures,
            auto_build=args.auto_build,
            max_parallel_jobs=args.max_parallel_jobs,
            options=args.option,
            recreate_output_dir=args.recreate,
            check_only=args.check_only,
            gpu=args.gpu,
        )
        if ctx and args.generate_release and should_generate_release_file(args.generate_release):
            generate_release_file(ctx.name, ctx.version, recipe, recipe_path)
        if ctx and args.build:
            build_and_run_container(
                ctx.dockerfile_name or dockerfile_name(ctx.name, ctx.version),
                ctx.name,
                ctx.version,
                ctx.tag or f"{ctx.name}:{ctx.version}".lower(),
                ctx.arch,
                recipe_path,
                ctx.build_directory or str(Path(args.output_directory) / ctx.name),
                login=args.login,
                build_sif=args.build_sif,
                generate_release=args.generate_release,
                gpu=args.gpu,
                use_buildkit=args.use_buildkit,
                use_podman=args.use_podman,
                load_into_docker=args.load_into_docker,
                offline_mode=args.offline_mode,
            )
            if args.test:
                run_tests(recipe_path, gpu=args.gpu, offline_mode=args.offline_mode)
        return
    parser.print_help()


if __name__ == "__main__":
    main(sys.argv[1:])
