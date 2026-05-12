from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from .adapters import BuildInputs, BuildKitAdapter, DockerAdapter, SifAdapter
from .config import default_config, resolve_recipe
from .dockerfile import render_dockerfile
from .recipe import compile_recipe
from .release import build_date_for_recipe, release_data, write_release_file
from .staging import materialize_plan
from .tester import ContainerTesterAdapter, TestRequest


def dockerfile_name(name: str, version: str) -> str:
    return f"{name}_{version.replace(':', '_')}".lower() + ".Dockerfile"


def write_build_files(
    repo_root: Path,
    compiled,
    output_root: Path,
    *,
    recreate: bool = False,
    stage: bool = False,
    download: bool = False,
) -> tuple[Path, Path]:
    build_dir = output_root / compiled.name
    if build_dir.exists() and recreate:
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    dockerfile_path = build_dir / dockerfile_name(compiled.name, compiled.version)
    dockerfile_path.write_text(render_dockerfile(compiled.definition))
    (build_dir / "README.md").write_text(compiled.readme.rstrip() + "\n")
    shutil.copy2(compiled.recipe_dir / "build.yaml", build_dir / "build.yaml")

    if stage:
        materialize_plan(
            compiled.staging_plan,
            compiled.recipe_dir,
            build_dir,
            http_cache_dir=output_root.parent / "httpcache",
            download=download,
        )
    return build_dir, dockerfile_path


def add_common_recipe_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("recipe", nargs="?", help="Recipe name or recipe directory")
    parser.add_argument("--architecture", default=None, help="Target architecture")
    parser.add_argument("--ignore-architectures", action="store_true")
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--local", action="append", default=[], help="Named local context KEY=PATH")
    parser.add_argument("--option", action="append", default=[], help="Set recipe option KEY=VALUE")


def local_contexts(values: list[str]) -> tuple[tuple[str, Path], ...]:
    contexts: list[tuple[str, Path]] = []
    for value in values:
        key, separator, path = value.partition("=")
        if not key:
            continue
        if separator != "=":
            raise ValueError("--local must be in KEY=PATH form")
        contexts.append((key, Path(path).resolve()))
    return tuple(contexts)


def local_keys(values: list[str]) -> set[str]:
    keys: set[str] = set()
    for value in values:
        key, _, _path = value.partition("=")
        if key:
            keys.add(key)
    return keys


def option_overrides(values: list[str]) -> dict[str, bool]:
    overrides: dict[str, bool] = {}
    for value in values:
        key, separator, raw = value.partition("=")
        if not key or separator != "=":
            raise ValueError("--option must be in KEY=VALUE form")
        overrides[key] = raw.strip().lower() in {"1", "true", "yes", "y", "on"}
    return overrides


def compile_from_args(args: argparse.Namespace):
    config = default_config()
    recipe_dir = resolve_recipe(config, args.recipe or str(Path.cwd()))
    return config, compile_recipe(
        recipe_dir,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        local_keys=local_keys(args.local),
        include_dirs=config.include_dirs,
        option_overrides=option_overrides(args.option),
    )


def cmd_generate(args: argparse.Namespace) -> int:
    config, compiled = compile_from_args(args)
    output_root = args.output_root or config.output_root
    build_dir, dockerfile_path = write_build_files(config.repo_root, compiled, output_root, recreate=args.recreate)
    print(f"Dockerfile generated: {dockerfile_path}")
    print(f"Build directory: {build_dir}")
    return 0


def cmd_stage(args: argparse.Namespace) -> int:
    config, compiled = compile_from_args(args)
    output_root = args.output_root or config.output_root
    build_dir, dockerfile_path = write_build_files(config.repo_root, compiled, output_root, recreate=args.recreate, stage=True)
    summary = {
        "name": compiled.name,
        "version": compiled.version,
        "architecture": compiled.architecture,
        "build_dir": str(build_dir),
        "dockerfile": str(dockerfile_path),
        "declared_files": sorted(compiled.staging_plan.files),
    }
    print(json.dumps(summary, indent=2))
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    config, compiled = compile_from_args(args)
    date = build_date_for_recipe(config.repo_root, compiled.recipe_dir)
    data = release_data(compiled.name, compiled.version, compiled.recipe, date)
    if args.write:
        path = write_release_file(config.repo_root, compiled.name, compiled.version, data)
        print(f"Release file written: {path}")
    else:
        print(json.dumps(data, indent=2))
    return 0


def build_inputs(
    compiled,
    build_dir: Path,
    dockerfile_path: Path,
    local_args: list[str] | None = None,
) -> BuildInputs:
    return BuildInputs(
        name=compiled.name,
        version=compiled.version,
        tag=compiled.tag,
        architecture=compiled.architecture,
        build_dir=build_dir,
        dockerfile_path=dockerfile_path,
        local_contexts=local_contexts(local_args or []),
    )


def cmd_build(args: argparse.Namespace) -> int:
    config, compiled = compile_from_args(args)
    output_root = args.output_root or config.output_root
    build_dir, dockerfile_path = write_build_files(
        config.repo_root,
        compiled,
        output_root,
        recreate=args.recreate,
        stage=True,
        download=not args.dry_run,
    )
    adapter = BuildKitAdapter() if args.method == "buildkit" else DockerAdapter()
    if args.method == "buildkit":
        command = adapter.run(
            build_inputs(compiled, build_dir, dockerfile_path, args.local),
            build_dir / f"{compiled.name}_{compiled.version}.docker.tar",
            dry_run=args.dry_run,
        )
    else:
        command = adapter.run(build_inputs(compiled, build_dir, dockerfile_path, args.local), dry_run=args.dry_run)
    print(" ".join(str(part) for part in command))
    if getattr(args, "generate_release", False) and not args.dry_run:
        date = build_date_for_recipe(config.repo_root, compiled.recipe_dir)
        path = write_release_file(
            config.repo_root,
            compiled.name,
            compiled.version,
            release_data(compiled.name, compiled.version, compiled.recipe, date),
        )
        print(f"Release file written: {path}")
    return 0


def cmd_test(args: argparse.Namespace) -> int:
    config, compiled = compile_from_args(args)
    output_root = args.output_root or config.output_root
    build_dir, dockerfile_path = write_build_files(
        config.repo_root,
        compiled,
        output_root,
        recreate=args.recreate,
        stage=True,
        download=args.build and not args.dry_run,
    )
    if args.build:
        DockerAdapter().run(build_inputs(compiled, build_dir, dockerfile_path, args.local), dry_run=args.dry_run)
    command = ContainerTesterAdapter().run(
        TestRequest(tag=compiled.tag, architecture=compiled.architecture, offline_mode=args.offline_mode),
        dry_run=args.dry_run,
    )
    print(" ".join(str(part) for part in command))
    return 0


def cmd_make(args: argparse.Namespace) -> int:
    config, compiled = compile_from_args(args)
    output_root = args.output_root or config.output_root
    build_dir, dockerfile_path = write_build_files(
        config.repo_root,
        compiled,
        output_root,
        recreate=args.recreate,
        stage=True,
        download=not args.dry_run,
    )
    inputs = build_inputs(compiled, build_dir, dockerfile_path, args.local)
    archive = build_dir / f"{compiled.name}_{compiled.version}.docker.tar"
    BuildKitAdapter().run(inputs, archive, dry_run=args.dry_run)
    sif_path = config.repo_root / "sifs" / f"{compiled.name}_{compiled.version}.sif"
    command = SifAdapter().run(archive, sif_path, dry_run=args.dry_run)
    print(" ".join(str(part) for part in command))
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    config = default_config()
    recipe_dir = config.repo_root / "recipes" / args.name
    recipe_dir.mkdir(parents=True, exist_ok=True)
    recipe_file = recipe_dir / "build.yaml"
    if recipe_file.exists():
        raise FileExistsError(f"recipe already exists: {recipe_file}")
    recipe_file.write_text(
        """name: {name}
version: {version}

architectures:
  - x86_64

copyright:
  - license: TODO
    url: TODO

build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives:
    - file:
        name: hello.txt
        contents: Hello, world!
    - run:
        - cat {{{{ get_file("hello.txt") }}}}
    - deploy:
        bins:
          - TODO

readme: TODO
""".format(name=args.name, version=args.version)
    )
    print(f"Recipe created: {recipe_file}")
    return 0


def cmd_cache(args: argparse.Namespace) -> int:
    config = default_config()
    cache_root = config.repo_root / "httpcache"
    if args.all:
        shutil.rmtree(cache_root, ignore_errors=True)
        print(f"Removed cache directory: {cache_root}")
        return 0
    if args.temp_files:
        count = 0
        for path in cache_root.rglob("*.tmp") if cache_root.exists() else []:
            path.unlink()
            count += 1
        print(f"Removed {count} temporary cache files")
        return 0
    print(cache_root)
    return 0


def cmd_login(args: argparse.Namespace) -> int:
    cmd_build(args)
    if args.dry_run:
        return 0
    config, compiled = compile_from_args(args)
    command = ["docker", "run", "--rm", "-it"]
    if args.offline_mode:
        command.extend(["--network", "none"])
    command.append(compiled.tag)
    print(" ".join(command))
    import subprocess

    subprocess.check_call(command)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NeuroContainers builder")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Generate a Dockerfile")
    add_common_recipe_args(generate)
    generate.set_defaults(func=cmd_generate)

    stage = subparsers.add_parser("stage", help="Generate a Dockerfile and stage files")
    add_common_recipe_args(stage)
    stage.set_defaults(func=cmd_stage)

    release = subparsers.add_parser("release", help="Generate release JSON")
    add_common_recipe_args(release)
    release.add_argument("--write", action="store_true", help="Write into releases/")
    release.set_defaults(func=cmd_release)

    build = subparsers.add_parser("build", help="Stage and build a recipe")
    add_common_recipe_args(build)
    build.add_argument("--method", choices=["docker", "buildkit"], default="docker")
    build.add_argument("--dry-run", action="store_true", help="Print the build command without executing it")
    build.add_argument("--generate-release", action="store_true", help="Write release metadata after a successful build")
    build.set_defaults(func=cmd_build)

    test = subparsers.add_parser("test", help="Run a built-container smoke test")
    add_common_recipe_args(test)
    test.add_argument("--build", action="store_true", help="Build before testing")
    test.add_argument("--dry-run", action="store_true", help="Print the test command without executing it")
    test.add_argument("--offline-mode", action="store_true")
    test.set_defaults(func=cmd_test)

    make = subparsers.add_parser("make", help="Build a Docker archive and convert it to SIF")
    add_common_recipe_args(make)
    make.add_argument("--dry-run", action="store_true", help="Print commands without executing them")
    make.set_defaults(func=cmd_make)

    init = subparsers.add_parser("init", help="Create a new recipe skeleton")
    init.add_argument("name")
    init.add_argument("version")
    init.set_defaults(func=cmd_init)

    cache = subparsers.add_parser("cache", help="Inspect or clean build cache files")
    cache.add_argument("--temp-files", action="store_true")
    cache.add_argument("--all", action="store_true")
    cache.set_defaults(func=cmd_cache)

    login = subparsers.add_parser("login", help="Build and open an interactive shell")
    add_common_recipe_args(login)
    login.add_argument("--method", choices=["docker", "buildkit"], default="docker")
    login.add_argument("--dry-run", action="store_true", help="Print the build command without executing it")
    login.add_argument("--offline-mode", action="store_true")
    login.add_argument("--generate-release", action="store_true", help="Write release metadata after a successful build")
    login.set_defaults(func=cmd_login)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())


def _run_with_default_command(command: str, argv: list[str] | None = None) -> int:
    return main([command, *(argv if argv is not None else sys.argv[1:])])


def sf_generate_main() -> None:
    raise SystemExit(_run_with_default_command("generate"))


def sf_build_main() -> None:
    raise SystemExit(_run_with_default_command("build"))


def sf_login_main() -> None:
    raise SystemExit(_run_with_default_command("login"))


def sf_test_main() -> None:
    raise SystemExit(_run_with_default_command("test"))


def sf_make_main() -> None:
    raise SystemExit(_run_with_default_command("make"))


def sf_init_main() -> None:
    raise SystemExit(_run_with_default_command("init"))


def sf_cache_main() -> None:
    raise SystemExit(_run_with_default_command("cache"))


def sf_test_remote_main() -> None:
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
    outcome = runner.run(
        TestRequest(
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
    )
    print(f"\nTest results written to {outcome.results_path}")
    raise SystemExit(0 if outcome.status == "passed" else 1)
