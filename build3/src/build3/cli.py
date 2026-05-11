from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from .adapters import BuildInputs, BuildKitAdapter, DockerAdapter, SifAdapter
from .config import default_config, resolve_recipe
from .dockerfile import render_dockerfile
from .recipe import compile_recipe
from .release import build_date_for_recipe, release_data, write_release_file
from .staging import materialize_plan
from .tester import ContainerTesterAdapter, TestRequest


def dockerfile_name(name: str, version: str) -> str:
    return f"{name}_{version.replace(':', '_')}.Dockerfile".lower()


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
    parser.add_argument("recipe", help="Recipe name or recipe directory")
    parser.add_argument("--architecture", default=None, help="Target architecture")
    parser.add_argument("--ignore-architectures", action="store_true")
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--local", action="append", default=[], help="Named local context KEY=PATH")


def local_keys(values: list[str]) -> set[str]:
    keys: set[str] = set()
    for value in values:
        key, _, _path = value.partition("=")
        if key:
            keys.add(key)
    return keys


def compile_from_args(args: argparse.Namespace):
    config = default_config()
    recipe_dir = resolve_recipe(config, args.recipe)
    return config, compile_recipe(
        recipe_dir,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        local_keys=local_keys(args.local),
        include_dirs=config.include_dirs,
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


def build_inputs(compiled, build_dir: Path, dockerfile_path: Path) -> BuildInputs:
    return BuildInputs(
        name=compiled.name,
        version=compiled.version,
        tag=compiled.tag,
        architecture=compiled.architecture,
        build_dir=build_dir,
        dockerfile_path=dockerfile_path,
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
            build_inputs(compiled, build_dir, dockerfile_path),
            build_dir / f"{compiled.name}_{compiled.version}.docker.tar",
            dry_run=args.dry_run,
        )
    else:
        command = adapter.run(build_inputs(compiled, build_dir, dockerfile_path), dry_run=args.dry_run)
    print(" ".join(str(part) for part in command))
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
        DockerAdapter().run(build_inputs(compiled, build_dir, dockerfile_path), dry_run=args.dry_run)
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
    inputs = build_inputs(compiled, build_dir, dockerfile_path)
    archive = build_dir / f"{compiled.name}_{compiled.version}.docker.tar"
    BuildKitAdapter().run(inputs, archive, dry_run=args.dry_run)
    sif_path = config.repo_root / "sifs" / f"{compiled.name}_{compiled.version}.sif"
    command = SifAdapter().run(archive, sif_path, dry_run=args.dry_run)
    print(" ".join(str(part) for part in command))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="build3 NeuroContainers builder prototype")
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
