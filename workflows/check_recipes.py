"""Validate recipes and generate every declared build without downloading files."""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

from builder.audit_updates import validate_update_policy
from builder.cli import write_build_files
from builder.recipe import compile_recipe
from builder.release_plan import recipe_names_from_paths
from builder.shared_inputs import SharedInputs
from builder.validation import validate_recipe_file
from builder.variants import concrete_variant_specs
from workflows.validate_openrecon_labels import SCHEMA_PATH, find_labels, validate_label


@dataclass(frozen=True)
class Failure:
    recipe: str
    check: str
    message: str


@dataclass
class CheckReport:
    recipes: list[str]
    generated: list[str] = field(default_factory=list)
    checked_labels: list[str] = field(default_factory=list)
    failures: list[Failure] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "passed": self.passed}


def read_recipe(path: Path) -> dict[str, Any]:
    recipe = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(recipe, dict):
        raise ValueError("recipe must be a YAML mapping")
    return recipe


def select_recipes(
    root: Path,
    *,
    changed_paths: list[str] | None = None,
    requested: list[str] | None = None,
) -> list[str]:
    """Select surviving recipes and consumers of changed shared inputs."""
    if not (root / "recipes").is_dir():
        raise ValueError(f"Recipe directory not found: {root / 'recipes'}")
    available = {
        path.parent.name: path for path in (root / "recipes").glob("*/build.yaml")
    }
    if requested is not None:
        missing = sorted(set(requested) - available.keys())
        if missing:
            raise ValueError(f"Unknown recipes: {', '.join(missing)}")
        return sorted(set(requested))
    if changed_paths is None:
        return sorted(available)
    selected = set(recipe_names_from_paths(changed_paths)) & available.keys()
    shared_changes = [path for path in changed_paths if path.startswith("macros/")]
    if shared_changes:
        for name, path in available.items():
            try:
                inputs = SharedInputs.from_recipe(read_recipe(path))
                if any(inputs.contains(changed) for changed in shared_changes):
                    selected.add(name)
            except Exception:
                # An unreadable declaration cannot prove it is unaffected. Check
                # it alongside consumers and report the actual error below.
                selected.add(name)
    return sorted(selected)


def git_output(root: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True
    )
    if process.returncode:
        raise ValueError(process.stderr.strip() or "git command failed")
    return process.stdout


def git_changes(root: Path, base: str, head: str) -> tuple[list[str], set[str]]:
    """Compare branch changes from the merge base against the current checkout."""
    commits = [
        git_output(
            root, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"
        ).strip()
        for ref in (base, head)
    ]
    commits[0] = git_output(root, "merge-base", *commits).strip()
    changed = git_output(root, "diff", "--name-only", "-z", *commits, "--").split("\0")

    def recipe_names(commit: str) -> set[str]:
        paths = git_output(
            root, "ls-tree", "-r", "--name-only", "-z", commit, "--", "recipes/"
        ).split("\0")
        return {
            Path(path).parent.name
            for path in paths
            if len(Path(path).parts) == 3 and Path(path).name == "build.yaml"
        }

    new_recipes = recipe_names(commits[1]) - recipe_names(commits[0])
    new_recipes = {
        name
        for name in new_recipes
        if (root / "recipes" / name / "build.yaml").is_file()
    }
    return [path for path in changed if path], new_recipes


def check_recipes(
    root: Path,
    names: list[str],
    *,
    new_recipes: set[str] | None = None,
    output_root: Path | None = None,
    all_labels: bool = False,
) -> CheckReport:
    report = CheckReport(recipes=names)
    output_root = output_root or root / "build"
    for label_path in find_labels(root / "recipes"):
        if not all_labels and label_path.parent.name not in names:
            continue
        report.checked_labels.append(str(label_path))
        try:
            errors = validate_label(
                label_path,
                root / "recipes" / SCHEMA_PATH.name,
                experimental_raw_return=True,
            )
            if errors:
                report.failures.append(
                    Failure(
                        label_path.parent.name,
                        "openrecon-label",
                        f"{label_path.name}: {'; '.join(errors)}",
                    )
                )
        except Exception as exc:
            report.failures.append(
                Failure(
                    label_path.parent.name,
                    "openrecon-label",
                    str(exc),
                )
            )
    for name in sorted(new_recipes or set()):
        if not (root / "recipes" / name / "fulltest.yaml").is_file():
            report.failures.append(
                Failure(
                    name,
                    "new-recipe-fulltest",
                    "new recipes require a focused fulltest.yaml runtime suite",
                )
            )
    for name in names:
        path = root / "recipes" / name / "build.yaml"
        try:
            recipe = read_recipe(path)
        except Exception as exc:
            report.failures.append(Failure(name, "load", str(exc)))
            continue
        for label, check in (
            ("validation", lambda: validate_recipe_file(str(path))),
            ("update-policy", lambda: validate_update_policy(recipe, recipe_path=path)),
        ):
            try:
                check()
            except Exception as exc:
                report.failures.append(Failure(name, label, str(exc)))
        try:
            specs = concrete_variant_specs(recipe)
        except Exception as exc:
            report.failures.append(Failure(name, "variants", str(exc)))
            continue
        for spec in specs:
            target = f"{spec['variant'] or 'default'}/{spec['architecture']}"
            try:
                compiled = compile_recipe(
                    path.parent,
                    architecture=spec["architecture"],
                    variant=spec["variant"],
                    include_dirs=(root,),
                    resolve_readme_url=False,
                )
                _, dockerfile = write_build_files(
                    root, compiled, output_root, recreate=True
                )
                report.generated.append(str(dockerfile))
            except Exception as exc:
                report.failures.append(Failure(name, f"generation:{target}", str(exc)))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--recipes", help="Comma-separated recipe directory names")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Check all recipes while retaining --base policy",
    )
    parser.add_argument(
        "--base", help="Base commit for changed selection and new recipe policy"
    )
    parser.add_argument(
        "--head",
        default="HEAD",
        help="Head commit for selecting changes; checks use the current checkout",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Generated Dockerfile directory, default ROOT/build",
    )
    parser.add_argument("--json", type=Path, help="Write the complete check report")
    args = parser.parse_args(argv)
    if args.all and args.recipes is not None:
        parser.error("--all and --recipes cannot be combined")
    try:
        changed, new_recipes = (
            git_changes(args.root, args.base, args.head) if args.base else (None, set())
        )
        requested = (
            [name.strip() for name in args.recipes.split(",") if name.strip()]
            if args.recipes is not None
            else None
        )
        names = select_recipes(
            args.root, changed_paths=None if args.all else changed, requested=requested
        )
        names = sorted(set(names) | new_recipes)
        report = check_recipes(
            args.root.resolve(),
            names,
            new_recipes=new_recipes,
            output_root=args.output_root,
            all_labels=(
                args.all
                or (changed is None and requested is None)
                or f"recipes/{SCHEMA_PATH.name}" in (changed or [])
                or "workflows/validate_openrecon_labels.py" in (changed or [])
            ),
        )
    except (ValueError, OSError) as exc:
        parser.exit(2, f"Recipe selection failed: {exc}\n")
    for failure in report.failures:
        print(f"FAIL {failure.recipe} [{failure.check}]: {failure.message}")
    print(
        f"Checked {len(report.recipes)} recipes; generated {len(report.generated)} "
        f"Dockerfiles; checked {len(report.checked_labels)} OpenRecon labels; "
        f"{len(report.failures)} failures."
    )
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(report.as_dict(), indent=2) + "\n", encoding="utf-8"
        )
    return int(not report.passed)


if __name__ == "__main__":
    raise SystemExit(main())
