from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import yaml

from .fragments import macro_path


def is_shared_path(path: str) -> bool:
    return path.startswith("macros/") or path.startswith("builder/templates/")


def _dynamic(value: str) -> bool:
    return any(marker in value for marker in ("{{", "{%", "{#"))


@dataclass(frozen=True)
class SharedInputs:
    roots: tuple[str, ...]

    @classmethod
    def from_recipe(
        cls,
        recipe: Mapping[str, object] | None,
        read_text: Callable[[str], str | None] | None = None,
    ) -> SharedInputs:
        """Discover fragments statically, optionally reading YAML from a selected tree.

        Never evaluate conditions or Jinja. Dynamic names conservatively depend on
        their entire fragment root. Callers handling Git revisions supply a reader
        for that revision, rather than looking at working-tree fragment contents.
        """
        if recipe is None:
            return cls(())
        paths: set[str] = set()
        policy = recipe.get("auto_update")
        if isinstance(policy, dict):
            for path in policy.get("local", []):
                if (
                    not isinstance(path, str)
                    or not path.startswith("macros/")
                    or ".." in PurePosixPath(path).parts
                ):
                    raise ValueError("shared recipe inputs must be paths under macros/")
                paths.add(path)
        visited: set[str] = set()
        extra: set[str] = set()

        def discover(path: str) -> None:
            extra.add(path)
            if read_text is None or path in visited or _dynamic(path):
                return
            visited.add(path)
            source = read_text(path)
            if source is None:
                return  # Keep deleted/missing paths for consumer selection.
            data = yaml.safe_load(source)
            if isinstance(data, dict):
                walk(data, str(PurePosixPath(path).parent))

        def walk(node: object, parent: str | None = None) -> None:
            if isinstance(node, dict):
                include = node.get("include")
                if isinstance(include, str):
                    path = macro_path(include)
                    paths.add("macros/" if _dynamic(path) else path)
                    if _dynamic(path):
                        paths.add("builder/templates/")
                    if not _dynamic(path):
                        discover(path)
                template = node.get("template")
                if isinstance(template, dict) and isinstance(template.get("name"), str):
                    name = template["name"]
                    if _dynamic(name):
                        paths.update(("builder/templates/", "macros/"))
                    elif name and all(c.isalnum() or c == "_" for c in name):
                        path = f"builder/templates/{name}.yaml"
                        paths.add(path)
                        discover(path)
                    else:
                        raise ValueError(f"invalid builtin template name: {name!r}")
                file = node.get("file")
                if parent is not None and isinstance(file, dict):
                    filename = file.get("filename")
                    if isinstance(filename, str):
                        if _dynamic(filename):
                            extra.add(parent)
                        else:
                            asset_path = PurePosixPath(parent) / filename
                            if asset_path.is_absolute() or ".." in asset_path.parts:
                                raise ValueError(
                                    f"fragment asset escapes its directory: {filename}"
                                )
                            extra.add(asset_path.as_posix())
                for value in node.values():
                    walk(value, parent)
            elif isinstance(node, list):
                for value in node:
                    walk(value, parent)

        walk(recipe.get("build", {}))
        # Preserve historical root overlap byte-for-byte. Only transitive inputs
        # already covered by an explicit root are omitted.
        declared = cls(tuple(sorted(paths)))
        paths.update(path for path in extra if not declared.contains(path))
        return cls(tuple(sorted(paths)))

    def contains(self, path: str) -> bool:
        return any(
            path == root or path.startswith(root.rstrip("/") + "/")
            for root in self.roots
        )

    def files(self, repo_root: Path) -> Iterator[Path]:
        """Expand roots in order, retaining overlap for fingerprint compatibility."""
        for root in self.roots:
            source = repo_root / root
            files = sorted(source.rglob("*")) if source.is_dir() else [source]
            for path in files:
                if path.is_dir():
                    continue
                if not path.resolve().is_relative_to(repo_root.resolve()):
                    raise RuntimeError(
                        f"Shared build input escapes the repository: {path}"
                    )
                yield path


def working_tree_inputs(
    recipe: Mapping[str, object] | None, repo_root: Path
) -> SharedInputs:
    def read(path: str) -> str | None:
        source = repo_root / path
        if not source.resolve().is_relative_to(repo_root.resolve()):
            raise ValueError(f"Shared build input escapes the repository: {path}")
        return source.read_text() if source.is_file() else None

    return SharedInputs.from_recipe(recipe, read)
