from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class SharedInputs:
    roots: tuple[str, ...]

    @classmethod
    def from_recipe(cls, recipe: Mapping[str, object] | None) -> SharedInputs:
        """Discover declared paths without reading macros or rendering templates."""
        if recipe is None:
            return cls(())
        paths: set[str] = set()
        policy = recipe.get("auto_update")
        if isinstance(policy, dict):
            paths.update(policy.get("local", []))

        def walk(node: object) -> None:
            if isinstance(node, dict):
                if isinstance(node.get("include"), str):
                    path = node["include"]
                    paths.add(path if path.startswith("macros/") else "macros/" + path)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(recipe.get("build", {}))
        for path in paths:
            if (
                not isinstance(path, str)
                or not path.startswith("macros/")
                or ".." in PurePosixPath(path).parts
            ):
                raise ValueError("shared recipe inputs must be paths under macros/")
        return cls(tuple(sorted(paths)))

    def contains(self, path: str) -> bool:
        return any(
            path == root or path.startswith(root.rstrip("/") + "/")
            for root in self.roots
        )

    def files(self, repo_root: Path) -> Iterator[Path]:
        """Expand each root in order, retaining overlap for fingerprint compatibility."""
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
