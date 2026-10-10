from pathlib import Path

import pytest

from builder.release_plan import plan_recipe_changes
from builder.shared_inputs import SharedInputs


def test_declared_inputs_and_matching_do_not_read_or_render_macros(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("static shared-input discovery accessed the filesystem")

    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(Path, "resolve", forbidden)
    monkeypatch.setattr(Path, "rglob", forbidden)
    recipe = {
        "auto_update": {"local": ["macros/shared", "macros/shared"]},
        "build": {
            "directives": [
                {"include": "shared/tool.yaml", "condition": "False"},
                {"group": [{"include": "macros/{{ context.macro }}.yaml"}]},
                {"file": {"contents": {"include": "nested/tool.yaml"}}},
            ]
        },
    }
    inputs = SharedInputs.from_recipe(recipe)

    assert inputs.roots == (
        "macros/nested/tool.yaml",
        "macros/shared",
        "macros/shared/tool.yaml",
        "macros/{{ context.macro }}.yaml",
    )
    assert inputs.contains("macros/shared/deleted.py")
    assert inputs.contains("macros/shared")
    assert not inputs.contains("macros/shared-other/tool.yaml")
    assert not SharedInputs.from_recipe(None).contains("macros/shared")
    plan = plan_recipe_changes(
        ["macros/shared/deleted.py"],
        {"demo": recipe},
        {"demo": recipe},
    )
    assert plan.candidate_recipes == ["demo"]


@pytest.mark.parametrize("root", ["recipes/demo", "macros/../outside", 1])
def test_invalid_declared_roots_are_rejected(root: object) -> None:
    with pytest.raises(ValueError, match="paths under macros/"):
        SharedInputs.from_recipe({"auto_update": {"local": [root]}})


def test_expansion_preserves_root_order_and_overlapping_files(tmp_path: Path) -> None:
    shared = tmp_path / "macros" / "shared"
    (shared / "subdir").mkdir(parents=True)
    (shared / "z.txt").write_text("last")
    (shared / "subdir" / "b.txt").write_text("nested")
    (shared / "a.txt").write_text("first")
    inputs = SharedInputs.from_recipe(
        {
            "auto_update": {"local": ["macros/shared/z.txt", "macros/shared"]},
        }
    )

    assert [
        path.relative_to(tmp_path).as_posix() for path in inputs.files(tmp_path)
    ] == [
        "macros/shared/a.txt",
        "macros/shared/subdir/b.txt",
        "macros/shared/z.txt",
        "macros/shared/z.txt",
    ]


def test_expansion_rejects_a_file_symlink_outside_the_repository(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    shared = repo / "macros" / "shared"
    shared.mkdir(parents=True)
    outside = tmp_path / "outside.txt"
    outside.write_text("external")
    (shared / "escape.txt").symlink_to(outside)
    inputs = SharedInputs.from_recipe({"auto_update": {"local": ["macros/shared"]}})

    with pytest.raises(RuntimeError, match="escapes the repository"):
        list(inputs.files(repo))
