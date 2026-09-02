from __future__ import annotations

from pathlib import Path

import pytest

from tools import sync_openrecon


def write_source_recipe(root: Path, recipe: str = "demo") -> None:
    recipe_dir = root / "recipes" / recipe
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "OpenReconLabel.json").write_text(
        '{"general": {"id": "demo"}}\n', encoding="utf-8"
    )
    (recipe_dir / "OpenReconLabel.gpu.json").write_text(
        '{"general": {"id": "demo_gpu"}}\n', encoding="utf-8"
    )
    (recipe_dir / "OpenReconREADME.md").write_text("# Demo\n", encoding="utf-8")
    (recipe_dir / "build.yaml").write_text(
        "name: demo\n"
        "architectures:\n"
        "  - x86_64\n"
        "variants:\n"
        "  gpu:\n"
        "    architecture: x86_64\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    "assignment",
    [
        "export version=0.1.0",
        "version=0.1.0",
        "export VERSION=0.1.0",
        "VERSION=0.1.0",
    ],
)
def test_prepare_recipe_updates_supported_version_assignments(
    tmp_path: Path, assignment: str
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        f"#!/bin/bash\n{assignment}\nexport untouched=yes\n", encoding="utf-8"
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "0.2.0"
    )

    assert prepared is not None
    params = (target / "params.sh").read_text(encoding="utf-8")
    assert assignment.replace("0.1.0", "0.2.0") in params
    assert "export untouched=yes" in params
    assert prepared.paths == (
        "recipes/demo/OpenReconLabel.json",
        "recipes/demo/params.sh",
        "recipes/demo/README.md",
    )
    assert (target / "OpenReconLabel.json").read_text(encoding="utf-8") == (
        '{"general": {"id": "demo"}}\n'
    )
    assert (target / "README.md").read_text(encoding="utf-8") == "# Demo\n"


def test_prepare_recipe_bootstraps_missing_target(tmp_path: Path) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "1.2.3"
    )

    assert prepared is not None
    target = openrecon_root / "recipes" / "demo"
    assert "export toolName=demo" in (target / "params.sh").read_text(
        encoding="utf-8"
    )
    assert "export version=1.2.3" in (target / "params.sh").read_text(
        encoding="utf-8"
    )
    assert prepared.notes[0].startswith("- Create `recipes/demo`")


def test_prepare_recipe_separates_two_part_container_and_openrecon_versions(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        "#!/bin/bash\n"
        "export toolName=demo\n"
        "export version=0.1\n"
        "export baseDockerImage=vnmd/${toolName}_${version}\n",
        encoding="utf-8",
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "0.2"
    )

    assert prepared is not None
    params = (target / "params.sh").read_text(encoding="utf-8")
    assert "export version=0.2\n" in params
    assert "export openrecon_version=0.2.0\n" in params
    assert "export baseDockerImage=vnmd/${toolName}_${version}\n" in params


def test_prepare_recipe_resolves_named_variant_to_concrete_container(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)

    prepared = sync_openrecon.prepare_recipe(
        source_root,
        openrecon_root,
        "demo",
        "1.2.3",
        variant="gpu",
    )

    assert prepared is not None
    target = openrecon_root / "recipes" / "demo_gpu"
    assert (target / "OpenReconLabel.json").read_text(encoding="utf-8") == (
        '{"general": {"id": "demo_gpu"}}\n'
    )
    assert "export toolName=demo_gpu" in (target / "params.sh").read_text(
        encoding="utf-8"
    )
    assert prepared.paths == (
        "recipes/demo_gpu/OpenReconLabel.json",
        "recipes/demo_gpu/params.sh",
        "recipes/demo_gpu/README.md",
    )


def test_prepare_recipe_skips_recipes_without_openrecon_label(tmp_path: Path) -> None:
    source_root = tmp_path / "neurocontainers"
    (source_root / "recipes" / "demo").mkdir(parents=True)

    prepared = sync_openrecon.prepare_recipe(
        source_root, tmp_path / "openrecon", "demo", "1.2.3"
    )

    assert prepared is None


def test_prepare_recipe_rejects_unsupported_params_file(tmp_path: Path) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text("#!/bin/bash\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="no supported version assignment"):
        sync_openrecon.prepare_recipe(
            source_root, openrecon_root, "demo", "1.2.3"
        )


def test_existing_pull_request_matches_exact_title(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sync_openrecon,
        "run_command",
        lambda *args, **kwargs: (
            '[{"title": "Update demo OpenRecon metadata to 1.2.3", '
            '"url": "https://example.test/pr/1"}, '
            '{"title": "Update other OpenRecon metadata to 1.2.3", '
            '"url": "https://example.test/pr/2"}]'
        ),
    )

    assert sync_openrecon.existing_pull_request(
        "neurodesk/openrecon", "Update demo OpenRecon metadata to 1.2.3"
    ) == "https://example.test/pr/1"


def test_unchanged_metadata_dispatches_openrecon_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = tmp_path / "neurocontainers"
    write_source_recipe(source_root)
    commands: list[list[str]] = []

    def fake_run_command(
        command: list[str],
        *,
        cwd: Path | None = None,
        capture_output: bool = False,
    ) -> str:
        commands.append(command)
        if command[:3] == ["gh", "pr", "list"]:
            return "[]"
        if command[:3] == ["gh", "repo", "clone"]:
            openrecon_root = Path(command[-1])
            target = openrecon_root / "recipes" / "demo"
            target.mkdir(parents=True)
            (target / "OpenReconLabel.json").write_text(
                '{"general": {"id": "demo"}}\n', encoding="utf-8"
            )
            (target / "README.md").write_text("# Demo\n", encoding="utf-8")
            (target / "params.sh").write_text(
                "#!/bin/bash\nexport version=1.2.3\n", encoding="utf-8"
            )
            return ""
        if command[:3] == ["git", "status", "--porcelain"]:
            return ""
        return ""

    monkeypatch.setattr(sync_openrecon, "run_command", fake_run_command)

    result = sync_openrecon.sync_recipe(
        source_root=source_root,
        recipe="demo",
        version="1.2.3",
        repository="neurodesk/openrecon",
        dispatch_unchanged=True,
    )

    assert result is None
    assert [
        "gh",
        "workflow",
        "run",
        "build-apps.yml",
        "--repo",
        "neurodesk/openrecon",
        "--ref",
        "main",
        "-f",
        'applications=["demo"]',
    ] in commands
