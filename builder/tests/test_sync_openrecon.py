from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tools import sync_openrecon


def test_sync_openrecon_runs_as_workflow_script(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    script = repo_root / "tools" / "sync_openrecon.py"
    isolated_runner = (
        "import runpy, sys, types; "
        "sys.modules['yaml'] = types.ModuleType('yaml'); "
        "sys.argv = ['sync_openrecon.py', '--help']; "
        f"runpy.run_path({str(script)!r}, run_name='__main__')"
    )

    result = subprocess.run(
        [sys.executable, "-S", "-c", isolated_runner],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


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


def write_release_metadata(
    root: Path, container: str, version: str, build_date: str
) -> None:
    release = root / "releases" / container
    release.mkdir(parents=True, exist_ok=True)
    (release / f"{version}.json").write_text(
        json.dumps(
            {
                "apps": {
                    f"{container} {version}": {
                        "version": build_date,
                        "exec": "",
                        "apptainer_args": [],
                    }
                },
                "categories": ["other"],
            }
        ),
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
    assert "export toolName=demo" in (target / "params.sh").read_text(encoding="utf-8")
    assert "export version=1.2.3" in (target / "params.sh").read_text(encoding="utf-8")
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

    prepared = sync_openrecon.prepare_recipe(source_root, openrecon_root, "demo", "0.2")

    assert prepared is not None
    params = (target / "params.sh").read_text(encoding="utf-8")
    assert "export version=0.2\n" in params
    assert "export openrecon_version=0.2.0\n" in params
    assert "export baseDockerImage=vnmd/${toolName}_${version}\n" in params


@pytest.mark.parametrize(
    ("image", "published_image"),
    [
        (
            "ghcr.io/neurodesk/${toolName}_${version}:20260907",
            "ghcr.io/neurodesk/demo_0.2.0:20260910",
        ),
        (
            "ghcr.io/neurodesk/${toolName}:${version}_20260907",
            "ghcr.io/neurodesk/demo:0.2.0_20260910",
        ),
        (
            "ghcr.io/neurodesk/${toolName}:0.2.0_20260907",
            "ghcr.io/neurodesk/demo:0.2.0_20260910",
        ),
    ],
)
def test_prepare_recipe_points_dated_image_at_the_published_build(
    tmp_path: Path,
    image: str,
    published_image: str,
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    write_release_metadata(source_root, "demo", "0.2.0", "20260910")
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        "#!/bin/bash\n"
        "export toolName=demo\n"
        "export version=0.1.0\n"
        f"export baseDockerImage={image}\n",
        encoding="utf-8",
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "0.2.0"
    )

    assert prepared is not None
    params = (target / "params.sh").read_text(encoding="utf-8")
    resolved = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; printf "%s\\n" "$baseDockerImage"',
            "bash",
            str(target / "params.sh"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert resolved.stdout.strip() == published_image
    assert f"export baseDockerImage={image.replace('20260907', '20260910')}\n" in params
    assert any("20260907" in note and "20260910" in note for note in prepared.notes)


@pytest.mark.parametrize(
    "tag", ["latest", "0.2.0", "${version}", "0.2.0_20260907_extra"]
)
def test_update_params_image_tag_keeps_non_dated_tags(tag: str) -> None:
    params = f"export baseDockerImage=ghcr.io/neurodesk/demo:{tag}\n"

    assert sync_openrecon.dated_image_tag(params) is None
    assert sync_openrecon.update_params_image_tag(params, "20260910") == params


@pytest.mark.parametrize(
    ("image", "expected_image"),
    [
        (
            "registry:5000/neurodesk/demo_20260907",
            "registry:5000/neurodesk/demo_20260907",
        ),
        (
            "registry:5000/neurodesk/20260907",
            "registry:5000/neurodesk/20260907",
        ),
        (
            "registry:5000/neurodesk/demo:20260907",
            "registry:5000/neurodesk/demo:20260910",
        ),
        (
            "registry:5000/neurodesk/demo:${version}_20260907",
            "registry:5000/neurodesk/demo:${version}_20260910",
        ),
    ],
)
def test_update_params_image_tag_distinguishes_registry_ports(
    image: str,
    expected_image: str,
) -> None:
    params = f"export baseDockerImage={image}\n"

    assert sync_openrecon.update_params_image_tag(params, "20260910") == (
        f"export baseDockerImage={expected_image}\n"
    )


def test_prepare_recipe_keeps_an_already_current_image_tag(tmp_path: Path) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    write_release_metadata(source_root, "demo", "0.2.0", "20260910")
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        "#!/bin/bash\n"
        "export version=0.1.0\n"
        "export baseDockerImage=ghcr.io/neurodesk/demo_0.2.0:20260910\n",
        encoding="utf-8",
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "0.2.0"
    )

    assert prepared is not None
    assert "ghcr.io/neurodesk/demo_0.2.0:20260910" in (target / "params.sh").read_text(
        encoding="utf-8"
    )
    assert not any("image tag" in note for note in prepared.notes)


def test_prepare_recipe_reports_an_unresolved_build_date(tmp_path: Path) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        "#!/bin/bash\n"
        "export version=0.1.0\n"
        "export baseDockerImage=ghcr.io/neurodesk/${toolName}_${version}:20260907\n",
        encoding="utf-8",
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "0.2.0"
    )

    assert prepared is not None
    assert "${version}:20260907\n" in (target / "params.sh").read_text(encoding="utf-8")
    assert any("20260907" in note for note in prepared.notes)


def test_prepare_recipe_resolves_the_build_date_of_a_named_variant(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    write_release_metadata(source_root, "demo_gpu", "1.2.3", "20260910")
    target = openrecon_root / "recipes" / "demo_gpu"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        "#!/bin/bash\n"
        "export version=1.2.2\n"
        "export baseDockerImage=ghcr.io/neurodesk/demo_gpu_1.2.2:20260904\n",
        encoding="utf-8",
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "1.2.3", variant="gpu"
    )

    assert prepared is not None
    assert "ghcr.io/neurodesk/demo_gpu_1.2.2:20260910" in (
        target / "params.sh"
    ).read_text(encoding="utf-8")


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
        sync_openrecon.prepare_recipe(source_root, openrecon_root, "demo", "1.2.3")


def test_existing_pull_request_matches_exact_title(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    assert (
        sync_openrecon.existing_pull_request(
            "neurodesk/openrecon", "Update demo OpenRecon metadata to 1.2.3"
        )
        == "https://example.test/pr/1"
    )


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


@pytest.mark.parametrize(
    ("container_version", "scanner_version"),
    [
        ("1.6.0.post1", "1.6.0"),
        ("1.0.0.post1", "1.0.0"),
        ("2.9.post2", "2.9.0"),
        ("2.9", "2.9.0"),
        ("2.10.0", "2.10.0"),
    ],
)
def test_container_versions_project_to_numeric_scanner_versions(
    container_version: str, scanner_version: str
) -> None:
    assert sync_openrecon.openrecon_version(container_version) == scanner_version


@pytest.mark.parametrize(
    "version",
    [
        "2.10.0-build20261005",
        "2.10.0-rc1",
        "2.10.0+build20261005",
        "2.10.0.post1.extra",
        "02.10.0",
        "2.١0.0",
    ],
)
@pytest.mark.parametrize("existing_target", [False, True])
def test_invalid_scanner_version_leaves_target_unchanged(
    tmp_path: Path, version: str, existing_target: bool
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    target = openrecon_root / "recipes" / "demo"
    if existing_target:
        target.mkdir(parents=True)
        (target / "params.sh").write_text("export version=1.0.0\n")
        (target / "OpenReconLabel.json").write_text("original label\n")
    before = {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    with pytest.raises(ValueError):
        sync_openrecon.prepare_recipe(source_root, openrecon_root, "demo", version)
    after = {
        p.relative_to(tmp_path): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    assert after == before
    assert target.exists() == existing_target


def test_post_release_container_version_is_kept_for_docker_operations(
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
        "export version=1.6.0\n"
        "export baseDockerImage=vnmd/${toolName}_${version}:20261005\n",
        encoding="utf-8",
    )

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", "1.6.0.post1"
    )

    assert prepared is not None
    params = (target / "params.sh").read_text(encoding="utf-8")
    assert "export version=1.6.0.post1\n" in params
    assert "export openrecon_version=1.6.0\n" in params
    assert "export baseDockerImage=vnmd/${toolName}_${version}:20261005\n" in params
    resolved = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; printf "%s\\n" "$version" "$openrecon_version" "$baseDockerImage"',
            "bash",
            str(target / "params.sh"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert resolved.stdout.splitlines() == [
        "1.6.0.post1",
        "1.6.0",
        "vnmd/demo_1.6.0.post1:20261005",
    ]


def write_research_source(root: Path) -> Path:
    from workflows.validate_openrecon_labels import SCHEMA_PATH

    write_source_recipe(root)
    recipe = root / "recipes" / "demo"
    label = json.loads(
        (SCHEMA_PATH.parent / "b0map" / "OpenReconLabel.json").read_text()
    )
    label["reconstruction"].update(
        emitter="raw", injector="raw", content_qualification_type="RESEARCH"
    )
    (recipe / "OpenReconLabel.json").write_text(json.dumps(label))
    (recipe / "wip_070_fire_demo.json").write_text(
        '{"parameters":{"config":"demo"},"research":{"enabled":true}}\n'
    )
    return recipe


def test_stock_sync_declines_raw_before_any_external_access(tmp_path, monkeypatch):
    source = tmp_path / "source"
    write_research_source(source)

    def external_access(*args, **kwargs):
        pytest.fail(
            "Stock sync attempted external access for experimental raw metadata"
        )

    monkeypatch.setattr(sync_openrecon, "existing_pull_request", external_access)
    monkeypatch.setattr(sync_openrecon, "run_command", external_access)
    assert sync_openrecon.sync_recipe(source, "demo", "1.0.0", "unused/repo") is None
    target = tmp_path / "target"
    assert sync_openrecon.prepare_recipe(source, target, "demo", "1.0.0") is None
    assert not target.exists()


def test_explicit_research_staging_preserves_raw_label_and_exact_config(tmp_path):
    source = tmp_path / "source"
    recipe = write_research_source(source)
    target = tmp_path / "target"
    prepared = sync_openrecon.prepare_recipe(
        source, target, "demo", "1.0.0", experimental_raw_return=True
    )
    assert prepared is not None
    for filename in ("OpenReconLabel.json", "wip_070_fire_demo.json"):
        assert (target / "recipes" / "demo" / filename).read_bytes() == (
            recipe / filename
        ).read_bytes()


OLD_DIGEST = "sha256:" + "0" * 64
NEW_DIGEST = "sha256:" + "1" * 64


@pytest.mark.parametrize(
    ("params", "version", "expected_scanner"),
    [
        (
            "export version=2.10.0-build20261009\nexport openrecon_version=2.10.3\n",
            "2.10.0",
            "2.10.3",
        ),
        ("export version=2.10.3\n", "2.10.0", "2.10.3"),
        (
            "export version=2.10.0-build20261009\nexport openrecon_version=2.10.3\n",
            "2.11",
            "2.11.0",
        ),
        ("export version=2.10.0\nexport openrecon_version=2.10.3\n", "2.11.0", None),
    ],
)
def test_update_params_version_never_regresses_the_scanner_version(
    params: str, version: str, expected_scanner: str | None
) -> None:
    updated = sync_openrecon.update_params_version(params, version)

    assert f"export version={version}\n" in updated
    if expected_scanner is None:
        assert "openrecon_version" not in updated
    else:
        assert f"export openrecon_version={expected_scanner}\n" in updated
    assert sync_openrecon.scanner_version(updated) == (expected_scanner or version)


@pytest.mark.parametrize(
    ("params", "version", "image", "reference"),
    [
        (
            "export toolName=demo\n"
            "export version=2.10.0-build20261009\n"
            "export openrecon_version=2.10.3\n",
            "2.10.0",
            "ghcr.io/neurodesk/demo_2.10.0:20261009",
            "ghcr.io/neurodesk/demo_2.10.0:20261010",
        ),
        (
            "export toolName=demo\nexport version=2.10.0\n",
            "2.10.0",
            "vnmd/${toolName}_${version}:20261009",
            "vnmd/demo_2.10.0:20261010",
        ),
    ],
)
def test_prepare_recipe_repins_a_digest_to_the_published_build(
    tmp_path: Path, params: str, version: str, image: str, reference: str
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    write_release_metadata(source_root, "demo", version, "20261010")
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    (target / "params.sh").write_text(
        f"#!/bin/bash\n{params}export baseDockerImage={image}@{OLD_DIGEST}\n",
        encoding="utf-8",
    )
    resolved: list[str] = []

    def resolve_digest(image_reference: str) -> str:
        resolved.append(image_reference)
        return NEW_DIGEST

    prepared = sync_openrecon.prepare_recipe(
        source_root, openrecon_root, "demo", version, resolve_digest=resolve_digest
    )

    assert prepared is not None
    assert resolved == [reference]
    updated = (target / "params.sh").read_text(encoding="utf-8")
    assert (
        f"export baseDockerImage={image.replace('20261009', '20261010')}@{NEW_DIGEST}\n"
        in updated
    )
    assert sync_openrecon.scanner_version(updated) == sync_openrecon.scanner_version(
        params
    )
    assert any(NEW_DIGEST in note for note in prepared.notes)


def test_prepare_recipe_fails_rather_than_keep_a_stale_digest(tmp_path: Path) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    write_release_metadata(source_root, "demo", "2.10.0", "20261010")
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    params = (
        "export version=2.10.0\n"
        f"export baseDockerImage=ghcr.io/neurodesk/demo_2.10.0:20261009@{OLD_DIGEST}\n"
    )
    (target / "params.sh").write_text(params, encoding="utf-8")

    def resolve_digest(image_reference: str) -> str:
        raise RuntimeError(f"Cannot resolve the digest of {image_reference}")

    with pytest.raises(RuntimeError, match="demo_2.10.0:20261010"):
        sync_openrecon.prepare_recipe(
            source_root, openrecon_root, "demo", "2.10.0", resolve_digest=resolve_digest
        )
    assert (target / "params.sh").read_text(encoding="utf-8") == params


@pytest.mark.parametrize(
    "image",
    [
        f"ghcr.io/neurodesk/demo@{OLD_DIGEST}",
        f"ghcr.io/neurodesk/demo:${{unknown}}@{OLD_DIGEST}",
    ],
)
def test_pinned_image_reference_requires_a_resolvable_tag(image: str) -> None:
    with pytest.raises(RuntimeError, match="Cannot refresh"):
        sync_openrecon.pinned_image_reference(f"export baseDockerImage={image}\n")


@pytest.mark.parametrize(
    ("reference", "manifest_url"),
    [
        (
            "ghcr.io/neurodesk/demo_2.10.0:20261010",
            "https://ghcr.io/v2/neurodesk/demo_2.10.0/manifests/20261010",
        ),
        (
            "vnmd/demo_2.10.0:20261010",
            "https://registry-1.docker.io/v2/vnmd/demo_2.10.0/manifests/20261010",
        ),
        (
            "ubuntu:24.04",
            "https://registry-1.docker.io/v2/library/ubuntu/manifests/24.04",
        ),
        (
            "index.docker.io/ubuntu:24.04",
            "https://registry-1.docker.io/v2/library/ubuntu/manifests/24.04",
        ),
    ],
)
def test_resolve_image_digest_follows_the_registry_token_challenge(
    monkeypatch: pytest.MonkeyPatch, reference: str, manifest_url: str
) -> None:
    import email.message
    import io
    import urllib.error

    requests: list[tuple[str, str | None]] = []

    class Response(io.BytesIO):
        def __init__(self, body: bytes = b"", headers: dict[str, str] | None = None):
            super().__init__(body)
            self.headers = email.message.Message()
            for key, value in (headers or {}).items():
                self.headers[key] = value

    def urlopen(request, timeout):
        if isinstance(request, str):
            assert request.startswith("https://auth.example/token?")
            assert "scope=repository" in request
            return Response(b'{"token": "anonymous"}')
        requests.append((request.full_url, request.get_header("Authorization")))
        if request.get_header("Authorization") is None:
            challenge = email.message.Message()
            challenge["WWW-Authenticate"] = (
                'Bearer realm="https://auth.example/token",service="registry",'
                'scope="repository:demo:pull"'
            )
            raise urllib.error.HTTPError(request.full_url, 401, "", challenge, None)
        return Response(headers={"Docker-Content-Digest": NEW_DIGEST})

    monkeypatch.setattr(sync_openrecon.REGISTRY_OPENER, "open", urlopen)

    assert sync_openrecon.resolve_image_digest(reference) == NEW_DIGEST
    assert requests == [(manifest_url, None), (manifest_url, "Bearer anonymous")]


def test_prepare_recipe_requires_release_metadata_for_a_digest_pin(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    params = (
        "export version=2.10.0\n"
        f"export baseDockerImage=ghcr.io/neurodesk/demo_2.10.0:20261009@{OLD_DIGEST}\n"
    )
    (target / "params.sh").write_text(params, encoding="utf-8")

    def resolve_digest(image_reference: str) -> str:
        raise AssertionError("a stale tag must not be resolved")

    with pytest.raises(RuntimeError, match="No release metadata"):
        sync_openrecon.prepare_recipe(
            source_root, openrecon_root, "demo", "2.10.0", resolve_digest=resolve_digest
        )
    assert sorted(path.name for path in target.iterdir()) == ["params.sh"]
    assert (target / "params.sh").read_text(encoding="utf-8") == params


@pytest.mark.parametrize(
    "assignment",
    [
        "export baseDockerImage={image} # pinned",
        "export baseDockerImage={image}  ",
        'export baseDockerImage="{image}"',
        "export baseDockerImage='{image}'",
    ],
)
def test_digest_pin_is_refreshed_whatever_the_assignment_form(assignment: str) -> None:
    image = f"ghcr.io/neurodesk/demo_2.10.0:20261009@{OLD_DIGEST}"
    params = assignment.format(image=image) + "\n"

    updated = sync_openrecon.update_params_image_digest(
        sync_openrecon.update_params_image_tag(params, "20261010"), NEW_DIGEST
    )

    assert sync_openrecon.pinned_image_reference(updated) == (
        "ghcr.io/neurodesk/demo_2.10.0:20261010"
    )
    assert updated == params.replace("20261009", "20261010").replace(
        OLD_DIGEST, NEW_DIGEST
    )


@pytest.mark.parametrize(
    "params",
    [
        "export version=1.0.0\nexport version=2.0.0\n",
        "export version=1.0.0\nexport openrecon_version=1.0.0\nopenrecon_version=2.9.0\n",
        "export version=1.0.0\nexport baseDockerImage=a:1\nexport baseDockerImage=b:2\n",
        "export version=1.0.0\nexport baseDockerImage=$(printf demo)\n",
    ],
)
def test_update_params_version_rejects_assignments_it_cannot_rewrite(
    params: str,
) -> None:
    with pytest.raises(RuntimeError):
        sync_openrecon.update_params_version(params, "2.11.0")


@pytest.mark.parametrize(
    "realm", ["http://127.0.0.1/token", "https://:secret@auth.example/token"]
)
def test_resolve_image_digest_refuses_an_unsafe_token_realm(
    monkeypatch: pytest.MonkeyPatch, realm: str
) -> None:
    import email.message
    import urllib.error

    def open_url(request, timeout):
        assert not isinstance(request, str), "token realm must not be requested"
        challenge = email.message.Message()
        challenge["WWW-Authenticate"] = f'Bearer realm="{realm}"'
        raise urllib.error.HTTPError(request.full_url, 401, "", challenge, None)

    monkeypatch.setattr(sync_openrecon.REGISTRY_OPENER, "open", open_url)

    with pytest.raises(RuntimeError, match="Refusing registry token realm"):
        sync_openrecon.resolve_image_digest("ghcr.io/neurodesk/demo:20261010")


def test_registry_redirects_stay_on_https_without_leaking_tokens() -> None:
    import urllib.request

    handler = sync_openrecon.SameOriginHTTPSRedirectHandler()
    request = urllib.request.Request(
        "https://ghcr.io/v2/demo/manifests/1",
        headers={"Authorization": "Bearer anonymous"},
        method="HEAD",
    )

    with pytest.raises(RuntimeError, match="non-HTTPS"):
        handler.redirect_request(
            request, None, 307, "", {}, "http://ghcr.io/v2/demo/manifests/1"
        )
    redirected = handler.redirect_request(
        request, None, 307, "", {}, "https://pkg.example/v2/demo/manifests/1"
    )
    assert redirected.get_header("Authorization") is None


def test_prepare_recipe_rejects_a_dated_tag_hidden_in_a_variable(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    write_release_metadata(source_root, "demo", "2.10.0", "20261010")
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    params = (
        "export toolName=demo\n"
        "export version=2.10.0\n"
        "export build_date=20261009\n"
        "export baseDockerImage=vnmd/${toolName}_${version}:${build_date}"
        f"@{OLD_DIGEST}\n"
    )
    (target / "params.sh").write_text(params, encoding="utf-8")

    def resolve_digest(image_reference: str) -> str:
        raise AssertionError("a stale tag must not be resolved")

    with pytest.raises(RuntimeError, match="write the build date literally"):
        sync_openrecon.prepare_recipe(
            source_root, openrecon_root, "demo", "2.10.0", resolve_digest=resolve_digest
        )
    assert (target / "params.sh").read_text(encoding="utf-8") == params


@pytest.mark.parametrize("code", [301, 302, 307, 308])
def test_registry_redirects_keep_the_head_method(code: int) -> None:
    import urllib.request

    if code == 308 and sys.version_info < (3, 11):
        pytest.skip("urllib follows 308 redirects from Python 3.11")

    request = urllib.request.Request(
        "https://ghcr.io/v2/demo/manifests/1", method="HEAD"
    )
    redirected = sync_openrecon.SameOriginHTTPSRedirectHandler().redirect_request(
        request, None, code, "", {}, "https://ghcr.io/v2/demo/manifests/2"
    )

    assert redirected.get_method() == "HEAD"


@pytest.mark.parametrize("published", [False, True])
def test_prepare_recipe_rejects_a_digest_pin_hidden_in_an_alias(
    tmp_path: Path, published: bool
) -> None:
    source_root = tmp_path / "neurocontainers"
    openrecon_root = tmp_path / "openrecon"
    write_source_recipe(source_root)
    if published:
        write_release_metadata(source_root, "demo", "2.10.0", "20261010")
    target = openrecon_root / "recipes" / "demo"
    target.mkdir(parents=True)
    params = (
        "export version=2.10.0\n"
        f"export pinned_image=vnmd/demo_2.10.0:20261009@{OLD_DIGEST}\n"
        "export baseDockerImage=${pinned_image}\n"
    )
    (target / "params.sh").write_text(params, encoding="utf-8")

    def resolve_digest(image_reference: str) -> str:
        raise AssertionError("a stale tag must not be resolved")

    with pytest.raises(RuntimeError, match="write the pinned reference directly"):
        sync_openrecon.prepare_recipe(
            source_root, openrecon_root, "demo", "2.10.0", resolve_digest=resolve_digest
        )
    assert (target / "params.sh").read_text(encoding="utf-8") == params
