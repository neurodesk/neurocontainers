from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from builder.release_artifact import (
    is_placeholder_reference,
    resolve_release_artifact,
    resolve_suite_container,
)
from builder.validation import resolve_fulltest_version

REPO_ROOT = Path(__file__).resolve().parents[2]


def write_release(releases_dir: Path, recipe: str, release_version: str, **app: object) -> Path:
    release_file = releases_dir / recipe / f"{release_version}.json"
    release_file.parent.mkdir(parents=True, exist_ok=True)
    release_file.write_text(
        json.dumps(
            {"apps": {f"{recipe} {release_version}": app}, "categories": ["programming"]}
        ),
        encoding="utf-8",
    )
    return release_file


def touch(containers_dir: Path, name: str) -> Path:
    containers_dir.mkdir(parents=True, exist_ok=True)
    path = containers_dir / name
    path.write_text("simg", encoding="utf-8")
    return path


def test_release_artifact_is_derived_from_build_date(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")

    artifact = resolve_release_artifact("tool", "1.2.3", releases)

    assert artifact is not None
    assert artifact.build_date == "20250101"
    assert artifact.filename == "tool_1.2.3_20250101.simg"


def test_release_artifact_prefers_declared_image_basename(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(
        releases,
        "tool",
        "1.2.3-arm64",
        version="20250101",
        image="tool_1.2.3_arm64",
    )

    artifact = resolve_release_artifact("tool", "1.2.3-arm64", releases)

    assert artifact is not None
    assert artifact.filename == "tool_1.2.3_arm64_20250101.simg"


def test_suite_without_container_resolves_from_release_metadata(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    expected = touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.error is None
    assert resolution.path == expected
    assert resolution.source == "release-metadata"


def test_stale_hardcoded_container_reports_the_conflict(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared="tool_1.2.3_20240101.simg",
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.path is None
    assert "tool_1.2.3_20240101.simg" in resolution.error
    assert "tool_1.2.3_20250101.simg" in resolution.error


def test_placeholder_container_is_ignored_in_favour_of_metadata(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    expected = touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared="tool_${version}_REFERENCE.simg",
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.error is None
    assert resolution.path == expected


def test_pinned_container_is_honoured_and_never_falls_back(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    historical = touch(containers, "tool_1.2.3_20240101.simg")
    touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared="tool_1.2.3_20240101.simg",
        pinned=True,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.error is None
    assert resolution.path == historical
    assert resolution.source == "pin"


def test_missing_pinned_container_does_not_substitute_another_build(tmp_path: Path) -> None:
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared="tool_1.2.3_20240101.simg",
        pinned=True,
        containers_dir=containers,
        releases_dir=None,
    )

    assert resolution.path is None
    assert "tool_1.2.3_20240101.simg" in resolution.error


def test_override_wins_over_release_metadata(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20250101.simg")
    candidate = touch(tmp_path / "candidates", "tool-candidate.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
        override=str(candidate),
    )

    assert resolution.error is None
    assert resolution.path == candidate
    assert resolution.source == "override"


def test_override_is_resolved_to_an_absolute_path(tmp_path: Path, monkeypatch) -> None:
    """run_tests.py runs the container runtime from the suite work dir.

    A relative override is resolved against that directory rather than the
    caller's, which hands the runtime a doubled path and fails every test in the
    suite on the container health check.
    """
    candidate = touch(tmp_path / "candidates", "tool-candidate.simg")
    monkeypatch.chdir(tmp_path)

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared=None,
        pinned=False,
        containers_dir=tmp_path / "containers",
        releases_dir=None,
        override="candidates/tool-candidate.simg",
    )

    assert resolution.error is None
    assert resolution.path is not None
    assert resolution.path.is_absolute()
    assert resolution.path == candidate.resolve()


def test_locally_built_sif_satisfies_the_release_artifact(tmp_path: Path) -> None:
    """`sf-make` writes sifs/<name>_<version>.sif, which carries no build date."""
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    sifs = tmp_path / "sifs"
    local = touch(sifs, "tool_1.2.3.sif")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared=None,
        pinned=False,
        containers_dir=sifs,
        releases_dir=releases,
    )

    assert resolution.error is None
    assert resolution.path == local
    assert resolution.notes


def test_release_lookup_refuses_a_different_version(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "2.0.0", version="20250101", exec="")
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20240101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="2.0.0",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.path is None
    assert "tool_2.0.0_20250101.simg" in resolution.error


def test_local_search_stays_within_the_requested_version(tmp_path: Path) -> None:
    """The old lookup truncated at the first underscore and sorted lexically."""
    containers = tmp_path / "containers"
    touch(containers, "tool_1.10.0_20250101.simg")
    expected = touch(containers, "tool_1.9.0_20240101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.9.0",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=None,
    )

    assert resolution.error is None
    assert resolution.path == expected


def test_ambiguous_undated_matches_are_reported_not_guessed(tmp_path: Path) -> None:
    containers = tmp_path / "containers"
    touch(containers, "tool_1.9.0.simg")
    touch(containers, "tool_2.0.0.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=None,
    )

    assert resolution.path is None
    assert "Ambiguous container lookup" in resolution.error
    assert "tool_1.9.0.simg" in resolution.error
    assert "tool_2.0.0.simg" in resolution.error


def test_unexpanded_version_template_is_rejected(tmp_path: Path) -> None:
    """A `${var}` that no top-level key defines must not resolve to some release."""
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="${tool_version}",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.path is None
    assert "unexpanded template" in resolution.error


def test_an_older_release_never_stands_in_for_the_current_version(tmp_path: Path) -> None:
    """A fulltest tests the container the recipe builds now, not its predecessor."""
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="9.9.9",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.path is None
    assert "tool_9.9.9" in resolution.error


def test_unreleased_version_uses_the_locally_built_container(tmp_path: Path) -> None:
    """Between a version bump and its first build, sf-make's SIF is the target."""
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="20250101", exec="")
    sifs = tmp_path / "sifs"
    local = touch(sifs, "tool_9.9.9.sif")

    resolution = resolve_suite_container(
        recipe="tool",
        version="9.9.9",
        declared=None,
        pinned=False,
        containers_dir=sifs,
        releases_dir=releases,
    )

    assert resolution.error is None
    assert resolution.path == local
    assert any("no release yet" in note for note in resolution.notes)


def test_malformed_release_metadata_is_reported(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    write_release(releases, "tool", "1.2.3", version="not-a-build-date", exec="")
    containers = tmp_path / "containers"
    touch(containers, "tool_1.2.3_20250101.simg")

    resolution = resolve_suite_container(
        recipe="tool",
        version="1.2.3",
        declared=None,
        pinned=False,
        containers_dir=containers,
        releases_dir=releases,
    )

    assert resolution.path is None
    assert "Build date missing" in resolution.error or "not-a-build-date" in resolution.error


def recipe_version(build_yaml: Path) -> str:
    match = re.search(r"^version:\s*(.+)$", build_yaml.read_text(encoding="utf-8"), re.M)
    if not match:
        return ""
    return re.sub(r"\s+#.*$", "", match.group(1).strip()).strip().strip("\"'")


def test_repository_fulltests_declare_a_resolvable_name_and_version() -> None:
    """Artifact resolution keys off these two fields, so both must be usable."""
    offenders = []
    for config_path in sorted((REPO_ROOT / "recipes").glob("*/fulltest.yaml")):
        recipe = config_path.parent.name
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        name = str(config.get("name", "") or "")
        version = resolve_fulltest_version(config)

        if name != recipe:
            offenders.append(f"{recipe}: name is {name or '(missing)'}")
        if not version:
            offenders.append(f"{recipe}: version is missing")
        elif "${" in version or version.startswith("$"):
            offenders.append(f"{recipe}: version {version} is an unexpanded template")

    assert not offenders, "\n".join(offenders)


def test_repository_fulltests_target_the_version_their_recipe_builds() -> None:
    """A fulltest lagging its recipe silently tests a container we no longer ship."""
    offenders = []
    for config_path in sorted((REPO_ROOT / "recipes").glob("*/fulltest.yaml")):
        build_yaml = config_path.parent / "build.yaml"
        if not build_yaml.is_file():
            continue
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        declared = resolve_fulltest_version(config)
        built = recipe_version(build_yaml)
        if built and declared != built:
            offenders.append(
                f"{config_path.parent.name}: fulltest tests {declared}, "
                f"build.yaml builds {built}"
            )

    assert not offenders, (
        "fulltest.yaml must declare the version its build.yaml builds, so the suite "
        "runs against the container the recipe produces:\n" + "\n".join(offenders)
    )


def test_repository_fulltests_do_not_hardcode_release_artifacts() -> None:
    """Artifact names come from releases/ metadata, not hand-maintained YAML."""
    offenders = []
    for config_path in sorted((REPO_ROOT / "recipes").glob("*/fulltest.yaml")):
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        declared = str(config.get("container", "") or "")
        if not declared or config.get("pin_container"):
            continue
        if not is_placeholder_reference(declared):
            offenders.append(f"{config_path.relative_to(REPO_ROOT)}: {declared}")

    assert not offenders, (
        "fulltest.yaml must not hardcode a release artifact; remove 'container:' so it "
        "is resolved from releases/<recipe>/<version>.json, or set 'pin_container: true' "
        "when a historical image is genuinely required:\n" + "\n".join(offenders)
    )
