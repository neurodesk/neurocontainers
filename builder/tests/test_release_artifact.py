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
from builder.openrecon_updates import OPENRECON_PINS
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


# Containers whose release label is deliberately their own, not the tracked
# software's version: a wrapper or bundle identity, or a base image pinned by
# date. Adding a recipe here is a statement that `ml <recipe>/<version>` is not
# meant to name the installed software's version.
INDEPENDENT_CONTAINER_VERSIONS = {
    "dicomtools": "bundles dcm2niix under its own 1.x line",
    "epirecon": "local reconstruction pipeline, tracks a pypi dependency",
    "esilpd": "local pipeline, tracks an upstream download",
    "fatsegnet": "1.0.gpu names the variant, not the tracked package",
    "flames": "local pipeline, tracks a pypi dependency",
    "lesymap": "tracks a base image pinned by date",
    "oshyx": "tracks a base image pinned by date",
    "vesselboost": "local pipeline, tracks the model release",
}

# Same shape as the amico 2.1.0.post2 mislabel fixed in #3137: a single tracked
# package whose version the label used to follow and no longer does. Relabelling
# a published container is a per-container decision, so the check tolerates them
# by name until each one is settled.
KNOWN_LABEL_DRIFT = {
    "datalad": "labelled 1.3.1, installs the apt package 1.1.5",
    "gimp": "labelled 2.10.18, installs the apt package 2.10.36",
    "lstai": "labelled 1.2.0.post1, installs 1.1",
    "palmettobug": "labelled 0.0.3.post1, installs 0.2.11",
    "vina": "labelled 1.2.3, installs the apt package 1.2.5",
}

SHARED_DEPENDENCY_VARIABLES = frozenset(OPENRECON_PINS)

# A commit, digest or listing pins bytes without naming a software version, so
# a label can never be expected to agree with one.
UNVERSIONED_SOURCE_METHODS = frozenset(
    {
        "artifact_listing",
        "git_commit",
        "github_commit",
        "github_release_asset",
        "http_digest",
        "oci_digest",
    }
)


def tracked_software_source(recipe: dict) -> dict | None:
    """Return the source whose version the container label should name."""
    policy = recipe.get("auto_update")
    if not isinstance(policy, dict) or policy.get("method") != "sources":
        return None
    sources = [source for source in policy.get("sources") or [] if isinstance(source, dict)]
    driver = policy.get("container_version")
    if driver:
        # The recipe says which software it is a distribution of, so a second
        # tracked dependency no longer makes the label unattributable.
        return next((source for source in sources if source.get("id") == driver), None)
    candidates = [
        source
        for source in sources
        if source.get("method") not in UNVERSIONED_SOURCE_METHODS
        and (source.get("target") or {}).get("variable")
        not in SHARED_DEPENDENCY_VARIABLES
    ]
    return candidates[0] if len(candidates) == 1 else None


def tracked_version(recipe: dict, source: dict) -> str:
    """Return the software version the recipe currently records for a source."""
    target = source.get("target") or {}
    variables = recipe.get("variables") or {}
    if "variable" in target:
        return str(variables.get(target["variable"], ""))
    for variable, field in (target.get("variables") or {}).items():
        if field == "version":
            return str(variables.get(variable, ""))
    return ""


def version_cores_agree(label: str, installed: str) -> bool:
    """Report whether one version's numeric core prefixes the other's."""
    label_core = re.findall(r"\d+", re.sub(r"\.post\d+$", "", label))
    installed_core = re.findall(r"\d+", installed)
    if not label_core or not installed_core:
        return False
    return (
        label_core == installed_core[: len(label_core)]
        or installed_core == label_core[: len(installed_core)]
    )


def test_single_package_recipes_label_the_software_version_they_install() -> None:
    """A label that stops following its one package misnames what users load."""
    offenders = []
    for build_yaml in sorted((REPO_ROOT / "recipes").glob("*/build.yaml")):
        recipe_name = build_yaml.parent.name
        if recipe_name in INDEPENDENT_CONTAINER_VERSIONS or recipe_name in KNOWN_LABEL_DRIFT:
            continue
        recipe = yaml.safe_load(build_yaml.read_text(encoding="utf-8")) or {}
        if not isinstance(recipe, dict):
            continue
        source = tracked_software_source(recipe)
        if source is None:
            continue
        installed = tracked_version(recipe, source)
        label = str(recipe.get("version", ""))
        if not installed or not label:
            continue
        if not version_cores_agree(label, installed):
            offenders.append(
                f"{recipe_name}: labelled {label}, installs {installed} "
                f"(source {source.get('id')} via {source.get('method')})"
            )

    assert not offenders, (
        "a recipe that installs one tracked package must label the release with "
        "that package's version, so `ml <recipe>/<version>` names what it ships. "
        "Either track the package directly (auto_update.method: pypi, "
        "github_release, ...) with {{ context.version }} in the install command, "
        "nominate it with auto_update.container_version so the updater keeps the "
        "label on the software, or record the independent identity in "
        "INDEPENDENT_CONTAINER_VERSIONS:\n" + "\n".join(offenders)
    )
