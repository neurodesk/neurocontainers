"""Resolve which release artifact a fulltest suite should run against.

`recipes/<name>/fulltest.yaml` used to hardcode a dated SIF filename such as
``tool_1.2.3_20250101.simg``, which had to be refreshed by hand after every
rebuild. ``releases/<recipe>/<version>.json`` already records the authoritative
build date, so it is the source of truth and ``container:`` is optional.

The contract implemented here:

* No ``container:`` key — the artifact is derived from release metadata. This is
  the form every recipe should use.
* ``container:`` plus ``pin_container: true`` — an explicit historical pin. The
  named file must exist; nothing is derived and nothing falls back.
* ``container:`` without ``pin_container`` — rejected when it disagrees with
  release metadata, so a stale reference is reported instead of being silently
  overridden or silently "fixed" by a wildcard lookup.

This module is imported by ``builder/run_tests.py``, which runs as a standalone
``uv run`` script, so it must stay on the standard library.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Tuple

BUILD_DATE_PATTERN = re.compile(r"^\d{8}$")
CONTAINER_SUFFIXES = (".simg", ".sif")

# Tokens recipes have used to mean "fill this in later". They are not artifacts.
PLACEHOLDER_TOKENS = ("REFERENCE", "PLACEHOLDER", "TODO", "TBD", "UNKNOWN", "LATEST")


class ReleaseArtifactError(RuntimeError):
    """Raised when a fulltest artifact reference cannot be honoured."""


@dataclass(frozen=True)
class ReleaseArtifact:
    """The published artifact described by a release metadata file."""

    recipe: str
    version: str
    build_date: str
    filename: str
    release_file: Path


@dataclass(frozen=True)
class ContainerResolution:
    """Outcome of resolving a fulltest suite's container reference."""

    path: Optional[Path] = None
    reference: str = ""
    source: str = "unresolved"
    notes: Tuple[str, ...] = field(default_factory=tuple)
    error: Optional[str] = None


def normalise_image_basename(image: Optional[str]) -> Optional[str]:
    """Return a bare SIF basename from a release ``image`` value or URL."""
    if not image:
        return None

    image_text = str(image).strip()
    parsed = urllib.parse.urlparse(image_text)
    image_path = parsed.path if parsed.scheme and parsed.path else image_text
    basename = image_path.replace("\\", "/").rsplit("/", 1)[-1]
    for suffix in CONTAINER_SUFFIXES:
        if basename.endswith(suffix):
            basename = basename[: -len(suffix)]
            break
    basename = basename.strip()
    if not basename or basename in {".", ".."}:
        return None
    return basename


def release_filename(image_basename: str, build_date: str) -> str:
    """Return the published SIF filename for an image basename and build date."""
    if image_basename.endswith(f"_{build_date}"):
        return f"{image_basename}.simg"
    return f"{image_basename}_{build_date}.simg"


def is_placeholder_reference(reference: Optional[str]) -> bool:
    """Report whether a ``container:`` value is a placeholder rather than a file."""
    if not reference:
        return True

    text = str(reference).strip()
    if not text:
        return True
    if "*" in text or "?" in text or "[" in text:
        return True
    if "${" in text or text.startswith("$"):
        return True

    stem = text
    for suffix in CONTAINER_SUFFIXES:
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    parts = {part.upper() for part in stem.split("_")}
    return any(token in parts for token in PLACEHOLDER_TOKENS)


def read_release_metadata(release_file: Path) -> Tuple[str, Optional[str]]:
    """Return ``(build_date, image_basename)`` from a release metadata file."""
    try:
        data = json.loads(Path(release_file).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseArtifactError(
            f"Unable to read release metadata {release_file}: {exc}"
        ) from exc

    apps = data.get("apps") or {}
    if not isinstance(apps, dict) or not apps:
        raise ReleaseArtifactError(f"No app entry in release metadata: {release_file}")

    first_app = next(iter(apps.values()))
    if isinstance(first_app, dict):
        raw_build_date = first_app.get("version", "")
        image = first_app.get("image")
    else:
        raw_build_date = first_app
        image = None

    # Some older metadata stores the build date as a JSON number.
    if isinstance(raw_build_date, float) and raw_build_date.is_integer():
        raw_build_date = int(raw_build_date)
    build_date = str(raw_build_date).strip()
    if not build_date:
        raise ReleaseArtifactError(f"Build date missing in release metadata: {release_file}")
    if not BUILD_DATE_PATTERN.match(build_date):
        # Anything else would silently compose an artifact name that cannot exist.
        raise ReleaseArtifactError(
            f"Build date {build_date!r} in {release_file} is not a YYYYMMDD date. "
            "Release files are generated by the build automation; regenerate rather "
            "than editing them by hand."
        )

    return build_date, normalise_image_basename(image)


def find_latest_release_file(
    release_dir: str | Path,
) -> Tuple[Optional[Path], Optional[str], Optional[str]]:
    """Select the most recent release metadata file for a recipe.

    Returns the path, release version and build date (if available).
    """
    release_path = Path(release_dir)
    if not release_path.is_dir():
        return None, None, None

    latest_path: Optional[Path] = None
    latest_build_date = ""
    latest_version = ""

    for entry in sorted(release_path.iterdir()):
        if entry.suffix != ".json":
            continue

        candidate_version = entry.stem
        try:
            build_date, _ = read_release_metadata(entry)
        except ReleaseArtifactError:
            continue

        if latest_path is None:
            latest_path = entry
            latest_build_date = build_date
            latest_version = candidate_version
            continue

        if build_date and (not latest_build_date or build_date > latest_build_date):
            latest_path = entry
            latest_build_date = build_date
            latest_version = candidate_version
        elif build_date == latest_build_date and candidate_version > latest_version:
            latest_path = entry
            latest_version = candidate_version

    if latest_path is None:
        return None, None, None

    return latest_path, latest_version, latest_build_date or None


def resolve_release_artifact(
    recipe: str,
    version: Optional[str],
    releases_dir: str | Path,
) -> Optional[ReleaseArtifact]:
    """Derive the artifact ``releases/<recipe>/<version>.json`` describes.

    The match is exact. A fulltest describes the container the recipe builds
    now, so an older release of the same recipe is never an acceptable stand-in
    — returning ``None`` sends the caller to the locally built SIF instead.
    """
    resolved_version = str(version or "").strip()
    if "${" in resolved_version or resolved_version.startswith("$"):
        raise ReleaseArtifactError(
            f"fulltest version for {recipe} is an unexpanded template: {resolved_version}. "
            "Declare the variable it refers to as a top-level key."
        )
    if not resolved_version:
        return None

    candidate = Path(releases_dir) / recipe / f"{resolved_version}.json"
    if not candidate.is_file():
        # Normal between a version bump and that version's first build.
        return None

    build_date, image_basename = read_release_metadata(candidate)
    basename = image_basename or f"{recipe}_{resolved_version}"
    return ReleaseArtifact(
        recipe=recipe,
        version=resolved_version,
        build_date=build_date,
        filename=release_filename(basename, build_date),
        release_file=candidate,
    )


def _container_candidates(containers_dir: Path, prefix: str) -> list[Path]:
    matches: list[Path] = []
    for suffix in CONTAINER_SUFFIXES:
        matches.extend(containers_dir.glob(f"{prefix}*{suffix}"))
    return sorted(set(matches))


def _newest_by_build_date(matches: Iterable[Path]) -> Optional[Path]:
    """Pick the match with the highest build date, if every match carries one."""
    dated: list[tuple[str, Path]] = []
    for match in matches:
        stem = match.stem
        build_date = stem.rsplit("_", 1)[-1]
        if not BUILD_DATE_PATTERN.match(build_date):
            return None
        dated.append((build_date, match))
    if not dated:
        return None
    return max(dated, key=lambda item: item[0])[1]


def locate_container(
    containers_dir: Path,
    *,
    filename: str,
    recipe: str = "",
    versions: Iterable[str] = (),
    allow_search: bool = True,
) -> Tuple[Optional[Path], Tuple[str, ...], Optional[str]]:
    """Find ``filename`` in ``containers_dir``, searching only when allowed.

    Any search is scoped to ``<recipe>_<version>``, so it can pick up a locally
    built SIF or a different build date of the same release but can never cross
    into a different version. A lookup that would have to choose between
    versions is reported as ambiguous rather than guessed at.
    """
    if not containers_dir.is_dir():
        return None, (), f"Containers directory not found: {containers_dir}"

    if filename:
        exact = containers_dir / filename
        if exact.is_file():
            return exact, (), None

    if not allow_search:
        return None, (), f"Container not found: {containers_dir / filename}"

    scoped_versions = [version for version in dict.fromkeys(versions) if version]
    prefixes = [f"{recipe}_{version}" for version in scoped_versions] if recipe else []
    if recipe and not prefixes:
        prefixes = [f"{recipe}_"]

    for prefix in prefixes:
        matches = _container_candidates(containers_dir, prefix)
        if not matches:
            continue
        if len(matches) == 1:
            chosen = matches[0]
        else:
            chosen = _newest_by_build_date(matches)
            if chosen is None:
                listing = ", ".join(match.name for match in matches)
                return None, (), (
                    f"Ambiguous container lookup for {recipe} in {containers_dir}: "
                    f"{listing}. Pin the artifact with 'container:' plus "
                    f"'pin_container: true', or remove the extra files."
                )
        note = (
            f"Expected artifact {filename} was not present; using {chosen.name} "
            f"from {containers_dir}."
            if filename
            else f"Using {chosen.name} from {containers_dir}."
        )
        return chosen, (note,), None

    target = filename or f"{prefixes[0] if prefixes else recipe}*.simg"
    return None, (), f"Container not found: {containers_dir / target}"


def resolve_suite_container(
    *,
    recipe: str,
    version: Optional[str],
    declared: Optional[str],
    pinned: bool,
    containers_dir: Path,
    releases_dir: Optional[str | Path],
    override: Optional[str | Path] = None,
) -> ContainerResolution:
    """Resolve the container a fulltest suite should run against.

    ``override`` is the artifact a caller already downloaded or built, and always
    wins — CI resolves the release itself and passes the resulting file through.
    """
    if override:
        # Absolute, because the caller's cwd and the test runner's cwd are not
        # the same directory: run_tests.py invokes the container runtime with
        # cwd set to the suite work dir, so a relative override resolves against
        # that instead and the runtime is handed a doubled path.
        override_path = Path(override).resolve()
        if not override_path.is_file():
            return ContainerResolution(
                reference=str(override_path),
                error=f"Container not found: {override_path}",
            )
        return ContainerResolution(
            path=override_path,
            reference=override_path.name,
            source="override",
        )

    declared_text = str(declared or "").strip()
    declared_is_usable = not is_placeholder_reference(declared_text)
    notes: list[str] = []

    if pinned:
        if not declared_is_usable:
            return ContainerResolution(
                reference=declared_text,
                error=(
                    "pin_container is set but 'container:' is not a concrete artifact "
                    f"name: {declared_text or '(missing)'}"
                ),
            )
        path, _, error = locate_container(
            containers_dir, filename=declared_text, allow_search=False
        )
        return ContainerResolution(
            path=path,
            reference=declared_text,
            source="pin",
            error=error,
        )

    artifact: Optional[ReleaseArtifact] = None
    if releases_dir is not None:
        try:
            artifact = resolve_release_artifact(recipe, version, releases_dir)
        except ReleaseArtifactError as exc:
            return ContainerResolution(reference=declared_text, error=str(exc))

    if artifact is None:
        if releases_dir is not None:
            notes.append(
                f"{recipe} {version or '(unversioned)'} has no release yet; looking for a "
                f"locally built container in {containers_dir}."
            )
        path, search_notes, error = locate_container(
            containers_dir,
            filename=declared_text if declared_is_usable else "",
            recipe=recipe,
            versions=(str(version or ""),),
        )
        return ContainerResolution(
            path=path,
            reference=declared_text or (path.name if path else ""),
            source="local",
            notes=tuple(notes) + search_notes,
            error=error,
        )

    if declared_is_usable and declared_text != artifact.filename:
        return ContainerResolution(
            reference=declared_text,
            error=(
                f"fulltest.yaml pins container '{declared_text}' but {artifact.release_file} "
                f"describes '{artifact.filename}' (build date {artifact.build_date}). "
                "Remove the 'container:' key so the artifact is resolved from release "
                "metadata, or set 'pin_container: true' if this test genuinely needs the "
                "historical image."
            ),
        )

    # Release metadata names the artifact exactly. A locally built SIF for the
    # same version is still acceptable; a different version never is.
    path, search_notes, error = locate_container(
        containers_dir,
        filename=artifact.filename,
        recipe=recipe,
        versions=(artifact.version, str(version or "")),
    )
    if error:
        error = (
            f"{error} (described by {artifact.release_file}, build date "
            f"{artifact.build_date})"
        )
    return ContainerResolution(
        path=path,
        reference=artifact.filename,
        source="release-metadata",
        notes=tuple(notes) + search_notes,
        error=error,
    )
