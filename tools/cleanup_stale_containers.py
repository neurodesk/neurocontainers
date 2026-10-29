#!/usr/bin/env python3
"""Remove old SIFs absent from every published domain and source release."""

import argparse
import json
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Set, Tuple
from urllib.parse import quote


def parse_iso8601(timestamp: str) -> Optional[datetime]:
    if not timestamp or not isinstance(timestamp, str):
        return None

    # rclone lsjson may emit nanosecond precision and different UTC offset forms.
    # Python 3.8's datetime.fromisoformat cannot parse >6 fractional digits.
    ts = timestamp.strip()
    if not ts:
        return None

    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"

    match = re.match(
        r"^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
        r"(?P<fraction>\.\d+)?"
        r"(?P<offset>[+-]\d{2}:\d{2}|[+-]\d{4})?$",
        ts,
    )
    if match:
        base = match.group("base")
        fraction = match.group("fraction") or ""
        offset = match.group("offset") or "+00:00"

        if fraction:
            fraction = "." + fraction[1:7].ljust(6, "0")

        if re.fullmatch(r"[+-]\d{4}", offset):
            offset = f"{offset[:3]}:{offset[3:]}"

        normalized = f"{base}{fraction}{offset}"
    else:
        normalized = ts

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_domain_catalog(catalog_root: Path) -> Tuple[Set[str], Set[str]]:
    """Read image identities from each domain's apps.json."""
    try:
        registry = json.loads(
            (catalog_root / "domains.json").read_text(encoding="utf-8")
        )
        domains = registry["domains"]
    except (OSError, ValueError, KeyError) as exc:
        raise SystemExit(f"[ERROR] Invalid catalog domains.json: {exc}") from exc
    if not isinstance(domains, dict) or not domains:
        raise SystemExit("[ERROR] Catalog has no configured domains")
    published: Set[str] = set()
    repositories: Set[str] = set()
    for domain, config in sorted(domains.items()):
        if not re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", domain):
            raise SystemExit(f"[ERROR] Invalid catalog domain: {domain}")
        if not isinstance(config, dict):
            raise SystemExit(f"[ERROR] Invalid configuration for {domain}")
        sources = config.get("source_repositories")
        if not isinstance(sources, list) or not sources:
            raise SystemExit(f"[ERROR] No source repositories for {domain}")
        for repository in sources:
            if not isinstance(repository, str) or not re.fullmatch(
                r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository
            ):
                raise SystemExit(f"[ERROR] Invalid source repository for {domain}")
            repositories.add(repository)
        apps_path = catalog_root / domain / "apps.json"
        try:
            catalog = json.loads(apps_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise SystemExit(
                f"[ERROR] Cannot read {domain} apps catalog: {exc}"
            ) from exc
        if not isinstance(catalog, dict):
            raise SystemExit(f"[ERROR] Invalid {domain} apps catalog")
        domain_images: Set[str] = set()
        for tool, entry in catalog.items():
            if (
                not isinstance(tool, str)
                or not isinstance(entry, dict)
                or not isinstance(entry.get("apps"), dict)
                or not entry["apps"]
                or not isinstance(entry.get("categories"), list)
            ):
                raise SystemExit(f"[ERROR] Invalid app group in {domain} apps catalog")
            primary_images = {}
            app_rows = []
            for name, app in entry["apps"].items():
                if not isinstance(name, str) or not isinstance(app, dict):
                    raise SystemExit(f"[ERROR] Invalid app in {domain} apps catalog")
                builddate = app.get("version")
                if not isinstance(builddate, str) or not re.fullmatch(
                    r"[0-9]{8}", builddate
                ):
                    raise SystemExit(f"[ERROR] Invalid build date for {tool}/{name}")
                try:
                    datetime.strptime(builddate, "%Y%m%d")
                except ValueError as exc:
                    raise SystemExit(
                        f"[ERROR] Invalid build date for {tool}/{name}"
                    ) from exc
                parts = name.rsplit(" ", 1)
                app_rows.append((name, app, parts))
                if len(parts) == 2 and parts[0] == tool:
                    image = app.get("image", f"{tool}_{parts[1]}")
                    if not isinstance(image, str) or not re.fullmatch(
                        r"[A-Za-z0-9][A-Za-z0-9._+-]*", image
                    ):
                        raise SystemExit(f"[ERROR] Invalid image for {tool}/{name}")
                    primary_images[(parts[1], builddate)] = image
            for name, app, parts in app_rows:
                if "image" in app:
                    image = app["image"]
                else:
                    image = (
                        primary_images.get((parts[-1], app["version"]))
                        if len(parts) == 2
                        else None
                    )
                    if image is None:
                        raise SystemExit(f"[ERROR] Image required for {tool}/{name}")
                if not isinstance(image, str) or not re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9._+-]*", image
                ):
                    raise SystemExit(f"[ERROR] Invalid image for {tool}/{name}")
                domain_images.add(f"{image}_{app['version']}.simg")
        published |= domain_images
    if not published:
        raise SystemExit("[ERROR] All published domain inventories are empty")
    return published, repositories


def load_source_release_keys(
    repositories: Set[str], sources_root: Optional[Path] = None
) -> Set[str]:
    """Protect source releases, including builds not yet in the daily catalog."""
    with tempfile.TemporaryDirectory(prefix="catalog-cleanup-sources-") as temporary:
        keys: Set[str] = set()
        for repository in sorted(repositories):
            if sources_root is None:
                source = Path(temporary) / quote(repository, safe="")
                subprocess.run(
                    [
                        "git",
                        "clone",
                        "--quiet",
                        "--depth=1",
                        f"https://github.com/{repository}.git",
                        str(source),
                    ],
                    check=True,
                )
            else:
                source = sources_root / quote(repository, safe="")
            if not source.is_dir():
                raise SystemExit(f"[ERROR] Source checkout not found: {source}")
            keys |= load_release_keys(source / "releases")
        return keys


def load_release_keys(releases_dir: Path) -> Set[str]:
    """Return image keys referenced by neurocontainers release metadata."""
    expected_keys: Set[str] = set()
    if not releases_dir.is_dir():
        return expected_keys

    release_paths = sorted(releases_dir.glob("*/*.json"))
    for release_path in release_paths:
        try:
            release = json.loads(release_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(
                f"[ERROR] Could not read release metadata {release_path}: {exc}"
            ) from exc

        if not isinstance(release, dict):
            raise SystemExit(
                f"[ERROR] Invalid release metadata {release_path}: expected an object"
            )

        apps = release.get("apps")
        if not isinstance(apps, dict) or not apps:
            raise SystemExit(
                f"[ERROR] Invalid release metadata {release_path}: "
                "'apps' must be a non-empty object"
            )

        for app_name, app in apps.items():
            if not isinstance(app, dict):
                raise SystemExit(
                    f"[ERROR] Invalid release metadata {release_path}: "
                    f"app {app_name!r} must be an object"
                )
            builddate = app.get("version")
            if not isinstance(builddate, str) or not re.fullmatch(
                r"[0-9]{8}", builddate
            ):
                raise SystemExit(
                    f"[ERROR] Invalid release metadata {release_path}: "
                    f"app {app_name!r} has invalid build date {builddate!r}"
                )
            try:
                datetime.strptime(builddate, "%Y%m%d")
            except ValueError as exc:
                raise SystemExit(
                    f"[ERROR] Invalid release metadata {release_path}: "
                    f"app {app_name!r} has invalid build date {builddate!r}"
                ) from exc

            if "image" in app:
                image = app["image"]
                if (
                    not isinstance(image, str)
                    or not image
                    or image != image.strip()
                    or "/" in image
                    or "\\" in image
                    or image.lower().endswith(".simg")
                ):
                    raise SystemExit(
                        f"[ERROR] Invalid release metadata {release_path}: "
                        f"app {app_name!r} has invalid image {image!r}"
                    )
            else:
                image = f"{release_path.parent.name}_{release_path.stem}"
            expected_keys.add(f"{image}_{builddate}.simg")

    return expected_keys


def list_nectar_objects(remote_root: str) -> List[dict]:
    try:
        lsjson = subprocess.check_output(
            ["rclone", "lsjson", "--recursive", "--files-only", remote_root],
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            f"[ERROR] Failed to list objects in {remote_root}: {exc}"
        ) from exc
    return json.loads(lsjson)


def list_nectar_objects_optional(remote_root: str) -> Optional[List[dict]]:
    result = subprocess.run(
        ["rclone", "lsjson", "--recursive", "--files-only", remote_root],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        err = (
            result.stderr.strip()
            or result.stdout.strip()
            or f"exit status {result.returncode}"
        )
        print(f"[DEBUG] Unable to list {remote_root}; skipping optional check ({err})")
        return None

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(
            f"[DEBUG] Unable to parse lsjson output for {remote_root}; skipping optional check ({exc})"
        )
        return None


def parse_remote_container(remote_root: str) -> Optional[Tuple[str, str]]:
    if ":" not in remote_root:
        return None

    remote, path = remote_root.split(":", 1)
    path = path.lstrip("/")
    if not remote or not path:
        return None

    container = path.split("/", 1)[0]
    if not container:
        return None

    return remote, container


def check_orphaned_segments(
    remote_root: str, dry_run: bool, protected_keys: Set[str], cutoff: datetime
) -> None:
    parsed = parse_remote_container(remote_root)
    if parsed is None:
        print(
            f"[DEBUG] Could not infer Swift container from {remote_root}; skipping orphaned-segment check"
        )
        return

    remote, container = parsed
    segments_remote = f"{remote}:/{container}_segments/"

    manifests = list_nectar_objects_optional(remote_root)
    if manifests is None:
        return

    legacy_segments = list_nectar_objects_optional(segments_remote)
    if legacy_segments is None:
        print(
            f"[DEBUG] Segment container {segments_remote} is not available; "
            "skipping legacy orphaned-segment check"
        )

    manifest_keys = {obj.get("Path", "") for obj in manifests if obj.get("Path")}
    orphaned_segments = []
    for source, segments in (
        (remote_root, manifests),
        (segments_remote, legacy_segments or []),
    ):
        for obj in segments:
            segment_path = obj.get("Path", "")
            if not segment_path:
                continue
            manifest_candidate = segment_path.split("/", 1)[0]
            if source == remote_root:
                if not segment_path.startswith(manifest_candidate + "/slo/"):
                    continue
                if (
                    not manifest_candidate.endswith(".simg")
                    or manifest_candidate in protected_keys
                ):
                    continue
                # Failed uploads may leave SLO segments in the public container.
                # Protect active images and allow the same retention window as images.
                modified = parse_iso8601(obj.get("ModTime", ""))
                if modified is None or modified >= cutoff:
                    continue
            if manifest_candidate and manifest_candidate not in manifest_keys:
                orphaned_segments.append(f"{source.rstrip('/')}/{segment_path}")

    print(
        f"[DEBUG] Orphaned-segment scan: checked {len(manifests)} public objects "
        f"and {len(legacy_segments or [])} legacy segments"
    )
    if not orphaned_segments:
        print("[DEBUG] Orphaned-segment scan: no orphaned segment objects found")
        return

    print(
        f"[WARNING] Orphaned-segment scan: found {len(orphaned_segments)} orphaned segment object(s)"
    )
    orphaned_segments = sorted(orphaned_segments)

    if dry_run:
        preview_limit = 20
        for segment_path in orphaned_segments[:preview_limit]:
            print(f"[DRY-RUN] Would delete orphaned segment: {segment_path}")
        if len(orphaned_segments) > preview_limit:
            print(
                f"[DRY-RUN] ... and {len(orphaned_segments) - preview_limit} more "
                "orphaned segment object(s)"
            )
        return

    deleted = 0
    failed = 0
    for segment_obj in orphaned_segments:
        try:
            print(f"[DEBUG] Deleting orphaned segment: {segment_obj}")
            subprocess.run(["rclone", "deletefile", segment_obj], check=True)
            deleted += 1
        except subprocess.CalledProcessError as exc:
            failed += 1
            print(f"[WARNING] Failed to delete orphaned segment {segment_obj}: {exc}")

    print(f"[DEBUG] Deleted {deleted} orphaned segment object(s)")
    if failed:
        print(f"[WARNING] Failed deleting {failed} orphaned segment object(s)")


def list_s3_objects(bucket: str) -> List[dict]:
    objects = []
    continuation_token = None

    while True:
        cmd = [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--output",
            "json",
            "--no-cli-pager",
        ]
        if continuation_token:
            cmd.extend(["--continuation-token", continuation_token])

        try:
            page_raw = subprocess.check_output(cmd, text=True)
        except subprocess.CalledProcessError as exc:
            raise SystemExit(
                f"[ERROR] Failed listing AWS objects for bucket {bucket}: {exc}"
            ) from exc

        page = json.loads(page_raw)
        objects.extend(page.get("Contents", []))

        if not page.get("IsTruncated"):
            break

        continuation_token = page.get("NextContinuationToken")
        if not continuation_token:
            break

    return objects


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find stale SIFs in Nectar and S3 using domain catalogs and source releases."
    )
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--remote-root", default="nectar:/neurodesk/", help="Nectar remote path"
    )
    parser.add_argument(
        "--catalog-root",
        type=Path,
        required=True,
        help="Domain catalog checkout; protect every domain and configured source",
    )
    parser.add_argument(
        "--sources-root",
        type=Path,
        help="Local source checkouts for validation instead of cloning",
    )
    parser.add_argument(
        "--retention-days", type=int, default=30, help="Retention period in days"
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Only print what would be deleted (default)",
    )
    parser.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help="Actually delete stale objects",
    )
    args = parser.parse_args()

    if args.retention_days < 0:
        parser.error("--retention-days must be non-negative")

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.retention_days)
    published, repositories = load_domain_catalog(args.catalog_root)
    source_keys = load_source_release_keys(repositories, args.sources_root)
    expected_keys = published | source_keys
    print(
        f"[DEBUG] Protected {len(published)} published images and "
        f"{len(source_keys)} source release images"
    )

    print(f"[DEBUG] Retention window for stale deletions: {args.retention_days} day(s)")
    if args.dry_run:
        print("[DEBUG] Deletion mode: DRY-RUN (no objects will be deleted)")
    else:
        print("[DEBUG] Deletion mode: LIVE")

    nectar_to_delete = []
    for obj in list_nectar_objects(args.remote_root):
        key = obj.get("Path", "")
        if not key.endswith(".simg") or key in expected_keys:
            continue

        mod_time = parse_iso8601(obj.get("ModTime", ""))
        if mod_time is None:
            print(
                f"[WARNING] Skipping Nectar deletion for {key}: missing/invalid ModTime"
            )
            continue

        if mod_time >= cutoff:
            print(
                f"[DEBUG] Keeping unreferenced Nectar object younger than {args.retention_days} days: "
                f"{key} (ModTime={mod_time.isoformat()})"
            )
            continue

        nectar_to_delete.append(key)

    for key in sorted(nectar_to_delete):
        remote_obj = args.remote_root.rstrip("/") + "/" + key
        if args.dry_run:
            print(f"[DRY-RUN] Would delete unreferenced Nectar object: {remote_obj}")
        else:
            print(f"[DEBUG] Deleting unreferenced Nectar object: {remote_obj}")
            subprocess.run(["rclone", "deletefile", remote_obj], check=True)

    if args.dry_run:
        print(
            f"[DRY-RUN] Would delete {len(nectar_to_delete)} stale object(s) from Nectar"
        )
    else:
        print(f"[DEBUG] Deleted {len(nectar_to_delete)} stale object(s) from Nectar")

    aws_to_delete = []
    for obj in list_s3_objects(args.s3_bucket):
        key = obj.get("Key", "")
        if not key.endswith(".simg") or key in expected_keys:
            continue

        last_modified = parse_iso8601(obj.get("LastModified", ""))
        if last_modified is None:
            print(
                f"[WARNING] Skipping AWS deletion for {key}: missing/invalid LastModified"
            )
            continue

        if last_modified >= cutoff:
            print(
                f"[DEBUG] Keeping unreferenced AWS object younger than {args.retention_days} days: "
                f"{key} (LastModified={last_modified.isoformat()})"
            )
            continue

        aws_to_delete.append(key)

    for key in sorted(aws_to_delete):
        if args.dry_run:
            print(
                f"[DRY-RUN] Would delete unreferenced AWS object: s3://{args.s3_bucket}/{key}"
            )
        else:
            print(
                f"[DEBUG] Deleting unreferenced AWS object: s3://{args.s3_bucket}/{key}"
            )
            subprocess.run(
                [
                    "aws",
                    "s3api",
                    "delete-object",
                    "--bucket",
                    args.s3_bucket,
                    "--key",
                    key,
                    "--no-cli-pager",
                ],
                check=True,
            )

    if args.dry_run:
        print(
            f"[DRY-RUN] Would delete {len(aws_to_delete)} stale object(s) from AWS S3 bucket {args.s3_bucket}"
        )
    else:
        print(
            f"[DEBUG] Deleted {len(aws_to_delete)} stale object(s) from AWS S3 bucket {args.s3_bucket}"
        )

    check_orphaned_segments(args.remote_root, args.dry_run, expected_keys, cutoff)


if __name__ == "__main__":
    main()
