"""Object storage cleanup follows published domains and current releases."""

import json
import sys
from pathlib import Path
from urllib.parse import quote

import pytest

from tools import cleanup_stale_containers as cleanup


def domain(
    root: Path, name: str, repositories: list[str], catalog: dict
) -> None:
    registry_path = root / "domains.json"
    registry = json.loads(registry_path.read_text()) if registry_path.exists() else {"domains": {}}
    registry["domains"][name] = {"source_repositories": repositories}
    registry_path.write_text(json.dumps(registry))
    directory = root / name
    directory.mkdir()
    (directory / "apps.json").write_text(json.dumps(catalog) + "\n")


def release(sources: Path, repository: str, tool: str, date: str) -> None:
    directory = sources / quote(repository, safe="") / "releases" / tool
    directory.mkdir(parents=True)
    (directory / "1.0.json").write_text(json.dumps({
        "apps": {f"{tool} 1.0": {"version": date}}
    }))


def test_apply_protects_every_domain_and_source_release(tmp_path: Path, monkeypatch) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    domain(catalog, "neuroimaging", ["Example/neuro"], {
        "published": {"apps": {"published 1.0": {"version": "20250101"}},
                      "categories": ["statistics"]}
    })
    domain(catalog, "microscopy", ["Example/micro"], {
        "viewer": {"apps": {"viewerGUI-viewer 2.0": {
            "version": "20250104", "image": "viewer_2.0"
        }}, "categories": ["visualization"]}
    })
    sources = tmp_path / "sources"
    release(sources, "Example/neuro", "neuro", "20250102")
    release(sources, "Example/micro", "micro", "20250103")
    old = "2000-01-01T00:00:00Z"
    recent = "2999-01-01T00:00:00Z"
    objects = [
        "published_1.0_20250101.simg",
        "neuro_1.0_20250102.simg",
        "micro_1.0_20250103.simg",
        "viewer_2.0_20250104.simg",
        "stale_1.0_20200101.simg",
        "recent_1.0_20261010.simg",
    ]
    monkeypatch.setattr(cleanup, "list_nectar_objects", lambda _: [
        {"Path": key, "ModTime": recent if key.startswith("recent") else old}
        for key in objects
    ])
    monkeypatch.setattr(cleanup, "list_s3_objects", lambda _: [
        {"Key": key, "LastModified": recent if key.startswith("recent") else old}
        for key in objects
    ])
    monkeypatch.setattr(cleanup, "check_orphaned_segments", lambda *args: None)
    calls = []
    monkeypatch.setattr(cleanup.subprocess, "run", lambda command, **_: calls.append(command))
    monkeypatch.setattr(sys, "argv", [
        "cleanup", "--s3-bucket", "neurocontainers", "--catalog-root", str(catalog),
        "--sources-root", str(sources), "--retention-days", "7", "--apply",
    ])

    cleanup.main()

    assert calls == [
        ["rclone", "deletefile", "nectar:/neurodesk/stale_1.0_20200101.simg"],
        ["aws", "s3api", "delete-object", "--bucket", "neurocontainers",
         "--key", "stale_1.0_20200101.simg", "--no-cli-pager"],
    ]


def test_bad_catalog_stops_before_storage_access(tmp_path: Path, monkeypatch) -> None:
    domain(tmp_path, "neuroimaging", ["Example/neuro"], {
        "published": {"apps": {"published 1.0": {
            "version": "20250101", "image": "../bad"
        }}, "categories": ["statistics"]}
    })
    monkeypatch.setattr(cleanup, "list_nectar_objects", lambda _: pytest.fail("storage accessed"))
    monkeypatch.setattr(cleanup, "list_s3_objects", lambda _: pytest.fail("storage accessed"))
    monkeypatch.setattr(sys, "argv", [
        "cleanup", "--s3-bucket", "neurocontainers", "--catalog-root", str(tmp_path),
        "--apply",
    ])

    with pytest.raises(SystemExit, match="Invalid image"):
        cleanup.main()


def test_gui_only_catalog_without_image_fails_closed(tmp_path: Path) -> None:
    domain(tmp_path, "neuroimaging", ["Example/neuro"], {
        "viewer": {"apps": {"viewerGUI-viewer 2.0": {"version": "20250104"}},
                   "categories": ["visualization"]}
    })

    with pytest.raises(SystemExit, match="Image required"):
        cleanup.load_domain_catalog(tmp_path)


def test_new_source_without_releases_is_allowed_but_missing_checkout_fails(
    tmp_path: Path,
) -> None:
    sources = tmp_path / "sources"
    empty = sources / quote("Example/empty", safe="")
    empty.mkdir(parents=True)

    assert cleanup.load_source_release_keys({"Example/empty"}, sources) == set()
    with pytest.raises(SystemExit, match="Source checkout not found"):
        cleanup.load_source_release_keys({"Example/missing"}, sources)
