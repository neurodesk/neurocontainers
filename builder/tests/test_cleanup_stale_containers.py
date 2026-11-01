"""Object storage cleanup follows published domains and current releases."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote

import pytest

from tools import cleanup_stale_containers as cleanup


@pytest.fixture
def public_inventory(monkeypatch):
    monkeypatch.setattr(
        cleanup,
        "inspect_published_containers",
        lambda: SimpleNamespace(
            ids=frozenset({"served_1.0_20250101"}), revision="123", root_hash="a" * 40
        ),
    )


@pytest.mark.parametrize("dry_run", [False, True])
def test_cleanup_keeps_served_image_while_replacement_is_pending(
    tmp_path: Path, monkeypatch, capsys, dry_run: bool
) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    domain(
        catalog,
        "neuroimaging",
        ["Example/neuro"],
        {
            "tool": {
                "apps": {"tool 1.0": {"version": "20261030"}},
                "categories": ["statistics"],
            }
        },
    )
    sources = tmp_path / "sources"
    release(sources, "Example/neuro", "tool", "20261030")
    monkeypatch.setattr(
        cleanup,
        "inspect_published_containers",
        lambda: SimpleNamespace(
            ids=frozenset({"tool_1.0_20261005"}),
            revision="70787",
            root_hash="a" * 40,
        ),
    )
    objects = ["tool_1.0_20261005.simg", "unreferenced_1.0_20200101.simg"]
    monkeypatch.setattr(
        cleanup,
        "list_nectar_objects",
        lambda _: [{"Path": key, "ModTime": "2000-01-01T00:00:00Z"} for key in objects],
    )
    monkeypatch.setattr(
        cleanup,
        "list_s3_objects",
        lambda _: [
            {"Key": key, "LastModified": "2000-01-01T00:00:00Z"} for key in objects
        ],
    )
    calls = []
    monkeypatch.setattr(
        cleanup.subprocess, "run", lambda command, **_: calls.append(command)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cleanup",
            "--s3-bucket",
            "neurocontainers",
            "--catalog-root",
            str(catalog),
            "--sources-root",
            str(sources),
            "--retention-days",
            "7",
            "--dry-run" if dry_run else "--apply",
        ],
    )

    cleanup.main()

    if dry_run:
        assert calls == []
        output = capsys.readouterr().out
        assert (
            "Would delete unreferenced Nectar object: nectar:/neurodesk/unreferenced_1.0_20200101.simg"
            in output
        )
        assert (
            "Would delete unreferenced AWS object: s3://neurocontainers/unreferenced_1.0_20200101.simg"
            in output
        )
        assert "tool_1.0_20261005.simg" not in output
        return

    assert calls == [
        ["rclone", "deletefile", "nectar:/neurodesk/unreferenced_1.0_20200101.simg"],
        [
            "aws",
            "s3api",
            "delete-object",
            "--bucket",
            "neurocontainers",
            "--key",
            "unreferenced_1.0_20200101.simg",
            "--no-cli-pager",
        ],
    ]


def domain(root: Path, name: str, repositories: list[str], catalog: dict) -> None:
    registry_path = root / "domains.json"
    registry = (
        json.loads(registry_path.read_text())
        if registry_path.exists()
        else {"domains": {}}
    )
    registry["domains"][name] = {"source_repositories": repositories}
    registry_path.write_text(json.dumps(registry))
    directory = root / name
    directory.mkdir()
    (directory / "apps.json").write_text(json.dumps(catalog) + "\n")


def release(sources: Path, repository: str, tool: str, date: str) -> None:
    directory = sources / quote(repository, safe="") / "releases" / tool
    directory.mkdir(parents=True)
    (directory / "1.0.json").write_text(
        json.dumps({"apps": {f"{tool} 1.0": {"version": date}}})
    )


@pytest.mark.parametrize("dry_run", [False, True])
def test_cleanup_protects_every_domain_and_source_release(
    tmp_path: Path, monkeypatch, capsys, dry_run: bool, public_inventory
) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    domain(
        catalog,
        "neuroimaging",
        ["Example/neuro"],
        {
            "published": {
                "apps": {"published 1.0": {"version": "20250101"}},
                "categories": ["statistics"],
            }
        },
    )
    domain(
        catalog,
        "microscopy",
        ["Example/micro"],
        {
            "viewer": {
                "apps": {
                    "viewerGUI-viewer 2.0": {
                        "version": "20250104",
                        "image": "viewer_2.0",
                    }
                },
                "categories": ["visualization"],
            }
        },
    )
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
    monkeypatch.setattr(
        cleanup,
        "list_nectar_objects",
        lambda _: [
            {"Path": key, "ModTime": recent if key.startswith("recent") else old}
            for key in objects
            + [
                "promotion-staging/"
                + "a" * 40
                + "/published/published_1.0_20250101.simg",
                "orphan_1.0_20200101.simg/slo/segment",
                "nested/unreferenced_1.0_20200101.simg",
            ]
        ],
    )
    monkeypatch.setattr(
        cleanup,
        "list_s3_objects",
        lambda _: [
            {"Key": key, "LastModified": recent if key.startswith("recent") else old}
            for key in objects
        ],
    )
    calls = []
    monkeypatch.setattr(
        cleanup.subprocess, "run", lambda command, **_: calls.append(command)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cleanup",
            "--s3-bucket",
            "neurocontainers",
            "--catalog-root",
            str(catalog),
            "--sources-root",
            str(sources),
            "--retention-days",
            "7",
            "--dry-run" if dry_run else "--apply",
        ],
    )

    cleanup.main()

    if dry_run:
        assert calls == []
        output = capsys.readouterr().out
        assert (
            "Would delete unreferenced Nectar object: nectar:/neurodesk/stale_1.0_20200101.simg"
            in output
        )
        assert "promotion-staging" not in output
        assert "nested/" not in output
        return

    assert calls == [
        ["rclone", "deletefile", "nectar:/neurodesk/stale_1.0_20200101.simg"],
        [
            "aws",
            "s3api",
            "delete-object",
            "--bucket",
            "neurocontainers",
            "--key",
            "stale_1.0_20200101.simg",
            "--no-cli-pager",
        ],
    ]


@pytest.mark.parametrize("dry_run", [False, True])
def test_unavailable_public_inventory_stops_before_storage_access(
    tmp_path: Path, monkeypatch, dry_run: bool
) -> None:
    domain(
        tmp_path,
        "neuroimaging",
        ["Example/neuro"],
        {
            "tool": {
                "apps": {"tool 1.0": {"version": "20261030"}},
                "categories": ["statistics"],
            }
        },
    )
    monkeypatch.setattr(cleanup, "load_source_release_keys", lambda *_: set())

    def unavailable():
        raise cleanup.PublicationUnavailable("signature verification failed")

    monkeypatch.setattr(cleanup, "inspect_published_containers", unavailable)
    monkeypatch.setattr(
        cleanup, "list_nectar_objects", lambda _: pytest.fail("storage accessed")
    )
    monkeypatch.setattr(
        cleanup, "list_s3_objects", lambda _: pytest.fail("storage accessed")
    )
    monkeypatch.setattr(
        cleanup.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("storage mutated"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cleanup",
            "--s3-bucket",
            "neurocontainers",
            "--catalog-root",
            str(tmp_path),
            "--dry-run" if dry_run else "--apply",
        ],
    )

    with pytest.raises(SystemExit, match="Cannot protect public CVMFS images"):
        cleanup.main()


def test_bad_catalog_stops_before_storage_access(tmp_path: Path, monkeypatch) -> None:
    domain(
        tmp_path,
        "neuroimaging",
        ["Example/neuro"],
        {
            "published": {
                "apps": {"published 1.0": {"version": "20250101", "image": "../bad"}},
                "categories": ["statistics"],
            }
        },
    )
    monkeypatch.setattr(
        cleanup, "list_nectar_objects", lambda _: pytest.fail("storage accessed")
    )
    monkeypatch.setattr(
        cleanup, "list_s3_objects", lambda _: pytest.fail("storage accessed")
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cleanup",
            "--s3-bucket",
            "neurocontainers",
            "--catalog-root",
            str(tmp_path),
            "--apply",
        ],
    )

    with pytest.raises(SystemExit, match="Invalid image"):
        cleanup.main()


def test_gui_only_catalog_without_image_fails_closed(tmp_path: Path) -> None:
    domain(
        tmp_path,
        "neuroimaging",
        ["Example/neuro"],
        {
            "viewer": {
                "apps": {"viewerGUI-viewer 2.0": {"version": "20250104"}},
                "categories": ["visualization"],
            }
        },
    )

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
