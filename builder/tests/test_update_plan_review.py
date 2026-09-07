"""Independent regression checks for composite update integration."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from builder.update_observations import SourceObservation
from builder.update_plan import plan_sources, validate_target_bindings
from builder import check_version
from tools import one_pr_release


def write_recipe(tmp_path: Path, recipe: dict, fulltest: dict) -> Path:
    root = tmp_path / recipe["name"]
    root.mkdir()
    path = root / "build.yaml"
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    (root / "fulltest.yaml").write_text(
        yaml.safe_dump(fulltest, sort_keys=False)
    )
    return path


def tagged_recipe() -> dict:
    return {
        "name": "demo",
        "version": "2.0.0.post2",
        "variables": {"image_tag": "release-v2.0.0-r4"},
        "auto_update": {
            "method": "sources",
            "sources": [
                {
                    "id": "image",
                    "method": "dockerhub",
                    "repo": "example/demo",
                    "version_regex": (
                        r"release-v(?P<version>\d+\.\d+\.\d+)-r\d+"
                    ),
                    "target": {"variable": "image_tag", "value": "tag"},
                }
            ],
        },
        "build": {
            "base-image": "example/demo:{{ context.image_tag }}",
            "directives": [],
        },
    }


def artifact_recipe() -> dict:
    return {
        "name": "demo",
        "version": "2.0.0.post2",
        "variables": {"runtime_version": "2.0.0"},
        "auto_update": {
            "method": "sources",
            "sources": [
                {
                    "id": "binary",
                    "method": "artifact_listing",
                    "url": "https://downloads.example/list",
                    "download_base": "https://downloads.example/",
                    "version_regex": r"tool-(?P<version>\d+\.\d+\.\d+)\.tgz",
                    "target": {
                        "file": "binary",
                        "variables": {"runtime_version": "version"},
                    },
                }
            ],
        },
        "files": [
            {
                "name": "binary",
                "url": "https://downloads.example/tool-2.0.0.tgz",
                "sha256": "a" * 64,
            }
        ],
        "build": {
            "base-image": "ubuntu:24.04",
            "directives": [
                {
                    "run": [
                        "tar -xf {{ get_file('binary') }} "
                        "-C /opt/tool-{{ context.runtime_version }}"
                    ]
                }
            ],
        },
    }


def test_raw_image_tags_cannot_downgrade_a_source_plan(tmp_path: Path) -> None:
    path = write_recipe(
        tmp_path,
        tagged_recipe(),
        {"name": "demo", "version": "2.0.0.post2", "tests": []},
    )
    older = SourceObservation(
        "1.9.0",
        "https://hub.docker.com/r/example/demo/tags",
        version="1.9.0",
        tag="release-v1.9.0-r9",
    )

    assert plan_sources(path, observations={"image": older}) is None


def test_artifact_metadata_version_cannot_downgrade_bundle(tmp_path: Path) -> None:
    recipe = artifact_recipe()
    path = write_recipe(
        tmp_path,
        recipe,
        {
            "name": "demo",
            "version": "2.0.0.post2",
            "runtime_version": "2.0.0",
            "tests": [],
        },
    )
    older = SourceObservation(
        "https://downloads.example/tool-1.9.0.tgz",
        "https://downloads.example/list",
        version="1.9.0",
        tag="tool-1.9.0.tgz",
        metadata={"version": "1.9.0", "sha256": "b" * 64},
    )

    assert plan_sources(path, observations={"binary": older}) is None


def test_apt_version_cannot_downgrade_a_source_plan(tmp_path: Path) -> None:
    recipe = {
        "name": "demo",
        "version": "1.0.0",
        "variables": {"apt_version": "2:1.0-1"},
        "auto_update": {
            "method": "sources",
            "sources": [
                {
                    "id": "package",
                    "method": "apt",
                    "package": "demo",
                    "urls": ["https://deb.example/Packages.gz"],
                    "target": {"variable": "apt_version"},
                }
            ],
        },
        "build": {
            "base-image": "ubuntu:24.04",
            "directives": [
                {"install": ["demo={{ context.apt_version }}"]},
            ],
        },
    }
    path = write_recipe(
        tmp_path,
        recipe,
        {"name": "demo", "version": "1.0.0", "tests": []},
    )
    older = SourceObservation(
        "1:9.9-1",
        "https://deb.example/Packages.gz",
        metadata={"package": "demo"},
    )

    assert plan_sources(path, observations={"package": older}) is None


def test_coupled_metadata_version_cannot_downgrade_a_source_plan(
    tmp_path: Path,
) -> None:
    recipe = {
        "name": "demo",
        "version": "1.0.0",
        "variables": {
            "source_commit": "a" * 40,
            "software_version": "2.0.0",
        },
        "auto_update": {
            "method": "sources",
            "sources": [
                {
                    "id": "source",
                    "method": "github_commit",
                    "repo": "example/demo",
                    "version_file": "version.txt",
                    "target": {
                        "variable": "source_commit",
                        "variables": {"software_version": "version"},
                    },
                }
            ],
        },
        "files": [
            {
                "name": "source",
                "url": (
                    "https://github.com/example/demo/archive/"
                    "{{ context.source_commit }}.tar.gz"
                ),
            }
        ],
        "build": {"base-image": "ubuntu:24.04", "directives": []},
    }
    path = write_recipe(
        tmp_path,
        recipe,
        {
            "name": "demo",
            "version": "1.0.0",
            "software_version": "2.0.0",
            "tests": [],
        },
    )
    older = SourceObservation(
        "b" * 40,
        "https://github.com/example/demo/commit/" + "b" * 40,
        metadata={"version": "1.9.0"},
    )

    assert plan_sources(path, observations={"source": older}) is None


def test_artifact_runtime_metadata_must_match_fulltest_baseline(
    tmp_path: Path,
) -> None:
    recipe = artifact_recipe()
    path = write_recipe(
        tmp_path,
        recipe,
        {
            "name": "demo",
            "version": "2.0.0.post2",
            "runtime_version": "1.9.0",
            "tests": [],
        },
    )

    with pytest.raises(ValueError, match="fulltest.runtime_version"):
        validate_target_bindings(recipe, path)


def test_copy_destination_is_not_an_installed_source_binding() -> None:
    recipe = tagged_recipe()
    recipe["build"]["base-image"] = "ubuntu:24.04"
    recipe["build"]["directives"] = [
        {
            "run": [
                "cp /opt/static/tool "
                "/usr/local/lib/tool-{{ context.image_tag }}"
            ]
        }
    ]

    with pytest.raises(ValueError, match="acquisition"):
        validate_target_bindings(recipe)


def test_http_digest_variable_uses_plain_sha256_protocol() -> None:
    recipe = {
        "name": "demo",
        "version": "1.0.0",
        "variables": {"archive_sha256": "sha256:" + "a" * 64},
        "auto_update": {
            "method": "sources",
            "sources": [
                {
                    "id": "archive",
                    "method": "http_digest",
                    "url": "https://downloads.example/archive.tgz",
                    "target": {"variable": "archive_sha256"},
                }
            ],
        },
        "files": [
            {
                "name": "archive",
                "url": "https://downloads.example/archive.tgz",
                "sha256": "{{ context.archive_sha256 }}",
            }
        ],
        "build": {"base-image": "ubuntu:24.04", "directives": []},
    }

    with pytest.raises(ValueError, match="64 hexadecimal"):
        validate_target_bindings(recipe)


def test_shared_macro_is_part_of_candidate_promotion_fingerprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recipe_dir = tmp_path / "recipes" / "demo"
    macro = tmp_path / "macros" / "shared" / "install.yaml"
    recipe_dir.mkdir(parents=True)
    macro.parent.mkdir(parents=True)
    (recipe_dir / "build.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "demo",
                "version": "1.0.0",
                "auto_update": {
                    "method": "sources",
                    "local": ["macros/shared/install.yaml"],
                    "sources": [],
                },
                "build": {
                    "base-image": "ubuntu:24.04",
                    "directives": [
                        {"include": "macros/shared/install.yaml"}
                    ],
                },
            },
            sort_keys=False,
        )
    )
    macro.write_text("- run: ['install v1']\n")
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)
    before = one_pr_release.recipe_fingerprint("demo")

    macro.write_text("- run: ['install v2']\n")

    assert one_pr_release.recipe_fingerprint("demo") != before


def test_moving_source_head_reuses_existing_revision_pr_without_using_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recipes = tmp_path / "recipes"
    for name in ("first", "second"):
        root = recipes / name
        root.mkdir(parents=True)
        (root / "build.yaml").write_text(
            yaml.safe_dump(
                {
                    "name": name,
                    "version": "1.0.0",
                    "variables": {"commit": "a" * 40},
                    "auto_update": {
                        "method": "sources",
                        "sources": [
                            {
                                "id": "source",
                                "method": "github_commit",
                                "repo": f"example/{name}",
                                "target": {"variable": "commit"},
                            }
                        ],
                    },
                    "files": [
                        {
                            "name": "source",
                            "url": (
                                f"https://github.com/example/{name}/archive/"
                                "{{ context.commit }}.tar.gz"
                            ),
                        }
                    ],
                    "build": {
                        "base-image": "ubuntu:24.04",
                        "directives": [],
                    },
                },
                sort_keys=False,
            )
        )
        (root / "fulltest.yaml").write_text(
            yaml.safe_dump(
                {"name": name, "version": "1.0.0", "tests": []},
                sort_keys=False,
            )
        )

    def fake_plan(path: Path, session: object) -> SimpleNamespace:
        name = path.parent.name
        return SimpleNamespace(
            branch=f"auto-update/{name}-1.0.0.post1-new-head-fingerprint",
            next_version="1.0.0.post1",
            fingerprint="fingerprint",
            upstream_urls=(f"https://github.com/example/{name}",),
            changes=("commit changed",),
        )

    opened: list[str] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(check_version, "REPO", "owner/containers")
    monkeypatch.setattr(check_version, "plan_sources", fake_plan)
    listed = Mock(
        return_value={"auto-update/first-1.0.0.post1-older-head-fingerprint"}
    )
    monkeypatch.setattr(check_version, "open_update_branches", listed)
    monkeypatch.setattr(
        check_version,
        "pull_request_state",
        lambda branch: None,
    )
    monkeypatch.setattr(
        check_version,
        "submit_bump",
        lambda path, *args, **kwargs: opened.append(Path(path).parent.name)
        or "opened",
    )
    monkeypatch.setattr(
        check_version.sys,
        "argv",
        [
            "check_version",
            "--dry-run",
            "--max-prs",
            "1",
            "--json",
            "report.json",
        ],
    )

    assert check_version.main() == 0
    assert opened == ["second"]
    listed.assert_called_once_with()
    rows = json.loads((tmp_path / "report.json").read_text())
    assert [row["status"] for row in rows] == ["pr-open", "would-open"]
    assert rows[0]["detail"] == (
        "auto-update/first-1.0.0.post1-older-head-fingerprint"
    )
