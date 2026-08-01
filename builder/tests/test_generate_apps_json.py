from __future__ import annotations

import json

import pytest

from tools.generate_apps_json import generate_apps_json, merge_container_releases


def test_merge_container_releases_preserves_visibility_flags(tmp_path) -> None:
    release_path = tmp_path / "1.0.0.json"
    release_path.write_text(
        json.dumps(
            {
                "show_in_menu": False,
                "show_in_applist": False,
                "apps": {
                    "tool 1.0.0": {
                        "version": "20260102",
                        "exec": "",
                        "apptainer_args": [],
                    }
                },
                "categories": ["workflows"],
            }
        )
    )

    merged = merge_container_releases("tool", [("1.0.0", str(release_path))])

    assert merged["show_in_menu"] is False
    assert merged["show_in_applist"] is False
    assert merged["apps"]["tool 1.0.0"]["version"] == "20260102"
    assert merged["categories"] == ["workflows"]


def test_generate_apps_json_rejects_duplicate_app_identity(tmp_path) -> None:
    releases_dir = tmp_path / "releases"
    for container, build_date in (
        ("legacy-container", "20240101"),
        ("canonical-container", "20260102"),
    ):
        release_dir = releases_dir / container
        release_dir.mkdir(parents=True)
        (release_dir / "latest.json").write_text(
            json.dumps(
                {
                    "apps": {
                        "rolling-tool latest": {
                            "version": build_date,
                            "exec": "",
                        }
                    },
                    "categories": ["workflows"],
                }
            )
        )

    with pytest.raises(
        ValueError,
        match=(
            "Duplicate app identity 'rolling-tool latest' found in release "
            "containers: canonical-container, legacy-container"
        ),
    ):
        generate_apps_json(str(releases_dir), str(tmp_path / "apps.json"))
