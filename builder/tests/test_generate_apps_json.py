from __future__ import annotations

import json

from tools.generate_apps_json import merge_container_releases


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
