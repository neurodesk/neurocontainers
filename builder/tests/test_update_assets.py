from unittest.mock import Mock

import pytest
import yaml

from builder.update_assets import rewrite_release_assets

RECIPE = """name: demo
version: 2.0
files:
  - name: main
    url: "{{ context.old_archive_url }}"
  - name: helper
    url: https://example.org/helper.tar.gz
"""


def asset_response(names):
    session = Mock()
    session.get.return_value.json.return_value = {
        "assets": [
            {
                "name": name,
                "browser_download_url": f"https://github.com/org/demo/releases/download/v2.0/{name}",
            }
            for name in names
        ]
    }
    return session


def test_asset_selection_changes_real_download_and_preserves_version_template():
    config = {"assets": {"main": r"demo-[0-9.]+-ubuntu\.zip"}}
    updated, changes = rewrite_release_assets(
        RECIPE,
        config,
        "org/demo",
        "v2.0",
        "2.0",
        asset_response(["demo-2.0-ubuntu.zip", "demo-2.0-macos.zip"]),
    )
    files = yaml.safe_load(updated)["files"]
    assert (
        files[0]["url"]
        == "https://github.com/org/demo/releases/download/v{{ context.version }}/demo-{{ context.version }}-ubuntu.zip"
    )
    assert files[1]["url"] == "https://example.org/helper.tar.gz"
    assert changes == ["`main` download: `demo-2.0-ubuntu.zip`"]


@pytest.mark.parametrize("names", [[], ["one.zip", "two.zip"]])
def test_missing_or_ambiguous_asset_does_not_propose_arbitrary_download(names):
    with pytest.raises(ValueError, match="expected one asset"):
        rewrite_release_assets(
            RECIPE,
            {"assets": {"main": r".*\.zip"}},
            "org/demo",
            "v2.0",
            "2.0",
            asset_response(names),
        )


def test_cannot_reuse_old_checksum_for_new_asset():
    recipe = RECIPE.replace("  - name: main", "  - sha256: abcdef\n    name: main")
    with pytest.raises(ValueError, match="checksum"):
        rewrite_release_assets(
            recipe,
            {"assets": {"main": r"demo.zip"}},
            "org/demo",
            "v2.0",
            "2.0",
            asset_response(["demo.zip"]),
        )
