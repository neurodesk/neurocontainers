import json

import pytest

from tools.merge_domain_catalogs import merge_catalogs


def test_merge_domains_preserves_shared_groups_and_rejects_conflicts(tmp_path):
    (tmp_path / "domains.json").write_text(json.dumps({"domains": {"a": {}, "b": {}}}))
    shared = {"apps": {"tool 1": {"version": "20260101"}}}
    for domain in ("a", "b"):
        (tmp_path / domain).mkdir()
        (tmp_path / domain / "apps.json").write_text(
            json.dumps({"tool": shared, domain: {"apps": {}}})
        )
    assert merge_catalogs(tmp_path) == {
        "tool": shared,
        "a": {"apps": {}},
        "b": {"apps": {}},
    }
    (tmp_path / "b" / "apps.json").write_text(json.dumps({"tool": {"apps": {}}}))
    with pytest.raises(ValueError, match="Conflicting"):
        merge_catalogs(tmp_path)


def test_missing_domain_inventory_aborts(tmp_path):
    (tmp_path / "domains.json").write_text(json.dumps({"domains": {"missing": {}}}))
    with pytest.raises(FileNotFoundError):
        merge_catalogs(tmp_path)
