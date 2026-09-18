import copy
import json

import pytest
import yaml

from tools.migrate_container_versions import migrate


@pytest.mark.parametrize("version,openrecon", [
    ("1.2.3.post2", False),
    ("1.2.3.r1", False),
    ("1.2.3", True),
    ("1.2.3.post1", True),
])
def test_migration_preserves_inputs_withdraws_records_and_replays(tmp_path, version, openrecon):
    directory = tmp_path / "recipes" / "demo"
    directory.mkdir(parents=True)
    recipe = {
        "name": "demo",
        "version": version,
        "architectures": ["x86_64", "aarch64"] if openrecon else ["x86_64"],
        "variables": {"upstream_version": "9.8.7"},
        "auto_update": {"method": "sources", "local": []},
        "build": {"directives": [
            {"include": "macros/openrecon/neurodocker.yaml"}
        ] if openrecon else []},
    }
    recipe_path = directory / "build.yaml"
    suite_path = directory / "fulltest.yaml"
    recipe_path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    suite_path.write_text(yaml.safe_dump({"name": "demo", "version": version, "tests": []}))
    release = tmp_path / "releases" / ("demo_arm64" if openrecon else "demo") / f"{version}.json"
    release.parent.mkdir(parents=True)
    release.write_text(json.dumps({"apps": {f"demo {version}": {"version": "20260909"}}}))
    retained = tmp_path / "releases" / "other" / "2.0.0.json"
    retained.parent.mkdir(parents=True)
    retained.write_text('{}\n')
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    preview = migrate(tmp_path)
    assert preview["versions"] == [{"recipe": "demo", "old": version, "new": "1.3.0"}]
    assert {p: p.read_bytes() for p in before} == before
    migrate(tmp_path, apply=True)

    expected = copy.deepcopy(recipe)
    expected.update(version="1.3.0", architectures=["x86_64"])
    assert yaml.safe_load(recipe_path.read_text()) == expected
    assert yaml.safe_load(suite_path.read_text())["version"] == "1.3.0"
    assert not release.exists()
    assert list((tmp_path / "releases").rglob("*.json")) == [retained]
    assert retained.read_bytes() == before[retained]
    assert migrate(tmp_path, apply=True) == {
        "versions": [], "withdrawn_releases": [], "applied": True,
    }
