from pathlib import Path
from types import SimpleNamespace

import pytest

from builder import oci_labels
from builder.image_fingerprint import fingerprint_inspect_data
from builder.ir import Definition, From
from builder.recipe import compile_recipe


def compiled():
    return SimpleNamespace(
        name="demo_arm64", base_name="demo", version="1.2.3", readme="fallback",
        metadata={"structured_readme": {"description": 'A tool with "quotes"\nand newlines'},
                  "copyright": [{"license": "MIT"}, {"license": "BSD-3-Clause"}]},
        definition=Definition(directives=[From("ubuntu:24.04")]),
    )


def test_labels_identify_recipe_and_do_not_inherit_base_version():
    labels = oci_labels.recipe_labels(compiled(), "20261003", "a" * 40)
    assert labels[oci_labels.PREFIX + "version"] == "1.2.3_20261003"
    assert labels[oci_labels.PREFIX + "base.name"] == "ubuntu:24.04"
    assert labels[oci_labels.PREFIX + "title"] == "demo_arm64"
    assert labels[oci_labels.PREFIX + "licenses"] == "MIT AND BSD-3-Clause"
    assert labels[oci_labels.PREFIX + "description"] == 'A tool with "quotes" and newlines'
    assert labels[oci_labels.PREFIX + "created"] == "2026-10-03T00:00:00Z"


def test_missing_license_is_not_misattributed_to_base_image():
    recipe = compiled()
    recipe.metadata = {}
    assert oci_labels.recipe_labels(recipe, "20261003", "a" * 40)[oci_labels.PREFIX + "licenses"] == "NOASSERTION"


@pytest.mark.parametrize("date,revision", [("20260230", "a" * 40), ("20261003", "abc")])
def test_invalid_identity_fails_before_build(date, revision):
    with pytest.raises(ValueError):
        oci_labels.recipe_labels(compiled(), date, revision)


def test_date_only_rebuild_matches_but_software_change_does_not():
    recipe = compiled()
    def fingerprint(date, revision):
        return fingerprint_inspect_data({"Config": {"Labels": oci_labels.recipe_labels(recipe, date, revision)}})
    previous = fingerprint("20261002", "a" * 40)
    assert previous == fingerprint("20261003", "b" * 40)
    recipe.version = "1.2.4"
    assert previous != fingerprint("20261003", "b" * 40)


def test_nul_arguments_preserve_spaces_quotes_and_shell_characters(capfdbinary):
    value = 'a "quote" $(command); spaces'
    oci_labels.emit({oci_labels.PREFIX + "description": value}, "annotations")
    assert capfdbinary.readouterr().out.split(b"\0") == [
        b"--annotation", (oci_labels.PREFIX + "description=" + value).encode(), b"",
    ]


def test_compiler_renders_metadata_from_recipe_variables(tmp_path):
    import yaml
    root = Path(__file__).resolve().parents[2]
    recipe = yaml.safe_load((root / "recipes/niimath/build.yaml").read_text())
    recipe["structured_readme"] = {"description": "Version {{ context.version }}", "example": "niimath"}
    recipe["copyright"] = [{"license": "{{ context.license_id }}"}]
    recipe.setdefault("variables", {})["license_id"] = "MIT"
    (tmp_path / "build.yaml").write_text(yaml.safe_dump(recipe))
    result = compile_recipe(tmp_path, architecture="x86_64")
    assert result.metadata["copyright"] == [{"license": "MIT"}]
    assert result.metadata["structured_readme"]["description"] == f"Version {result.version}"


def test_image_annotations_preserve_the_built_labels(monkeypatch):
    labels = oci_labels.recipe_labels(compiled(), "20261003", "a" * 40)
    class Client:
        def __init__(self, *args, **kwargs):
            pass
        def get_manifest(self, repository, reference):
            return {"config": {"digest": "sha256:config"}}
        def get_config_blob(self, repository, digest):
            return {"config": {"Labels": labels | {"GITHUB_SHA": "abc"}}}
    monkeypatch.setattr(oci_labels, "RegistryClient", Client)
    monkeypatch.setattr(oci_labels, "resolve_credentials", lambda *args: None)
    assert oci_labels.image_labels("example.test/demo:1.2.3", "amd64") == labels
