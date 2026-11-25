import pytest
import yaml

from builder.versioning import software_version, validate_container_version
from builder.update_plan import plan_sources
from builder.tests.test_update_plan import make_recipe, observations
from builder import check_version
from builder.template import RenderContext, TemplateRenderer


@pytest.mark.parametrize(
    "upstream,label",
    [
        ("7.1", "7.1.0"),
        ("7.2", "7.2.0"),
        ("9.21.0", "9.21.0"),
        ("7.1.3", "7.1.3"),
        ("6.0.7.22", "6.0.7.22"),
        ("26.0.rc3", "26.0.0.rc3"),
        ("25.1.2.post1", "25.1.2.post1"),
        ("2025b", "2025b"),
        ("23a", "23a"),
        ("20260920", "20260920"),
        ("alpha119", "alpha119"),
        ("11_3_3", "11.3.3"),
    ],
)
def test_software_version_preserves_upstream_components(upstream, label):
    assert software_version(upstream) == label


@pytest.mark.parametrize(
    "debian_version,expected",
    [
        ("2.10.42-1ubuntu2", "2.10.42"),
        ("2.1.0-1~nd120+1", "2.1.0"),
        ("2025.07~dfsg-2", "2025.07.0"),
        ("2.13.2+git20240821+ds-4", "2.13.2"),
        ("2.13.2+ds+git20240821-4", "2.13.2"),
        ("2.13.3~git20240822+ds-4", "2.13.3-git20240822"),
        ("1.0+ds~git20251231-1", "1.0.0-git20251231"),
        ("1:3.4.1+dfsg1-1", "3.4.1"),
        ("1.0+b1", "1.0.0"),
        ("0:2.0-1", "2.0.0"),
    ],
)
def test_apt_container_version_strips_debian_repack_and_git_suffixes(
    debian_version, expected
):
    recipe = {
        "version": expected,
        "variables": {"package_version": debian_version},
        "auto_update": {
            "method": "sources",
            "container_version": "pkg",
            "sources": [
                {
                    "id": "pkg",
                    "method": "apt",
                    "package": "demo",
                    "urls": [
                        "https://archive.ubuntu.com/ubuntu/dists/resolute/universe/binary-amd64/Packages.gz"
                    ],
                    "target": {"variable": "package_version"},
                }
            ],
        },
    }
    validate_container_version(recipe)


def test_manual_container_bump_is_rejected():
    recipe = {
        "version": "9.21.1",
        "variables": {"upstream_version": "9.21.0"},
        "auto_update": {"container_version": {"variable": "upstream_version"}},
    }
    with pytest.raises(ValueError, match="must match software version"):
        validate_container_version(recipe)
    recipe["version"] = "9.21.0"
    validate_container_version(recipe)


def test_drift_does_not_suppress_a_new_upstream_release(tmp_path):
    path = make_recipe(tmp_path)
    recipe = yaml.safe_load(path.read_text())
    recipe["version"] = "8.0.0"
    path.write_text(yaml.safe_dump(recipe))
    plan = plan_sources(path, observations=observations("7.4.2"))
    assert plan.next_version == "7.4.2"
    plan.apply()
    assert plan_sources(path, observations=observations("7.4.2")) is None


def test_dependency_rebuilds_have_distinct_plans_without_changing_software(tmp_path):
    path = make_recipe(tmp_path)
    first = plan_sources(path, observations=observations(helper="b" * 40))
    second = plan_sources(path, observations=observations(helper="c" * 40))
    assert first.next_version == second.next_version == "7.3.0"
    assert first.branch != second.branch
    first.apply()
    assert plan_sources(path, observations=observations(helper="b" * 40)) is None


def test_direct_updates_pad_labels_without_rewriting_upstream_urls(tmp_path):
    path = tmp_path / "build.yaml"
    path.write_text("""name: demo
version: 7.0.0
auto_update:
  method: github_release
  repo: example/demo
files:
  - name: archive
    url: https://example.org/v{{ context.version }}/archive.tar.gz
""")
    path.with_name("fulltest.yaml").write_text(
        'name: demo\nversion: 7.0.0\ntests:\n  - command: demo --version\n    expected_output_contains: "${version}"\n'
    )
    label = check_version.tag_to_recipe_version("v7.1", "7.0.0")
    assert label == "7.1.0"
    updated, _ = check_version.prepare_bump(
        path, "7.0.0", label, "example/demo", "v7.1"
    )
    recipe = yaml.safe_load(updated)
    context = RenderContext("demo", label, "x86_64", values=recipe["variables"])
    assert (
        TemplateRenderer().render_string(recipe["files"][0]["url"], context)
        == "https://example.org/v7.1/archive.tar.gz"
    )
    _, suite, _ = check_version.prepare_fulltest_bump(path, label, updated)
    assert yaml.safe_load(suite)["upstream_version"] == "7.1"
    assert yaml.safe_load(suite)["version"] == label


def test_missing_primary_version_policy_is_rejected():
    from builder.update_plan import validate_sources_config

    with pytest.raises(ValueError, match="requires container_version"):
        validate_sources_config({"method": "sources", "local": []})


def test_padded_label_does_not_overwrite_indirect_software_assertion(tmp_path):
    path = make_recipe(tmp_path)
    suite_path = path.with_name("fulltest.yaml")
    suite = yaml.safe_load(suite_path.read_text())
    suite["version"] = "${tool_version}"
    suite_path.write_text(yaml.safe_dump(suite))
    plan = plan_sources(path, observations=observations("7.4"))
    plan.apply()
    suite = yaml.safe_load(suite_path.read_text())
    assert suite["version"] == "7.4.0"
    assert suite["tool_version"] == "7.4"


def test_short_version_with_a_raw_tag_binding_remains_auditable(tmp_path):
    from builder.audit_updates import validate_update_policy

    path = tmp_path / "build.yaml"
    path.write_text("""name: demo
version: 7.0.0
variables: {upstream_tag: v7.0.0}
auto_update: {method: github_tags, repo: example/demo, tag_variable: upstream_tag}
build:
  directives:
    - run: git clone --branch {{ context.upstream_tag }} https://github.com/example/demo
""")
    updated, _ = check_version.prepare_bump(
        path, "7.0.0", "7.1.0", "example/demo", "v7.1"
    )
    recipe = yaml.safe_load(updated)
    validate_update_policy(recipe)
    assert recipe["variables"]["upstream_tag"] == "v7.1"
    assert recipe["variables"]["upstream_version"] == "7.1"
    assert recipe["version"] == "7.1.0"


def test_debian_upstream_source_version_normalizes_zero_epoch_and_native_binnmu():
    from builder.update_observations import (
        debian_upstream_source_version,
        debian_version_equivalence_key,
    )

    assert debian_upstream_source_version("0:2.0-1") == "2.0"
    assert debian_upstream_source_version("00:2.0-1") == "2.0"
    assert debian_upstream_source_version("2.0-1") == "2.0"
    assert debian_upstream_source_version("1:2.0-1") == "1:2.0"
    assert debian_upstream_source_version("01:2.0-1") == "1:2.0"
    assert debian_upstream_source_version("1.0+b1") == "1.0"
    assert debian_upstream_source_version("1.0+ds+b2") == "1.0"
    assert debian_upstream_source_version("1.0") == "1.0"
    assert debian_version_equivalence_key("2.016.0") == debian_version_equivalence_key(
        "2.16.0"
    )
    assert debian_version_equivalence_key("1a") == debian_version_equivalence_key("1a0")
    assert debian_version_equivalence_key("1.0") != debian_version_equivalence_key(
        "1.0.0"
    )


def test_variable_backed_container_version_enforces_multiarch_apt_peers():
    recipe = {
        "version": "2.0.0",
        "variables": {"amd64": "2.0.0", "arm64": "1.0.0"},
        "auto_update": {
            "method": "sources",
            "container_version": {"variable": "amd64"},
            "sources": [
                {
                    "id": "pkg-amd64",
                    "method": "apt",
                    "package": "demo",
                    "urls": [
                        "https://archive.ubuntu.com/ubuntu/dists/resolute/universe/binary-amd64/Packages.gz"
                    ],
                    "target": {"variables": {"amd64": "version"}},
                },
                {
                    "id": "pkg-arm64",
                    "method": "apt",
                    "package": "demo",
                    "urls": [
                        "https://ports.ubuntu.com/ubuntu-ports/dists/resolute/universe/binary-arm64/Packages.gz"
                    ],
                    "target": {"variables": {"arm64": "version"}},
                },
            ],
        },
    }
    with pytest.raises(ValueError, match="multi-architecture APT sources"):
        validate_container_version(recipe)
    recipe["variables"]["arm64"] = "2.0.0"
    validate_container_version(recipe)
