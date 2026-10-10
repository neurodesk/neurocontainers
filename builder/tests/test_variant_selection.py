from __future__ import annotations

import warnings

import pytest

from builder.variants import select_concrete_variant


@pytest.fixture
def recipe():
    return {
        "name": "tool",
        "architectures": ["x86_64", "aarch64"],
        "variants": {
            "gpu": {
                "architectures": ["x86_64", "aarch64"],
                "options": {"gpu": True},
            },
        },
    }


def select(recipe, *, host="x86_64", platform="Linux", **kwargs):
    return select_concrete_variant(
        recipe, host_architecture=host, host_platform=platform, **kwargs
    )


@pytest.mark.parametrize(
    "selector,host,architecture,expected",
    [
        (None, "AMD64", None, "tool"),
        (None, "arm64", None, "tool_arm64"),
        (None, "arm64", "", "tool_arm64"),
        (None, "aarch64", "amd64", "tool"),
        (None, "x86_64", "ARM64", "tool_arm64"),
        ("gpu", "x86_64", None, "tool_gpu"),
        ("gpu", "aarch64", None, "tool_gpu_arm64"),
        ("arm64", "x86_64", None, "tool_arm64"),
        ("gpu_arm64", "x86_64", None, "tool_gpu_arm64"),
    ],
)
def test_selects_using_selectors_aliases_and_host(
    recipe, selector, host, architecture, expected
):
    spec = select(recipe, host=host, variant=selector, architecture=architecture)
    assert spec["name"] == expected
    assert spec["options"] == ({"gpu": True} if selector and "gpu" in selector else {})


def test_explicit_selector_does_not_require_supported_host(recipe):
    assert select(recipe, host="unknown", variant="arm64")["architecture"] == "aarch64"


@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({"variant": "missing"}, "unknown variant/architecture 'missing'/default"),
        (
            {"variant": "arm64", "architecture": "x86_64"},
            "unknown variant/architecture 'arm64'/x86_64",
        ),
        ({"architecture": "sparc"}, "unsupported architecture: sparc"),
    ],
)
def test_invalid_selection_preserves_error(recipe, kwargs, message):
    with pytest.raises(ValueError) as caught:
        select(recipe, **kwargs)
    assert message in str(caught.value)


def test_variant_architecture_must_be_allowed_by_recipe():
    recipe = {
        "name": "tool",
        "architectures": ["x86_64"],
        "variants": {"gpu": {"architecture": "aarch64"}},
    }
    with pytest.raises(ValueError, match="architecture aarch64 not supported by tool"):
        select(recipe, variant="gpu_arm64")
    assert (
        select(recipe, variant="gpu_arm64", ignore_architecture=True)["name"]
        == "tool_gpu_arm64"
    )


@pytest.mark.parametrize(
    "variant,architecture,host,expected",
    [
        (None, "arm64", "x86_64", "tool_arm64"),
        (None, None, "aarch64", "tool_arm64"),
        ("arm64", None, "x86_64", "tool_arm64"),
        ("gpu_arm64", None, "x86_64", "tool_gpu_arm64"),
    ],
)
def test_ignore_architectures_forces_identity(
    recipe, variant, architecture, host, expected
):
    recipe["architectures"] = ["x86_64"]
    recipe["variants"]["gpu"]["architectures"] = ["x86_64"]
    spec = select(
        recipe,
        variant=variant,
        architecture=architecture,
        host=host,
        ignore_architecture=True,
    )
    assert spec["name"] == expected


def test_forcing_unknown_variant_is_rejected(recipe):
    with pytest.raises(ValueError, match="does not declare variant 'missing'"):
        select(recipe, variant="missing_arm64", ignore_architecture=True)


def test_disabled_default_is_not_substituted_with_named_variant(recipe):
    recipe["build_default"] = False
    with pytest.raises(
        ValueError, match="unknown variant/architecture 'default'/x86_64"
    ):
        select(recipe)
    assert select(recipe, variant="gpu")["name"] == "tool_gpu"


@pytest.mark.parametrize("variant,expected", [(None, "tool"), ("gpu", "tool_gpu")])
def test_mac_arm_falls_back_with_warning(recipe, variant, expected):
    recipe["architectures"] = ["x86_64"]
    recipe["variants"]["gpu"]["architectures"] = ["x86_64"]
    with pytest.warns(UserWarning, match="on macOS; automatically selecting x86_64"):
        spec = select(recipe, host="arm64", platform="Darwin", variant=variant)
    assert spec["name"] == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {"platform": "Linux"},
        {"platform": "Darwin", "architecture": "arm64"},
        {"platform": "Darwin", "variant": "arm64"},
    ],
)
def test_fallback_does_not_override_platform_or_explicit_request(recipe, kwargs):
    recipe["architectures"] = ["x86_64"]
    with warnings.catch_warnings(record=True) as emitted:
        with pytest.raises(ValueError, match="unknown variant/architecture"):
            select(recipe, host="aarch64", **kwargs)
    assert not emitted


def test_mac_ignore_architectures_forces_host_without_fallback(recipe):
    recipe["architectures"] = ["x86_64"]
    with warnings.catch_warnings(record=True) as emitted:
        spec = select(
            recipe, host="aarch64", platform="Darwin", ignore_architecture=True
        )
    assert spec["name"] == "tool_arm64"
    assert not emitted
