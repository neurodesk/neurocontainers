from __future__ import annotations

from builder.image_fingerprint import fingerprint_inspect_data


def _inspect_data(env: list[str], github_sha: str = "abc123") -> dict[str, object]:
    return {
        "Config": {
            "Cmd": ["/bin/bash"],
            "Env": env,
            "Labels": {
                "GITHUB_REPOSITORY": "neurodesk/neurocontainers",
                "GITHUB_SHA": github_sha,
                "recipe": "samri",
            },
        },
        "RootFS": {
            "Type": "layers",
            "Layers": ["sha256:layer"],
        },
    }


def test_image_fingerprint_includes_runtime_config_env() -> None:
    original = _inspect_data(["DEPLOY_PATH=/opt/bru2:/opt/ants"])
    changed = _inspect_data(["DEPLOY_PATH=/opt/bru2:/opt/ants:/opt/miniconda/bin"])

    assert fingerprint_inspect_data(original) != fingerprint_inspect_data(changed)


def test_image_fingerprint_ignores_workflow_identity_labels() -> None:
    original = _inspect_data(["DEPLOY_PATH=/opt/bru2"], github_sha="abc123")
    rebuilt = _inspect_data(["DEPLOY_PATH=/opt/bru2"], github_sha="def456")

    assert fingerprint_inspect_data(original) == fingerprint_inspect_data(rebuilt)
