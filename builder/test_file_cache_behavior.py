#!/usr/bin/env python3
"""
Tests for declared file cache mount behavior.
"""

import os
import sys
import tempfile
from unittest import mock
import yaml

# Add parent directory to path to import builder modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import builder.build as build_module
from builder.build import (
    BuildContext,
    LocalBuildContext,
    NEUROCONTAINER_CACHE_CONTEXT_NAME,
    build_and_run_container,
    generate_from_description,
    get_guest_filename,
    get_repo_path,
)


def test_get_guest_filename_uses_url_basename():
    url = "https://example.com/releases/sample-package_1.2.3_amd64.deb"
    assert (
        get_guest_filename("package_deb", url)
        == "sample-package_1.2.3_amd64.deb"
    )


def test_generated_dockerfile_preserves_downloaded_file_basename():
    recipe = {
        "name": "test-download-name",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [
                {"run": ['ls {{ get_file("package_deb") }}']},
            ],
        },
        "files": [
            {
                "name": "package_deb",
                "url": "https://example.com/releases/sample-package_1.2.3_amd64.deb",
            }
        ],
        "readme": "Test recipe",
        "copyright": [
            {"license": "MIT", "url": "https://example.com/license"},
        ],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        recipe_dir = os.path.join(tmpdir, "recipe")
        os.makedirs(recipe_dir)

        with open(os.path.join(recipe_dir, "build.yaml"), "w") as f:
            yaml.safe_dump(recipe, f, sort_keys=False)

        downloaded_file = os.path.join(tmpdir, "downloaded-cache-entry")
        with open(downloaded_file, "wb") as f:
            f.write(b"deb payload")

        persistent_cache = os.path.join(tmpdir, "persistent-cache")
        with (
            mock.patch.object(
                build_module,
                "download_with_cache",
                lambda *args, **kwargs: downloaded_file,
            ),
            mock.patch.object(
                build_module,
                "get_cache_dir",
                lambda: persistent_cache,
            ),
        ):
            build_dir = os.path.join(tmpdir, "build")
            ctx = generate_from_description(
                get_repo_path(),
                recipe_dir,
                recipe,
                build_dir,
                architecture="x86_64",
                recreate_output_dir=True,
                check_only=True,
            )

        dockerfile_path = os.path.join(
            build_dir,
            ctx.name,
            ctx.dockerfile_name,
        )
        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()

        assert "sample-package_1.2.3_amd64.deb" in dockerfile_content
        assert "/.neurocontainer-cache/" in dockerfile_content
        assert f"from={NEUROCONTAINER_CACHE_CONTEXT_NAME}" in dockerfile_content
        assert "source=cache/" not in dockerfile_content


def test_context_cache_disambiguates_duplicate_download_basenames():
    with tempfile.TemporaryDirectory() as tmpdir:
        build_dir = os.path.join(tmpdir, "build")
        os.makedirs(build_dir)

        first_source = os.path.join(tmpdir, "first-cache-entry")
        second_source = os.path.join(tmpdir, "second-cache-entry")

        with open(first_source, "wb") as f:
            f.write(b"first")
        with open(second_source, "wb") as f:
            f.write(b"second")

        ctx = BuildContext(
            base_path=tmpdir,
            recipe_path=tmpdir,
            name="test-app",
            version="1.0.0",
            arch="x86_64",
            check_only=True,
        )
        ctx.build_directory = build_dir
        ctx.files = {
            "first": {
                "cached_path": first_source,
                "url": "https://example.com/downloads/shared-name.deb",
                "guest_filename": "shared-name.deb",
            },
            "second": {
                "cached_path": second_source,
                "url": "https://mirror.example.com/downloads/shared-name.deb",
                "guest_filename": "shared-name.deb",
            },
        }

        persistent_cache = os.path.join(tmpdir, "persistent-cache")
        with mock.patch.object(
            build_module,
            "get_cache_dir",
            lambda: persistent_cache,
        ):
            local = LocalBuildContext(ctx, "cache-id")
            first_path = local.get_file("first")
            second_path = local.get_file("second")

        assert first_path.endswith("/shared-name.deb")
        assert second_path.endswith(".deb")
        assert second_path != first_path

        first_cached = os.path.join(
            persistent_cache, "build-context", "cache-id", "shared-name.deb"
        )
        second_cached = os.path.join(
            persistent_cache,
            "build-context",
            "cache-id",
            os.path.basename(second_path),
        )

        with open(first_cached, "rb") as f:
            assert f.read() == b"first"
        with open(second_cached, "rb") as f:
            assert f.read() == b"second"


def test_docker_build_receives_file_cache_named_context():
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile_name = "Dockerfile"
        dockerfile_path = os.path.join(tmpdir, dockerfile_name)
        with open(dockerfile_path, "w") as f:
            f.write(
                "FROM scratch\n"
                f"RUN --mount=type=bind,from={NEUROCONTAINER_CACHE_CONTEXT_NAME},"
                "source=/cache-id,target=/.neurocontainer-cache/cache-id,readonly true\n"
            )

        calls = []

        def record_check_call(cmd, *args, **kwargs):
            calls.append(cmd)

        persistent_cache = os.path.join(tmpdir, "persistent-cache")
        with (
            mock.patch.object(
                build_module.shutil,
                "which",
                lambda name: f"/usr/bin/{name}",
            ),
            mock.patch.object(build_module.subprocess, "check_call", record_check_call),
            mock.patch.object(build_module, "_docker_buildx_available", lambda: True),
            mock.patch.object(
                build_module,
                "run_container_tester",
                lambda *args, **kwargs: None,
            ),
            mock.patch.object(build_module, "get_cache_dir", lambda: persistent_cache),
        ):
            build_and_run_container(
                dockerfile_name=dockerfile_name,
                name="test-app",
                version="1.0.0",
                tag="test-app:1.0.0",
                architecture="x86_64",
                recipe_path=tmpdir,
                build_directory=tmpdir,
            )

        assert calls
        build_cmd = calls[0]
        assert build_cmd[:3] == ["docker", "buildx", "build"]
        assert "--load" in build_cmd
        assert "--cache-from" in build_cmd
        assert "--cache-to" in build_cmd
        cache_from = build_cmd[build_cmd.index("--cache-from") + 1]
        cache_to = build_cmd[build_cmd.index("--cache-to") + 1]
        assert cache_from.startswith(
            f"type=local,src={os.path.join(persistent_cache, 'docker-buildx')}"
        )
        assert cache_to.startswith(
            f"type=local,dest={os.path.join(persistent_cache, 'docker-buildx')}"
        )
        assert cache_to.endswith(",mode=max")
        assert "--build-context" in build_cmd
        assert (
            f"{NEUROCONTAINER_CACHE_CONTEXT_NAME}="
            f"{os.path.join(persistent_cache, 'build-context')}"
        ) in build_cmd
