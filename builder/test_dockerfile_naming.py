#!/usr/bin/env python3
"""
Tests for Dockerfile naming behavior.
"""

import os
import sys
import tempfile

import yaml

# Add parent directory to path to import builder modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from builder.build import generate_from_description, get_repo_path


def test_generated_dockerfile_name_lowercases_stem_only():
    recipe = {
        "name": "TestApp",
        "version": "1.2.3-RC1",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [
                {"run": ["echo ok"]},
            ],
        },
        "deploy": {
            "bins": ["testapp"],
        },
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

        assert ctx.dockerfile_name == "testapp_1.2.3-rc1.Dockerfile"
