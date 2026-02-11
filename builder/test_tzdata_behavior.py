#!/usr/bin/env python3
"""
Tests for tzdata installation behavior based on add-default-template setting.
"""

import os
import tempfile
import yaml
import sys

# Add parent directory to path to import builder modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from builder.build import generate_from_description, get_repo_path


def test_tzdata_disabled_with_add_default_template_false():
    """Test that tzdata is NOT installed when add-default-template is false"""
    recipe = {
        "name": "test-no-tzdata",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "add-default-template": False,
            "directives": [
                {"install": ["curl"]}
            ]
        },
        "readme": "Test recipe",
        "copyright": [
            {"license": "MIT", "url": "https://example.com"}
        ]
    }
    
    repo_path = get_repo_path()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        recipe_dir = os.path.join(tmpdir, "recipe")
        os.makedirs(recipe_dir)
        
        # Write recipe file
        with open(os.path.join(recipe_dir, "build.yaml"), "w") as f:
            yaml.dump(recipe, f)
        
        build_dir = os.path.join(tmpdir, "build")
        
        ctx = generate_from_description(
            repo_path,
            recipe_dir,
            recipe,
            build_dir,
            architecture="x86_64",
            recreate_output_dir=True,
            check_only=True,
        )
        
        assert ctx is not None, "Context should not be None"
        
        # Read generated Dockerfile
        dockerfile_path = os.path.join(
            build_dir,
            ctx.name,
            f"{ctx.name}_{ctx.version}.Dockerfile"
        )
        
        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()
        
        # Verify tzdata is NOT present
        assert "tzdata" not in dockerfile_content.lower(), \
            "tzdata should not be installed when add-default-template is false"
        
        print("✅ Test passed: tzdata not installed with add-default-template: false")


def test_tzdata_enabled_by_default():
    """Test that tzdata IS installed by default (add-default-template not specified)"""
    recipe = {
        "name": "test-default-tzdata",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [
                {"install": ["curl"]}
            ]
        },
        "readme": "Test recipe",
        "copyright": [
            {"license": "MIT", "url": "https://example.com"}
        ]
    }
    
    repo_path = get_repo_path()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        recipe_dir = os.path.join(tmpdir, "recipe")
        os.makedirs(recipe_dir)
        
        # Write recipe file
        with open(os.path.join(recipe_dir, "build.yaml"), "w") as f:
            yaml.dump(recipe, f)
        
        build_dir = os.path.join(tmpdir, "build")
        
        ctx = generate_from_description(
            repo_path,
            recipe_dir,
            recipe,
            build_dir,
            architecture="x86_64",
            recreate_output_dir=True,
            check_only=True,
        )
        
        assert ctx is not None, "Context should not be None"
        
        # Read generated Dockerfile
        dockerfile_path = os.path.join(
            build_dir,
            ctx.name,
            f"{ctx.name}_{ctx.version}.Dockerfile"
        )
        
        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()
        
        # Verify tzdata IS present
        assert "tzdata" in dockerfile_content.lower(), \
            "tzdata should be installed by default"
        
        print("✅ Test passed: tzdata installed by default")


def test_tzdata_explicit_override():
    """Test that add-tzdata: true overrides add-default-template: false"""
    recipe = {
        "name": "test-override-tzdata",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "add-default-template": False,
            "add-tzdata": True,
            "directives": [
                {"install": ["curl"]}
            ]
        },
        "readme": "Test recipe",
        "copyright": [
            {"license": "MIT", "url": "https://example.com"}
        ]
    }
    
    repo_path = get_repo_path()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        recipe_dir = os.path.join(tmpdir, "recipe")
        os.makedirs(recipe_dir)
        
        # Write recipe file
        with open(os.path.join(recipe_dir, "build.yaml"), "w") as f:
            yaml.dump(recipe, f)
        
        build_dir = os.path.join(tmpdir, "build")
        
        ctx = generate_from_description(
            repo_path,
            recipe_dir,
            recipe,
            build_dir,
            architecture="x86_64",
            recreate_output_dir=True,
            check_only=True,
        )
        
        assert ctx is not None, "Context should not be None"
        
        # Read generated Dockerfile
        dockerfile_path = os.path.join(
            build_dir,
            ctx.name,
            f"{ctx.name}_{ctx.version}.Dockerfile"
        )
        
        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()
        
        # Verify tzdata IS present due to explicit override
        assert "tzdata" in dockerfile_content.lower(), \
            "tzdata should be installed when explicitly enabled with add-tzdata: true"
        
        print("✅ Test passed: tzdata installed with explicit override")


def test_tzdata_explicit_false():
    """Test that add-tzdata: false overrides add-default-template: true"""
    recipe = {
        "name": "test-no-tzdata-explicit",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "add-default-template": True,
            "add-tzdata": False,
            "directives": [
                {"install": ["curl"]}
            ]
        },
        "readme": "Test recipe",
        "copyright": [
            {"license": "MIT", "url": "https://example.com"}
        ]
    }
    
    repo_path = get_repo_path()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        recipe_dir = os.path.join(tmpdir, "recipe")
        os.makedirs(recipe_dir)
        
        # Write recipe file
        with open(os.path.join(recipe_dir, "build.yaml"), "w") as f:
            yaml.dump(recipe, f)
        
        build_dir = os.path.join(tmpdir, "build")
        
        ctx = generate_from_description(
            repo_path,
            recipe_dir,
            recipe,
            build_dir,
            architecture="x86_64",
            recreate_output_dir=True,
            check_only=True,
        )
        
        assert ctx is not None, "Context should not be None"
        
        # Read generated Dockerfile
        dockerfile_path = os.path.join(
            build_dir,
            ctx.name,
            f"{ctx.name}_{ctx.version}.Dockerfile"
        )
        
        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()
        
        # Verify tzdata is NOT present due to explicit disable
        assert "tzdata" not in dockerfile_content.lower(), \
            "tzdata should not be installed when explicitly disabled with add-tzdata: false"
        
        print("✅ Test passed: tzdata not installed with explicit disable")


if __name__ == "__main__":
    print("Running tzdata behavior tests...\n")
    
    test_tzdata_disabled_with_add_default_template_false()
    test_tzdata_enabled_by_default()
    test_tzdata_explicit_override()
    test_tzdata_explicit_false()
    
    print("\n✅ All tzdata behavior tests passed!")
