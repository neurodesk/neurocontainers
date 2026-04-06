#!/usr/bin/env python3
"""
Tests for the validation module.
"""

import os
import shutil
import tempfile
import yaml
from builder.validation import (
    validate_recipe_dict,
    validate_recipe_file,
    get_validation_errors,
    ContainerRecipe,
    CustomCopyrightInfo,
    SPDXCopyrightInfo,
    GUIApp,
    DeployInfo,
    FileInfo
)
from builder.build import load_description_file


def test_valid_minimal_recipe():
    """Test validation of a minimal valid recipe"""
    recipe = {
        "name": "test-app",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["other"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [
                {"install": ["curl"]}
            ]
        }
    }
    
    result = validate_recipe_dict(recipe)
    assert isinstance(result, ContainerRecipe)
    assert result.name == "test-app"
    assert result.version == "1.0.0"
    assert result.architectures == ["x86_64"]


def test_valid_complex_recipe():
    """Test validation of a complex recipe with many fields"""
    recipe = {
        "name": "complex-app",
        "version": "2.1.0",
        "architectures": ["x86_64", "aarch64"],
        "copyright": [
            {"license": "MIT", "url": "https://opensource.org/licenses/MIT"},
            {"name": "Custom License", "url": "https://example.com/license"}
        ],
        "categories": ["image registration", "machine learning"],
        "gui_apps": [
            {"name": "GUI App", "exec": "/usr/bin/gui-app"}
        ],
        "deploy": {
            "path": ["/opt/app/bin"],
            "bins": ["app-binary"]
        },
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [
                {"environment": {"PATH": "/opt/app:$PATH"}},
                {"install": ["curl", "wget"]},
                {"run": ["echo 'setup complete'"]},
                {"user": "appuser"}
            ]
        }
    }
    
    result = validate_recipe_dict(recipe)
    assert isinstance(result, ContainerRecipe)
    assert result.name == "complex-app"
    assert len(result.copyright) == 2
    assert isinstance(result.copyright[0], SPDXCopyrightInfo)
    assert isinstance(result.copyright[1], CustomCopyrightInfo)
    assert len(result.gui_apps) == 1
    assert isinstance(result.gui_apps[0], GUIApp)


def test_invalid_recipe_missing_required_fields():
    """Test validation fails for recipe missing required fields"""
    recipe = {
        "name": "incomplete-app",
        # Missing version, architectures, build
    }
    
    errors = get_validation_errors(recipe)
    print(f"Errors: {errors}")  # Debug output
    assert len(errors) > 0


def test_invalid_recipe_empty_name():
    """Test validation fails for empty name"""
    recipe = {
        "name": "",
        "version": "1.0.0", 
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": []
        }
    }
    
    errors = get_validation_errors(recipe)
    assert len(errors) > 0
    assert any("name" in error.lower() and "empty" in error.lower() for error in errors)


def test_invalid_architecture():
    """Test validation fails for unsupported architecture"""
    recipe = {
        "name": "test-app",
        "version": "1.0.0",
        "architectures": ["invalid-arch"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04", 
            "pkg-manager": "apt",
            "directives": []
        }
    }
    
    errors = get_validation_errors(recipe)
    assert len(errors) > 0
    assert any("architecture" in error.lower() for error in errors)


def test_invalid_recipe_missing_categories():
    """Test validation fails when categories are missing"""
    recipe = {
        "name": "test-app",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": []
        }
    }

    errors = get_validation_errors(recipe)
    assert len(errors) > 0
    assert any("categories" in error.lower() for error in errors)


def test_invalid_category():
    """Test validation fails for unsupported category"""
    recipe = {
        "name": "test-app",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["invalid-category"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt", 
            "directives": []
        }
    }
    
    errors = get_validation_errors(recipe)
    assert len(errors) > 0
    assert any("category" in error.lower() for error in errors)


def test_validate_recipe_file():
    """Test validation of a YAML file"""
    recipe = {
        "name": "file-test-app",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["other"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [{"install": ["curl"]}]
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(recipe, f)
        temp_file = f.name
    
    try:
        result = validate_recipe_file(temp_file)
        assert isinstance(result, ContainerRecipe)
        assert result.name == "file-test-app"
    finally:
        os.unlink(temp_file)


def test_validate_nonexistent_file():
    """Test validation fails for non-existent file"""
    try:
        validate_recipe_file("/nonexistent/file.yaml")
        assert False, "Should have raised FileNotFoundError"
    except FileNotFoundError:
        pass


def test_directive_validation():
    """Test validation of various directive types"""
    recipe = {
        "name": "directive-test",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["other"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [
                {"environment": {"TEST_VAR": "value"}},
                {"install": ["package1", "package2"]},
                {"run": ["echo 'test'", "ls -la"]},
                {"workdir": "/app"},
                {"user": "testuser"},
                {"copy": ["src", "dest"]},
                {"template": {"name": "test-template", "param": "value"}},
                {"entrypoint": "/bin/bash"},
                {"variables": {"VAR1": "value1"}},
                {"file": {"name": "test.txt", "contents": "hello world"}}
            ]
        }
    }
    
    result = validate_recipe_dict(recipe)
    assert isinstance(result, ContainerRecipe)
    assert result.name == "directive-test"


def test_structured_readme_allows_missing_optional_fields():
    """Test structured_readme accepts omitted documentation and citation fields."""
    recipe = {
        "name": "structured-readme-test",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["other"],
        "structured_readme": {
            "description": "Example tool",
            "example": "example-tool --help",
        },
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [{"install": ["curl"]}],
        },
    }

    result = validate_recipe_dict(recipe)

    assert isinstance(result, ContainerRecipe)
    assert result.structured_readme is not None
    assert result.structured_readme.documentation == ""
    assert result.structured_readme.citation == ""


def test_file_info_accepts_refresh_flag():
    """Test validation accepts URL-backed files that force refresh."""
    file_info = FileInfo(
        name="upstream.zip",
        url="https://example.com/upstream.zip",
        refresh=True,
    )

    assert file_info.refresh is True


def test_invalid_jinja_template_in_file_url():
    """Test validation fails early for malformed Jinja2 in rendered file URLs."""
    recipe = {
        "name": "broken-template-app",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["other"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [],
        },
        "files": [
            {
                "name": "downloaded_file",
                "url": "https://example.com/releases/download/{{",
            }
        ],
    }

    errors = get_validation_errors(recipe)

    assert len(errors) > 0
    assert any("files[0].url" in error for error in errors)
    assert any("template syntax error" in error.lower() for error in errors)


def test_non_string_file_name_fails_validation():
    """Test validation fails early for non-string file names used by the builder."""
    recipe = {
        "name": "typed-file-app",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["other"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [],
        },
        "files": [
            {
                "name": 491,
                "url": "https://example.com/download.tar.gz",
            }
        ],
    }

    errors = get_validation_errors(recipe)

    assert len(errors) > 0
    assert any("files[0].name must be a string" in error for error in errors)


def test_two_digit_version_yaml_parsing():
    """Test that two-digit versions like '1.1' are handled correctly"""
    # Create a recipe with a two-digit version that YAML would parse as float
    recipe = {
        "name": "version-test",
        "version": "1.1",  # This will be parsed as float by YAML
        "architectures": ["x86_64"],
        "categories": ["other"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:22.04",
            "pkg-manager": "apt",
            "directives": [{"install": ["curl"]}]
        }
    }
    
    # Create a temporary directory for the recipe
    temp_dir = tempfile.mkdtemp()
    try:
        # Write the build.yaml directly to the temp directory
        build_yaml = os.path.join(temp_dir, "build.yaml")
        with open(build_yaml, 'w') as f:
            yaml.dump(recipe, f)
        
        # Load using the actual function
        loaded = load_description_file(temp_dir)
        
        # Version should be converted to string
        assert isinstance(loaded["version"], str), f"Version should be string, got {type(loaded['version'])}"
        assert loaded["version"] == "1.1", f"Version should be '1.1', got '{loaded['version']}'"
        
        # Name should also be string
        assert isinstance(loaded["name"], str), f"Name should be string, got {type(loaded['name'])}"
        
        # Should be able to call string methods on version
        assert loaded["version"].replace(":", "_") == "1.1"
        
    finally:
        # Clean up
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    # Run basic tests
    test_valid_minimal_recipe()
    test_valid_complex_recipe()
    test_invalid_recipe_missing_required_fields()
    test_invalid_recipe_empty_name()
    test_invalid_architecture()
    test_invalid_recipe_missing_categories()
    test_invalid_category()
    test_validate_recipe_file()
    test_validate_nonexistent_file()
    test_directive_validation()
    test_structured_readme_allows_missing_optional_fields()
    test_file_info_accepts_refresh_flag()
    test_invalid_jinja_template_in_file_url()
    test_non_string_file_name_fails_validation()
    test_two_digit_version_yaml_parsing()
    
    print("✅ All validation tests passed!")
