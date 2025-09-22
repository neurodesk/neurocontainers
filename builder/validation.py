#!/usr/bin/env python3
"""
YAML Recipe validation using attrs.

This module provides validation for neurocontainer YAML recipes using attrs classes.
The schema matches the Zod schema from https://github.com/neurodesk/neurocontainers-ui/blob/main/lib/zodSchema.ts
"""

import attrs
from typing import List, Dict, Union, Optional, Any, Literal
import yaml


# ============================================================================
# Base Types and Enums
# ============================================================================

ARCHITECTURES = ["x86_64", "aarch64"]

# Categories from the UI - this should match CATEGORIES in the UI
CATEGORIES = [
    "image registration",
    "structural imaging",
    "image segmentation",
    "functional imaging",
    "rodent imaging",
    "diffusion imaging",
    "spine",
    "connectomics",
    "electrophysiology",
    "microscopy",
    "data management",
    "machine learning",
    "visualization",
    "quantitative imaging",
    "image reconstruction",
    "conversion",
    "file conversion",  # Legacy category name
    "quality control",
    "tractography",
    "preprocessing",
    "motion correction",
    "statistical analysis",
    "statistics",  # Legacy category name
    "modeling",
    "simulation",
    "workflows",
    "data organisation",
    "bids apps",
    "programming",  # For development tools
    "phase processing",  # For phase imaging tools
    "molecular biology",  # For molecular/biological tools
    "hippocampus",  # For hippocampus-specific tools
    "spectroscopy",  # For spectroscopy tools
    "other",
]

INCLUDE_MACROS = [
    "openrecon/neurodocker.yaml",
    "macros/openrecon/neurodocker.yaml",  # Support both formats
]


# ============================================================================
# Validation Functions
# ============================================================================


def validate_architecture(instance, attribute, value):
    """Validate architecture is in allowed list"""
    if value not in ARCHITECTURES:
        raise ValueError(
            f"Architecture '{value}' not supported. Must be one of: {ARCHITECTURES}"
        )


def validate_category(instance, attribute, value):
    """Validate category is in allowed list"""
    if value not in CATEGORIES:
        raise ValueError(
            f"Category '{value}' not supported. Must be one of: {CATEGORIES}"
        )


def validate_non_empty_string(instance, attribute, value):
    """Validate string is not empty"""
    if not value or (isinstance(value, str) and value.strip() == ""):
        raise ValueError(f"{attribute.name} cannot be empty")


def validate_url(instance, attribute, value):
    """Basic URL validation"""
    if value and not (value.startswith("http://") or value.startswith("https://")):
        raise ValueError(
            f"{attribute.name} must be a valid URL starting with http:// or https://"
        )


# ============================================================================
# Copyright Info
# ============================================================================


@attrs.define
class CustomCopyrightInfo:
    name: str = attrs.field(validator=validate_non_empty_string)
    url: Optional[str] = attrs.field(
        default=None, validator=attrs.validators.optional(validate_url)
    )


@attrs.define
class SPDXCopyrightInfo:
    license: str = attrs.field(validator=validate_non_empty_string)
    url: Optional[str] = attrs.field(
        default=None, validator=attrs.validators.optional(validate_url)
    )


# ============================================================================
# Structured Readme
# ============================================================================


@attrs.define
class StructuredReadme:
    description: str = attrs.field()
    example: str = attrs.field()
    documentation: str = attrs.field()
    citation: str = attrs.field()


# ============================================================================
# GUI Apps Info
# ============================================================================


@attrs.define
class GUIApp:
    name: str = attrs.field(validator=validate_non_empty_string)
    exec: str = attrs.field(validator=validate_non_empty_string)


@attrs.define
class DeployInfo:
    path: Optional[List[str]] = attrs.field(default=None)
    bins: Optional[List[str]] = attrs.field(default=None)


# ============================================================================
# File Info
# ============================================================================


@attrs.define
class FileInfo:
    name: str = attrs.field(validator=validate_non_empty_string)
    filename: Optional[str] = attrs.field(default=None)
    contents: Optional[str] = attrs.field(default=None)
    url: Optional[str] = attrs.field(
        default=None, validator=attrs.validators.optional(validate_url)
    )
    executable: Optional[bool] = attrs.field(default=None)
    insecure: Optional[bool] = attrs.field(default=None)
    retry: Optional[int] = attrs.field(default=None)


# ============================================================================
# Test Info
# ============================================================================


@attrs.define
class BuiltinTest:
    name: str = attrs.field(validator=validate_non_empty_string)
    builtin: Literal["test_deploy.sh"] = attrs.field()


@attrs.define
class ScriptTest:
    name: str = attrs.field(validator=validate_non_empty_string)
    script: str = attrs.field(validator=validate_non_empty_string)
    executable: Optional[str] = attrs.field(default=None)
    manual: Optional[bool] = attrs.field(default=None)


# ============================================================================
# Template
# ============================================================================


@attrs.define
class Template:
    name: str = attrs.field(validator=validate_non_empty_string)
    # Allow additional properties as a dict
    additional_props: Dict[str, Any] = attrs.field(factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Template":
        """Create Template from dict, separating name from additional properties"""
        name = data.pop("name")
        return cls(name=name, additional_props=data)


# ============================================================================
# Directive Base Classes
# ============================================================================


@attrs.define
class BaseDirective:
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class EnvironmentDirective:
    environment: Dict[str, str] = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class InstallDirective:
    install: Union[str, List[str]] = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class WorkingDirectoryDirective:
    workdir: str = attrs.field(validator=validate_non_empty_string)
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class RunCommandDirective:
    run: List[str] = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class VariableDirective:
    variables: Dict[str, Any] = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class TemplateDirective:
    template: Template = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class DeployDirective:
    deploy: DeployInfo = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class UserDirective:
    user: str = attrs.field(validator=validate_non_empty_string)
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class CopyDirective:
    copy: Union[List[str], str] = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class FileDirective:
    file: FileInfo = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class TestDirective:
    test: Union[BuiltinTest, ScriptTest] = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class EntrypointDirective:
    entrypoint: str = attrs.field(validator=validate_non_empty_string)
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class IncludeDirective:
    include: str = attrs.field()
    condition: Optional[str] = attrs.field(default=None)

    @include.validator
    def _validate_include(self, attribute, value):
        if value not in INCLUDE_MACROS:
            raise ValueError(
                f"Include macro '{value}' not supported. Must be one of: {INCLUDE_MACROS}"
            )


@attrs.define
class BoutiquesDescriptor:
    name: str = attrs.field(validator=validate_non_empty_string)
    description: str = attrs.field(validator=validate_non_empty_string)
    tool_version: str = attrs.field(validator=validate_non_empty_string)
    schema_version: Literal["0.5"] = attrs.field()
    command_line: str = attrs.field(validator=validate_non_empty_string)
    inputs: List[Dict[str, Any]] = attrs.field()
    # Allow additional properties
    additional_props: Dict[str, Any] = attrs.field(factory=dict)


@attrs.define
class BoutiqueDirective:
    boutique: BoutiquesDescriptor = attrs.field()
    condition: Optional[str] = attrs.field(default=None)


@attrs.define
class GroupDirective:
    group: List[Any] = attrs.field()  # Forward reference to Directive
    condition: Optional[str] = attrs.field(default=None)
    custom: Optional[str] = attrs.field(default=None)
    customParams: Optional[Dict[str, Any]] = attrs.field(default=None)


# ============================================================================
# Build Recipe
# ============================================================================


@attrs.define
class NeuroDockerBuildRecipe:
    kind: Literal["neurodocker"] = attrs.field()
    base_image: str = attrs.field(validator=validate_non_empty_string)
    pkg_manager: str = attrs.field(validator=validate_non_empty_string)
    directives: List[Any] = attrs.field()  # List of directives
    add_default_template: Optional[bool] = attrs.field(default=None)
    add_tzdata: Optional[bool] = attrs.field(default=None)
    fix_locale_def: Optional[bool] = attrs.field(default=None)


# ============================================================================
# Main Container Recipe Schema
# ============================================================================


@attrs.define
class ContainerRecipe:
    name: str = attrs.field(validator=validate_non_empty_string)
    version: str = attrs.field(validator=validate_non_empty_string)
    architectures: List[str] = attrs.field(validator=attrs.validators.min_len(1))
    build: NeuroDockerBuildRecipe = attrs.field()
    icon: Optional[str] = attrs.field(default=None)
    copyright: Optional[List[Union[CustomCopyrightInfo, SPDXCopyrightInfo]]] = (
        attrs.field(default=None)
    )
    readme: Optional[str] = attrs.field(default=None)
    readme_url: Optional[str] = attrs.field(
        default=None, validator=attrs.validators.optional(validate_url)
    )
    structured_readme: Optional[StructuredReadme] = attrs.field(default=None)
    files: Optional[List[FileInfo]] = attrs.field(default=None)
    deploy: Optional[DeployInfo] = attrs.field(default=None)
    tests: Optional[List[Union[BuiltinTest, ScriptTest]]] = attrs.field(default=None)
    categories: Optional[List[str]] = attrs.field(default=None)
    gui_apps: Optional[List[GUIApp]] = attrs.field(default=None)
    apptainer_args: Optional[List[str]] = attrs.field(default=None)
    draft: Optional[bool] = attrs.field(default=None)
    description: Optional[str] = attrs.field(default=None)
    options: Optional[Dict[str, Any]] = attrs.field(default=None)
    variables: Optional[Dict[str, Any]] = attrs.field(default=None)
    epoch: Optional[int] = attrs.field(default=None)

    @architectures.validator
    def _validate_architectures(self, attribute, value):
        for arch in value:
            if arch not in ARCHITECTURES:
                raise ValueError(
                    f"Architecture '{arch}' not supported. Must be one of: {ARCHITECTURES}"
                )

    @categories.validator
    def _validate_categories(self, attribute, value):
        if value:
            for category in value:
                if category not in CATEGORIES:
                    raise ValueError(
                        f"Category '{category}' not supported. Must be one of: {CATEGORIES}"
                    )


# ============================================================================
# Validation Functions
# ============================================================================


def parse_directive_from_dict(directive_dict: Dict[str, Any]) -> Any:
    """Parse a directive dict into the appropriate directive class"""
    # Remove condition if present for processing
    condition = directive_dict.get("condition")

    if "environment" in directive_dict:
        return EnvironmentDirective(
            condition=condition, environment=directive_dict["environment"]
        )
    elif "install" in directive_dict:
        return InstallDirective(condition=condition, install=directive_dict["install"])
    elif "workdir" in directive_dict:
        return WorkingDirectoryDirective(
            condition=condition, workdir=directive_dict["workdir"]
        )
    elif "run" in directive_dict:
        return RunCommandDirective(condition=condition, run=directive_dict["run"])
    elif "variables" in directive_dict:
        return VariableDirective(
            condition=condition, variables=directive_dict["variables"]
        )
    elif "template" in directive_dict:
        template = Template.from_dict(directive_dict["template"].copy())
        return TemplateDirective(condition=condition, template=template)
    elif "deploy" in directive_dict:
        deploy_info = DeployInfo(**directive_dict["deploy"])
        return DeployDirective(condition=condition, deploy=deploy_info)
    elif "user" in directive_dict:
        return UserDirective(condition=condition, user=directive_dict["user"])
    elif "copy" in directive_dict:
        return CopyDirective(condition=condition, copy=directive_dict["copy"])
    elif "file" in directive_dict:
        file_info = FileInfo(**directive_dict["file"])
        return FileDirective(condition=condition, file=file_info)
    elif "test" in directive_dict:
        test_data = directive_dict["test"]
        if "builtin" in test_data:
            test_obj = BuiltinTest(**test_data)
        else:
            test_obj = ScriptTest(**test_data)
        return TestDirective(condition=condition, test=test_obj)
    elif "include" in directive_dict:
        return IncludeDirective(condition=condition, include=directive_dict["include"])
    elif "entrypoint" in directive_dict:
        return EntrypointDirective(
            condition=condition, entrypoint=directive_dict["entrypoint"]
        )
    elif "boutique" in directive_dict:
        # Handle boutique descriptor
        boutique_data = directive_dict["boutique"].copy()
        # Handle tool-version field (hyphenated in YAML)
        if "tool-version" in boutique_data:
            boutique_data["tool_version"] = boutique_data.pop("tool-version")
        if "schema-version" in boutique_data:
            boutique_data["schema_version"] = boutique_data.pop("schema-version")
        if "command-line" in boutique_data:
            boutique_data["command_line"] = boutique_data.pop("command-line")

        # Extract core fields
        core_fields = [
            "name",
            "description",
            "tool_version",
            "schema_version",
            "command_line",
            "inputs",
        ]
        core_data = {k: boutique_data.pop(k) for k in core_fields if k in boutique_data}

        boutique_descriptor = BoutiquesDescriptor(
            **core_data, additional_props=boutique_data
        )
        return BoutiqueDirective(condition=condition, boutique=boutique_descriptor)
    elif "group" in directive_dict:
        # Handle group directive recursively
        group_directives = []
        for group_item in directive_dict["group"]:
            group_directives.append(parse_directive_from_dict(group_item))

        return GroupDirective(
            condition=condition,
            custom=directive_dict.get("custom"),
            customParams=directive_dict.get("customParams"),
            group=group_directives,
        )
    else:
        raise ValueError(f"Unknown directive type in: {directive_dict}")


def parse_copyright_from_dict(
    copyright_dict: Dict[str, Any],
) -> Union[CustomCopyrightInfo, SPDXCopyrightInfo]:
    """Parse copyright dict into appropriate copyright class"""
    if "license" in copyright_dict:
        return SPDXCopyrightInfo(**copyright_dict)
    elif "name" in copyright_dict:
        return CustomCopyrightInfo(**copyright_dict)
    else:
        raise ValueError(
            "Copyright entry must have either 'license' (SPDX) or 'name' (custom) field"
        )


def parse_test_from_dict(test_dict: Dict[str, Any]) -> Union[BuiltinTest, ScriptTest]:
    """Parse test dict into appropriate test class"""
    if "builtin" in test_dict:
        return BuiltinTest(**test_dict)
    elif "script" in test_dict:
        return ScriptTest(**test_dict)
    else:
        raise ValueError("Test must have either 'builtin' or 'script' field")


def validate_recipe_dict(recipe_dict: Dict[str, Any]) -> ContainerRecipe:
    """
    Validate a recipe dictionary and return a ContainerRecipe instance.

    Args:
        recipe_dict: Dictionary loaded from YAML

    Returns:
        ContainerRecipe instance

    Raises:
        ValueError: If validation fails
    """
    try:
        # Make a copy to avoid modifying the original
        recipe_copy = recipe_dict.copy()

        # Parse copyright if present
        if "copyright" in recipe_copy and recipe_copy["copyright"]:
            copyright_list = []
            for copyright_item in recipe_copy["copyright"]:
                copyright_obj = parse_copyright_from_dict(copyright_item)
                copyright_list.append(copyright_obj)
            recipe_copy["copyright"] = copyright_list

        # Parse structured_readme if present
        if "structured_readme" in recipe_copy and recipe_copy["structured_readme"]:
            recipe_copy["structured_readme"] = StructuredReadme(
                **recipe_copy["structured_readme"]
            )

        # Parse build recipe
        build_dict = recipe_copy["build"].copy()

        # Handle hyphenated field names
        if "base-image" in build_dict:
            build_dict["base_image"] = build_dict.pop("base-image")
        if "pkg-manager" in build_dict:
            build_dict["pkg_manager"] = build_dict.pop("pkg-manager")
        if "add-default-template" in build_dict:
            build_dict["add_default_template"] = build_dict.pop("add-default-template")
        if "add-tzdata" in build_dict:
            build_dict["add_tzdata"] = build_dict.pop("add-tzdata")
        if "fix-locale-def" in build_dict:
            build_dict["fix_locale_def"] = build_dict.pop("fix-locale-def")

        # Parse directives
        directives = []
        for directive_dict in build_dict["directives"]:
            directive_obj = parse_directive_from_dict(directive_dict)
            directives.append(directive_obj)
        build_dict["directives"] = directives

        build_recipe = NeuroDockerBuildRecipe(**build_dict)
        recipe_copy["build"] = build_recipe

        # Parse files if present
        if "files" in recipe_copy and recipe_copy["files"]:
            files_list = []
            for file_dict in recipe_copy["files"]:
                file_obj = FileInfo(**file_dict)
                files_list.append(file_obj)
            recipe_copy["files"] = files_list

        # Parse deploy if present
        if "deploy" in recipe_copy and recipe_copy["deploy"]:
            recipe_copy["deploy"] = DeployInfo(**recipe_copy["deploy"])

        # Parse tests if present
        if "tests" in recipe_copy and recipe_copy["tests"]:
            tests_list = []
            for test_dict in recipe_copy["tests"]:
                test_obj = parse_test_from_dict(test_dict)
                tests_list.append(test_obj)
            recipe_copy["tests"] = tests_list

        # Parse gui_apps if present
        if "gui_apps" in recipe_copy and recipe_copy["gui_apps"]:
            gui_apps_list = []
            for gui_app_dict in recipe_copy["gui_apps"]:
                gui_app_obj = GUIApp(**gui_app_dict)
                gui_apps_list.append(gui_app_obj)
            recipe_copy["gui_apps"] = gui_apps_list

        # Create and return the container recipe
        return ContainerRecipe(**recipe_copy)

    except Exception as e:
        raise ValueError(f"Recipe validation failed: {str(e)}")


def validate_recipe_file(file_path: str) -> ContainerRecipe:
    """
    Validate a YAML recipe file.

    Args:
        file_path: Path to the YAML file

    Returns:
        ContainerRecipe instance

    Raises:
        ValueError: If validation fails
        FileNotFoundError: If file doesn't exist
    """
    try:
        with open(file_path, "r") as f:
            recipe_dict = yaml.safe_load(f)

        if not recipe_dict:
            raise ValueError("Recipe file is empty or invalid YAML")

        return validate_recipe_dict(recipe_dict)

    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML syntax: {str(e)}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Recipe file not found: {file_path}")


def get_validation_errors(recipe_dict: Dict[str, Any]) -> List[str]:
    """
    Get a list of validation errors for a recipe without raising exceptions.

    Args:
        recipe_dict: Dictionary loaded from YAML

    Returns:
        List of error messages (empty if valid)
    """
    try:
        validate_recipe_dict(recipe_dict)
        return []
    except ValueError as e:
        return [str(e)]
    except Exception as e:
        return [f"Unexpected validation error: {str(e)}"]


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Validate neurocontainer YAML recipes")
    parser.add_argument("file", help="Path to YAML recipe file to validate")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    try:
        recipe = validate_recipe_file(args.file)
        print(f"✓ Recipe {recipe.name} v{recipe.version} is valid")
        if args.verbose:
            print(f"  - Name: {recipe.name}")
            print(f"  - Version: {recipe.version}")
            print(f"  - Architectures: {recipe.architectures}")
            if recipe.categories:
                print(f"  - Categories: {recipe.categories}")
    except (ValueError, FileNotFoundError) as e:
        print(f"✗ Validation failed: {e}")
        sys.exit(1)
