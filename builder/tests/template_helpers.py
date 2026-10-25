"""Exercise builtin installation methods through the production recipe interpreter."""

from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from builder.recipe import compile_recipe


def apply_builtin_template(name, params, pkg_manager, add):
    with TemporaryDirectory() as directory:
        path = Path(directory)
        recipe = {
            "name": "fixture",
            "version": "1.0.0",
            "architectures": ["x86_64"],
            "readme": "Template execution fixture",
            "categories": ["programming"],
            "icon": "data:image/png;base64,aWNvbg==",
            "build": {
                "kind": "neurodocker",
                "base-image": "ubuntu:24.04",
                "pkg-manager": pkg_manager,
                "add-default-template": False,
                "directives": [{"template": {"name": name, **params}}],
            },
        }
        (path / "build.yaml").write_text(yaml.safe_dump(recipe))
        compiled = compile_recipe(path, architecture="x86_64")
        recipe["build"]["directives"] = []
        (path / "build.yaml").write_text(yaml.safe_dump(recipe))
        envelope = compile_recipe(path, architecture="x86_64")
        prefix = len(envelope.definition.directives) - 4
        for directive in compiled.definition.directives[prefix:-4]:
            add(directive)


def write_fragment_recipe(root, directives):
    recipe = root / "recipes" / "fixture"
    recipe.mkdir(parents=True)
    data = {
        "name": "fixture",
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "readme": "Fixture",
        "categories": ["programming"],
        "icon": "data:image/png;base64,aWNvbg==",
        "variables": {"value": "outer"},
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": directives,
        },
    }
    (recipe / "build.yaml").write_text(yaml.safe_dump(data))
    return recipe, data
