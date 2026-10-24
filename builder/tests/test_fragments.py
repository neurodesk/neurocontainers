from pathlib import Path

import pytest
import yaml

from builder.dockerfile import render_dockerfile
from builder.fragments import load_fragment, resolve_macro, validate_macro_tree
from builder.recipe import compile_recipe
from builder.shared_inputs import SharedInputs
from builder.release_plan import plan_recipe_changes
from builder.template_backend import expand_builtin_template


from builder.tests.template_helpers import write_fragment_recipe as write_recipe


def test_include_scopes_nested_variables_conditions_and_local_assets(tmp_path):
    macro = tmp_path / "macros" / "fixture"
    macro.mkdir(parents=True)
    (macro / "asset.txt").write_text("payload")
    (macro / "inner.yaml").write_text(
        yaml.safe_dump(
            {
                "directives": [
                    {"file": {"name": "asset", "filename": "asset.txt"}},
                    {
                        "run": "cat {{ get_file('asset') }}; echo {{ context.value }}",
                        "condition": "context.value == 'inner'",
                    },
                ]
            }
        )
    )
    (macro / "outer.yaml").write_text(
        yaml.safe_dump(
            {
                "directives": [
                    {"run": "echo {{ context.value }}"},
                    {"include": "fixture/inner.yaml", "with": {"value": "inner"}},
                    {"run": "echo {{ context.value }}"},
                ]
            }
        )
    )
    recipe, _ = write_recipe(
        tmp_path,
        [
            {"include": "macros/fixture/outer.yaml", "with": {"value": "supplied"}},
            {"run": "echo {{ context.value }}"},
        ],
    )
    result = compile_recipe(recipe, include_dirs=(tmp_path,), architecture="x86_64")
    rendered = render_dockerfile(result.definition)
    assert rendered.count("RUN echo supplied") == 2
    assert "RUN echo outer" in rendered
    assert "readonly cat /.neurocontainer-cache/" in rendered
    assert "; echo inner" in rendered
    assert (
        Path(result.staging_plan.files["asset"].filename)
        == macro.resolve() / "asset.txt"
    )


@pytest.mark.parametrize(
    "name", ["../escape.yaml", "/tmp/escape.yaml", "macros/../escape.yaml"]
)
def test_escaping_macro_paths_fail(tmp_path, name):
    with pytest.raises(ValueError, match="escapes macros"):
        resolve_macro(name, (tmp_path,))


def test_macro_validation_reports_malformed_missing_cycles_and_assets(tmp_path):
    macro = tmp_path / "macros"
    macro.mkdir()
    with pytest.raises(FileNotFoundError, match="include not found"):
        validate_macro_tree([{"include": "absent.yaml"}], (tmp_path,))
    (macro / "bad.yaml").write_text("directives: nope")
    with pytest.raises(ValueError, match="directives list"):
        load_fragment(macro / "bad.yaml")
    (macro / "a.yaml").write_text("directives:\n- include: b.yaml\n")
    (macro / "b.yaml").write_text("directives:\n- include: a.yaml\n")
    with pytest.raises(ValueError, match="include cycle.*a.yaml.*b.yaml.*a.yaml"):
        validate_macro_tree([{"include": "a.yaml"}], (tmp_path,))
    (macro / "bad.yaml").write_text(
        "directives:\n- file: {name: missing, filename: absent}\n"
    )
    with pytest.raises(FileNotFoundError, match="fragment asset not found"):
        validate_macro_tree([{"include": "bad.yaml"}], (tmp_path,))


def test_static_dependency_closure_uses_selected_reader_without_rendering():
    documents = {
        "builder/templates/example.yaml": "binaries:\n  directives:\n  - include: outer.yaml\n",
        "macros/outer.yaml": "directives:\n- include: nested/inner.yaml\n",
        "macros/nested/inner.yaml": "directives:\n- file: {name: asset, filename: asset.txt}\n",
    }
    recipe = {"build": {"directives": [{"group": [{"template": {"name": "example"}}]}]}}
    inputs = SharedInputs.from_recipe(recipe, documents.get)
    assert inputs.contains("macros/nested/asset.txt")
    assert inputs.contains("builder/templates/example.yaml")
    assert not inputs.contains("builder/templates/unrelated.yaml")
    plan = plan_recipe_changes(
        ["macros/nested/asset.txt"],
        {"fixture": recipe},
        {"fixture": recipe},
        shared_dependencies={"fixture": inputs},
    )
    assert plan.candidate_recipes == ["fixture"]
    # A different revision removes the transitive dependency, without changing
    # either the recipe mapping or anything on the working filesystem.
    changed = dict(documents, **{"macros/outer.yaml": "directives: []"})
    assert not SharedInputs.from_recipe(recipe, changed.get).contains(
        "macros/nested/asset.txt"
    )


@pytest.mark.parametrize(
    "directive",
    [{"include": "{{ danger() }}"}, {"template": {"name": "{{ danger() }}"}}],
)
def test_dynamic_fragment_names_conservatively_track_both_roots(directive):
    inputs = SharedInputs.from_recipe({"build": {"directives": [directive]}})
    assert inputs.contains("macros/nested/asset.txt")
    assert inputs.contains("builder/templates/example.yaml")


def test_builtin_requires_native_directives(monkeypatch):
    monkeypatch.setattr(
        "builder.template_backend._load_template",
        lambda name: {"binaries": {"env": {"A": "legacy"}, "instructions": "echo old"}},
    )
    with pytest.raises(ValueError, match="directives must be a list"):
        expand_builtin_template("example", {}, "apt")


def test_builtin_executes_full_fragment_path(tmp_path, monkeypatch):
    macro = tmp_path / "macros"
    macro.mkdir()
    (macro / "nested.yaml").write_text("directives:\n- run: echo {{ context.value }}\n")
    monkeypatch.setattr(
        "builder.template_backend._load_template",
        lambda name: {
            "binaries": {
                "directives": [
                    {"file": {"name": "payload", "contents": "cached payload"}},
                    {"variables": {"value": "scoped"}},
                    {"include": "nested.yaml"},
                    {
                        "run": "cat {{ get_file('payload') }}",
                        "condition": "context.value == 'scoped'",
                    },
                    {"deploy": {"bins": ["example"]}},
                ]
            }
        },
    )
    recipe, _ = write_recipe(
        tmp_path,
        [{"template": {"name": "example"}}, {"run": "echo {{ context.value }}"}],
    )
    result = compile_recipe(recipe, include_dirs=(tmp_path,), architecture="x86_64")
    rendered = render_dockerfile(result.definition)
    assert "RUN echo scoped" in rendered
    assert "RUN echo outer" in rendered
    assert "readonly cat /.neurocontainer-cache/" in rendered
    assert 'DEPLOY_BINS="example"' in rendered


def test_group_condition_precedes_parameters_and_nested_values_restore(tmp_path):
    recipe, _ = write_recipe(
        tmp_path,
        [
            {
                "group": [
                    {"variables": {"value": "changed"}},
                    {"run": "echo {{ context.value }}"},
                    {
                        "group": [{"run": "echo {{ context.value }}"}],
                        "with": {"value": "inner"},
                    },
                    {"run": "echo {{ context.value }}"},
                ],
                "with": {"value": "supplied"},
                "condition": "context.value == 'outer'",
            },
            {"run": "echo {{ context.value }}"},
        ],
    )
    result = compile_recipe(recipe, architecture="x86_64")
    rendered = render_dockerfile(result.definition)
    assert rendered.count("RUN echo changed") == 2
    assert "RUN echo inner" in rendered
    assert "RUN echo outer" in rendered


@pytest.mark.parametrize("include_macro", [False, True])
@pytest.mark.parametrize("interpolate", [False, True])
def test_builtin_preserves_python_self_in_declared_file_contents(
    tmp_path, monkeypatch, include_macro, interpolate
):
    source = (
        "# release {{ self.version }}; literal self.value\n"
        "class Thing:\n"
        "    value = 'self.value'\n"
        "    def f(self):\n"
        "        return self.value\n"
    )
    if not interpolate:
        source = source.replace("{{ self.version }}", "1.2.3")
    directives = [{"file": {"name": "python-source", "contents": source}}]
    if include_macro:
        macros = tmp_path / "macros"
        macros.mkdir()
        (macros / "python.yaml").write_text(yaml.safe_dump({"directives": directives}))
        directives = [{"include": "python.yaml"}]
    monkeypatch.setattr(
        "builder.template_backend._load_template",
        lambda name: {"binaries": {"directives": directives}},
    )
    recipe, _ = write_recipe(
        tmp_path, [{"template": {"name": "example", "version": "1.2.3"}}]
    )
    result = compile_recipe(recipe, include_dirs=(tmp_path,), architecture="x86_64")
    contents = result.staging_plan.files["python-source"].contents
    expected = source.replace("{{ self.version }}", "1.2.3").rstrip("\n")
    assert contents == expected
    namespace = {}
    exec(contents, namespace)
    assert namespace["Thing"]().f() == "self.value"
