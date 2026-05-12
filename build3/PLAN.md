# build3 Plan

## Goal

Create a small isolated Python project that prototypes a new NeuroContainers builder based on the Go builder design in `../builder`, while preserving the operational behavior that this repository currently depends on.

The core goal is to remove external dependencies from Dockerfile generation. `build3` should generate Dockerfiles directly from recipe data and local Python code, without requiring NeuroDocker or any other external Dockerfile renderer at generation time.

This project should be independent from the existing `builder/` package until it has enough compatibility coverage to justify integration.

Current checkpoint: `build3 generate` must stay on the native Python rendering
path. The existing builder can be used as a parity-test oracle, but not as the
implementation path.

## Non-Goals

- `builder/build.py` has been removed; build3 is now the active builder backend.
- Do not change existing `sf-*` console scripts yet.
- Do not edit generated release files.
- Do not rewrite recipes to fit the new builder unless a recipe exposes an existing ambiguity that needs a documented compatibility decision.
- Do not require Docker, BuildKit, or Apptainer for basic parsing, validation, staging, and Dockerfile generation tests.
- Do not depend on NeuroDocker for Dockerfile generation.

## Compatibility Baseline

The first milestone is behavioral compatibility with the current Python builder for existing recipes. The Go builder is the design reference, but current repo behavior is the migration contract.

Must preserve:

- Recipe format from `recipes/*/build.yaml`.
- Jinja2 rendering with `context`, `local`, `arch`, and `parallel_jobs`.
- `{{ context.version }}` and top-level recipe variables.
- `{{ get_file("filename") }}` for declared files.
- `has_local()` and `get_local()` for optional local build contexts.
- Top-level `files`, `deploy`, `variables`, `readme`, `readme_url`, `categories`, `gui_apps`, `apptainer_args`, and `options`.
- Build directive forms currently accepted by build3.
- Architecture checks for `x86_64` and `aarch64`.
- Declared file caching/staging semantics, including downloaded file basename preservation where the current builder relies on it.
- Generated release JSON shape used by workflows and `releases/`.
- Container tester integration semantics, even if the first prototype only stubs the actual runtime execution.

## Proposed Project Layout

```text
build3/
  PLAN.md
  pyproject.toml
  README.md
  src/build3/
    __init__.py
    cli.py
    config.py
    recipe.py
    validation.py
    template.py
    ir.py
    dockerfile.py
    staging.py
    cache.py
    release.py
    tester.py
  tests/
    test_recipe_loading.py
    test_template_rendering.py
    test_ir_generation.py
    test_dockerfile_generation.py
    test_staging.py
    test_release.py
    fixtures/
```

Keep `build3` importable as its own package. Avoid importing from existing `builder/` except in explicit parity tests.

## Architecture

Use a small pipeline:

1. Load recipe YAML into typed Python models.
2. Validate syntax and structural constraints.
3. Render recipe values with Jinja2.
4. Convert directives into a simple intermediate representation.
5. Generate Dockerfile text from the IR.
6. Produce a staging plan for declared files and `COPY` sources.
7. Optionally build/test/release through separate adapters.

The IR should be intentionally narrow:

- `From`
- `Env`
- `Run`
- `RunWithMounts`
- `Copy`
- `Workdir`
- `User`
- `Entrypoint`
- `LiteralFile`

This mirrors the useful part of the Go builder while keeping Python implementation small.

## Phase 1: Skeleton and Read-Only Generation

Deliverables:

- `pyproject.toml` for a standalone package.
- CLI command equivalent to:

  ```bash
  python -m build3 generate <recipe>
  ```

- Recipe discovery from this repo's `recipes/` directory.
- Typed recipe loading.
- Basic validation.
- Jinja2 rendering with strict undefined variables.
- IR generation for:
  - `install`
  - `run`
  - `workdir`
  - `user`
  - `entrypoint`
  - `environment`
  - `copy`
  - `variables`
  - `group`
  - `include`
  - `file`
  - top-level `deploy`
- Dockerfile generation without building.
- No NeuroDocker dependency in the Dockerfile generation path.

Validation gate:

```bash
python -m build3 generate dcm2niix
python -m build3 generate template
python -m pytest build3/tests
```

## Phase 2: Staging and Cache Semantics

Deliverables:

- Staging plan for top-level and directive-level files.
- Local file staging.
- Literal file staging.
- HTTP download cache.
- `get_file()` BuildKit mount path support.
- `get_local()` named context support.
- Safe staging of `COPY` sources into an isolated build directory.

Compatibility requirements:

- Preserve current Python behavior for URL-derived guest filenames.
- Disambiguate duplicate downloaded basenames.
- Keep cache data outside container runtime paths.
- Never depend on `/home` for files needed at container runtime.

Validation gate:

```bash
python -m build3 stage dcm2niix
python -m build3 stage ants
python -m pytest build3/tests/test_staging.py
```

## Phase 3: Parity Tests Against Existing Builder

Historical parity tests compared build3 with the retired builder. New tests should validate build3 behavior directly.

Initial recipe set:

- `template`
- `dcm2niix`
- `ants`
- `afni`
- `connectomeworkbench`
- `bidscoin`
- `neurodesktop`
- one recipe using `files.filename`
- one recipe using `files.contents`
- one recipe using conditional variables
- one recipe using includes/macros

Compare:

- Recipe metadata after rendering.
- Effective architecture selection.
- Declared files and staged paths.
- Presence of required Dockerfile instructions.
- `DEPLOY_PATH` and `DEPLOY_BINS`.
- `README.md` and `build.yaml` inclusion.
- Release JSON output.

Avoid exact Dockerfile text comparison except for narrow formatting invariants.

## Phase 4: Build and Test Adapters

Deliverables:

- Docker build adapter.
- BuildKit/buildctl adapter.
- Optional Podman adapter.
- Container tester adapter.
- `sf-make`-style SIF path as a separate command.

The adapters should be thin. Generation and staging must remain testable without Docker.

Validation gate:

```bash
python -m build3 build dcm2niix --architecture x86_64
python -m build3 test dcm2niix --architecture x86_64
python -m build3 make dcm2niix --architecture x86_64
```

## Phase 5: Optional Go Builder Features

Only after Python compatibility is stable:

- Native template macro backend inspired by `../builder/pkg/recipe/template_specs`.
- Starlark directive support.
- LLB generation.
- Build event streaming.
- Web UI/API.
- Full `test-all` equivalent.

Each feature should land behind tests and should not perturb existing recipe behavior.

## Open Decisions

- How closely `build3` Dockerfile output should match current Python/NeuroDocker output while being generated without NeuroDocker.
- Whether to port existing NeuroDocker template behavior into local Python code, port Go macro templates, or support both through a local template backend.
- Whether Starlark is required for this repository, or only for future recipes.
- Whether `build3` should eventually become `builder/` or stay as a separate package with an adapter layer.
- How strict validation should be compared with the current permissive Python builder.

## Risks

- Dockerfile differences can cause real build regressions even when generated instructions look equivalent.
- Some recipes rely on NeuroDocker side effects, including startup script behavior and generated spec footer.
- Cache path changes can break `get_file()` users.
- `deploy` behavior affects NeuroDesk module loading and release tests.
- Full recipe validation may reveal existing recipes that only work because the current builder is permissive.

## Recommended First Implementation Slice

Start with the smallest useful vertical slice:

1. Load `recipes/dcm2niix/build.yaml`.
2. Render variables and directives.
3. Register top-level files.
4. Generate IR.
5. Emit a Dockerfile.
6. Produce a staging plan.
7. Assert `DEPLOY_PATH`, `get_file()`, `README.md`, and `build.yaml` behavior.

This gives a concrete path through the system without committing to full template macro parity on day one.
