# CLAUDE.md - NeuroContainers Development Guide

## Project Overview

NeuroContainers is a Python-based automated system for building, testing, and distributing Docker and Singularity/Apptainer containers for neuroscience applications through [NeuroDesk](https://neurodesk.org). It uses declarative YAML recipes processed by [NeuroDocker](https://github.com/ReproNim/neurodocker) to generate Dockerfiles.

The repository contains ~190 container recipes for neuroscience tools (FSL, FreeSurfer, AFNI, ANTs, MRtrix3, etc.) with full CI/CD automation for building, testing, and publishing to multiple container registries.

## Repository Structure

```
recipes/                  # Container recipes (~190 neuroscience tools)
  <toolname>/
    build.yaml            # Container definition (required)
    test.yaml             # Test configuration (optional)
    README.md             # Tool documentation (optional)
    *.sh, *.py, etc.      # Supporting files referenced by build.yaml

builder/                  # Python build system
  build.py                # Main CLI: generate, build, test, login, init, make, cache
  validation.py           # Recipe schema validation (attrs-based, matches UI zod schema)
  check_version.py        # Automated version update checker (weekly)
  tester/                 # Go-based container tester (built on-demand during sf-build)
    main.go               # Tester entry point
    Dockerfile            # Tester helper image

workflows/                # Testing infrastructure (Python + bash)
  test_all.sh             # Validate all recipes (validation + check-only build)
  validate_all.sh         # Schema-only validation of all recipes
  container_tester.py     # Multi-runtime container test executor
  test_runner.py          # High-level test runner with result reporting
  pr_test_runner.py       # PR-specific test automation
  full_container_test.py  # Full matrix testing across all containers

releases/                 # Auto-generated release files (JSON metadata)
  <toolname>/
    <version>.json

tools/                    # Utilities
  generate_apps_json.py   # Generate apps.json for neurocommand integration
  generate_webapps_json.py

macros/                   # Shared recipe macros (e.g., openrecon)
dashboard/                # Status dashboard
.github/workflows/        # CI/CD automation (17 workflow files)
```

## Environment Setup

```bash
python3 -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

Requires Python 3.10+. Uses `uv` as an alternative (`uv run sf-build <name>`).

## Key Commands

### Recipe Development

```bash
sf-init <name> <version>                  # Create new recipe from template
sf-generate <name>                        # Generate Dockerfile from recipe
sf-build <name>                           # Generate + build Docker image + run tests
sf-login <name>                           # Build and drop into interactive shell
sf-test <name>                            # Run container tests
sf-make <recipe_dir>                      # Build SIF via BuildKit (no Docker daemon)
```

### Build Options

```bash
sf-build <name> --architecture aarch64    # Build for ARM64
sf-build <name> --ignore-architectures    # Skip architecture checks
sf-build <name> --generate-release        # Generate release JSON after build
```

### Direct build.py Usage

```bash
python builder/build.py generate <name> --recreate --check-only     # Validate only
python builder/build.py generate <name> --recreate --build --test   # Full build+test
python builder/build.py generate <name> --recreate --build --test --timeout 3600
```

### Validation (Run Before Committing)

```bash
codespell .                                          # Spell check
source env/bin/activate && ./workflows/test_all.sh   # Validate + check-only all recipes
python3 builder/validation.py recipes/<name>/build.yaml  # Validate single recipe
```

## Recipe Format (build.yaml)

Recipes are YAML files with Jinja2 templating. Key sections:

```yaml
name: toolname
version: 1.0.0

copyright:
  - license: MIT                    # SPDX identifier required
    url: https://example.com/license

architectures:
  - x86_64
  - aarch64                         # Optional ARM64 support

variables:                          # Template variables with conditional logic
  download_url:
    try:
      - value: "https://example.com/tool-x86_64.tar.gz"
        condition: arch=="x86_64"
      - value: "https://example.com/tool-aarch64.tar.gz"
        condition: arch=="aarch64"

build:
  kind: neurodocker                 # Only supported builder kind
  base-image: ubuntu:24.04          # Debian/Ubuntu/RedHat base images
  pkg-manager: apt                  # apt or rpm

  directives:
    - environment:                  # Set env vars
        DEBIAN_FRONTEND: noninteractive
    - install:                      # System packages (via apt/rpm)
        - curl
        - wget
    - template:                     # NeuroDocker templates (ants, fsl, etc.)
        name: ants
        version: "{{ context.version }}"
    - workdir: /opt/tool            # Set working directory
    - run:                          # Shell commands (joined with &&)
        - curl -fLO https://example.com/tool.tar.gz
        - tar -xzf tool.tar.gz
        - rm tool.tar.gz
    - copy:                         # Copy declared files into container
        - script.sh
        - /opt/

deploy:
  bins:                             # Individual executables to expose
    - toolname
  path:                             # Directories with executables
    - /opt/tool/bin

files:                              # Declare files for build context
  - name: script.sh
    filename: script.sh             # Local file next to build.yaml
  - name: config.txt
    contents: |                     # Inline content
      key=value

categories:                         # NeuroDesk UI categories
  - "structural imaging"

readme: |                           # Inline documentation (supports Jinja2)
  ## toolname/{{ context.version }}
  Description here.
```

### Test Format (test.yaml)

```yaml
tests:
  - name: Test tool version
    script: |
      toolname --version
  - name: Test basic functionality
    script: |
      toolname --help
```

## Validation Schema

Recipe validation (`builder/validation.py`) enforces:
- **Architectures**: Must be `x86_64` or `aarch64`
- **Categories**: Must match predefined list (~35 categories including "image registration", "structural imaging", "diffusion imaging", "data organisation", etc.)
- **Licenses**: Should use SPDX identifiers
- **Required fields**: `name`, `version`, `build` (with `kind`, `base-image`, `pkg-manager`, `directives`), `deploy`

The validation schema matches the Zod schema from `neurocontainers-ui`.

## CI/CD Workflows

### Automatic Build Pipeline
- **auto-build.yml**: Triggers on pushes to `main` modifying `recipes/**`; detects changed recipes and builds them
- **build-app.yml**: Reusable workflow - generates Dockerfile, builds Docker image, pushes to GHCR + DockerHub, converts to SIF, uploads to S3 + Nectar, creates release PR

### Validation
- **validate-recipes.yml**: Runs on PRs; validates changed recipe YAML files
- **test-builder.yml**: Tests builder code changes

### Testing
- **test-release-pr.yml**: Tests containers when release files are modified in PRs
- **full-container-test.yml**: Manual dispatch; tests all released containers in matrix
- **recipes-ci.yml**: Manual dispatch; builds and tests recipes in isolated Apptainer containers

### Other
- **auto-update.yml**: Weekly check for upstream version updates (Monday 3 AM)
- **manual-build.yml**: Manual workflow dispatch for custom builds
- **update-apps-json.yml / update-webapps-json.yml**: Regenerate app metadata after release merges

### Build Configuration
`.github/workflows/build-config.json` controls per-app CI behavior:
```json
{
  "default": { "autoBuild": true, "freeUpSpace": false },
  "qsmxt": { "autoBuild": true, "freeUpSpace": true }
}
```

## Container Distribution

Built containers are distributed to:
1. **GHCR** (ghcr.io/neurodesk/): Primary Docker registry
2. **DockerHub** (neurodesk/): Mirror
3. **AWS S3** (neurocontainers bucket): SIF files
4. **Nectar Object Storage**: SIF files for Australian users

## Code Conventions

### YAML Recipes
- 2-space indentation
- Use Jinja2 `{{ context.version }}` and `{{ context.variable_name }}` for templating
- Use `{{ get_file("filename") }}` to reference declared files in run directives
- Combine run directives where possible (each becomes a single Docker layer)
- Clean up temporary files in the same layer they are created
- Pin exact versions for reproducibility
- Use HTTPS for all downloads

### Python Code
- PEP 8 style
- Type hints (Python 3.10+ style)
- Standard import order: stdlib, third-party, local

### Commit Messages
- Descriptive, explain what and why
- Reference issue numbers when applicable (`Fix #123: ...`)
- Skip CI with `[skipci]` in commit message when needed

## Common Development Tasks

### Add a New Container Recipe
1. `sf-init <toolname> <version>` to scaffold
2. Edit `recipes/<toolname>/build.yaml` with build instructions
3. Optionally create `recipes/<toolname>/test.yaml`
4. Validate: `python builder/build.py generate <toolname> --recreate --check-only`
5. Build and test: `sf-build <toolname>` or `sf-login <toolname>` for interactive debugging

### Update a Container Version
1. Update `version` field in `build.yaml`
2. Update any version-specific URLs or checksums
3. Validate and rebuild

### Debug a Failing Build
1. `python builder/build.py generate <name> --recreate --check-only` to inspect generated Dockerfile
2. Read the Dockerfile at `build/<name>/<name>_<version>.Dockerfile`
3. `sf-login <name>` to get an interactive shell in the built container
4. For CI failures, check GitHub Actions logs

### Validate All Recipes
```bash
./workflows/test_all.sh              # Full validation + check-only build
./workflows/validate_all.sh          # Schema validation only
```

## Important Notes

- Container builds can take 1-60+ minutes depending on complexity. Do not cancel prematurely.
- Some existing recipes have known YAML issues or broken builds from upstream changes. Focus on your changes rather than fixing unrelated existing issues.
- The `template` recipe (recipes/template/) serves as the reference example.
- NeuroDocker templates exist for many common neuroscience tools - check NeuroDocker docs before writing custom install steps.
- The `deploy` section controls how tools appear as loadable modules in NeuroDesk via Transparent Singularity.
- Release files in `releases/` are auto-generated; do not edit manually.

## External Documentation

- [Adding New Tools to NeuroDesk](https://www.neurodesk.org/developers/new_tools/)
- [NeuroContainers Documentation](https://www.neurodesk.org/docs/getting-started/neurocontainers/)
- [NeuroDocker GitHub](https://github.com/ReproNim/neurodocker)
- [Builder README](builder/README.md) - Detailed recipe syntax reference
