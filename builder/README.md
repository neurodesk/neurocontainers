# NeuroContainers YAML Build System

## Setup

Either use [UV](https://docs.astral.sh/uv/) and `uv run` in front of the other commands or use `venv` with...

```sh
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
pip install -e .
```

`requirements.txt` pins the versions CI uses; `pip install -e .` alone is enough
to run the `sf-*` commands but not the validation and test tooling.

## Introduction

Run `sf-init <name> <version>` to create a new recipe.

Run `sf-generate <name>` to generate a `Dockerfile`. If your in the same directory as a recipe the name is optional and automatically detected.

Run `sf-build <name>` to generate, stage, and build the Docker image. Use `sf-test <name> --build` when you also want to run the smoke test after building.

Run `sf-make <recipe_dir>` to build a Singularity/Apptainer SIF using BuildKit without a Docker daemon. This generates a docker-archive and then builds a SIF (saved under `./sifs/`).

A common workflow involves building the container and running a command inside it. You can run `sf-login <name>` to build a container and immediately drop into a shell.

### Flattened base images

Set `build.flatten-base-image: true` when an upstream image has substantial
waste from deleted or replaced files in inherited layers. Staging resolves the
source digest, converts it to OCI, and uses BuildKit to copy its final filesystem
into one layer. This retains runtime configuration, including environment,
entrypoint, command, user, working directory, labels, and volume declarations.
Files inside declared volumes remain present. Only filesystem layer references
and their history associations change.

Flattening requires Docker Buildx or a configured `buildctl` daemon, plus the
Skopeo conversion described below. It works with either final build backend.
It also implies OCI conversion, so `convert-base-image` is optional when
flattening. The cache separates flattened images from ordinary conversions.
Large images need disk space for both the source and flattened OCI blobs while
staging. No image commands run during flattening.

### Legacy base images

Set `build.convert-base-image: true` when an upstream image uses Docker schema 1,
which current Docker versions cannot pull. Staging resolves the selected tag to
an immutable digest and converts that image into an OCI layout with Skopeo. It
preserves the upstream layers and runtime configuration. Other base images use
the ordinary registry build path.

Install `skopeo` for builds without Docker. When Docker is available, staging
runs a pinned Skopeo image as the current user. The converter reads public
registry images without credentials. Converted images are cached by source
digest, architecture, and converter identity under `httpcache/oci`; `sf-cache --all`
clears them. A native Skopeo binary is identified by its executable SHA-256.

The `sf-*` build commands pass the staged OCI context to Docker or BuildKit.
For a direct buildx invocation, load its arguments after staging:

```bash
python -m builder stage bidsappaa --recreate --download
python -m builder staged-context-args build/bidsappaa > /tmp/image-context-args
mapfile -d '' -t image_context_args < /tmp/image-context-args
docker buildx build build/bidsappaa --load \
  --file build/bidsappaa/bidsappaa_0.3.0.Dockerfile \
  --build-context neurocontainer-cache=build/bidsappaa/cache \
  "${image_context_args[@]}"
```

This uses Docker's [OCI layout context support](https://docs.docker.com/reference/cli/docker/buildx/build/#use-an-oci-layout-directory-as-build-context).
The layout stays outside the primary Docker build context through `.dockerignore`.

### Architecture Options

All build commands (`sf-build`, `sf-login`, `sf-test`, `sf-make`) support architecture options:

- `--architecture <arch>`: Specify the target architecture (e.g., `x86_64`, `aarch64`, `arm64`). Defaults to the current machine's architecture.
- `--variant <name>`: Build an arbitrary named alternative declared by the recipe, such as `gpu` or `cuda12`.
- `--ignore-architectures`: Ignore architecture compatibility checks in the recipe.
- `--generate-release`: Generate release files after successful build (for `sf-build` and `sf-login`).

Example usage:
```sh
# Build for ARM64 architecture
sf-build myrecipe --architecture aarch64

# Equivalent: build the automatically named myrecipe_arm64 container
sf-build myrecipe --variant arm64

# Build a declared GPU alternative
sf-build myrecipe --variant gpu

# Build ignoring architecture restrictions
sf-build myrecipe --architecture x86_64 --ignore-architectures

# Build and generate release files
sf-build myrecipe --generate-release

# Make a SIF via BuildKit + Apptainer
sf-make ./recipes/myrecipe
```

## Recipe Syntax

Recipes are written in [YAML](https://en.wikipedia.org/wiki/YAML) and most fields support [jinja](https://jinja.palletsprojects.com/en/stable/) for template variables.

Recipes are split into a few sections...

### Metadata

This is `name`, `version`, `architectures`, and `readme`.

The optional `copyright` list records upstream license information. The `license`
field accepts an [SPDX identifier or expression](https://spdx.org/licenses/).
Custom `LicenseRef-` identifiers require a `url`. SPDX values `NOASSERTION` and
`NONE` also require a URL and do not identify a license for DOI publication.

For a custom license, `name` and `url` preserve its title and terms. To publish
that recipe through Neurocommand's Zenodo integration, also set `license` to
`other-open` or `other-closed`, according to the upstream terms:

```yaml
copyright:
  - license: other-closed
    name: Academic non-commercial research license
    url: https://example.org/LICENSE
```

Zenodo uses a controlled license vocabulary. The publisher validates its license
ID before it creates a draft and includes the original title and URL in the
record description. A `LicenseRef-` identifier or SPDX expression can be valid
recipe metadata without being a supported Zenodo ID. Suggested IDs in an error
message are candidates for review, not replacements for the upstream terms.

```yaml
name: qsmxt
version: 8.0.0

copyright:
  - license: GPL-3.0-only # has to be SPDX Identifier
    url: https://github.com/QSMxT/QSMxT/blob/main/LICENSE

architectures:
  - x86_64
  - aarch64

variants:
  gpu:
    architectures:
      - x86_64
      - aarch64
    description: CUDA-enabled build
    options:
      gpu: true

options:
  gpu:
    description: Install GPU dependencies
    default: false

readme: |
    This is a recipe for {{ context.name }}/{{ context.version }}
```

Architectures are concrete variants automatically: `aarch64` produces
`qsmxt_arm64`. Declared alternatives are independent of architecture, so the
example above produces `qsmxt_gpu` and `qsmxt_gpu_arm64`. Variant `options`
preset the same boolean recipe options used by `context.options.<name>`
conditions.

`build_default` defaults to `true`. Setting it to `false` excludes unnamed
builds on every architecture, leaving only the declared `variants`. At least
one variant must remain. Build a retained variant with
`sf-build spinalcordtoolbox --variant lite`.

Each concrete identity is a container in its own right: it builds as its own PR
candidate, publishes to its own `ghcr.io`/`quay.io` repository, and gets its own
`releases/<container>/<version>.json`. Adding `aarch64` to a recipe therefore
adds an ARM64 build to every PR that touches it, so declare it only once the
recipe actually builds on ARM64.

If your readme is externally hosted you can include it using `readme_url`.

```yaml
readme_url: https://raw.githubusercontent.com/QSMxT/QSMxT/main/docs/container_readme.md
```

### Variables

The variables declared in the `variables` block can be referenced anywhere else in the recipe.

```yaml
variables:
  conda_version: "4.12.0"
  conda_download_url:
    try:
      - value: "https://repo.anaconda.com/miniconda/Miniconda3-py37_{{ context.conda_version }}-Linux-x86_64.sh"
        condition: arch=="x86_64"
      - value: "https://repo.anaconda.com/miniconda/Miniconda3-py37_{{ context.conda_version }}-Linux-aarch64.sh"
        condition: arch=="aarch64"
```

For example we can use these variables to download Miniconda...

```yaml
build:
  # ...
  directives:
    # ...
    - run:
      - export PATH="/opt/miniconda-{{ context.conda_version }}/bin:$PATH"
      - echo "Downloading Miniconda installer ..."
      - conda_installer="/tmp/miniconda.sh"
      - curl -fsSL -o "$conda_installer" {{ context.conda_download_url }}
      - bash "$conda_installer" -b -p /opt/miniconda-{{ context.conda_version }}
      - rm -f "$conda_installer"
```

### Build

The build section includes `kind` and is otherwise passed to the builder for processing. The only builder kind currently supported is `neurodocker`. More details for the syntax in this section are specified later in the document.

```yaml
build:
  kind: neurodocker

  base-image: ubuntu:22.04
  pkg-manager: apt

  directives:
    - environment:
        DEBIAN_FRONTEND: noninteractive
```

To copy files from another image, set `from_image` on a `copy` directive.
Set `from_platform` when the source image uses a different architecture from the
container being built. The builder creates a source stage and copies files
without executing the source image or looking for those files locally.

```yaml
- copy:
    - /app/src
    - /opt/tool/src
  from_image: ghcr.io/example/tool:{{ context.upstream_version }}
  from_platform: linux/amd64
```

`from_platform` requires `from_image`. Repeated copies from the same image and
platform share one source stage. Image references participate in automatic
update binding checks.

### Deploy

NeuroDesk includes Transparent Singularity to automatically generate loadable modules from Singularity/Apptainer containers. The deploy section adds environment variables to the container to control this.

```yaml
deploy:
  path:
    - /opt/ants-2.4.3/
    - /opt/FastSurfer
    - /opt/QSMxT-UI
  bins:
    - nipypecli
    - bet
```

`path` adds all binaries in each directory to the path when the module is loaded.

`bins` adds single binaries in the `PATH` of the container.

### Files

Additional files need to be declared to be added to the build.

```yaml
files:
  - name: install.packages.jl
    contents: |
      using Pkg
      ENV["JULIA_PKG_PRECOMPILE_AUTO"]=0
      Pkg.add(Pkg.PackageSpec(name="ArgParse", version=v"1.1.5"))
```

Files can either be added inline or loaded from a local file next to the `build.yaml` file.

```yaml
files:
  - name: hello.sh
    filename: hello.sh
```

Files can be downloaded from the internet. Files will be cached locally on the system and reused between builds...

```yaml
files:
  - name: hello.zip
    filename: https://example.com/hello.zip
```

Files can be referenced directly in run directives with...

```yaml
build:
  # ...
  directives:
    # ...
    - run:
      # Install cat12
      - unzip -q {{ get_file("hello.zip") }} -d /tmp
```

## NeuroContainers Builder

Builder is the active local Python Dockerfile generator for NeuroContainers. It reads recipe YAML directly and does not shell out to NeuroDocker.

The simplest way to explain the format is with a series of examples. More advanced directives are also supported but the following should be enough for most applications.

### Basic Metadata

```yaml
build:
  kind: neurodocker

  base-image: ubuntu:22.04
  pkg-manager: apt

  directives:
    - run:
      - echo Hello, World
```

A do nothing build block looks like the above YAML. You need at least `kind`, `base-image`, and `pkg-manager`.

The base image can be any Docker image based on Debian or Red Hat.

Builder accepts `apt` or `rpm` as the package manager.

### Environment Directive

Environment variables set environmental variables inside the container.

They are specified as key value paris and support template replacement.

```yaml
build:
  # ...
  directives:
    # ...
    - environment:
        DEBIAN_FRONTEND: noninteractive
```

### Workdir Directive

Workdir sets the current working directory.

```yaml
build:
  # ...
  directives:
    # ...
    - workdir: /opt/bet2
```

### Run Directive

The run directive runs a series of commands. These are run as if they are a single layer with `&&` between each line.

You should try to combine run directives where possible. Files added in one layer and deleted in the next are hidden from the final image rather than being truly deleted.

```yaml
build:
  # ...
  directives:
    # ...
    - run:
        - export PATH="/opt/miniconda-{{ context.conda_version }}/bin:$PATH"
        - echo "Downloading Miniconda installer ..."
        - conda_installer="/tmp/miniconda.sh"
        - curl -fsSL -o "$conda_installer" {{ context.conda_download_url }}
        - bash "$conda_installer" -b -p /opt/miniconda-{{ context.conda_version }}
        - rm -f "$conda_installer"
```

### Install Directive

The install directive is used as a wrapper for `dnf` and `apt-get`. You should always use the install directive since it handles updating and removing caches automatically.

```yaml
build:
  # ...
  directives:
    # ...
    - install:
        bzip2 ca-certificates unzip cmake dbus-x11 libgtk2.0-0 git graphviz wget
```

### Template Directive

Builder includes local templates for the subset of template directives used by current NeuroContainers recipes. Template methods are stored as macro-style `builder: neurodocker` directive fragments, with method-specific arguments and URLs alongside the directives.

```yaml
build:
  # ...
  directives:
    # ...
    - template:
        name: ants
        version: 2.4.3
        method: source
```

### Copy Directive

The copy directive copies a file from the definition into a container. The following syntax is preferred since it gracefully handles variable references.

```yaml
build:
  # ...
  directives:
    # ...
    - copy:
      - install.packages.jl
      - /opt
```

## Shared fragments

`include: macros/example/setup.yaml` and `include: example/setup.yaml` both load
`macros/example/setup.yaml`. A fragment is a YAML mapping with a `directives`
list. Paths must stay inside a configured repository's `macros/` directory,
including after resolving symlinks. Missing fragments, malformed directives,
and include cycles fail validation or compilation with the offending path.

For example, a recipe can call a parameterized fragment:

```yaml
- include: macros/example/setup.yaml
  with:
    install_path: /opt/example
    enabled: true
```

The fragment can declare an asset beside its own YAML file and call another
fragment. `helper.sh` below belongs to `macros/example/`, and the nested include
path starts at the repository macro root:

```yaml
# macros/example/setup.yaml
directives:
  - file:
      name: example-helper
      filename: helper.sh
  - include: example/dependencies.yaml
  - run: cp {{ get_file("example-helper") }} {{ context.install_path }}/helper.sh
    condition: context.enabled
```

`file.filename` resolves beside its declaring fragment, including files inside
nested groups. `get_file` uses the ordinary cached build context and read-only
BuildKit mount. Runtime assets must be copied to a runtime path such as `/opt`.

`include.with` and `group.with` render their values in the caller's context,
then overlay those values for the entire nested directive list. Nested scopes
can override outer values. Variables declared inside the list remain visible
to subsequent directives in that list; all variable changes disappear when the
scope exits. A scope's own `condition` runs before applying its `with` values.
Child conditions run inside the scope. Include paths can use the scoped values.

Builtin `template` calls select a method and its argument defaults, then send
that method's directives through the same interpreter as ordinary fragments.
The compatible `self.*` method helpers, dependencies, package manager, URL maps,
and architecture arguments remain available. Builtins can declare files, include
macros, set scoped variables, and use directive conditions. Method bodies require
`directives`; the obsolete `env`/`instructions` representation is rejected.
There is no external NeuroDocker runtime dependency.

Builtin archive downloads can migrate individually to declared cached files.
FreeSurfer already accepts a recipe-declared archive. Remaining builtin download
commands stay in their method bodies pending source-pin and cache migrations.
Recipe source versions and updater policies continue to belong to recipes.

### Shared inputs and releases

Static discovery tracks literal template names inside groups, include paths,
nested fragments, and fragment-owned local files. It reads YAML without rendering
Jinja or evaluating conditions. Revision-based release operations read fragment
contents from that Git revision. Dynamic include or template names conservatively
depend on both the entire `macros/` and `builder/templates/` trees, so nested calls
cannot hide a dependency. Literal references exclude unrelated templates.

Consumer selection, recipe fingerprints, and build dates use this dependency
information. Explicit `auto_update.local` macro roots retain their historical
ordering and overlap in fingerprints. Conditions, architectures, and named
variants do not narrow static dependencies: every possible consumer is rebuilt,
and candidate CI selects its declared variants through the existing build matrix.

Template-only and template-plus-recipe changes can enter candidate publication
when every changed path belongs to the selected recipes or their shared inputs.
Unrelated builder Python changes still fail the recipe-only publication policy.
Selection considers dependencies from both the base and head revision, including
deleted or renamed fragment paths. A missing required input fails generation or
fingerprinting instead of substituting another template.
