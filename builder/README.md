# NeuroContainers YAML Build System

## Setup

Either use [UV](https://docs.astral.sh/uv/) and `uv run` in front of the other commands or use `venv` with...

```sh
python3 -m venv env
source env/bin/activate
pip install -e .
```

## Introduction

Run `sf-init <name> <version>` to create a new recipe.

Run `sf-generate <name>` to generate a `Dockerfile`. If your in the same directory as a recipe the name is optional and automatically detected.

Run `sf-build <name>` to generate and build the `Dockerfile`. This should be used instead of `docker build` since the context is generated in a separate `build` directory.

Run `sf-make <recipe_dir>` to build a Singularity/Apptainer SIF using BuildKit without a Docker daemon. This generates a docker-archive and then builds a SIF (saved under `./sifs/`).

A common workflow involves building the container and running a command inside it. You can run `sf-login <name>` to build a container and immediately drop into a shell.

### Architecture Options

All build commands (`sf-build`, `sf-login`, `sf-test`) support architecture options:

- `--architecture <arch>`: Specify the target architecture (e.g., `x86_64`, `aarch64`, `arm64`). Defaults to the current machine's architecture.
- `--ignore-architectures`: Ignore architecture compatibility checks in the recipe.
- `--generate-release`: Generate release files after successful build (for `sf-build` and `sf-login`).

Example usage:
```sh
# Build for ARM64 architecture
sf-build myrecipe --architecture aarch64

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

You can optionally add `copyright` with license information. The license has to be a SPDX identifier from https://spdx.org/licenses/.

```yaml
name: qsmxt
version: 8.0.0

copyright:
  - license: GPL-3.0-only # has to be SPDX Identifier
    url: https://github.com/QSMxT/QSMxT/blob/main/LICENSE

architectures:
  - x86_64
  - aarch64

readme: |
    This is a recipe for {{ context.name }}/{{ context.version }}
```

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

## NeuroDocker Builder

[NeuroDocker](https://github.com/ReproNim/neurodocker) is a command line tool for easily generating Dockerfiles from a structured series of command line arguments.

The YAML builder generates these command lines automatically so you don't need to manually run NeuroDocker.

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

NeuroDocker only accepts `apt` or `rpm` as the package manager.

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

NeuroDocker has a powerful set of templates for installing existing software. You should refer to the NeuroDocker documentation for a list and usage.

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
