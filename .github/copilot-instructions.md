# NeuroContainers - Container Build System

NeuroContainers is a Python-based automated system for building and testing Docker and Singularity/Apptainer containers for neuroscience applications distributed through NeuroDesk. The system uses YAML recipes to generate Dockerfiles via NeuroDocker and includes comprehensive testing and release automation.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap Environment
Always set up the Python environment before doing any work:
```bash
python3 -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```
- Environment setup takes ~45 seconds
- Python 3.10+ required (tested with 3.12+)
- Virtual environment is essential for isolating dependencies

### Build Containers
Generate and build containers using the builder system:
```bash
# Generate Dockerfile only (check syntax/validate recipe)
python builder/build.py generate <recipe-name> --recreate --check-only

# Build container and test it - NEVER CANCEL: builds take 1-45+ minutes depending on complexity
python builder/build.py generate <recipe-name> --recreate --build --test --timeout 3600

# Quick build for simple containers (template: ~60 seconds)
python builder/build.py generate template --recreate --build --test

# Complex neuroimaging tools can take 30+ minutes - be patient
python builder/build.py generate afni --recreate --build --test

# Generate release files after successful build
python builder/build.py generate <recipe-name> --recreate --build --generate-release
```

### Command-Line Tools
The builder package provides several command-line tools:
```bash
# Using sf- prefixed commands (requires pip install -e .)
sf-generate <recipe-name>           # Generate Dockerfile
sf-build <recipe-name>              # Generate and build
sf-login <recipe-name>              # Build and start interactive shell
sf-test <recipe-name>               # Test container
sf-init <recipe-name> <version>     # Create new recipe template

# Architecture options
sf-build <recipe-name> --architecture aarch64                # Build for ARM64
sf-build <recipe-name> --ignore-architectures               # Skip architecture checks
```

### Test Containers
Use the comprehensive testing system:
```bash
# Test using convenience wrapper
./test-containers.sh help                    # Show all testing commands
./test-containers.sh list                    # List containers in CVMFS
./test-containers.sh test <container:version> # Test specific container
./test-containers.sh test-recipe <recipe>    # Test using recipe config
./test-containers.sh test-pr                 # Test containers in PR changes

# Direct testing (more control)
sf-test-remote <name> --version <version> --runtime docker --location local --cleanup
sf-test-remote <name> --version <version> --test-config recipes/<name>/test.yaml --cleanup
```

### Validation and Linting
Always run validation before committing:
```bash
# Spell checking - runs in ~1.5 seconds
codespell .

# Recipe validation - check all recipes in ~5 seconds  
source env/bin/activate && ./workflows/test_all.sh

# Test specific recipe generation
python builder/build.py generate <recipe-name> --recreate --check-only
```

## Timing Expectations and Critical Warnings

### Build Times - NEVER CANCEL
- **Simple containers (template, datalad)**: 60 seconds - 2 minutes
- **Medium complexity (most tools)**: 5-15 minutes  
- **Complex neuroimaging tools (AFNI, FSL, FreeSurfer)**: 15-45+ minutes
- **Compilation from source**: Can exceed 60 minutes

**CRITICAL**: Set timeouts of 3600+ seconds (60+ minutes) for build commands. Builds may appear to hang but are often downloading large datasets or compiling software. Never cancel builds before 60 minutes have elapsed.

### Other Command Times
- Environment setup: ~45 seconds
- Generate Dockerfile only: <1 second
- Recipe validation (test_all.sh): ~5 seconds  
- Spell checking: ~1.5 seconds
- Container testing: 30 seconds - 5 minutes

## Repository Structure

### Key Directories
```
recipes/              # Container recipes (YAML definitions)
├── template/         # Example/template recipe
├── afni/            # AFNI neuroimaging software
├── fsl/             # FSL neuroimaging tools
└── ...              # 100+ neuroscience applications

builder/             # Python build system
├── build.py         # Main build script
├── container_tester.py  # Container testing tool
├── pr_test_runner.py    # PR validation
└── test_all.sh      # Recipe validation script

.github/workflows/   # CI/CD automation
├── build-app.yml    # Main container build workflow
├── test-builder.yml # Builder testing
└── ...

releases/           # Generated release files (JSON)
build/              # Generated Dockerfiles and build contexts
tools/              # Additional utilities
```

### Recipe Structure
Each recipe contains:
- `build.yaml` - Container definition (required)
- `test.yaml` - Test configuration (optional)
- `README.md` - Documentation (optional)
- Additional files (scripts, configs, etc.)

## Code Style and Conventions

### Python Code Style
- Follow PEP 8 style guidelines for Python code
- Use type hints where appropriate (Python 3.10+ style)
- Import statements at the top of files in standard order: stdlib, third-party, local
- Use descriptive variable names and add comments for complex logic
- Keep functions focused and modular

### YAML Recipe Style
- Use 2-space indentation for YAML files
- Keep recipe files organized with clear sections: metadata, variables, build, deploy, files
- Use descriptive names for variables and files
- Add comments to explain complex build steps or workarounds
- Validate YAML syntax before committing using `--check-only` flag

### Commit Messages
- Use descriptive commit messages that explain what and why
- Reference issue numbers when applicable (e.g., "Fix #123: Update AFNI recipe")
- Keep commits focused on a single logical change

### Documentation
- Update README.md files in recipe directories when adding new features
- Keep inline comments concise but informative
- Document any non-obvious dependencies or requirements

## Contribution Workflow

### Making Changes
1. Create a new branch from `main` for your changes
2. Set up the development environment using the Bootstrap instructions
3. Make focused, minimal changes to address the specific issue
4. Test changes thoroughly before committing

### Pull Request Process
1. Run validation before creating PR:
   - `codespell .` for spell checking
   - `./workflows/test_all.sh` for recipe validation
   - Build and test any modified containers
2. Create PR with descriptive title and detailed description
3. Ensure CI workflows pass (build, test, validation)
4. Address review feedback promptly
5. Keep PR scope focused - avoid unrelated changes

### CI/CD Integration
- GitHub Actions automatically build changed recipes on PR
- All validation checks must pass before merge
- Build failures are often due to network timeouts or upstream source changes
- Check workflow logs for detailed error information

### Review Guidelines
- PRs require approval before merging
- Reviewers check: recipe syntax, build success, test coverage, documentation
- Breaking changes require explicit justification
- Security considerations are mandatory for new dependencies

## Security Considerations

### Container Security
- Use official base images from trusted sources (Ubuntu, Debian, NeuroDebian)
- Specify exact versions for base images and dependencies
- Avoid running containers as root when possible
- Clean up temporary files and caches in the same layer they're created
- Minimize the number of layers and image size

### Dependency Management
- Pin versions for all dependencies in recipes
- Verify checksums for downloaded files when possible
- Use HTTPS for all downloads
- Document licensing information in the recipe copyright section
- Avoid including secrets or credentials in recipes or containers

### Recipe Security Best Practices
- Review upstream source code changes before updating versions
- Test containers in isolated environments before production
- Document known security considerations in recipe README
- Use the SPDX identifier for license declarations

## Recipe Development Guidelines

### Creating a New Recipe

#### Step 1: Initialize
```bash
sf-init <toolname> <version>
```
This creates the basic recipe structure in `recipes/<toolname>/`

#### Step 2: Configure Metadata
Edit `recipes/<toolname>/build.yaml`:
- Set `name`, `version`, and `architectures`
- Add copyright/license information (SPDX identifiers)
- Write or link a README explaining the tool

#### Step 3: Define Build Steps
Configure the build section:
- Choose appropriate `base-image` (Ubuntu LTS versions preferred)
- Set `pkg-manager` to `apt` or `rpm`
- Use directives in logical order:
  1. `environment` - Set environment variables
  2. `install` - Install system packages
  3. `template` - Use NeuroDocker templates when available
  4. `run` - Execute installation commands
  5. `copy` - Add files from recipe directory

#### Step 4: Configure Deployment
Set up the deploy section:
- `bins`: List individual executables to expose
- `path`: List directories containing multiple executables
- These control how modules load in NeuroDesk

#### Step 5: Validate and Test
```bash
# Validate recipe syntax
python builder/build.py generate <toolname> --recreate --check-only

# Build and test locally
python builder/build.py generate <toolname> --recreate --build --test

# Test interactively
sf-login <toolname>
```

### Modifying Existing Recipes

#### Version Updates
- Update the `version` field in `build.yaml`
- Update download URLs and checksums
- Test that the new version builds and functions correctly
- Document any breaking changes in the recipe README

#### Adding Features
- Keep changes minimal and focused
- Test that existing functionality still works
- Update tests if behavior changes
- Document new features in recipe README

#### Fixing Build Issues
- Check if the issue is upstream (source unavailable, moved URLs)
- Look for architecture-specific problems
- Use `sf-login` to debug interactively
- Document workarounds in comments

### Testing Strategy
Always test recipes thoroughly:
```bash
# Basic validation
python builder/build.py generate <recipe> --recreate --check-only

# Full build with testing
python builder/build.py generate <recipe> --recreate --build --test

# Interactive testing
sf-login <recipe>

# Test specific functionality
docker run --rm <container>:<version> <primary-command> --version
docker run --rm <container>:<version> <primary-command> --help
```

### Common Recipe Patterns

#### Installing from Source
```yaml
- run:
    - cd /tmp
    - curl -fsSL -O https://example.com/tool.tar.gz
    - tar -xzf tool.tar.gz
    - cd tool-1.0
    - ./configure --prefix=/opt/tool-1.0
    - make -j$(nproc)
    - make install
    - rm -rf /tmp/tool*
```

#### Using Conda/Mamba
```yaml
- template:
    name: miniconda
    version: latest
    env_name: myenv
    conda_install:
      - numpy
      - scipy
      - matplotlib
```

#### Python Package Installation
```yaml
- run:
    - pip install --no-cache-dir package-name==1.2.3
```

#### Architecture-Specific Downloads
```yaml
variables:
  download_url:
    try:
      - value: "https://example.com/tool-x86_64.tar.gz"
        condition: arch=="x86_64"
      - value: "https://example.com/tool-aarch64.tar.gz"
        condition: arch=="aarch64"
```

## Common Tasks

### Create New Recipe
```bash
# Initialize new recipe template
sf-init <toolname> <version>

# Edit the generated recipes/<toolname>/build.yaml
# Test the recipe
python builder/build.py generate <toolname> --recreate --check-only
python builder/build.py generate <toolname> --recreate --build --test
```

### Debug Build Issues
```bash
# Generate Dockerfile and examine it
python builder/build.py generate <recipe> --recreate --check-only
cat build/<recipe>/<recipe>_<version>.Dockerfile

# Build with verbose Docker output
cd build/<recipe>
docker build -f <recipe>_<version>.Dockerfile .

# Interactive debugging
sf-login <recipe>  # Drops into container shell after build
```

### Validate Changes
```bash
# Full validation workflow
source env/bin/activate
./workflows/test_all.sh                    # Validate all recipes (~5 seconds)
codespell .                             # Check spelling (~1.5 seconds)  
python workflows/pr_test_runner.py        # Test PR changes

# Test specific container end-to-end
./test-containers.sh test-recipe <recipe-name>
```

### Manual Container Testing
After building containers, always test functionality:
```bash
# Test container directly
docker run --rm <container>:<version> <command>

# Examples
docker run --rm template:1.1.5 datalad --version
docker run --rm afni:25.2.03 afni_proc.py -help
docker run --rm fsl:6.0.7.6 bet

# Interactive testing
docker run --rm -it <container>:<version> /bin/bash
```

## Build System Details

### NeuroDocker Integration
The system uses NeuroDocker to generate Dockerfiles from YAML recipes:
- Base images: Debian/Ubuntu distributions, NeuroDebian
- Package managers: apt, rpm
- Templates: Pre-built installations for common tools
- Custom directives: run, install, environment, copy, workdir

### Recipe Syntax Key Points
```yaml
name: toolname
version: 1.0.0
architectures: [x86_64, aarch64]

build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives:
    - install: [curl, wget, build-essential]
    - template:
        name: ants
        version: 2.4.3
    - run:
        - curl -O https://example.com/software.tar.gz
        - tar -xzf software.tar.gz

deploy:
  bins: [toolname, tool-cli]
  path: [/opt/tool/bin]
```

### CI/CD Integration
- GitHub Actions automatically build changed recipes
- Builds push to GitHub Container Registry and DockerHub
- Singularity images uploaded to cloud storage
- Release files auto-generated for NeuroDesk integration

## Troubleshooting

### Common Build Failures
1. **Network timeouts**: Some tools download large files - builds may take 45+ minutes
2. **Architecture mismatches**: Use `--ignore-architectures` for cross-platform testing
3. **Missing dependencies**: Check base image and ensure all required packages listed
4. **Recipe syntax errors**: Use `--check-only` to validate YAML syntax

### Container Runtime Issues
1. **Docker not found**: Ensure Docker is installed and service running
2. **Permission errors**: User may need to be in docker group
3. **Storage space**: Container builds can use several GB of space

### Testing Problems
1. **Network access**: Some tests download from CVMFS or remote repositories
2. **Missing test configs**: Not all recipes have test.yaml files
3. **Runtime selection**: Tests auto-select Docker > Apptainer > Singularity

### Known Repository Issues
The repository contains some recipes with YAML syntax errors or broken builds. These are existing issues not related to your changes:
- Some recipes have malformed YAML (e.g., clinicadl)
- Some builds fail due to upstream source unavailability
- Some containers are marked as drafts and skip auto-building

The validation tools (`./workflows/test_all.sh`) will continue processing even when encountering broken recipes. Focus on ensuring your changes work correctly rather than fixing unrelated existing issues.

## Validation Workflows

### Pre-commit Validation
Always run before committing changes:
```bash
source env/bin/activate
./workflows/test_all.sh         # Validate recipe syntax
codespell .                   # Check spelling
```

### Pull Request Testing
For testing PR changes:
```bash
python workflows/pr_test_runner.py --verbose --report markdown
./test-containers.sh test-pr
```

### Manual End-to-End Testing
After building containers, manually verify core functionality:
```bash
# Build and test a container
python builder/build.py generate <recipe> --recreate --build --test

# Verify the primary tools work
docker run --rm <container>:<version> <primary-command> --help
docker run --rm <container>:<version> <primary-command> --version

# Test with sample data if available
docker run --rm -v /path/to/data:/data <container>:<version> <command> /data/input.file
```

The testing validation should exercise the main functionality the container was designed to provide, not just check that it starts.

## Performance Tips

- Use `--check-only` for rapid recipe validation during development
- Leverage Docker layer caching - similar recipes build faster
- Build simple containers first to validate environment setup
- Use `--recreate` when recipe changes, skip for re-testing existing builds
- Monitor disk space - builds can consume several GB per container

## Integration Points

### NeuroDesk Integration
- Containers become modules in NeuroDesk via Transparent Singularity
- `deploy.bins` and `deploy.path` control module behavior
- Release files in `releases/` directory trigger app updates

### Container Registries  
- Built containers push to ghcr.io/neurodesk/
- DockerHub mirror at neurodesk/ organization
- Singularity images stored in cloud object storage

Always test your changes with actual container functionality, not just successful builds. The goal is working neuroscience software accessible to researchers.

## Best Practices Summary

### For All Changes
1. **Start Clean**: Always bootstrap the environment before making changes
2. **Validate Early**: Use `--check-only` to catch syntax errors immediately
3. **Test Thoroughly**: Build and test containers, don't just generate Dockerfiles
4. **Keep it Minimal**: Make the smallest changes necessary to achieve the goal
5. **Document Changes**: Update READMEs and add comments for complex logic
6. **Check Security**: Review dependencies and follow security best practices
7. **Run Validation**: Always run `codespell .` and `./workflows/test_all.sh` before committing

### For Recipe Changes
1. **Use Templates**: Leverage NeuroDocker templates when available
2. **Pin Versions**: Always specify exact versions for reproducibility
3. **Optimize Layers**: Combine related commands to minimize image size
4. **Clean Up**: Remove temporary files in the same layer they're created
5. **Test Functionality**: Verify the tool actually works, not just that it builds
6. **Consider Architectures**: Test on multiple architectures when possible
7. **Document Licensing**: Include copyright information with SPDX identifiers

### For Python Code
1. **Follow PEP 8**: Use consistent style throughout
2. **Add Type Hints**: Help with code clarity and IDE support
3. **Handle Errors**: Include appropriate error handling and messages
4. **Keep It Simple**: Prefer clarity over cleverness
5. **Test Changes**: Ensure existing functionality isn't broken

### Time Management
- Budget sufficient time for builds (30-60+ minutes for complex tools)
- Use `--check-only` for rapid iteration during development
- Don't cancel builds prematurely - they may be downloading or compiling
- Plan for multiple test cycles when making significant changes

## Quick Reference Commands

```bash
# Environment setup
python3 -m venv env && source env/bin/activate && pip install --upgrade pip && pip install -r requirements.txt && pip install -e .

# Validation (run before committing)
codespell . && source env/bin/activate && ./workflows/test_all.sh

# Recipe workflow
sf-init <name> <version>                                           # Create new recipe
python builder/build.py generate <name> --recreate --check-only    # Validate syntax
sf-build <name>                                                    # Build and test
sf-login <name>                                                    # Interactive debugging

# Testing
./test-containers.sh test-recipe <name>                            # Test recipe
docker run --rm <container>:<version> <command> --version          # Verify functionality

# Architecture-specific
sf-build <name> --architecture aarch64                             # Build for ARM64
sf-build <name> --ignore-architectures                             # Skip arch checks

# Release
sf-build <name> --generate-release                                 # Generate release files
```

## Getting Help

- Check this document first for guidance on common tasks
- Review `builder/README.md` for detailed recipe syntax
- Examine existing recipes in `recipes/` for examples
- Check GitHub Actions logs for CI failure details
- Review NeuroDocker documentation for template options: https://github.com/ReproNim/neurodocker
- Visit NeuroDesk documentation: https://www.neurodesk.org/docs/
