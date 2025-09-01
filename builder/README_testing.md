# Enhanced Container Testing Framework

This directory contains an enhanced testing framework for NeuroContainers that improves upon the existing testing system with better portability, automation, and multi-runtime support.

## New Testing Tools

### 1. `container_tester.py` - Portable Container Testing Tool

A unified interface for testing containers across different runtimes and storage systems.

**Features:**
- Multi-runtime support (Docker, Apptainer, Singularity)
- CVMFS integration for distributed container access
- Test definition extraction from embedded YAML files
- Portable test execution environment

**Usage:**
```bash
# Test a container from CVMFS
./container_tester.py fsl:6.0.4 --location cvmfs --verbose

# Test with a specific runtime
./container_tester.py mrtrix3:3.0.4 --runtime apptainer

# Test with custom test configuration
./container_tester.py freesurfer:7.4.1 --test-config ../recipes/freesurfer/test.yaml

# List available containers in CVMFS
./container_tester.py --list-containers

# Test with GPU support
./container_tester.py fsl:6.0.4 --gpu --verbose

# Save results to file
./container_tester.py dcm2niix:1.0.20240202 --output results.json --report markdown
```

### 2. `pr_test_runner.py` - Automatic PR Testing

Automatically tests containers modified in pull requests.

**Features:**
- Detects modified build.yaml files in PRs
- Downloads corresponding .sif containers
- Runs comprehensive test suites
- Generates detailed test reports

**Usage:**
```bash
# Test all modified containers in current PR
./pr_test_runner.py --verbose

# Test against specific git references
./pr_test_runner.py --base-ref origin/master --head-ref feature-branch

# Generate detailed reports
./pr_test_runner.py --report markdown --report-file pr-test-report.md

# Use only local/CVMFS containers (no downloading)
./pr_test_runner.py --no-download --verbose

# Save results for CI integration
./pr_test_runner.py --output pr-results.json --report html --report-file report.html
```

## Enhanced Build System

### Embedded YAML Files in Containers

The build system now includes `build.yaml` files directly in containers, enabling:

1. **Test Definition Extraction**: Tests can be extracted from any container without external files
2. **Self-Documenting Containers**: Each container carries its own build and test metadata
3. **Portable Testing**: Tests travel with containers across different environments

**Implementation:**
- `build.yaml` is automatically copied to `/build.yaml` in each container
- Test extractors can retrieve test definitions from live containers
- Supports both standalone test files and embedded definitions

## GitHub Actions Integration

### Automatic PR Testing Workflow

The `.github/workflows/test-release-pr.yml` workflow automatically:

1. **Detects Changes**: Identifies modified `build.yaml` files in PRs
2. **Runs Tests**: Tests each modified container using the new testing framework
3. **Reports Results**: Posts detailed test results as PR comments
4. **Provides Summary**: Shows overall pass/fail status for the PR

**Workflow Features:**
- Matrix strategy for parallel testing of multiple containers
- Intelligent caching of downloaded containers
- Support for both Docker and Apptainer runtimes
- Detailed error reporting and debugging information
- Integration with existing CI/CD pipeline

## Testing Strategies

### 1. Multi-Runtime Testing
```bash
# Test the same container with different runtimes
./container_tester.py fsl:6.0.4 --runtime docker
./container_tester.py fsl:6.0.4 --runtime apptainer
```

### 2. Location-Aware Testing
```bash
# Test from CVMFS (fastest for distributed systems)
./container_tester.py mrtrix3:3.0.4 --location cvmfs

# Test local .sif files
./container_tester.py freesurfer:7.4.1 --location local

# Auto-detect best location
./container_tester.py dcm2niix:1.0.20240202 --location auto
```

### 3. Comprehensive PR Testing
The PR test runner automatically:
- Identifies all modified recipes
- Finds containers using multiple strategies (CVMFS, download, local)
- Runs complete test suites for each container
- Generates summary reports for review

## Integration with Existing System

### Backward Compatibility
- All existing test scripts (.sh files) continue to work
- Existing YAML test definitions are fully supported
- Current CI/CD workflows remain functional

### Enhanced Capabilities
- **Portable Execution**: Tests run consistently across different environments
- **Multi-Runtime Support**: Switch between Docker, Apptainer, Singularity seamlessly
- **Automatic Discovery**: Find containers in CVMFS, local storage, or download as needed
- **Rich Reporting**: Generate markdown, HTML, or JSON test reports

## Example Workflows

### For Developers
```bash
# Test a recipe during development
cd recipes/fsl
../../builder/container_tester.py fsl:6.0.4 --verbose

# Test all changes before creating PR
../../builder/pr_test_runner.py --verbose --report markdown
```

### For CI/CD
```bash
# In GitHub Actions or other CI systems
python builder/pr_test_runner.py \
  --base-ref origin/master \
  --head-ref HEAD \
  --output test-results.json \
  --report html \
  --report-file test-report.html
```

### For Release Testing
```bash
# Test specific containers before release
./container_tester.py --list-containers | grep "fsl\|freesurfer" | \
while read container; do
  ./container_tester.py "$container" --verbose
done
```

## Configuration

### Environment Variables
- `CVMFS_BASE`: Override default CVMFS mount point
- `CONTAINER_CACHE_DIR`: Override container cache directory
- `PREFERRED_RUNTIME`: Set default container runtime

### Test Configuration Files
Tests can be defined in multiple ways:
1. **Embedded in build.yaml**: Test definitions in the `directives` section
2. **Separate test.yaml**: Standalone test configuration files
3. **Inline in containers**: Extracted from `/build.yaml` inside containers

## Benefits

### For Maintainers
- **Automated Testing**: PR testing reduces manual review overhead
- **Consistent Results**: Same tests run regardless of environment
- **Better Coverage**: Multi-runtime testing catches compatibility issues

### For Users
- **Reliable Containers**: More thorough testing improves quality
- **Portable Testing**: Test containers in your preferred environment
- **Clear Documentation**: Each container includes its test definitions

### For Infrastructure
- **CVMFS Integration**: Leverage distributed file systems for faster access
- **Efficient Caching**: Smart caching reduces download times
- **Scalable Testing**: Matrix strategies enable parallel testing

## Migration Guide

### From Existing Testing
1. Existing test scripts continue to work without changes
2. New containers automatically include embedded test definitions
3. PR testing workflow activates automatically for modified recipes

### Adding Enhanced Testing
1. Use `container_tester.py` for interactive testing
2. Add test definitions to `build.yaml` files
3. Leverage PR testing for automated validation

This enhanced testing framework maintains full compatibility with the existing system while adding powerful new capabilities for portable, automated, and comprehensive container testing.