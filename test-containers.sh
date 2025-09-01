#!/bin/bash
# Convenient wrapper script for testing containers locally

# Activate virtual environment
source env/bin/activate

case "$1" in
    "list")
        echo "Listing available containers in CVMFS..."
        python builder/container_tester.py --list-containers
        ;;
    "test")
        if [ -z "$2" ]; then
            echo "Usage: $0 test <container:version>"
            echo "Example: $0 test dcm2niix:v1.0.20240202"
            exit 1
        fi
        echo "Testing container: $2"
        python builder/container_tester.py "$2" --location auto --verbose
        ;;
    "test-recipe")
        if [ -z "$2" ]; then
            echo "Usage: $0 test-recipe <recipe-name>"
            echo "Example: $0 test-recipe dcm2niix"
            exit 1
        fi
        echo "Testing recipe: $2"
        
        # Read version from build.yaml
        BUILD_FILE="recipes/$2/build.yaml"
        if [ ! -f "$BUILD_FILE" ]; then
            echo "Error: Build file not found: $BUILD_FILE"
            exit 1
        fi
        
        # Extract name and version from build.yaml
        NAME=$(grep "^name:" "$BUILD_FILE" | sed 's/name: *//')
        VERSION=$(grep "^version:" "$BUILD_FILE" | sed 's/version: *//')
        
        echo "Found container: $NAME:$VERSION"
        python builder/container_tester.py "$NAME:$VERSION" --test-config "$BUILD_FILE" --location auto --verbose
        ;;
    "test-pr")
        echo "Testing containers modified in current PR..."
        python builder/pr_test_runner.py --verbose --report markdown
        ;;
    "test-release")
        if [ -z "$2" ]; then
            echo "Usage: $0 test-release <release-file.json>"
            echo "Example: $0 test-release releases/dcm2niix/v1.0.20240202.json"
            exit 1
        fi
        
        if [ ! -f "$2" ]; then
            echo "Error: Release file not found: $2"
            exit 1
        fi
        
        # Extract container info from release file path
        RELEASE_DIR=$(dirname "$2")
        CONTAINER_NAME=$(basename "$RELEASE_DIR")
        VERSION=$(basename "$2" .json)
        
        echo "Testing release: $CONTAINER_NAME:$VERSION"
        echo "Release file: $2"
        
        # Use the release file for proper build date extraction and container download
        python builder/container_tester.py "$CONTAINER_NAME:$VERSION" --location auto --release-file "$2" --verbose
        ;;
    "help"|"")
        cat << EOF
NeuroContainers Testing Tool

Usage: $0 <command> [arguments]

Commands:
  list                     List available containers in CVMFS
  test <container:version> Test a specific container
  test-recipe <name>       Test a container using its recipe
  test-pr                  Test all containers modified in current PR
  test-release <file.json> Test a container from a release file
  help                     Show this help message

Examples:
  $0 list
  $0 test dcm2niix:v1.0.20240202
  $0 test-recipe dcm2niix
  $0 test-pr
  $0 test-release releases/dcm2niix/v1.0.20240202.json

Environment:
  The script will automatically activate the 'env' virtual environment.
  Make sure you have run 'python -m venv env && source env/bin/activate && pip install -r requirements.txt' first.

For more detailed options, use the tools directly:
  python builder/container_tester.py --help
  python builder/pr_test_runner.py --help
EOF
        ;;
    *)
        echo "Unknown command: $1"
        echo "Run '$0 help' for usage information."
        exit 1
        ;;
esac