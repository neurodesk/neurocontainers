#!/usr/bin/env bash
# Extract Boutiques descriptors from Neurocontainers
# Usage: extract_descriptors.sh <container_path> <output_dir>

set -e

CONTAINER_PATH=${1:-}
OUTPUT_DIR=${2:-descriptors}

# Function to display usage
usage() {
    echo "Usage: $0 <container_path> <output_dir>"
    echo ""
    echo "Extract Boutiques descriptors from a Neurocontainers image"
    echo ""
    echo "Arguments:"
    echo "  container_path  - Path to container image (.sif, .simg, or docker://)"
    echo "  output_dir      - Directory to save extracted descriptors"
    echo ""
    echo "Examples:"
    echo "  $0 qsmxt-ci.sif descriptors/"
    echo "  $0 docker://vnmd/qsmxt-ci:latest /tmp/descriptors"
    exit 1
}

# Check arguments
if [ -z "$CONTAINER_PATH" ]; then
    usage
fi

echo "Extracting Boutiques descriptors from: $CONTAINER_PATH"
echo "Output directory: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if container exists
if [[ "$CONTAINER_PATH" == docker://* ]]; then
    echo "Using Docker image: $CONTAINER_PATH"
    CONTAINER_TYPE="docker"
elif [[ -f "$CONTAINER_PATH" ]]; then
    echo "Using Singularity image: $CONTAINER_PATH"
    CONTAINER_TYPE="singularity"
else
    echo "Error: Container not found at $CONTAINER_PATH"
    exit 1
fi

# Extract descriptors based on container type
if [ "$CONTAINER_TYPE" = "singularity" ]; then
    # Check if Singularity is available
    if ! command -v singularity &> /dev/null; then
        echo "Error: Singularity not found. Please install Singularity."
        exit 1
    fi
    
    # List contents of /boutique directory first
    echo "Checking for Boutiques descriptors in container..."
    DESCRIPTORS=$(singularity exec "$CONTAINER_PATH" find /boutique -name "*.json" 2>/dev/null | wc -l) || DESCRIPTORS=0
    
    if [ "$DESCRIPTORS" -eq 0 ]; then
        echo "Warning: No descriptors found in /boutique directory"
        echo "Checking if /boutique directory exists..."
        if ! singularity exec "$CONTAINER_PATH" test -d /boutique 2>/dev/null; then
            echo "Error: /boutique directory does not exist in container"
            echo "This may not be a Neurocontainers image with embedded Boutiques descriptors"
            exit 1
        fi
    else
        echo "Found $DESCRIPTORS descriptor(s) in container"
    fi
    
    # Extract descriptors using bind mount
    echo "Extracting descriptors..."
    singularity exec --bind "$OUTPUT_DIR:/tmp/extract_out" "$CONTAINER_PATH" \
        bash -c "
        if [ -d /boutique ]; then
            cp /boutique/*.json /tmp/extract_out/ 2>/dev/null || echo 'No JSON files found in /boutique'
            ls -la /boutique/
        else
            echo 'No /boutique directory found'
            exit 1
        fi
        "
    
elif [ "$CONTAINER_TYPE" = "docker" ]; then
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker not found. Please install Docker."
        exit 1
    fi
    
    # Extract using Docker
    echo "Extracting descriptors using Docker..."
    CONTAINER_ID=$(docker create "$CONTAINER_PATH")
    docker cp "$CONTAINER_ID:/boutique/" "$OUTPUT_DIR/" || {
        echo "Error: Could not extract /boutique directory from Docker container"
        docker rm "$CONTAINER_ID"
        exit 1
    }
    docker rm "$CONTAINER_ID"
    
    # Move files up one level (docker cp creates boutique subdir)
    if [ -d "$OUTPUT_DIR/boutique" ]; then
        mv "$OUTPUT_DIR/boutique"/*.json "$OUTPUT_DIR/" 2>/dev/null || true
        rmdir "$OUTPUT_DIR/boutique" 2>/dev/null || true
    fi
fi

# Verify extraction
EXTRACTED_COUNT=$(find "$OUTPUT_DIR" -name "*.json" | wc -l)
if [ "$EXTRACTED_COUNT" -eq 0 ]; then
    echo "Error: No descriptors were extracted"
    exit 1
fi

echo "Successfully extracted $EXTRACTED_COUNT descriptor(s):"
ls -la "$OUTPUT_DIR"/*.json

# Validate extracted descriptors if Boutiques is available
if command -v bosh &> /dev/null; then
    echo ""
    echo "Validating extracted descriptors..."
    for descriptor in "$OUTPUT_DIR"/*.json; do
        echo "Validating $(basename "$descriptor")..."
        if bosh validate "$descriptor" >/dev/null 2>&1; then
            echo "   Valid"
        else
            echo "   Invalid"
            echo "  Running detailed validation..."
            bosh validate "$descriptor"
        fi
    done
else
    echo ""
    echo "Note: Boutiques (bosh) not found. Install with 'pip install boutiques' to validate descriptors."
fi

echo ""
echo "Descriptor extraction complete!"
echo "You can now run algorithms using:"
echo "  bosh exec launch $OUTPUT_DIR/<algorithm>.json inputs.json --imagepath $CONTAINER_PATH"