#!/usr/bin/env bash

set -e

# Script to generate and extract all Dockerfiles in a single file
# Similar to test_all.sh but generates and outputs Dockerfiles instead of testing

OUTPUT_FILE="all_dockerfiles.txt"

echo "Generating and extracting all Dockerfiles to ${OUTPUT_FILE}..."

# Remove output file if it exists
rm -f "${OUTPUT_FILE}"

# Counter for generated Dockerfiles
DOCKERFILE_COUNT=0

# Loop through each subdirectory under recipes
for dir in recipes/*/; do
    name=$(basename "$dir")

    # If it doesn't contain build.yaml, skip it
    if [[ ! -f "${dir}build.yaml" ]]; then
        continue
    fi

    echo "Generating Dockerfile for ${name}..."

    # Generate the Dockerfile using the builder
    if python3 builder/build.py generate "$name" --recreate --architecture x86_64 --check-only --auto-build > /dev/null 2>&1; then
        # Look for the generated Dockerfile (could be versioned like afni_25.2.03.Dockerfile)
        dockerfile_path=$(find "build/${name}/" -name "*.Dockerfile" -o -name "Dockerfile" 2>/dev/null | head -1)
        
        if [[ -f "$dockerfile_path" ]]; then
            echo "========================================" >> "${OUTPUT_FILE}"
            echo "Recipe: ${name}" >> "${OUTPUT_FILE}"
            echo "File: ${dockerfile_path}" >> "${OUTPUT_FILE}"
            echo "========================================" >> "${OUTPUT_FILE}"
            echo "" >> "${OUTPUT_FILE}"
            cat "$dockerfile_path" >> "${OUTPUT_FILE}"
            echo "" >> "${OUTPUT_FILE}"
            echo "" >> "${OUTPUT_FILE}"
            ((++DOCKERFILE_COUNT))
            echo "  ✅ Extracted Dockerfile for ${name}"
        else
            echo "  ⚠️  Dockerfile not found for ${name} in build/${name}/"
        fi
    else
        echo "  ❌ Failed to generate Dockerfile for ${name}"
    fi
done

echo ""
echo "✅ All Dockerfiles extracted to ${OUTPUT_FILE}"
echo "Generated and extracted ${DOCKERFILE_COUNT} Dockerfile(s)"