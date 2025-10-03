#!/usr/bin/env bash

set -e

# Enhanced test script that includes validation

echo "Running validation tests..."
VALIDATION_FAILED=false

# loop through each subdirectory under recipes
for dir in recipes/*/; do
    name=$(basename "$dir")

    # If it doesn't contain build.yaml, skip it
    if [[ ! -f "${dir}build.yaml" ]]; then
        continue
    fi

    echo "Checking ${name}..."

    # First run validation
    if ! python3 builder/validation.py "${dir}build.yaml" > /dev/null 2>&1; then
        echo "  ❌ Validation failed for ${name}"
        python3 builder/validation.py "${dir}build.yaml" 2>&1 | sed 's/^/    /'
        VALIDATION_FAILED=true
    else
        echo "  ✅ Validation passed for ${name}"
    fi

    # Then run the existing check-only build
    python3 builder/build.py generate $name --recreate --architecture x86_64 --check-only --auto-build
done

if [[ "$VALIDATION_FAILED" == "true" ]]; then
    echo ""
    echo "❌ Some recipes failed validation! Please fix the validation errors above."
    exit 1
else
    echo ""
    echo "✅ All validation checks passed!"
fi
