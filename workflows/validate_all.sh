#!/usr/bin/env bash

set -e

# Validate all YAML recipes using attrs schema

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Validating all YAML recipes..."

total_recipes=0
valid_recipes=0
invalid_recipes=0

# Loop through each subdirectory under recipes
for dir in "$REPO_ROOT/recipes"/*/; do
    name=$(basename "$dir")
    
    # If it doesn't contain build.yaml, skip it
    if [[ ! -f "${dir}build.yaml" ]]; then
        continue
    fi
    
    total_recipes=$((total_recipes + 1))
    echo -n "Validating ${name}... "
    
    if python3 "$REPO_ROOT/builder/validation.py" "${dir}build.yaml" > /dev/null 2>&1; then
        echo "✓"
        valid_recipes=$((valid_recipes + 1))
    else
        echo "✗"
        echo "  Error details:"
        python3 "$REPO_ROOT/builder/validation.py" "${dir}build.yaml" 2>&1 | sed 's/^/    /'
        invalid_recipes=$((invalid_recipes + 1))
    fi
done

echo ""
echo "Validation Summary:"
echo "  Total recipes: $total_recipes"
echo "  Valid recipes: $valid_recipes"
echo "  Invalid recipes: $invalid_recipes"

if [[ $invalid_recipes -gt 0 ]]; then
    echo ""
    echo "❌ Some recipes failed validation!"
    exit 1
else
    echo ""
    echo "✅ All recipes are valid!"
    exit 0
fi