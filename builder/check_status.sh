#!/usr/bin/env bash

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
GREY='\033[0;37m'
NC='\033[0m' # No Color

verbose() {
    echo -e "${GREY}$1${NC}"
}

warn() {
    echo -e "${YELLOW}$1${NC}"
}

error() {
    echo -e "${RED}$1${NC}"
}

ok() {
    echo -e "${GREEN}$1${NC}"
}

set -e

file_exists() {
    local name
    name="$1/$2"
    if [[ -f $name ]]; then
        return 0
    else
        return 1
    fi
}

MIGRATION_START_TIME=1738335264

migrated_recipes=0
unmigrated_recipes=0

# loop through each subdirectory under recipes
for dir in recipes/*/; do
    name=$(basename "$dir")

    # Check if it contains both build.sh and build.yaml
    if file_exists $dir "build.sh" && file_exists $dir "build.yaml"; then
        error "${name} contains both build.sh and build.yaml"
        exit 1
    fi

    # Check if it contains a build.sh file
    if file_exists $dir "build.sh"; then
        # If build.sh has been modified in git since 21-March-2025, warn the user
        mod_time=$(git log -n 1 --pretty=format:%cd --date=unix -- $dir/build.sh)
        if [[ $mod_time -gt $MIGRATION_START_TIME ]]; then
            warn "[-] ${name} needs migration"
        else
            verbose "[ ] ${name}"
        fi
        unmigrated_recipes=$((unmigrated_recipes + 1))
        continue
    fi

    # Check if it contains a build.yaml file
    if ! file_exists $dir "build.yaml"; then
        error "${name} does not contain build.yaml"
        exit 1
    fi

    ok "[x] ${name}"
    migrated_recipes=$((migrated_recipes + 1))

    if file_exists $dir "README.md"; then
        warn "${name} contains README.md"
        # check if CHECK_MODIFY is set to true and if so remove the README.md file
        if [[ "$CHECK_MODIFY" == "true" ]]; then
            rm -f "${dir}README.md"
            ok "${name} README.md removed"
        fi
    fi

    if file_exists $dir "test.sh"; then
        warn "${name} contains test.sh, should be migrated to test.yaml"

        # if the file is empty then print a warning
        if [[ ! -s "${dir}test.sh" ]]; then
            warn "${name} test.sh is empty"
            # check if CHECK_MODIFY is set to true and if so remove the test.sh file
            if [[ "$CHECK_MODIFY" == "true" ]]; then
                rm -f "${dir}test.sh"
                ok "${name} test.sh removed"
            fi
        fi
    fi

    # Look for other files in the directory
    for file in "$dir"*; do
        # Check if the file is not build.yaml, test.sh, or README.md
        if [[ "$file" != *"build.yaml" && "$file" != *"test.yaml" && "$file" != *"LICENSE" ]]; then
            warn "${name} contains ${file##*/}"
        fi
    done
done

# Print the summary
echo -e "\n${YELLOW}Summary:${NC}"
echo -e "${GREEN}Migrated recipes: ${migrated_recipes}${NC}"
echo -e "${RED}Unmigrated recipes: ${unmigrated_recipes}${NC}"