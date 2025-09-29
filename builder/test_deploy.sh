#!/usr/bin/env bash

set -o pipefail

RESULTS_FILE=$(mktemp)
TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0

declare -A VISITED_FILES

cleanup() {
    rm -f "$RESULTS_FILE"
}
trap cleanup EXIT

record_result() {
    local name="$1"
    local status="$2"
    local message="${3:-}"

    # Normalise whitespace and special characters so JSON encoding is straightforward later
    message="${message//\\/\\\\}"
    message="${message//\"/\\\"}"
    message="${message//$'\n'/\\n}"
    message="${message//$'\r'/\\r}"
    message="${message//$'\t'/\\t}"

    printf '%s\t%s\t%s\n' "$name" "$status" "$message" >> "$RESULTS_FILE"

    ((TOTAL++))
    case "$status" in
        passed) ((PASSED++)) ;;
        failed) ((FAILED++)) ;;
        skipped) ((SKIPPED++)) ;;
    esac
}

get_file_magic() {
    local filename="$1"

    # Resolve symbolic links to inspect the target file
    if [ -L "$filename" ]; then
        filename=$(readlink -f "$filename")
    fi

    file "$filename"
}

normalise_path() {
    local value="$1"
    if [ -z "$value" ]; then
        printf ''
        return
    fi

    if [[ "$value" == /* ]]; then
        printf '%s' "$value"
    else
        command -v "$value" 2>/dev/null || printf ''
    fi
}

test_file() {
    local filename="$1"

    if [ -z "$filename" ]; then
        return
    fi

    if [[ -n "${VISITED_FILES[$filename]:-}" ]]; then
        return
    fi
    VISITED_FILES["$filename"]=1

    if [ -d "$filename" ]; then
        record_result "file.directory:$filename" "skipped" "Path $filename is a directory."
        return
    fi

    if [ -f "$filename" ]; then
        record_result "file.exists:$filename" "passed" "File $filename exists."
    else
        record_result "file.exists:$filename" "failed" "File $filename does not exist."
        return
    fi

    if [ -x "$filename" ]; then
        record_result "file.executable:$filename" "passed" "File $filename is executable."
    else
        record_result "file.executable:$filename" "failed" "File $filename is not executable."
        return
    fi

    test_file_linking "$filename"
}

parse_ldd_output() {
    local binary="$1"
    local output="$2"

    while IFS= read -r line; do
        [ -z "$line" ] && continue

        local lib_label
        local lib_path=""

        if [[ "$line" == *"=>"* ]]; then
            lib_label=$(printf '%s' "$line" | awk '{print $1}')
            lib_path=$(printf '%s' "$line" | awk '{for (i=1; i<=NF; i++) if ($i ~ /^\//) {print $i; exit}}')
        else
            lib_label=$(printf '%s' "$line" | awk '{print $1}')
            if [[ "$line" == /* ]]; then
                lib_path=$(printf '%s' "$line" | awk '{print $1}')
            fi
        fi

        if [ -n "$lib_path" ] && [ "$lib_path" != "not" ]; then
            if [ -f "$lib_path" ]; then
                record_result "ldd:$binary:$lib_label" "passed" "Library $lib_label resolved to $lib_path."
            else
                record_result "ldd:$binary:$lib_label" "failed" "Library $lib_label missing (expected at $lib_path)."
            fi
        else
            record_result "ldd:$binary:$lib_label" "skipped" "No filesystem path to validate for $lib_label."
        fi
    done <<< "$output"
}

test_file_linking() {
    local filename="$1"

    if get_file_magic "$filename" | grep -q "ELF"; then
        local ldd_output
        if ldd_output=$(ldd "$filename" 2>&1); then
            record_result "file.linkage:$filename" "passed" "File $filename is dynamically linked."
            parse_ldd_output "$filename" "$ldd_output"
        else
            if [[ "$ldd_output" == *"not a dynamic executable"* ]] || [[ "$ldd_output" == *"statically linked"* ]]; then
                record_result "file.linkage:$filename" "passed" "File $filename is statically linked."
            else
                record_result "file.linkage:$filename" "failed" "ldd error: $ldd_output"
            fi
        fi
    else
        local first_line=""
        if IFS= read -r first_line < "$filename"; then
            if [[ $first_line == \#!* ]]; then
                local interpreter_line="${first_line#\#!}"
                interpreter_line="${interpreter_line#"${interpreter_line%%[![:space:]]*}"}"

                local interpreter=${interpreter_line%% *}
                local args="${interpreter_line#"$interpreter"}"
                args="${args#"${args%%[![:space:]]*}"}"

                record_result "script:$filename" "passed" "Script uses interpreter: $interpreter $args"

                local resolved
                resolved=$(normalise_path "$interpreter")
                if [ -n "$resolved" ]; then
                    test_file "$resolved"
                else
                    record_result "script.interpreter:$filename" "failed" "Interpreter $interpreter not found on PATH."
                fi
            else
                record_result "file.type:$filename" "skipped" "File $filename is not an ELF binary or recognised script."
            fi
        else
            record_result "file.read:$filename" "failed" "Unable to read file header for $filename."
        fi
    fi
}

process_deploy_bins() {
    local bins="${DEPLOY_BINS:-}"

    if [ -z "$bins" ]; then
        record_result "deploy_bins" "skipped" "DEPLOY_BINS not set."
        return
    fi

    while IFS= read -r entry; do
        [ -z "$entry" ] && continue

        local resolved=""
        if [[ "$entry" == /* ]] || [[ "$entry" == .* ]]; then
            if [ -f "$entry" ]; then
                resolved="$entry"
            fi
        fi

        if [ -z "$resolved" ]; then
            resolved=$(command -v "$entry" 2>/dev/null || true)
        fi

        if [ -n "$resolved" ]; then
            record_result "deploy_bin:$entry" "passed" "Binary $entry found at $resolved."
            test_file "$resolved"
        else
            record_result "deploy_bin:$entry" "failed" "Binary $entry not found on PATH."
        fi
    done < <(printf '%s\n' "$bins" | tr ':' '\n')
}

process_deploy_paths() {
    local paths="${DEPLOY_PATH:-}"

    if [ -z "$paths" ]; then
        record_result "deploy_path" "skipped" "DEPLOY_PATH not set."
        return
    fi

    while IFS= read -r dir; do
        [ -z "$dir" ] && continue

        if [ -d "$dir" ]; then
            record_result "deploy_dir:$dir" "passed" "Directory $dir exists."

            while IFS= read -r target; do
                [ -z "$target" ] && continue
                test_file "$target"
            done < <(
                find "$dir" -maxdepth 1 \
                    \( -type f -o \( -type l -a ! -xtype d \) \) \
                    -perm -111 -print 2>/dev/null
            )
        else
            record_result "deploy_dir:$dir" "failed" "Directory $dir does not exist."
        fi
    done < <(printf '%s\n' "$paths" | tr ':' '\n')
}

escape_json_string() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\n'/\\n}"
    value="${value//$'\r'/\\r}"
    value="${value//$'\t'/\\t}"
    printf '%s' "$value"
}

generate_report() {
    printf '{\n'
    printf '  "total": %d,\n' "$TOTAL"
    printf '  "passed": %d,\n' "$PASSED"
    printf '  "failed": %d,\n' "$FAILED"
    printf '  "skipped": %d,\n' "$SKIPPED"
    printf '  "tests": [\n'

    local first_entry=true
    while IFS=$'\t' read -r name status message; do
        [ -z "$name" ] && continue

        if [ "$first_entry" = true ]; then
            first_entry=false
        else
            printf ',\n'
        fi

        local safe_name safe_status
        safe_name=$(escape_json_string "$name")
        safe_status=$(escape_json_string "$status")

        printf '    {\n'
        printf '      "name": "%s",\n' "$safe_name"
        printf '      "status": "%s",\n' "$safe_status"
        printf '      "message": "%s"\n' "$message"
        printf '    }'
    done < "$RESULTS_FILE"

    printf '\n  ]\n}\n'
}

main() {
    process_deploy_bins
    process_deploy_paths
    generate_report

    if [ "$FAILED" -gt 0 ]; then
        exit 1
    fi
    exit 0
}

main
