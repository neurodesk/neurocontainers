#!/usr/bin/env bash

set -e

function test_file {
    filename=$1

    # Check if the file exists
    if [ -f "$filename" ]; then
        echo "File $filename exists."
    else
        echo "File $filename does not exist."
        exit 1
    fi

    # Check if the file is executable
    if [ -x "$filename" ]; then
        echo "File $filename is executable."
    else
        echo "File $filename is not executable."
        exit 1
    fi

    test_file_linking $filename
}

function get_file_magic {
    filename=$1

    # if the file is a symbolic link, get the target file
    if [ -L "$filename" ]; then
        filename=$(readlink -f "$filename")
    fi

    file "$filename"
}

function test_file_linking {
    local filename=$1

    # If the file is an ELF binary check if it is statically or dynamically linked
    if get_file_magic "$filename" | grep -q "ELF"; then
        if ldd "$filename" &>/dev/null; then
            echo "File $filename is dynamically linked."
            while read -r _ _ libpath _; do
                if [ -n "$libpath" ] && [ "$libpath" != "not" ]; then
                    if [ -f "$libpath" ]; then
                        echo "Library $libpath exists."
                    else
                        echo "Library $libpath does not exist."
                        exit 1
                    fi
                fi
            done < <(ldd "$filename")
        else
            echo "File $filename is statically linked."
        fi
    else
        # Check for shebang
        local first_line
        IFS= read -r first_line < "$filename"
        if [[ $first_line == \#!* ]]; then
            # Strip '#!' and leading whitespace
            local interpreter_line="${first_line#\#!}"
            interpreter_line="${interpreter_line#"${interpreter_line%%[![:space:]]*}"}"

            # Split interpreter+args
            local interpreter=${interpreter_line%% *}
            local args=${interpreter_line#"$interpreter"}
            args="${args#"${args%%[![:space:]]*}"}" # trim leading spaces if any

            echo "File $filename is a script using interpreter: $interpreter ${args}"

            # Validate interpreter exists
            test_file "$interpreter"

            return 0
        else
            echo "File $filename is not an ELF binary or a script."
        fi
    fi
}

function main {
    echo "Testing DEPLOY_BINS and DEPLOY_PATH..."

    # Get every file in DEPLOY_BINS split with :
    for i in $(echo $DEPLOY_BINS | tr ":" "\n"); do
        # Check if the file is on the PATH
        if [ ! -f "$(which $i)" ]; then
            echo "File $i does not exist on the PATH."
            exit 1
        fi
        
        filename=$(which $i)

        test_file $filename
    done

    # Get every directory in DEPLOY_PATH split with :
    for i in $(echo $DEPLOY_PATH | tr ":" "\n"); do
        # Check if the directory exists
        if [ -d "$i" ]; then
            echo "Directory $i exists."
        else
            echo "Directory $i does not exist."
            exit 1
        fi

        echo "Testing directory $i..."

        # For each executable file in the directory test it.
        for j in $(ls $i); do
            filename=$i$j

            # if the file is not executable skip it
            if [ ! -x "$filename" ]; then
                continue
            fi

            test_file $filename
        done
    done
}

main
