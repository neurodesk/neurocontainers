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

function test_file_linking {
    filename=$1

    # if the file starts with a shabang test it recursively
    if [ "$(head -n 1 $filename | grep '#!')" ]; then
        # Get just the interpreter
        interpreter=$(head -n 1 $filename | grep '#!' | awk '{print $1}' | cut -c 3-)

        echo "File $filename is a script using $interpreter."

        # Check if the interpreter works
        test_file $interpreter

        return 0
    fi

    # If the file is dynamically linked, check if the libraries exist
    if ldd "$filename" &>/dev/null; then
        echo "File $filename is dynamically linked."
        for j in $(ldd "$filename" | awk '{print $3}'); do
            if [ -f "$j" ]; then
                echo "Library $j exists."
            else
                echo "Library $j does not exist."
                exit 1
            fi
        done
    else
        echo "File $i is staticky linked."
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
