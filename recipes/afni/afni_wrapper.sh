#!/bin/bash
# AFNI wrapper script - ensures AFNI runs without detaching from the container
# This wrapper intercepts calls to 'afni' and adds the -no_detach flag

# If no arguments or help-related arguments are provided, pass through with -no_detach
case "${1}" in
    -help|--help|-h|--version|-v)
        # Pass through help and version flags without modification
        exec /usr/local/abin/afni "$@"
        ;;
    *)
        # Add -no_detach flag to keep AFNI in foreground
        exec /usr/local/abin/afni -no_detach "$@"
        ;;
esac
