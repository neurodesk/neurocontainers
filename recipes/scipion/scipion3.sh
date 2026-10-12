#!/bin/sh
# Neurodesk wrapper around the launcher written by scipion-installer.
# The installation in /opt/scipion is read-only, so per-user state (projects,
# logs, downloaded test data, Matplotlib cache) must live somewhere writable.
set -eu

writable_dir() {
    [ -n "$1" ] && mkdir -p "$1" 2>/dev/null && [ -w "$1" ]
}

if [ -z "${SCIPION_USER_DATA:-}" ]; then
    if [ -n "${HOME:-}" ] && writable_dir "$HOME/ScipionUserData"; then
        SCIPION_USER_DATA="$HOME/ScipionUserData"
    else
        SCIPION_USER_DATA="${TMPDIR:-/tmp}/ScipionUserData-$(id -u)"
        echo "scipion3: home directory is not writable; projects go to $SCIPION_USER_DATA." >&2
        echo "scipion3: set SCIPION_USER_DATA to a persistent writable directory to keep them." >&2
    fi
fi
export SCIPION_USER_DATA

# Test datasets default to SCIPION_HOME/data/tests, which is inside the image.
export SCIPION_TESTS="${SCIPION_TESTS:-$SCIPION_USER_DATA/data/tests}"

if [ -z "${MPLCONFIGDIR:-}" ] && [ -z "${XDG_CACHE_HOME:-}" ] &&
    ! { [ -n "${HOME:-}" ] && writable_dir "$HOME/.cache/matplotlib"; }; then
    export MPLCONFIGDIR="$SCIPION_USER_DATA/.matplotlib"
fi

exec /opt/miniforge3/envs/scipion3/bin/python /opt/scipion/scipion3 "$@"
