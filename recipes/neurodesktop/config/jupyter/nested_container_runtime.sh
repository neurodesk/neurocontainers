#!/bin/bash

# Configure the runtime used by Neurodesk app wrappers when Neurodesktop is
# itself running inside Apptainer/Singularity.

neurodesktop_path_prepend_once() {
        local dir="$1"
        if [ -z "${dir}" ] || [ ! -d "${dir}" ]; then
                return 0
        fi
        case ":${PATH:-}:" in
                *":${dir}:"*) ;;
                *) export PATH="${dir}${PATH:+:${PATH}}" ;;
        esac
}

neurodesktop_status_file="${NEURODESKTOP_PROC_STATUS:-/proc/self/status}"
neurodesktop_no_new_privs=0
if [ -r "${neurodesktop_status_file}" ] \
        && awk '/^NoNewPrivs:[[:space:]]*1$/ { found=1 } END { exit found ? 0 : 1 }' "${neurodesktop_status_file}" 2>/dev/null; then
        neurodesktop_no_new_privs=1
fi

neurodesktop_bindpath="${NEURODESKTOP_CONTAINER_BINDPATH:-/data,/mnt,/neurodesktop-storage,/tmp,/cvmfs}"
if [ -z "${APPTAINER_BINDPATH:-}" ] && [ -z "${SINGULARITY_BINDPATH:-}" ]; then
        export APPTAINER_BINDPATH="${neurodesktop_bindpath}"
        export SINGULARITY_BINDPATH="${neurodesktop_bindpath}"
elif [ -z "${APPTAINER_BINDPATH:-}" ]; then
        export APPTAINER_BINDPATH="${SINGULARITY_BINDPATH}"
elif [ -z "${SINGULARITY_BINDPATH:-}" ]; then
        export SINGULARITY_BINDPATH="${APPTAINER_BINDPATH}"
fi

neurodesktop_nested_runtime_mode="$(printf '%s' "${NEURODESKTOP_NESTED_CONTAINER_RUNTIME:-auto}" | tr '[:upper:]' '[:lower:]')"
neurodesktop_host_singularity_bin="${NEURODESKTOP_HOST_SINGULARITY_BIN:-/opt/host-singularity-bin/singularity}"
neurodesktop_host_singularity_dir="$(dirname "${neurodesktop_host_singularity_bin}")"

export NEURODESKTOP_NESTED_CONTAINER_RUNTIME_ACTIVE=image
case "${neurodesktop_nested_runtime_mode}" in
        auto|host)
                if [ -x "${neurodesktop_host_singularity_bin}" ]; then
                        neurodesktop_path_prepend_once "${neurodesktop_host_singularity_dir}"
                        export NEURODESKTOP_NESTED_CONTAINER_RUNTIME_ACTIVE=host
                        export NEURODESKTOP_HOST_SINGULARITY_BIN="${neurodesktop_host_singularity_bin}"
                elif [ "${neurodesktop_nested_runtime_mode}" = "host" ]; then
                        export NEURODESKTOP_NESTED_CONTAINER_WARNING="NEURODESKTOP_NESTED_CONTAINER_RUNTIME=host was requested, but ${neurodesktop_host_singularity_bin} is not executable."
                fi
                ;;
        image)
                ;;
        *)
                export NEURODESKTOP_NESTED_CONTAINER_WARNING="Unknown NEURODESKTOP_NESTED_CONTAINER_RUNTIME='${NEURODESKTOP_NESTED_CONTAINER_RUNTIME}'. Expected auto, host, or image."
                ;;
esac

if [ "${neurodesktop_no_new_privs}" = "1" ] \
        && [ "${NEURODESKTOP_NESTED_CONTAINER_RUNTIME_ACTIVE}" != "host" ] \
        && [ -z "${NEURODESKTOP_NESTED_CONTAINER_WARNING:-}" ]; then
        export NEURODESKTOP_NESTED_CONTAINER_WARNING="Nested Neurodesk app containers may fail because the outer Apptainer/Singularity container has NoNewPrivs=1. Launch the outer container with setuid support and bind a host Singularity runtime to /opt/host-singularity-bin for nested app-container execution."
fi

if [ -n "${NEURODESKTOP_NESTED_CONTAINER_WARNING:-}" ] \
        && [ -z "${NEURODESKTOP_NESTED_CONTAINER_WARNING_SHOWN:-}" ] \
        && { [[ $- == *i* ]] || [ -t 1 ]; }; then
        export NEURODESKTOP_NESTED_CONTAINER_WARNING_SHOWN=1
        echo "[WARN] ${NEURODESKTOP_NESTED_CONTAINER_WARNING}" >&2
fi

unset neurodesktop_bindpath neurodesktop_host_singularity_bin neurodesktop_host_singularity_dir
unset neurodesktop_nested_runtime_mode neurodesktop_no_new_privs neurodesktop_status_file
