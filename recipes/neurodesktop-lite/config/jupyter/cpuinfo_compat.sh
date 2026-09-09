#!/usr/bin/env bash
set -euo pipefail

source_path="${NEURODESKTOP_CPUINFO_SOURCE:-/proc/cpuinfo}"
output_path="${NEURODESKTOP_CPUINFO_OUTPUT:-${HOME:-/tmp}/.local/cpuinfo_with_MHz_fix}"
mount_target="${NEURODESKTOP_CPUINFO_MOUNT_TARGET:-/proc/cpuinfo}"

if grep -Eiq '^[[:space:]]*cpu[[:space:]]+mhz[[:space:]]*:' "${source_path}"; then
    exit 0
fi

install -d -m 0755 "$(dirname "${output_path}")"
temporary_path="$(mktemp "${output_path}.tmp.XXXXXX")"
trap 'rm -f "${temporary_path}"' EXIT

# MATLAB Runtime's Linux CPU-frequency probe expects one cpu MHz field in
# every processor record. Native ARM /proc/cpuinfo omits it, even when an
# amd64 application is running through binfmt/QEMU.
awk '
    function add_frequency() {
        if (!have_frequency && have_record) {
            print "cpu MHz         : 2400.000"
        }
    }
    /^[[:space:]]*cpu[[:space:]]+MHz[[:space:]]*:/ {
        have_frequency=1
    }
    /^[[:space:]]*$/ {
        add_frequency()
        print
        have_frequency=0
        have_record=0
        next
    }
    {
        print
        have_record=1
    }
    END {
        add_frequency()
    }
' "${source_path}" > "${temporary_path}"

chmod 0644 "${temporary_path}"
mv -f "${temporary_path}" "${output_path}"
trap - EXIT

if [ "${NEURODESKTOP_CPUINFO_SKIP_MOUNT:-0}" = "1" ]; then
    exit 0
fi

if [ "$(id -u)" = "0" ]; then
    mount --bind "${output_path}" "${mount_target}"
elif sudo -n true 2>/dev/null; then
    sudo mount --bind "${output_path}" "${mount_target}"
else
    echo "[WARN] Passwordless sudo is unavailable; skipping the MATLAB CPU-frequency workaround." >&2
    exit 0
fi

if ! grep -Eiq '^[[:space:]]*cpu[[:space:]]+mhz[[:space:]]*:' "${mount_target}"; then
    echo "[WARN] The MATLAB CPU-frequency compatibility mount did not take effect." >&2
    exit 1
fi
