#!/bin/sh
# Preserve application status and argument boundaries in the FreeSurfer 6 MCR launchers.
set -u

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <deployedMCRroot> [application arguments...]" >&2
    exit 64
fi

case "${0##*/}" in
    run_SegmentSubject.sh) application=segmentSubject ;;
    run_SegmentSubfieldsT1Longitudinal.sh) application=SegmentSubfieldsT1Longitudinal ;;
    run_segmentSubjectT1_autoEstimateAlveusML.sh) application=segmentSubjectT1_autoEstimateAlveusML ;;
    run_segmentSubjectT1T2_autoEstimateAlveusML.sh) application=segmentSubjectT1T2_autoEstimateAlveusML ;;
    run_segmentSubjectT2_autoEstimateAlveusML.sh) application=segmentSubjectT2_autoEstimateAlveusML ;;
    *) echo "Unsupported FreeSurfer MCR launcher: $0" >&2; exit 64 ;;
esac

exe_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
MCRROOT=$1
shift
MCRJRE=${MCRROOT}/sys/java/jre/glnxa64/jre/lib/amd64
LD_LIBRARY_PATH=.:${MCRROOT}/runtime/glnxa64:${MCRROOT}/bin/glnxa64:${MCRROOT}/sys/os/glnxa64:${MCRJRE}/native_threads:${MCRJRE}/server:${MCRJRE}/client:${MCRJRE}:${LD_LIBRARY_PATH:-}
XAPPLRESDIR=${MCRROOT}/X11/app-defaults
export LD_LIBRARY_PATH XAPPLRESDIR
unset JAVA_TOOL_OPTIONS

MCR_CACHE_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/fs6-mcr.XXXXXX") || exit 1
export MCR_CACHE_ROOT
trap 'rm -rf -- "$MCR_CACHE_ROOT"' 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

"${exe_dir}/${application}" "$@"
application_status=$?
exit "$application_status"
