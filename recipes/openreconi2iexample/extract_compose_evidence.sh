#!/usr/bin/env bash
# Extract the decisive whole-body composing evidence from a Siemens syngo trace log.
# Usage: ./extract_compose_evidence.sh <path-to.log> [out.txt]
# The trace is ISO-8859 with very long lines, so we force LC_ALL=C and grep -a everywhere.
set -u
LOG="${1:?usage: extract_compose_evidence.sh <log> [out]}"
OUT="${2:-${LOG%.*}_compose_evidence.txt}"
export LC_ALL=C

{
  echo "===================================================================="
  echo "COMPOSE EVIDENCE EXTRACT for: $LOG"
  echo "===================================================================="

  echo; echo "### 0. Converter / OpenRecon functor present? (empty => native, no OpenRecon)"
  grep -a -oE "Using converter library: [A-Za-z0-9._]+|Inserting injector functor [A-Za-z0-9._]+|OpenRecon[._][A-Za-z0-9._]+" "$LOG" | sort | uniq -c

  echo; echo "### 1. Protocol name per MeasUID (which test is which)"
  awk -F'|' '
    /ctrl.Properties.Queue.ProtocolName/ { if (match($0,/ProtocolName, [A-Za-z0-9_]+/)) pn=substr($0,RSTART+14,RLENGTH-14) }
    /Prepare job received: MeasUID/ { if (match($5,/MeasUID [0-9]+/)) print substr($5,RSTART+8,RLENGTH-8)" -> "pn }
  ' "$LOG" | sort -n -u

  echo; echo "### 2. THE failure(A) signature: lowest position vector / TRA combining"
  grep -a -nE "Transversal lowest position vector|lowest slice position has not been set|Overlap checks have failed|Error during final processing of TRA|Composing of transversely oriented images failed" "$LOG" \
    | awk -F'|' '{print $1" | "$3" | "$5}'

  echo; echo "### 3. failure(B) signature: composer switched off / cloning failed (segments)"
  grep -a -nE "Composer has been switched off|Cloning of image data failed" "$LOG" \
    | awk -F'|' '{print $1" | "$3" | "substr($5,1,120)}'

  echo; echo "### 4. Per-channel outcome: did each FILTER channel find overlap or just fail?"
  grep -a -E "center of overlap|No composing due to|Error during final processing of TRA" "$LOG" \
    | awk -F'|' '{print $1" | "$5}' | sort -u

  echo; echo "### 5. Does the master FILTER ever report a TRA image count? (key: empty => no main volume)"
  grep -a -iE "FILTER: .*(number of TRA|TRA images|main ori|images to compose|received)" "$LOG" \
    | awk -F'|' '{print $1" | "$5}' | sort -u

  echo; echo "### 6. Compose channel property dumps (compose type / export-import alignment)"
  echo "    (export=true => this channel is an alignment MASTER/exporter; key to the FILTER-only failure)"
  grep -a -E "compose type|export\(=true\) or import|use shared alignment parameters|composing function" "$LOG" \
    | awk -F'|' '{print $5}' | sed -E 's/ +/ /g' | sort | uniq -c

  echo; echo "### 7. Reconstructed contrasts: is there a NON-Dixon main/in-phase image?"
  echo "--- Dixon-tokened ImageTypes (count):"
  grep -a -oE 'DERIVED\\PRIMARY\\DIXON\\[A-Z_]+' "$LOG" | sort | uniq -c
  echo "--- candidate MAIN / IN-PHASE / OPP / magnitude ImageTypes (count):"
  grep -a -oE '(ORIGINAL|DERIVED)\\PRIMARY\\(M|IN[A-Z_]*|OPP[A-Z_]*)\\[A-Z_\\]+' "$LOG" | sort | uniq -c
  grep -a -oE '\\(IN_PHASE|OPP_PHASE|IN|OPP)\\' "$LOG" | sort | uniq -c

  echo; echo "### 8. Counts (sanity)"
  printf "  composing-fail (A): "; grep -a -c "lowest slice position has not been set" "$LOG"
  printf "  cloning-fail (B):   "; grep -a -c "Cloning of image data failed" "$LOG"
  echo "===================================================================="
  echo "DONE. Send this file back for diagnosis."
} > "$OUT" 2>&1

echo "Wrote: $OUT"
wc -l "$OUT"
