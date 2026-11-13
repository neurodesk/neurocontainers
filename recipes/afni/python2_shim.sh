#!/usr/bin/env bash
# nipype's AFNIPythonCommand (AlignEpiAnatPy, OneDToolPy) runs
# `python2 $(which align_epi_anat.py) ...`. On Neurodesk that path is the
# host-side bash wrapper generated for the AFNI module, so Python cannot parse
# it, and current images have no python2 at all. When the script argument is
# not a Python file but names an AFNI script, run the in-container copy instead.
if [[ $# -gt 0 && $1 != -* ]]; then
  afni_script=/usr/local/abin/${1##*/}
  if [[ -f $afni_script ]] && ! head -n 1 -- "$1" 2>/dev/null | grep -q '^#!.*python'; then
    set -- "$afni_script" "${@:2}"
  fi
fi
exec python3 "$@"
