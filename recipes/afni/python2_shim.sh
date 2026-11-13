#!/usr/bin/env bash
# nipype's AFNIPythonCommand (AlignEpiAnatPy, OneDToolPy) runs
# `python2 $(which align_epi_anat.py) ...`. On Neurodesk that path is the
# host-side bash wrapper generated for the AFNI module, so Python cannot parse
# it, and current images have no python2 at all. When the script argument names
# an AFNI script and is either not visible here or a non-Python #! script (the
# wrapper), run the in-container copy instead. Interpreter options before the
# script are not handled: nipype never passes any.
if [[ $# -gt 0 && $1 != -* ]]; then
  afni_script=/usr/local/abin/${1##*/}
  if [[ -f $afni_script ]]; then
    first_line=
    [[ -r $1 ]] && IFS= read -r first_line < "$1"
    if [[ ! -e $1 || ($first_line == '#!'* && $first_line != *python*) ]]; then
      set -- "$afni_script" "${@:2}"
    fi
  fi
fi
exec python3 "$@"
