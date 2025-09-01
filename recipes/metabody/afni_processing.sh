#!/bin/bash

# Usage: ./process_epi_afni_fixedstim.sh /path/to/dicoms output_prefix TR
 
set -euo pipefail

# Parse arguments.
PARSED=$(getopt --options "" --long input:,output:,n-threads:,tr:,stim-dur:,skip-trs: --name "$0" -- "$@")
# Terminate script if failed to parse arguments properly.
if [[ $? -ne 0 ]]; then
    echo "Error parsing options" >&2
    exit 1
fi

# Reset the positional parameters to the parsed arguments.
eval set -- "$PARSED"

INPUT=""
OUTPUT=""
# Use the max amount of available threads as the default value.
# TODO: Is this safe on scanner computers?
NTHREADS=`nproc`
TR=1
STIM_DUR=10
SKIP_TRS=0

# Extract values from arguments. `--` indicates the end of arguments.
while true; do
    case "$1" in
        --input)
            INPUT="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --n-threads)
            NTHREADS="$2"
            shift 2
            ;;
        --tr)
            TR="$2"
            shift 2
            ;;
        --stim-dur)
			STIM_DUR="$2"
			shift 2
			;;
        --skip-trs)
			SKIP_TRS="$2"
			shift 2
			;;
        --)
            shift
            break
            ;;
        *)
            echo "Unexpected option: $1"
            exit 1
            ;;
    esac
done

mkdir -p $OUTPUT
tmp_dir="/tmp/afni"
mkdir -p $tmp_dir

# For getting back here at the end.
orig_pwd=$PWD

# apply 3dTcat to copy input dsets to temp dir,
# while removing the first 0 TRs
echo "${INPUT}[${SKIP_TRS}..$]"
3dTcat -prefix $tmp_dir/pb00.r01.tcat  "${INPUT}[${SKIP_TRS}..$]"
cd $tmp_dir

echo "Running despiking..."
# apply 3dDespike to each run
3dDespike -NEW -nomask -prefix "pb01.r01.despike" "pb00.r01.tcat+orig"
 
# Step 3: Motion correction with 3dvolreg
echo "Running 3dvolreg..."
# extract MIN_OUTLIER index for current run
3dToutcount -automask -fraction -polort 4 -legendre pb00.r01.tcat+orig > outcount.r01.1D
# \' is needed to get the index number.
min_outlier_index=`3dTstat -argmin -prefix - "outcount.r01.1D"\'`

# extract volreg base for this run
3dbucket -prefix "vr_base_per_run_r01" "pb01.r01.despike+orig[${min_outlier_index}]"

# register each volume to the base image
3dvolreg -verbose -zpad 1 -base "vr_base_per_run_r01+orig"          \
            -1Dfile "dfile.r01.1D" -prefix "pb01.r01.volreg+orig"   \
            -Fourier                                                \
            -1Dmatrix_save "mat.r01.vr.aff12.1D"                    \
            "pb01.r01.despike+orig"
 
# Step 4: Get number of volumes
NVOLS=$(3dinfo -nv "pb01.r01.volreg+orig")
NDUR=$(echo "${NVOLS} * ${TR}" | bc)
 
# Step 5: Generate fixed timing files
# echo "Generating fixed block timing files..."
# mkdir -p timing
 
# conds=(left_arm right_arm left_foot right_foot left_hand right_hand lips)
 
# for i in "${!conds[@]}"; do
#   cond=${conds[$i]}
#   start_time=$(echo "$i * 10" | bc)
#   times=""
#   t=$start_time
#   while (( $(echo "$t + 10 <= $NDUR" | bc -l) )); do
#     times="$times $t"
#     t=$(echo "$t + 70" | bc)  # 7 conditions * 10s
#   done
#   echo $times > timing/${cond}.1D
# done
 
# Step 6: Run 3dDeconvolve
echo "Running 3dDeconvolve..."

# # compute de-meaned motion parameters (for use in regression)
# 1d_tool.py -infile dfile_rall.1D -set_nruns 1                              \
#            -demean -write motion_demean.1D

# # create censor file motion_${subj}_censor.1D, for censoring motion 
# 1d_tool.py -infile dfile_rall.1D -set_nruns 1                              \
#     -show_censor_count -censor_prev_TR                                     \
#     -censor_motion 0.3 motion_censor

# For motion censoring add:
#   -ortvec motion_demean.1D mot_demean \
# To use auto-generated timing files (see the code above):
#   -stim_times 1 timing/left_arm.1D "BLOCK(${STIM_DUR},1)" -stim_label 1 LA \
#   -stim_times 2 timing/right_arm.1D "BLOCK(${STIM_DUR},1)" -stim_label 2 RA \
#   -stim_times 3 timing/left_foot.1D "BLOCK(${STIM_DUR},1)" -stim_label 3 LF \
#   -stim_times 4 timing/right_foot.1D "BLOCK(${STIM_DUR},1)" -stim_label 4 RF \
#   -stim_times 5 timing/left_hand.1D "BLOCK(${STIM_DUR},1)" -stim_label 5 LH \
#   -stim_times 6 timing/right_hand.1D "BLOCK(${STIM_DUR},1)" -stim_label 6 RH \
#   -stim_times 7 timing/lips.1D "BLOCK(${STIM_DUR},1)" -stim_label 7 LP \
3dDeconvolve \
  -input "pb01.r01.volreg+orig" \
  -polort 4 -num_stimts 7 -local_times \
  -stim_times 1 "/opt/code/stim_times/sub-1_run-1_LEFT ELBOW_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 1 LA \
  -stim_times 2 "/opt/code/stim_times/sub-1_run-1_RIGHT ELBOW_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 2 RA \
  -stim_times 3 "/opt/code/stim_times/sub-1_run-1_LEFT FOOT_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 3 LF \
  -stim_times 4 "/opt/code/stim_times/sub-1_run-1_RIGHT FOOT_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 4 RF \
  -stim_times 5 "/opt/code/stim_times/sub-1_run-1_LEFT HAND_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 5 LH \
  -stim_times 6 "/opt/code/stim_times/sub-1_run-1_RIGHT HAND_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 6 RH \
  -stim_times 7 "/opt/code/stim_times/sub-1_run-1_TONGUE_2024_10_31_00_07.1D" "BLOCK(${STIM_DUR},1)" -stim_label 7 LP \
  -gltsym 'SYM: LA -0.16667*LF -0.16667*LH -0.16667*LP -0.16667*RA -0.16667*RF -0.16667*RH' -glt_label 1 LA-others \
  -gltsym 'SYM: LF -0.16667*LA -0.16667*LH -0.16667*LP -0.16667*RA -0.16667*RF -0.16667*RH' -glt_label 2 LF-others \
  -gltsym 'SYM: LH -0.16667*LA -0.16667*LF -0.16667*LP -0.16667*RA -0.16667*RF -0.16667*RH' -glt_label 3 LH-others \
  -gltsym 'SYM: LP -0.16667*LA -0.16667*LF -0.16667*LH -0.16667*RA -0.16667*RF -0.16667*RH' -glt_label 4 LP-others \
  -gltsym 'SYM: RA -0.16667*LA -0.16667*LF -0.16667*LH -0.16667*LP -0.16667*RF -0.16667*RH' -glt_label 5 RA-others \
  -gltsym 'SYM: RF -0.16667*LA -0.16667*LF -0.16667*LH -0.16667*LP -0.16667*RA -0.16667*RH' -glt_label 6 RF-others \
  -gltsym 'SYM: RH -0.16667*LA -0.16667*LF -0.16667*LH -0.16667*LP -0.16667*RA -0.16667*RF' -glt_label 7 RH-others \
  -x1D design.xmat.1D \
  -bucket stats.nii \
  -tout -fout -bout \
  -jobs $NTHREADS
 
echo "Done."

# TODO: censoring outliers ('outcount_${subj}_censor.1D')
# TODO: censoring motion (see above)
# TODO: deobliquing? See AFNI's warnings
# TODO: better generation of timing files (hash as random seed?)
# TODO: Avg all of the volumes - align to the avg and show results on the avg
# TODO: Take a median idx/image
# TODO: Avg over time after reallignment
# TODO: Upsample the avg image (after realignment) to 1mm iso
# TODO: Can we use synthSR to upsample/turn into anat

3dAFNItoNIFTI -prefix output_image.nii pb01.r01.volreg+orig

cd $orig_pwd
cp $tmp_dir/stats.nii $OUTPUT/stats.nii
cp $tmp_dir/output_image.nii $OUTPUT/output_image.nii
rm -r $tmp_dir