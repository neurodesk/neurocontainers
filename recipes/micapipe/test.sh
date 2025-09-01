which micapipe
# Test 

bids=${PWD}/bids
out=${bids}/micapipe_output
tmp=${bids}/micapipe_tmp
mkdir -p ${out} ${tmp} 
#untar the test data into the bids folder 
tar -xvzf test.tar.gz -C ${bids}
#point to the license file
fs_lic=${bids}/license.txt 
cp ${PWD}/license.txt ${fs_lic} 

micapipe \
        -bids /bids/ -out /out/ -fs_licence '/opt/licence.txt' \
        -sub 'sub-TS-APPA' -proc_structural \
        -proc_surf \
        -proc_dwi \
        -dwi_main /bids/sub-TS-APPA/dwi/sub-TS_ses-01_AP_BLOCK_1_DIFFUSION_30DIR_dir-AP_dwi.nii.gz,/bids/sub-TS-APPA/dwi/sub-TS_ses-01_AP_BLOCK_2_DIFFUSION_30DIR_dir-AP_dwi.nii.gz \
        -dwi_rpe /bids/sub-TS-APPA/dwi/sub-TS_ses-01_PA_BLOCK_1_DIFFUSION_30DIR_dir-PA_dwi.nii.gz,/bids/sub-TS-APPA/dwi/sub-TS_ses-01_PA_BLOCK_2_DIFFUSION_30DIR_dir-PA_dwi.nii.gz \
        -QC_subj "sub-TS-APPA"



