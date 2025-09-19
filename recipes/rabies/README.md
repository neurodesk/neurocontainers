## rabies/0.5.3 ##

RABIES (Rodent Automated Bold Improvement of EPI Sequences) is an open source image processing pipeline for rodent fMRI. It conducts state-of-the-art preprocessing and confound correction, and supplies standard resting-state functional connectivity analyses.

**Key Features:**
- Robust registration workflow with automatically-adapting parameters
- Head motion correction and susceptibility distortion correction  
- Brain parcellation and resampling to native or common space
- Confound regression with multiple nuisance regressor options
- ICA-AROMA denoising and frame censoring
- Seed-based and whole-brain connectivity analyses
- Comprehensive data quality assessment tools

**Main Processing Stages:**
1. **Preprocessing**: Motion correction, distortion correction, registration, parcellation
2. **Confound Correction**: Detrending, regression, filtering, smoothing  
3. **Analysis**: Connectivity analysis, group-ICA, dual regression, data diagnosis

**Example Usage:**

The following section describes the basic syntax to run RABIES with an example dataset available here http://doi.org/10.5281/zenodo.3937697

**Preprocessing:**
```bash
rabies -p MultiProc preprocess test_dataset/ preprocess_outputs/ --TR 1.0s --no_STC
```
This runs the minimal preprocessing step on the test dataset and stores outputs into preprocess_outputs/ folder. The option `-p MultiProc` specifies to run the pipeline in parallel according to available local threads.

**Confound Correction:**
```bash
rabies -p MultiProc confound_correction preprocess_outputs/ confound_correction_outputs/ --TR 1.0s --commonspace_bold --smoothing_filter 0.3 --conf_list WM_signal CSF_signal vascular_signal mot_6
```
This conducts the modeling and regression of confounding sources with custom denoising options. In this case, we apply highpass filtering at 0.01Hz, together with voxelwise regression of the 6 rigid realignment parameters and the mean WM, CSF and vascular signals derived from masks provided with the anatomical template. Finally, a 0.3mm smoothing filter is applied.

**Analysis:**
```bash
rabies -p MultiProc analysis confound_correction_outputs analysis_outputs/ --TR 1.0s --group_ICA --DR_ICA
```
This runs group independent component analysis (--group_ICA) using FSL's MELODIC function, followed by dual regression (--DR_ICA) to back propagate the group components onto individual subjects.

**Requirements:**
- Input data must follow BIDS format
- Requires substantial computational resources for full pipeline
- Processing time ranges from hours to days depending on data size

**Documentation:** https://rabies.readthedocs.io/en/latest/  
**GitHub:** https://github.com/CoBrALab/RABIES

**Citation:**
```
Desrosiers-Grégoire, et al. RABIES: a fully open source image processing toolbox for rodent fMRI. Nat Commun 15, 6708 (2024). https://doi.org/10.1038/s41467-024-50826-8
```

**License:** Academic and educational use only. Commercial use requires separate license from CoBrALab.

To run applications outside of this container: ml rabies/0.5.3