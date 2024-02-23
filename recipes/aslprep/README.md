
----------------------------------
## aslprep/toolVersion ##
This pipeline is developed by the Satterthwaite lab at the University of Pennysilvania for use at the The Lifespan Informatics and Neuroimaging Center at the University of Pennylvannia, as well as for open-source software distribution.

ASLPrep is a Arterial Spin Labeling (ASL) data preprocessing and Cerebral Blood FLow (CBF) computation pipeline that is designed to provide an easily accessible, state-of-the-art interface that is robust to variations in scan acquisition protocols and that requires minimal user input, while providing easily interpretable and comprehensive error and output reporting. It performs basic processing steps (coregistration, normalization, unwarping, noise component extraction, segmentation, skullstripping etc.), CBF computation, denoising CBF, CBF partial volume correction and providing outputs that can be easily submitted to a variety of group level analyses, including task-based or resting-state CBF, graph theory measures, surface or volume-based statistics, etc.

The input dataset is required to be in valid BIDS format, and it must include at least one T1w structural image.

Example:
```
aslprep data/bids_root/ out/ participant -w work/

```

More documentation can be found here: https://aslprep.readthedocs.io/en/latest/usage.html#usage

To run container outside of this environment: ml aslprep/toolVersion

----------------------------------
