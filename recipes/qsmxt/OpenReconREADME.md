# QSMxT OpenRecon

QSMxT OpenRecon creates quantitative susceptibility maps from MRI magnitude and phase images and returns the results to the scanner database. Use it to reconstruct and compare QSM maps from brain gradient-echo acquisitions within your OpenRecon workflow.

The package supports single-echo and multi-echo data, a choice of reconstruction methods, adjustable brain masks, and optional SWI, SMWI, T2*, and R2* maps. You can process distortion-corrected images, non-distortion-corrected images, or both.

## Prepare your acquisition

Use a plain gradient-echo (GRE) sequence with both magnitude and **unfiltered phase** reconstruction enabled. Filtered phase from an SWI sequence is not a suitable input. Magnitude and phase images must have matching echoes and the same distortion-correction setting.

An [example Siemens 3 T GRE protocol](https://github.com/neurodesk/neurocontainers/blob/main/recipes/qsmxt/gre_qsm.pro) is available as a starting point. It acquires five echoes at 5, 10, 15, 20, and 25 ms. Review the acquisition and safety settings on your scanner before use. The example saves different reconstruction settings from the package defaults, so check the OpenRecon controls after loading it.

## Run a reconstruction

With the QSMxT package installed in OpenRecon:

1. Select **qsmxt** as the reconstruction package. Leave **config** set to **qsmxt**.
2. Set **Input images** to match your acquisition. The default is **Distortion corrected**.
3. For an initial reconstruction, leave **Pipeline preset** on **Custom algorithm controls** with **QSM algorithm** set to **HD-QSM**, **Unwrap** to **ROMEO**, and **Background** to **iSMV**.
4. Leave **Mask preset** on **BET (recommended)** to identify the brain from the magnitude image.
5. Leave **Output maps** on **QSM**. The source magnitude and phase series are always returned to the scanner database.
6. Run the acquisition with OpenRecon enabled. When reconstruction finishes, look for the **QSMxT QSM** series alongside the original images.

## Choose the input images

**Input images** offers these options:

| Option | When to use it |
| --- | --- |
| Distortion corrected | Process the corrected magnitude and phase images. This is the default. |
| Not distortion corrected (ND) | Process the magnitude and phase series marked `ND`. |
| Both | Compare corrected and uncorrected results. Both magnitude/phase pairs must be available. |

**Both** returns separate `DC` and `ND` result series and takes roughly twice as long as processing one pair. If reconstruction reports missing input images, check that both magnitude and unfiltered phase are available for the selected correction setting.

## Choose the output maps

Use **Output maps** to select what returns to the scanner database:

| Option | Result |
| --- | --- |
| QSM | Quantitative susceptibility map. This is the default. |
| All available | All available maps from the run, including the brain mask and combined magnitude image. |
| Magnitude | Combined magnitude image. |
| Mask | Brain mask used for reconstruction, useful for checking brain coverage. |
| SWI | Susceptibility-weighted image. |
| SMWI | Susceptibility map-weighted images: the magnitude weighted by the QSM map. Returns a paramagnetic and a diamagnetic series. |
| T2 star | T2* relaxation map. Use at least three equally spaced echoes. |
| R2 star | R2* relaxation-rate map. Use a multi-echo acquisition. |

Original magnitude and phase images are always returned before the selected output maps.

**SMWI** returns **QSMxT SMWI paramagnetic**, which darkens paramagnetic sources such as veins, iron, and microbleeds, and **QSMxT SMWI diamagnetic**, which darkens diamagnetic sources such as calcification. Unlike SWI, the weighting comes from the susceptibility map, so the contrast sits on the source instead of spreading with its field. SMWI always runs QSM. It uses QSMxT's default weighting, a 1 ppm threshold with power 4, which emphasizes strong sources over subtle tissue contrast.

For quantitative analysis, use a viewer that applies DICOM rescaling. QSM DICOM values are in parts per billion (ppb); divide by 1000 to convert to parts per million (ppm).

T2* DICOM values are in milliseconds. The default window covers zero to the 95th percentile of positive fits and stays constant across slices. Scanner storage retains steps of 1 ms or finer, with values above 4095 ms saturating. Use the full-precision NIfTI output to inspect extreme fits.

## Separate susceptibility sources

Set **Source separation** to **R2*-QSM** or **DECOMPOSE** to add paramagnetic,
diamagnetic, and separated-total maps to your selected outputs. The default is
**Off**. Both methods use the same GRE acquisition and require at least three
magnitude and unfiltered phase echoes. No spin-echo acquisition is needed.
Use equally spaced echoes for R2*-QSM's R2* fit.

The returned series are **QSMxT paramagnetic**, **QSMxT diamagnetic**, and
**QSMxT separated total**, with a `DC` or `ND` suffix matching the input images.
The diamagnetic map contains positive magnitudes. The separated-total map is
paramagnetic minus diamagnetic. These maps use the same DICOM units as QSM,
parts per billion. Divide rescaled DICOM values by 1000 to obtain ppm.

## Change the reconstruction method

Use **Pipeline preset** to choose a predefined combination of processing methods. The menu also includes complete reconstruction methods such as QSMART, TGV, AutoQSM, NeXtQSM, iQSM, and iQSM+. Packaged deep-learning methods work offline and run on the CPU.

A preset overrides **QSM algorithm**, **Unwrap**, and **Background**. To choose those stages yourself, select **Custom algorithm controls**:

- **Unwrap** removes phase jumps. The OpenRecon default is ROMEO.
- **Background** removes background-field contributions. The default is iSMV.
- **QSM algorithm** calculates susceptibility. The default is HD-QSM.

With **Custom algorithm controls**, **Default (HD-QSM)** uses HD-QSM, **Default (ROMEO)** uses ROMEO, and **Default (iSMV)** uses iSMV. Any other **Pipeline preset** overrides all three choices, even if you change their displayed values. Reconstruction time and results depend on the method, acquisition, and available hardware. See the [QSMxT algorithm reference](https://qsmxt.github.io/QSMxT/reference/algorithms/) for descriptions of the methods. The upstream command-line defaults differ from the OpenRecon defaults listed here.

## Adjust the brain mask

The mask defines the region included in reconstruction. To inspect its coverage alongside the QSM map, select **All available** under **Output maps**.

Start with **BET (recommended)**. If the mask excludes brain tissue, lower **BET threshold** from its default of **0.5** to make the mask larger.

For threshold-based masking, choose **Robust threshold** or **BET + threshold union** under **Mask preset**. The union includes regions selected by either method. **Threshold input** selects the image used for thresholding, and **Threshold method** selects Otsu or Percentile. **Mask percentile** sets the cutoff when you choose Percentile, with a default of **65%**.

**Mask cleanup** can fill holes and close small gaps. Its default is **Close and fill holes**. Mask settings apply independently of the pipeline preset.

## Citation
Stewart, Ashley Wilton, Simon Daniel Robinson, Kieran O’Brien, et al. “QSMxT: Robust Masking and Artifact Reduction for Quantitative Susceptibility Mapping.” Magnetic Resonance in Medicine 87, no. 3 (2022): 1289–300. https://doi.org/10.1002/mrm.29048.

## Help and further reading

For problems or feature requests, [open a NeuroContainers issue](https://github.com/neurodesk/neurocontainers/issues). For questions about using the package, visit the [Neurodesk discussion forum](https://github.com/orgs/neurodesk/discussions).

Implementation details and benchmark results are in the [OpenRecon technical reference](https://github.com/neurodesk/neurocontainers/blob/main/recipes/qsmxt/OpenReconTechnicalReference.md).
