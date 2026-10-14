# QSMxT OpenRecon

QSMxT OpenRecon creates quantitative susceptibility maps from MRI magnitude and phase images and returns the results to the scanner database. Use it to reconstruct and compare QSM maps from brain gradient-echo acquisitions within your OpenRecon workflow.

The package supports single-echo and multi-echo data, a choice of reconstruction methods, adjustable brain masks, and optional SWI, SMWI, T2*, and R2* maps. You can process distortion-corrected images, non-distortion-corrected images, or both.

This package uses QSMxT 10.0.0. SWI now uses CLEAR-SWI across every echo, and V-SHARP no longer deconvolves by default. Together with the masking changes, these settings change the results relative to QSMxT 9.22.0.

## Prepare your acquisition

Use a plain gradient-echo (GRE) sequence with both magnitude and **unfiltered phase** reconstruction enabled. Filtered phase from an SWI sequence is not a suitable input. Magnitude and phase images must have matching echoes and the same distortion-correction setting.

An [example Siemens 3 T GRE protocol](https://github.com/neurodesk/neurocontainers/blob/main/recipes/qsmxt/gre_qsm.pro) is available as a starting point. It acquires five echoes at 5, 10, 15, 20, and 25 ms. Review the acquisition and safety settings on your scanner before use. The example saves WH-QSM, HARPERELLA, and Robust threshold masking instead of the package defaults RTS, V-SHARP, and Robust threshold. Check the OpenRecon algorithm and mask controls after loading it.

## Run a reconstruction

With the QSMxT package installed in OpenRecon:

1. Select **qsmxt** as the reconstruction package. Leave **Config** set to **standard GRE**.
2. Set **Input images** to match your acquisition. The default is **Prefer distortion corrected**.
3. Leave **Pipeline preset** on **Custom algorithm controls**. Leave **Dipole inversion** set to **RTS**, **Unwrap** to **ROMEO**, and **Background** to **V-SHARP**.
4. Leave **Mask preset** on **Robust threshold** to threshold the combined magnitude image.
5. Leave **Output maps** on **QSM**. The source magnitude and phase series are always returned to the scanner database.
6. Run the acquisition with OpenRecon enabled. When reconstruction finishes, look for the **QSMxT QSM** series alongside the original images.

## Choose the input images

**Input images** offers these options:

| Option | When to use it |
| --- | --- |
| Prefer distortion corrected | Prefer the corrected pair; use a complete ND pair if a preferred component is missing. This is the default. |
| Not distortion corrected (ND) | Process the magnitude and phase series marked `ND`. |
| Both | Compare corrected and uncorrected results. Both magnitude/phase pairs must be available. |

Series without a separate `ND` name token use the preferred slot, including unmarked acquisitions. This naming convention does not verify physical distortion correction. The default falls back only when a preferred magnitude or phase component is missing. Ambiguous inputs, incompatible echoes or geometry, and reconstruction errors do not trigger fallback. Explicit ND and Both remain strict. Fallback results carry `ND` labels.

**Both** returns separate `DC` and `ND` result series and takes roughly twice as long as processing one pair. If reconstruction reports missing input images, check that both magnitude and unfiltered phase are available for the selected correction setting.

For clearQsm GRE acquisitions, the adapter selects the matching `Aspire_M` and `Aspire_P` source
series. It excludes the accompanying ROMEO mask, unwrapped phase, B0 map, sensitivity-corrected
magnitude, and CLEAR-SWI products from QSM input. These processed companions are also excluded from
original image return. Genuine magnitude and phase sources return with their pixels, voxel geometry,
and echo identities preserved, including both DC and ND sources when only one variant is
reconstructed. If several complete source pairs remain, reconstruction reports an ambiguity instead
of choosing by arrival order. Selected magnitude and phase must have matching echoes and voxel
geometry.

## Process dual-polarity 3D EPI data

Set **Config** to **3D EPI dual polarity QSM** for a single-TE 3D EPI acquisition that repeats the volume with alternating readout polarity. The package needs at least two repetitions in both the magnitude and the phase series.

Before QSMxT runs, the repetitions are combined with the basic preprocessing pipeline from [ep3d-highres-preprocessing](https://github.com/mrphysics-bonn/ep3d-highres-preprocessing):

1. FSL MCFLIRT estimates motion on the magnitude images. The same transforms are applied to the real and imaginary parts.
2. The smooth phase differences between repetitions, including the polarity-dependent offset, are removed relative to the first repetition.
3. The repetitions are averaged as complex images.

QSMxT then reconstructs the averaged magnitude and phase as a single-echo dataset with the other controls as set. Choose **3D EPI dual polarity QSM + NLM denoise** to also apply complex non-local-means denoising (ANTs) to the average. This increases the processing time. Source separation needs three echoes, so it is not available in this mode.

## Choose the output maps

Use **Output maps** to select what returns to the scanner database:

| Option | Result |
| --- | --- |
| QSM | Quantitative susceptibility map. This is the default. |
| All available | Compatible maps from the run, including the brain mask. |
| Magnitude | Combined magnitude image. Currently unavailable; see the note below. |
| Mask | Brain mask used for reconstruction, useful for checking brain coverage. |
| SWI | Susceptibility-weighted image. |
| SMWI | Susceptibility map-weighted images: the magnitude weighted by the QSM map. Returns a paramagnetic and a diamagnetic series. |
| T2 star | T2* relaxation map. Use at least three equally spaced echoes. |
| R2 star | R2* relaxation-rate map. Use at least three equally spaced echoes. |

**All available** omits T2* and R2* when a selected input pair has fewer than three echoes. Other compatible maps still return. GRE echo limits apply before this check, and EPI repetitions produce one averaged echo. Selecting T2* or R2* explicitly with fewer than three echoes stops that pair before reconstruction or EPI preprocessing.

Combined magnitude is currently not returned by this adapter, including with **All available**. See [issue #3631](https://github.com/neurodesk/neurocontainers/issues/3631). Original magnitude and phase images are always returned before the selected output maps.

**SMWI** returns **QSMxT SMWI paramagnetic**, which darkens paramagnetic sources such as veins,
iron, and microbleeds, and **QSMxT SMWI diamagnetic**, which darkens diamagnetic sources such
as calcification. Unlike SWI, the weighting comes from the susceptibility map, so the contrast
sits on the source instead of spreading with its field. SMWI always runs QSM. It uses QSMxT's
default weighting, a 1 ppm threshold with power 4, which emphasizes strong sources over subtle
tissue contrast.

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

Leave **Pipeline preset** on **Custom algorithm controls** to choose **Unwrap**, **Background**, and **Dipole inversion** independently. The default stages are ROMEO, V-SHARP, and RTS. **iLSQR** is also available in **Dipole inversion**. Packaged deep-learning methods run on the CPU with local models.

Reconstruction time and results depend on the method, acquisition, and hardware. The upstream command-line defaults differ from the explicit OpenRecon defaults.

**Pipeline preset** also offers ten historical stage combinations and six complete methods. Named presets override the three individual stage controls.

Complete methods ignore stages they replace, even when those controls show other choices. QSMART, TGV, AutoQSM, NeXtQSM, iQSM, and iQSM+ handle background removal internally. iQSM and iQSM+ also replace field mapping. TGV takes single-echo phase directly, but uses field mapping for multi-echo input. Historical measurements do not validate QSMxT 10 or your acquisition.

## Adjust the brain mask

The mask defines the region included in reconstruction. To inspect its coverage alongside the QSM map, select **All available** under **Output maps**.

Start with **Robust threshold**. It uses Otsu thresholding of combined magnitude, then closes small gaps and fills holes. It adds no dilation or erosion. **HD-BET**, **BET**, and **BET + threshold union** remain alternatives in **Mask preset**.

Threshold input, threshold method, BET fractional intensity, and percentile cutoff remain advanced configuration options. They no longer occupy scanner controls. See the [technical reference](https://github.com/neurodesk/neurocontainers/blob/main/recipes/qsmxt/OpenReconTechnicalReference.md) for their keys and defaults.

## Enable two-pass QSM or echo motion alignment

Set **Two-pass QSM** on to reconstruct a second time using a reliable phase-quality mask. QSMxT keeps the reliable mask unfilled and uses ordinary-pass values for its holes. The returned QSM map is the combined result. The intermediate single-pass map is not returned as the main QSM series.

Set **Config** to **standard GRE + echo motion alignment** to align GRE echoes using magnitude anatomy and transform the complex signal. This configuration always enables alignment, even if advanced `motioncorrection` is false.

The first echo is the default reference. Advanced `motionreference` can select `last` or a positive echo index. Standard GRE continues to accept advanced `motioncorrection` and `motionreference`. Echo alignment is separate from EPI repetition correction.
The default Config remains standard GRE. Two-pass QSM defaults to off.

## Reference QSM to an anatomical region

**QSM reference** defaults to `mean`. Set it to `none` to preserve the unreferenced map, or enter any packaged SynthSeg v1 region or FreeSurfer label id. Examples include `thalamus`, `left-thalamus`, `ventricles`, `4,43`, and `left-thalamus,right-caudate`.

A region reference runs packaged SynthSeg on combined magnitude anatomy.
QSMxT subtracts the mean susceptibility within the selected region and
reconstructed support from the whole supported map. Unknown names, unsupported
ids, and empty intersections fail. `ventricles` covers labels 4, 5, 14, 15, 43,
and 44. `csf` is a different SynthSeg v2 label whose public weights are unavailable
in this package. The
[technical reference](https://github.com/neurodesk/neurocontainers/blob/main/recipes/qsmxt/OpenReconTechnicalReference.md#anatomical-reference-regions)
lists every v1 region.

## Process contiguous 2D stacks

**V-SHARP grid** defaults to **Native**. **Finest voxel** and **Isotropic** select the working grid for background removal while retaining the acquired output geometry. **SWI unwrapping** defaults to **Laplacian**. Select **Slice Laplacian** for slice-wise SWI phase unwrapping independently of the QSM unwrap algorithm.

Supply acquired `SliceThickness` and `MRAcquisitionType` in MRD image metadata or
user parameters. Advanced `slicethicknessmm` and `acquisitiontype` can supply
explicit overrides. The adapter retains measured slice spacing, validates
consistent metadata, and rejects nonuniform 2D positions. QSMxT rejects gaps by
default. Advanced `ignoreslicegap` explicitly overrides gap refusal and produces
a warning. When thickness is absent, the adapter warns that gap checking is
unavailable. Thickness is never inferred from affine spacing or slab field of view.

These controls expose native 2D processing. The smoke tests use synthetic stacks and MNI anatomy. They do not establish scanner-specific reconstruction quality.

## Adjust homogeneity correction

**Homogeneity correction** corrects magnitude intensity inhomogeneity for masks, combined magnitude, and SMWI.
**Standard** is the default. It uses 7 mm smoothing and 3 tissue-estimation boxes per axis.
**Fine** uses the same 7 mm smoothing and 15 tissue-estimation boxes per axis for comparison.
SMWI uses the corrected combined magnitude image. CLEAR-SWI uses the individual echo magnitudes and applies its own receive-sensitivity correction independently of this control.
The box count controls local segmentation, not the number of smoothing passes.
Select **Off** to disable this magnitude correction. CLEAR-SWI retains its internal correction.
Fine has not been shown to improve 7 T reconstruction quality. Compare the profiles on your acquisition before choosing a setting.

## Citation
Stewart, Ashley Wilton, Simon Daniel Robinson, Kieran O’Brien, et al. “QSMxT: Robust Masking and Artifact Reduction for Quantitative Susceptibility Mapping.” Magnetic Resonance in Medicine 87, no. 3 (2022): 1289–300. https://doi.org/10.1002/mrm.29048.

For 3D EPI dual polarity QSM, also cite the [ep3d-highres-preprocessing](https://github.com/mrphysics-bonn/ep3d-highres-preprocessing) pipeline by Rüdiger Stirnberg (DZNE).

## Help and further reading

For problems or feature requests, [open a NeuroContainers issue](https://github.com/neurodesk/neurocontainers/issues). For questions about using the package, visit the [Neurodesk discussion forum](https://github.com/orgs/neurodesk/discussions).

Implementation details and benchmark results are in the [OpenRecon technical reference](https://github.com/neurodesk/neurocontainers/blob/main/recipes/qsmxt/OpenReconTechnicalReference.md).
