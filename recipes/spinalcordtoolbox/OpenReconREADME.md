# Spinal Cord Toolbox OpenRecon

This OpenRecon app runs Spinal Cord Toolbox (SCT) image-processing steps. It is an image-in/image-out app: it expects already reconstructed magnitude images and returns the derived segmentation or labeling series.

This OpenRecon tool is based on the spinalcordtoolbox Toolbox (https://github.com/spinalcordtoolbox/spinalcordtoolbox)

SCT tools process MRI data (NIfTI files) and can do fully automatic tasks such as:
  - Segmentation of the spinal cord and gray matter
  - Segmentation of pathologies (eg. multiple sclerosis lesions)

Reference: Jan Valošek, Julien Cohen-Adad, Reproducible Spinal Cord Quantitative MRI Analysis with the Spinal Cord Toolbox, Magnetic Resonance in Medical Sciences, 2024, Volume 23, Issue 3, Pages 307-315, Released on J-STAGE July 01, 2024, Advance online publication March 12, 2024, Online ISSN 1880-2206, Print ISSN 1347-3182, https://doi.org/10.2463/mrms.rev.2023-0159, https://www.jstage.jst.go.jp/article/mrms/23/3/23_rev.2023-0159/_article/-char/en,

## Parameters

| Parameter id | Type | Default | Description |
| --- | --- | --- | --- |
| `config` | choice | `spinalcordtoolbox` | Selects this OpenRecon app. |
| `sendoriginal` | boolean | `true` | If enabled, returns the original source images before the derived SCT output series. |
| `analysis` | choice | `sct_deepseg_spinalcord` | Selects the SCT command/task to run. |

## Available Analyses

| Analysis id | SCT command | Required input data |
| --- | --- | --- |
| `sct_deepseg_spinalcord` | `sct_deepseg spinalcord` | 3D spinal cord MRI. This model is intended to be contrast-agnostic and was tested here with T1w, T2w, MT, and DWI-derived mean images. |
| `sct_deepseg_sc_epi` | `sct_deepseg sc_epi` | EPI-BOLD fMRI image volume containing the spinal cord. |
| `sct_deepseg_sc_lumbar_t2` | `sct_deepseg sc_lumbar_t2` | Lumbar spinal cord T2-weighted MRI. |
| `sct_deepseg_sc_mouse_t1` | `sct_deepseg sc_mouse_t1` | Mouse spinal cord T1-weighted MRI. |
| `sct_deepseg_graymatter` | `sct_deepseg_gm` | 3D T2*-like spinal cord image for gray matter segmentation. This path matches SCT `batch_processing.sh`. |
| `sct_deepseg_gm_sc_7t_t2star` | `sct_deepseg gm_sc_7t_t2star` | 7T T2*-weighted spinal cord image for cord/gray matter segmentation. |
| `sct_deepseg_gm_wm_exvivo_t2` | `sct_deepseg gm_wm_exvivo_t2` | Ex vivo human T2-weighted spinal cord image for gray/white matter segmentation. |
| `sct_deepseg_gm_mouse_t1` | `sct_deepseg gm_mouse_t1` | Mouse spinal cord MRI for gray matter segmentation. |
| `sct_deepseg_lesion_ms` | `sct_deepseg lesion_ms` | Spinal cord MRI with multiple-sclerosis lesions. |
| `sct_deepseg_lesion_ms_axial_t2` | `sct_deepseg lesion_ms_axial_t2` | Axial T2-weighted spinal cord MRI with intramedullary MS lesions. |
| `sct_deepseg_lesion_ms_mp2rage` | `sct_deepseg lesion_ms_mp2rage` | Cropped MP2RAGE spinal cord data for MS lesion segmentation. |
| `sct_deepseg_lesion_sci_t2` | `sct_deepseg lesion_sci_t2` | T2-weighted spinal cord MRI with intramedullary spinal cord injury lesion. Returns separate lesion and spinal cord segmentation series. |
| `sct_deepseg_tumor_t2` | `sct_deepseg tumor_t2` | T2-weighted spinal cord MRI with tumor. |
| `sct_deepseg_rootlets` | `sct_deepseg rootlets` | T2w or MP2RAGE-derived images for spinal nerve rootlet segmentation. |
| `sct_deepseg_sc_canal_t2` | `sct_deepseg sc_canal_t2` | T2-weighted image for spinal canal segmentation. |
| `sct_deepseg_totalspineseg` | `sct_deepseg totalspineseg` | Spine MRI suitable for intervertebral disc labeling and vertebrae segmentation. |
| `sct_label_vertebrae` | `sct_deepseg spinalcord`, then `sct_label_vertebrae -c t2` | T2-weighted anatomical image. The wrapper first creates a spinal cord segmentation, then labels vertebral levels using SCT's T2 contrast mode. |

## Combined Analyses

Combined modes run several SCT analyses on the same input NIfTI and return one derived MRD series per analysis. They are intended for data where the contrast and anatomy are suitable for all analyses in the bundle.

| Analysis id | Runs | Required input data |
| --- | --- | --- |
| `sct_bundle_t2_anatomy` | `sct_deepseg_spinalcord`, `sct_label_vertebrae`, `sct_deepseg_sc_canal_t2`, `sct_deepseg_totalspineseg` | T2-weighted anatomical spine image. |
| `sct_bundle_t2_ms` | `sct_deepseg_spinalcord`, `sct_deepseg_lesion_ms`, `sct_deepseg_lesion_ms_axial_t2` | T2-weighted spinal cord image with suspected MS lesions. The axial T2 lesion model is most appropriate for axial T2 chunks. |
| `sct_bundle_t2s_gm` | `sct_deepseg_spinalcord`, `sct_deepseg_graymatter` | T2*-like spinal cord image suitable for gray matter segmentation. |
| `sct_bundle_mouse_t1` | `sct_deepseg_sc_mouse_t1`, `sct_deepseg_gm_mouse_t1` | Mouse T1-weighted spinal cord image. |

## Outputs

The app returns one derived MRD image series for a single selected analysis, or one derived series per SCT output for multiclass analyses and combined modes. Multiclass outputs are split into separate MRD series for cord/gray matter/white matter/lesion/spine labels as generated by SCT. The series description is based on the input series description plus the output id, for example `SOURCE_sct_deepseg_spinalcord`.

## Notes

- The selected SCT model or bundle determines the required image contrast and anatomy. Sending the wrong contrast can produce poor or empty segmentations even if the OpenRecon run succeeds.
- `sct_label_vertebrae` currently uses the wrapper's built-in T2 mode (`-c t2`). Use T2-weighted source images for that analysis.
- SCT's `tumor_edema_cavity_t1_t2` task is not exposed in OpenRecon because it requires separate T1/T2 input images and contrast labels, while this wrapper currently accepts one MRD image series.
- SCT's `gm_wm_mouse_t1` task is not exposed in OpenRecon because the current test sweep did not identify a valid official mouse T1 dataset for this task.
