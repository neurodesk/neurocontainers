# VesselBoost OpenRecon

VesselBoost runs a deep learning pipeline for vessel segmentation of high-resolution time-of-flight magnetic resonance angiography (TOF-MRA). It uses a UNet3D-based segmentation workflow designed to be sensitive to small vessels. The OpenRecon configuration exposes the prediction workflow and returns a derived series named `<source>_vesselboost`.

The normal VesselBoost software suite includes three command-line modules: prediction, test-time adaptation, and boost. In this OpenRecon application, the scanner GUI only makes the VesselBoost prediction pipeline available.

## Input and Output

Use this reconstruction pipeline on 3D TOF-MRA image data.

The main derived output is named `<source>_vesselboost`, where `<source>` is the incoming source `SeriesDescription`. If the source series has no description, the fallback name is `vesselboost`. By default, OpenRecon also returns the original MRA images before the derived output. Optional sagittal and coronal reformat series are named `<source>_vesselboost_sagittal` and `<source>_vesselboost_coronal`.

## GUI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `vesselboost` | Selects the MRD server configuration. The available GUI option is `vesselboost`. |
| Keep original images | `sendoriginal` | boolean | `true` | Return the original MRA images together with the `vesselboost_segmentation` output. Disable this to return only derived VesselBoost output series. |
| Gaussian blending | `vbuseblending` | boolean | `false` | Experimental option that enables Gaussian blending across inference patches. This can smooth patch boundaries, but substantially increases runtime. |
| Blend overlap % | `vboverlap` | integer | `50` | Patch overlap percentage used only when Gaussian blending is enabled. Valid GUI range: 0 to 99. |
| N4 bias field correction | `vbbiasfieldcorrection` | boolean | `true` | Enable N4 bias field correction before VesselBoost inference. |
| Denoising | `vbdenoising` | boolean | `false` | Enable non-local means denoising before VesselBoost inference. |
| Brain masking | `vbbrainextraction` | boolean | `true` | Enable SynthStrip brain extraction during preprocessing. This is used only when N4 bias field correction or denoising is enabled. |
| Reslice sagittal | `vbreslicesagittal` | boolean | `false` | Emit an additional sagittal reformat series of the segmentation. |
| Reslice coronal | `vbreslicecoronal` | boolean | `false` | Emit an additional coronal reformat series of the segmentation. |

## Preprocessing Combinations

N4 bias field correction and denoising determine the VesselBoost preprocessing mode:

| N4 bias field correction | Denoising | Effective preprocessing |
| --- | --- | --- |
| `true` | `false` | N4 bias field correction |
| `false` | `true` | Non-local means denoising |
| `true` | `true` | N4 bias field correction and denoising |
| `false` | `false` | No preprocessing |

Brain masking is applied during preprocessing. If both N4 bias field correction and denoising are disabled, the brain masking setting is ignored.

## Runtime Notes

Gaussian blending is marked experimental in the OpenRecon label. Use it only when smoother patch boundaries are worth the longer reconstruction time.

The OpenRecon label declares GPU support and requests at least 1 GPU, 10048 MB GPU memory, 40096 MB system memory, and 32 CPU cores.

## Citation

Please cite VesselBoost if you use this reconstruction in research:

```bibtex
@article{xuVesselBoostPythonToolbox2024a,
  title = {{{VesselBoost}}: {{A Python Toolbox}} for {{Small Blood Vessel Segmentation}} in {{Human Magnetic Resonance Angiography Data}}},
  shorttitle = {{{VesselBoost}}},
  author = {Xu, Marshall and Ribeiro, Fernanda L. and Barth, Markus and Bernier, Micha{\"e}l and Bollmann, Steffen and Chatterjee, Soumick and Cognolato, Francesco and Gulban, Omer F. and Itkyal, Vaibhavi and Liu, Siyu and Mattern, Hendrik and Polimeni, Jonathan R. and Shaw, Thomas B. and Speck, Oliver and Bollmann, Saskia},
  year = {2024},
  month = sep,
  journal = {Aperture Neuro},
  volume = {4},
  publisher = {Organization for Human Brain Mapping},
  issn = {2957-3963},
  doi = {10.52294/001c.123217},
  urldate = {2024-09-17},
  copyright = {http://creativecommons.org/licenses/by/4.0},
  langid = {english}
}
```
