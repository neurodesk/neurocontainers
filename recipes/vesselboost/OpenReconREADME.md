# VesselBoost OpenRecon

VesselBoost runs a deep learning pipeline for vessel segmentation of high-resolution time-of-flight magnetic resonance angiography (TOF-MRA). It uses a UNet3D-based segmentation workflow designed to be sensitive to small vessels. The OpenRecon configuration exposes the prediction workflow and returns a derived series named `<source>_vesselboost`.

The normal VesselBoost software suite includes three command-line modules: prediction, test-time adaptation, and boost. In this OpenRecon application, the scanner GUI only makes the VesselBoost prediction pipeline available.

## Input and Output

Use this reconstruction pipeline on 3D TOF-MRA image data.

The main derived output is named `<source>_vesselboost`, where `<source>` is the incoming source `SeriesDescription` or, when that is absent, the incoming `SequenceDescription`. If neither source name is available, the fallback name is `vesselboost`. Returned scanner series use `OR` as the short OpenRecon suffix. By default, OpenRecon returns restamped original MRA images first as `<source>_original`, then sends one source-image-header 2D VesselBoost segmentation image per source image. Optional sagittal and coronal reformat series are named `<source>_vesselboost_sagittal` and `<source>_vesselboost_coronal`.

## GUI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `vesselboost` | Selects the MRD server configuration. The available GUI option is `vesselboost`. |
| Keep original images | `sendoriginal` | boolean | `true` | Return restamped original MRA images first, before the `vesselboost` output. Disable this to return only derived VesselBoost output series. |
| Debug threshold segmentation | `vbdebugthresholdsegment` | boolean | `false` | Skip VesselBoost inference and use the simple threshold segmentation from `openreconi2iexample` to exercise the send path quickly. |
| Gaussian blending | `vbuseblending` | boolean | `false` | Experimental option that enables Gaussian blending across inference patches. This can smooth patch boundaries, but substantially increases runtime. |
| Blend overlap % | `vboverlap` | integer | `50` | Patch overlap percentage used only when Gaussian blending is enabled. Valid GUI range: 0 to 99. |
| N4 bias field correction | `vbbiasfieldcorrection` | boolean | `true` | Enable N4 bias field correction before VesselBoost inference. |
| Denoising | `vbdenoising` | boolean | `false` | Enable non-local means denoising before VesselBoost inference. |
| Brain masking | `vbbrainextraction` | boolean | `true` | Enable SynthStrip brain extraction during preprocessing. This is used only when N4 bias field correction or denoising is enabled. |
| Reslice sagittal | `vbreslicesagittal` | boolean | `false` | Emit an additional sagittal reformat series of the segmentation. |
| Reslice coronal | `vbreslicecoronal` | boolean | `false` | Emit an additional coronal reformat series of the segmentation. |
| Segmentation MIPs | `vbsegmentationmips` | boolean | `false` | Use the `openreconi2iexample` 2D segmentation-header delivery mode so scanner post-processing targets the segmentation stream. |

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

`vbdebugthresholdsegment` is a diagnostic flag. When enabled, the wrapper still receives and sorts the source TOF images, but skips the VesselBoost model command and creates a simple threshold plus largest-component mask using the same logic as `openreconi2iexample`.

The OpenRecon label declares GPU support and requests at least 1 GPU, 10048 MB GPU memory, 40096 MB system memory, and 32 CPU cores.

Returned images are always emitted as new scanner-visible series. Restamped originals are sent first with `Keep_image_geometry = 1` and a patched source `IceMiniHead`, so scanner-side processing and scanner-created MIPs stay attached to the original MRA geometry. By default, the VesselBoost segmentation follows as a separate source-image-header 2D stream with `Keep_image_geometry = 1`, `DataRole = Image`, `SegmentSourceGeometry = 1`, `SegmentSourceImageHeader = 1`, `SegmentOutputGeometry = 2d`, the scanner post-processing child role, and the source `ImageType`, `DicomImageType`, and `ImageTypeValue4` identity. In normal mode, the segment stream strips `ImageTypeValue3` from MRD metadata and `IceMiniHead` so Siemens MIP functors that select `ImageTypeValue3 = M` stay attached to the original stream.

When `vbsegmentationmips` is enabled, VesselBoost instead mirrors the `openreconi2iexample` `2d_segment_header` path: the segmentation is sent as a 2D source-geometry segmentation-header stream with `DataRole = Segmentation`, `SegmentSourceGeometry = 1`, `SegmentOutputGeometry = 2d`, no `SegmentSourceImageHeader`, scanner post-processing child-role metadata matching the segmentation `image_series_index`, and no `ImageTypeValue3`. This lets scanner-created segmentation MIPs appear as their own post-processed segmentation series instead of being folded into original MIP side products. Reformatted sagittal and coronal outputs remain explicit packed 3D MRD volumes without source-image-header stamping.

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
