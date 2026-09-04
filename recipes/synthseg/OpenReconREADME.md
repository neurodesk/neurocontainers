# SynthSeg OpenRecon

SynthSeg segments brain MRI scans of any contrast and resolution without
retraining or preprocessing. It is trained purely on synthetic data generated
with domain randomisation, which is what makes a single network usable on
whatever contrast the scanner happens to produce. The OpenRecon configuration
runs `mri_synthseg` inline on the reconstructed image stream and returns a
derived label-map series named `<source>_synthseg`.

The container packages `mri_synthseg` and the trained weights published with
FreeSurfer 8.2.0. The scanner GUI exposes the three MRI networks (SynthSeg 2.0,
SynthSeg-robust 2.0, SynthSeg 1.0), the cortical parcellation network and the
automated QC network. Photo-SynthSeg is installed in the container but is not
offered in the scanner GUI, because it segments 3D-reconstructed dissection
photographs rather than MR images.

## Input and Output

Use this reconstruction pipeline on 3D brain image data. Any contrast works;
no bias correction, skull stripping or intensity normalisation is required.
For an MP2RAGE scan, only the denoised uniform (`UNI-DEN`) contrast is
processed; INV1, INV2, UNI, and other contrasts in the same stream are ignored.
If an MP2RAGE stream has no `UNI-DEN` contrast, the first magnitude image series
is processed instead. Other sequences, including MPRAGE and GRE, are not
filtered by contrast and each magnitude image series is processed normally.

SynthSeg always segments at 1 mm isotropic internally. The wrapper always runs
`mri_synthseg --keepgeom`, so the returned label map is resampled with nearest
neighbour interpolation back onto the incoming MRD slice grid and there is
exactly one output image per source image.

The main derived output is named `<source>_synthseg`, where `<source>` is the
incoming source `SeriesDescription` or, when that is absent, the incoming
`SequenceDescription`. If neither source name is available, the fallback name is
`synthseg`. Most returned scanner series use `OR` as the short OpenRecon suffix.
When `sssegmentheader` is enabled, the source-geometry segmentation stream uses
the `openrecon` suffix to mirror `openreconi2iexample`.

By default, OpenRecon returns restamped original images first as
`<source>_original`, then sends one source-image-header 2D SynthSeg label image
per source image.

**Pixel values are FreeSurfer label indices, not intensities.** They are passed
through unscaled so they can be looked up in
`$FREESURFER_HOME/FreeSurferColorLUT.txt` (for example 2 = left cerebral white
matter, 17 = left hippocampus, and the 1000/2000 series for cortical parcels).
Only the returned display window is derived from the label range, so the
segmentation is visible without manual windowing.

## GUI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `synthseg` | Selects the MRD server configuration. The available GUI option is `synthseg`. |
| Keep original images | `sendoriginal` | boolean | `true` | Return restamped original images first, before the `synthseg` label map. Disable this to return only the derived SynthSeg series. |
| SynthSeg model | `ssmodel` | choice | `synthseg` | Trained network used for segmentation: `synthseg` (SynthSeg 2.0), `robust` (SynthSeg-robust 2.0) or `v1` (SynthSeg 1.0). |
| Cortical parcellation | `ssparc` | boolean | `false` | Also run the cortical parcellation network, adding the Desikan-Killiany cortical parcels to the label map. |
| Fast mode | `ssfast` | boolean | `true` | Bypass topological refinement and left/right flipping for a faster prediction. |
| Use GPU | `ssusegpu` | boolean | `false` | Run inference on the reconstruction GPU. CPU inference is the default to avoid GPU memory exhaustion. |
| Crop mode or size | `sscrop` | integer | `0` | Use `-1` to disable cropping, `0` to crop automatically to the non-zero bounding box, or a positive voxel size to crop every RAS axis. Positive values are rounded up to a multiple of 32. |
| CPU threads | `ssthreads` | integer | `8` | TensorFlow inter/intra-op thread count. Valid GUI range: 1 to 64. |
| Report region volumes | `ssvolumes` | boolean | `false` | Compute per-structure volumes in mm3 and write them to the reconstruction log as CSV. |
| Report QC scores | `ssqc` | boolean | `false` | Run the automated QC network and write per-structure QC scores to the reconstruction log as CSV. |
| Segmentation header | `sssegmentheader` | boolean | `false` | Use the `openreconi2iexample` 2D segmentation-header delivery mode so scanner post-processing targets the segmentation stream. |
| Sagittal reformat | `ssreslicesagittal` | boolean | `false` | Send an additional sagittal 3D reformat of the label map. |
| Coronal reformat | `ssreslicecoronal` | boolean | `false` | Send an additional coronal 3D reformat of the label map. |
| Debug threshold segmentation | `ssdebugthresholdsegment` | boolean | `false` | Skip SynthSeg inference and use the simple threshold segmentation from `openreconi2iexample` to exercise the send path quickly. |

## Model Combinations

| `ssmodel` | `mri_synthseg` flags | Notes |
| --- | --- | --- |
| `synthseg` | (none) | SynthSeg 2.0, the recommended default. |
| `robust` | `--robust` | Slower but more reliable on low-quality clinical scans. `mri_synthseg` forces fast mode for this model, so `ssfast` is reported as enabled regardless of the GUI value. |
| `v1` | `--v1` | The original 2021 SynthSeg model with the 1.0 label set, kept for reproducing older results. |

`ssparc` adds `--parc` and loads `synthseg_parc_2.0.h5`. `ssqc` adds `--qc` and
loads `synthseg_qc_2.0.h5`. Every model file required by the requested
combination is checked before the subprocess starts, so a missing weight file
fails immediately rather than part-way through inference.

## Runtime Notes

Runtime is dominated by the segmentation network. Fast mode on a GPU is the
quickest configuration; disabling fast mode, enabling parcellation or QC, or
forcing CPU inference each add a substantial amount of time. `robust` is the
slowest model.

OpenRecon uses CPU inference by default because the parcellation network can
exhaust scanner GPU memory on a full 1 mm volume. Enable `ssusegpu` only when
the reconstruction GPU has enough free memory for the selected model and crop.

Automatic cropping is enabled by the default `sscrop=0` to reduce the 3D
network's peak GPU memory use. It finds the non-zero bounding box after SynthSeg
normalizes the input. Set `sscrop=-1` to disable cropping, or set it to a
positive isotropic size for a predictable memory bound. The wrapper rounds a
positive size up to the next multiple of 32. FreeSurfer restores cropped
predictions to the original image shape, but anatomy outside a manual crop is
returned as background. Start with a crop large enough to contain the full
brain across the expected scanner positioning.

`ssdebugthresholdsegment` is a diagnostic flag. When enabled, the wrapper still
receives and sorts the source images, but skips the `mri_synthseg` command and
creates a simple threshold plus largest-component mask using the same logic as
`openreconi2iexample`. In that mode the returned pixel values are 12-bit
intensities rather than FreeSurfer label indices.

The OpenRecon label declares GPU support and requests at least 1 GPU, 10048 MB
GPU memory, 40096 MB system memory, and 8 CPU cores.

Returned images are always emitted as new scanner-visible series. Restamped
originals are sent first with `Keep_image_geometry = 1` and a patched source
`IceMiniHead`, so scanner-side processing stays attached to the original
geometry. By default, the SynthSeg label map follows as a separate
source-image-header 2D stream with `Keep_image_geometry = 1`,
`DataRole = Image`, `SegmentSourceGeometry = 1`, `SegmentSourceImageHeader = 1`,
`SegmentOutputGeometry = 2d`, the scanner post-processing child role, and the
source `ImageType`, `DicomImageType`, and `ImageTypeValue4` identity. In normal
mode, the segment stream strips `ImageTypeValue3` from MRD metadata and
`IceMiniHead` so Siemens functors that select `ImageTypeValue3 = M` stay
attached to the original stream.

When `sssegmentheader` is enabled, SynthSeg instead mirrors the
`openreconi2iexample` `2d_segment_header` path: the label map is sent as a 2D
source-geometry segmentation-header stream with `DataRole = Segmentation`,
`SegmentSourceGeometry = 1`, `SegmentOutputGeometry = 2d`,
`SequenceDescriptionAdditional = openrecon`, no `SegmentSourceImageHeader`,
scanner post-processing child-role metadata matching the segmentation
`image_series_index`, and no `ImageTypeValue3`.

Scanner logs include the resolved options on a single
`OpenRecon SynthSeg options: ...` line, a `SynthSeg label map: ...` line with the
distinct label values that were returned, a
`SYNTHSEG_OPENRECON_POSTPROCESSING target=...` marker, and one
`SYNTHSEG_OPENRECON_BATCH ...` marker before every MRD image send. Region volume
and QC CSVs, when requested, are written to the reconstruction log in full.

## Citation

Please cite SynthSeg if you use this reconstruction in research:

```bibtex
@article{billot2023synthseg,
  title = {{SynthSeg}: Segmentation of brain {MRI} scans of any contrast and resolution without retraining},
  author = {Billot, Benjamin and Greve, Douglas N. and Puonti, Oula and Thielscher, Axel and Van Leemput, Koen and Fischl, Bruce and Dalca, Adrian V. and Iglesias, Juan Eugenio},
  journal = {Medical Image Analysis},
  volume = {86},
  pages = {102789},
  year = {2023},
  doi = {10.1016/j.media.2023.102789}
}

@article{billot2023robust,
  title = {Robust machine learning segmentation for large-scale analysis of heterogeneous clinical brain {MRI} datasets},
  author = {Billot, Benjamin and Magdamo, Colin and Cheng, You and Arnold, Steven E. and Das, Sudeshna and Iglesias, Juan Eugenio},
  journal = {Proceedings of the National Academy of Sciences},
  volume = {120},
  number = {9},
  pages = {e2216399120},
  year = {2023},
  doi = {10.1073/pnas.2216399120}
}
```

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/synthseg

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
