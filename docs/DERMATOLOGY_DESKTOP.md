# Dermatology Imaging Coverage and Desktop Plan

This document tracks clinical dermatology, dermoscopy, dermatopathology, 3D total-body
photography (3D-TBP), and non-invasive skin imaging support in NeuroContainers,
including the category rollout, container additions, licensing guardrails, and the
multi-domain tagging list for a `dermatology` sciget desktop domain.

## Domain Scope

The `dermatology` distribution is imaging- and computational-dermatology-centric:

* **In scope:** dermoscopy and clinical macro photography, dermatopathology whole-slide
  imaging (WSI), 3D total-body photography (3D-TBP) and wound/scar surface morphometry,
  non-invasive skin imaging exports (OCT/OCTA, LC-OCT, reflectance confocal microscopy,
  multiphoton/FLIM, high-frequency ultrasound, hyperspectral/multispectral), clinical
  colorimetry, and DICOM/PHI curation.
* **Out of scope for v1:** skin microbiome sequencing (`QIIME 2`, `MetaPhlAn`, `DADA2`)
  and sequencing-centric transcriptomics (`Scanpy`, `Squidpy`, `SpatialData`, `Seurat`)
  belong in a separate `bioinformatics`/`omics` domain. Bridge only at the imaging
  interface (multiplexed skin tissue imaging and H&E/IHC segmentation).
* **Vendor exports as boundary:** proprietary acquisition suites (VECTRA WB360,
  FotoFinder, DermEngine, VivaScope, DAMAE deepLive, VivoSight, HALO) are not
  containerized; containers target their open exports (`.obj`/`.ply`, DICOM Supplements
  145 and 221, `.svs`/`.ndpi`/`.tiff`, OME-Zarr).

## Existing Coverage

### Dual-use recipes already in NeuroContainers

| Workflow Role | Recipes | Dermatology Role |
| --- | --- | --- |
| Dermatopathology & WSI analysis | [qupath](../recipes/qupath/build.yaml), [fiji](../recipes/fiji/build.yaml), [cellprofiler](../recipes/cellprofiler/build.yaml), [ilastik](../recipes/ilastik/build.yaml) | WSI annotation and quantification (`qupath` 0.7.0 bundles `qupath-extension-cellpose` with Cellpose 3 and `cyto3`), calibrated wound planimetry (`fiji`), interactive pixel/lesion segmentation (`ilastik`), batch phenotyping (`cellprofiler`). |
| Multiplex skin tissue imaging | [palmettobug](../recipes/palmettobug/build.yaml) *(conditional)* | Imaging mass cytometry of skin immune infiltrates. Resolve or isolate its documented `.fonts` side effect before promoting to desktop defaults. |
| Volumetric OCT / RCM / HFUS & vascular analysis | [napari](../recipes/napari/build.yaml), [slicer](../recipes/slicer/build.yaml), [itksnap](../recipes/itksnap/build.yaml), [vessynth](../recipes/vessynth/build.yaml), [vesselvio](../recipes/vesselvio/build.yaml), [skan](../recipes/skan/build.yaml), [terastitcher](../recipes/terastitcher/build.yaml) | 3D stack review and interactive `nnInteractive` segmentation (`napari`), volumetric ROI delineation (`slicer`, `itksnap`), OCT vessel segmentation (`vessynth`), dermal microvasculature/network morphology (`vesselvio`, `skan`), and confocal mosaic stitching (`terastitcher`). |
| 3D-TBP surfaces & clinical photography | [blender](../recipes/blender/build.yaml), [slicersalt](../recipes/slicersalt/build.yaml), [gimp](../recipes/gimp/build.yaml) | Textured `.obj`/`.ply` inspection (`blender`), 3D surface shape analysis (`slicersalt`), and 2D photo review/cropping (`gimp`). |
| Registration & evaluation | [ants](../recipes/ants/build.yaml), [elastix](../recipes/elastix/build.yaml), [niftyreg](../recipes/niftyreg/build.yaml), [convert3d](../recipes/convert3d/build.yaml), [panoptica](../recipes/panoptica/build.yaml) | Longitudinal lesion/histology registration and instance-segmentation benchmarking. |
| De-identification, DICOM & curation | [mede](../recipes/mede/build.yaml), [dicomtools](../recipes/dicomtools/build.yaml), [openrefine](../recipes/openrefine/build.yaml), [xnat](../recipes/xnat/build.yaml), [datalad](../recipes/datalad/build.yaml), [globus](../recipes/globus/build.yaml) | WSI/DICOM PHI scrubbing (`mede`; note `other-closed` Apache-2.0 + Commons Clause), DCMTK utilities (`dicomtools`; follow-up needed to expose `dcmdump`, `dcm2json`, `dcmodify`, and `img2dcm` in `deploy.bins`), cohort metadata cleanup (`openrefine`), and dataset management. |
| Statistics & notebooks | [rstudio](../recipes/rstudio/build.yaml), [jamovi](../recipes/jamovi/build.yaml), [irkernel](../recipes/irkernel/build.yaml), [libreoffice](../recipes/libreoffice/build.yaml) | Shared statistics and reporting baseline matching `microscopy` and `preclinical`. |

## First-Wave Dermatology Container Additions

| Recipe | Upstream | License | Primary Role |
| --- | --- | --- | --- |
| `isic-cli` | [ImageMarkup/isic-cli](https://github.com/ImageMarkup/isic-cli) (PyPI `isic-cli`) | `Apache-2.0` | Official ISIC Archive CLI for searching, downloading, uploading, and curating dermoscopy, clinical macro, and 3D-TBP cohorts and metadata |
| `wsidicomizer` | [imi-bigpicture/wsidicomizer](https://github.com/imi-bigpicture/wsidicomizer) (PyPI `wsidicomizer`) | `Apache-2.0` + `LGPL-2.1-only` (OpenSlide) | Convert proprietary dermatopathology WSI formats (`.svs`, `.ndpi`, `.mrxs`, `.czi`, `.tiff`) to DICOM VL Whole Slide Microscopy (Supplement 145) |
| `colour-science` | [colour-science/colour](https://github.com/colour-science/colour) + [colour-checker-detection](https://github.com/colour-science/colour-checker-detection) | `BSD-3-Clause` | Classical ColorChecker chart detection, CIELAB and Individual Typology Angle (ITA) skin-tone quantification, and sRGB-derived erythema/melanin proxy indices without the optional AGPL ML backend |
| `dicom-validator` | [pydicom/dicom-validator](https://github.com/pydicom/dicom-validator) + [ImagingDataCommons/highdicom](https://github.com/ImagingDataCommons/highdicom) | `MIT` | Validate DICOM Dermoscopic Photography (Supplement 221) and WSI (Supplement 145) IOD conformance against PS3.3 definitions, paired with `highdicom` for DICOM segmentation, SR, and annotation workflows |
| `labelme` | [wkentaro/labelme](https://github.com/wkentaro/labelme) (PyPI `labelme`) | `GPL-3.0-only` | Offline 2D polygon and contour annotation GUI for dermoscopic lesions and chronic wounds |
| `meshlab` | [cnr-isti-vclab/meshlab](https://github.com/cnr-isti-vclab/meshlab) + `pymeshlab` | `GPL-3.0-or-later` | 3D-TBP textured mesh cleaning, measurement, remeshing, and Hausdorff surface-distance analysis |
| `cloudcompare` | [CloudCompare/CloudCompare](https://github.com/CloudCompare/CloudCompare) | `GPL-2.0-or-later` | Point-cloud and mesh ICP registration and longitudinal change detection for 3D-TBP and 3D wounds |
| `bioformats2raw` | [glencoesoftware/bioformats2raw](https://github.com/glencoesoftware/bioformats2raw) | `GPL-2.0-or-later` | Convert proprietary WSI, RCM, and multiphoton acquisitions to cloud-native OME-Zarr |
| `instanseg` | [instanseg/instanseg](https://github.com/instanseg/instanseg) (PyPI `instanseg-torch`) | `Apache-2.0` | Permissively licensed brightfield H&E and fluorescence/RCM nuclei and cell instance segmentation |
| `tiatoolbox` | [TissueImageAnalytics/tiatoolbox](https://github.com/TissueImageAnalytics/tiatoolbox) (PyPI `tiatoolbox`) | `BSD-3-Clause` *(code only)* | Batch WSI tissue masking, Macenko/Vahadane stain normalization, and tiling (code-only image; see Weight Policy) |

## Second-Wave Candidates

* **Dermatopathology & DL:** `histoqc` (`BSD-3-Clause-Clear`, pinned GitHub commit — not published on PyPI), `valis` (`MIT`, serial H&E/IHC registration), `nnunetv2` (`Apache-2.0`, standalone 2D/3D training CLI), `slideflow` (`Apache-2.0` core only), `lazyslide` (`MIT`, code only), and `stardist` (`BSD-3-Clause`). Standalone `cellpose` (`4.2.1.1`, `BSD-3-Clause`) is already packaged on `main` and tagged with `dermatology`.
* **Photography & 3D:** `darktable` (`GPL-3.0-or-later`, RAW DSLR clinical photography), `colmap` (`BSD-3-Clause`), `open3d` (`MIT`).
* **Non-invasive optical imaging:** `hyperspy` (`GPL-3.0-or-later`), `spectral` (`MIT`), `phasorpy` (`MIT`, multiphoton/FLIM), and `octproz` (`GPL-3.0-or-later`, raw OCT interferogram reconstruction if lab fixtures are available).

## Model Weight, Licensing, and Runtime Cache Policy

1. **Never bake non-commercial or gated model checkpoints into container images.**
   This includes `PanDerm` (`CC-BY-NC-ND-4.0`), `DermFM-Zero` (`CC-BY-NC-4.0`),
   `Google DermFoundation` (gated HAI-DEF), `MONET` (`CC-BY-NC-SA-4.0`), `UNI`/`UNI2-h`
   and `TRIDENT` (`CC-BY-NC-ND-4.0`), `GrandQC` (`CC-BY-NC-SA-4.0`), `Prov-GigaPath`,
   `Virchow`, `H-optimus-0`, `TIAToolbox` NC checkpoints, and `slideflow-noncommercial`.
   Ship permissively licensed code only and let users supply authorized checkpoints at
   runtime.
2. **Avoid ophthalmic-only OCT tools and legacy non-commercial scripts.** Do not
   package original `DullRazor` (non-commercial) or retinal-only `OCT-Converter`/`OCTolyzer`.
3. **No `/home` runtime state.** Redirect `XDG_CACHE_HOME`, `XDG_CONFIG_HOME`,
   `TORCH_HOME`, `HF_HOME`, `TIATOOLBOX_HOME`, `CELLPOSE_LOCAL_MODELS_PATH`, and
   standard/DICOM caches to `/opt` (read-only baked assets) or writable `/tmp`/bind paths,
   and verify CLI startup with `HOME=/sys` in `fulltest.yaml`.

## Category Rollout

Two modality categories are added to `CATEGORIES` in `builder/validation.py`:

* `digital pathology` — gives WSI and dermatopathology containers (`qupath`,
  `wsidicomizer`, `tiatoolbox`, `instanseg`, `histoqc`, `bioformats2raw`) a dedicated
  discovery category shared across `dermatology` and `microscopy`.
* `optical imaging` — groups dermoscopy, clinical photography colorimetry, OCT/RCM,
  multiphoton/FLIM, and hyperspectral containers across `dermatology`, `microscopy`,
  and `preclinical`.

Registry sync prerequisite: [NeuroContainers UI](https://github.com/neurodesk/neurocontainers-ui)
maintains the `CATEGORIES` map in `components/common.ts`, and `lib/zodSchema.ts`
derives its category enum from `Object.keys(CATEGORIES)`. Keep syncing both new
entries into `components/common.ts` (and verifying the derived schema accepts
them) as a prerequisite before editing or publishing recipes that use either
category through the web UI.

## Multi-Domain Tagging List for the `dermatology` Desktop

NeuroContainers routes recipes to domain desktops through `domains:` in each
`build.yaml`, validated against the
[apps-catalog `domains.json`](https://apps-catalog.scigetorg.workers.dev/domains.json)
registry. The `dermatology` domain is registered in `apps-catalog`, and existing
dual-use recipes on `main` were tagged in PR #3958. Rollout steps:

1. Merge the groundwork PR (category definitions in `builder/validation.py`,
   Debian version-suffix / multi-arch APT peer agreement in `builder/`, unit
   tests, `AGENTS.md`, and this plan) separately from recipe additions.
   Automated container candidate builds and promotion require recipe-only PRs.
2. Open recipe-only PRs for the first-wave and second-wave containers tagged
   with `dermatology` (and `microscopy` where shared across both desktops).

### 1. First-wave dermatology additions (`domains: [dermatology, ...]`)

* `isic-cli`, `wsidicomizer`, `colour-science`, `dicom-validator`, `labelme`,
  `meshlab`, `cloudcompare`, `bioformats2raw`, `instanseg`, `tiatoolbox`

### 2. Proposed shared dual-use candidates from `microscopy` / `neuroimaging` / `preclinical`

Some of these (including `cellpose`, `cellprofiler`, `fiji`, `qupath`, `napari`,
`slicer`, `ants`, `gimp`, `blender`, `rstudio`) were tagged in PR #3958; the
remaining entries (such as `itksnap`, `vessynth`, `vesselvio`, `terastitcher`)
are proposed follow-up candidates pending curation review:

* **Dermatopathology & WSI:** `cellprofiler`, `fiji`, `ilastik`, `qupath` (plus `palmettobug` after font-isolation check)
* **Non-invasive optical / volumetric & vascular:** `itksnap`, `napari`, `skan`, `slicer`, `terastitcher`, `vesselvio`, `vessynth`
* **3D-TBP & clinical photography:** `blender`, `gimp`, `slicersalt`
* **Registration, math & evaluation:** `ants`, `convert3d`, `elastix`, `niftyreg`, `panoptica`
* **De-identification, DICOM & curation:** `datalad`, `dicomtools`, `globus`, `mede`, `openrefine`, `xnat`
* **Statistics & reporting:** `irkernel`, `jamovi`, `libreoffice`, `rstudio`
