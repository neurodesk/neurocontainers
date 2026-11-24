# PET, Molecular Imaging & Preclinical Coverage and Desktop Plan

This document tracks Positron Emission Tomography (PET), molecular imaging, and
preclinical imaging support in NeuroContainers, including the category rollout
and multi-domain catalog tagging for the `preclinical` and `molecular-imaging`
domains.

## PET & Preclinical Coverage

### Dedicated PET & molecular imaging recipes

| Recipe | Upstream | License | Role | Categories |
| --- | --- | --- | --- | --- |
| [petprep](../recipes/petprep/build.yaml) | [nipreps/petprep](https://github.com/nipreps/petprep) | `Apache-2.0` | BIDS App for PET preprocessing (motion correction, registration, PVC, TAC extraction) | `molecular imaging`, `quantitative imaging`, `structural imaging` |
| [stir](../recipes/stir/build.yaml) | [UCL/STIR](https://github.com/UCL/STIR) | `Apache-2.0` (+ LGPL-2.1/MIT/MPL-2.0/PARAPET) | CPU PET/SPECT tomographic image reconstruction (`OSMAPOSL`, `FBP2D`, `FBP3DRP`) | `molecular imaging`, `image reconstruction` |
| [pypet2bids](../recipes/pypet2bids/build.yaml) | [openneuropet/PET2BIDS](https://github.com/openneuropet/PET2BIDS) (PyPI `pypet2bids`) | `MIT`, `BSD-2-Clause` | Standalone CLI/Python packaging for PET DICOM/ECAT + blood/injection metadata conversion to PET-BIDS (complements the older version bundled inside `ezbids`) | `molecular imaging`, `data organisation`, `bids apps` |
| [petpvc](../recipes/petpvc/build.yaml) | [UCL/PETPVC](https://github.com/UCL/PETPVC) | `Apache-2.0` | Standalone partial volume correction CLI suite (GTM, RBV, Labbe, MG, IY, RL) | `molecular imaging`, `preclinical imaging`, `quantitative imaging` |
| [tpcclib](../recipes/tpcclib/build.yaml) | [vesoik/tpcclib](https://gitlab.utu.fi/vesoik/tpcclib) | `GPL-3.0-or-later` | Turku PET Centre C CLI suite for dynamic PET, TACs, input functions, and kinetic modeling | `molecular imaging`, `preclinical imaging`, `quantitative imaging` |
| [amide](../recipes/amide/build.yaml) | [ferdymercury/amide](https://github.com/ferdymercury/amide) / Ubuntu `amide` | `GPL-2.0-or-later` | Multimodal clinical and preclinical microPET/microCT/MRI viewer and ROI/SUV analyzer | `molecular imaging`, `preclinical imaging`, `visualization` |
| [moosez](../recipes/moosez/build.yaml) | [ENHANCE-PET/MOOSE](https://github.com/ENHANCE-PET/MOOSE) (PyPI `moosez`) | `Apache-2.0` | nnU-Net multi-organ segmentation covering clinical whole-body PET/CT/MR models plus preclinical CT leg-muscle and preclinical multi-organ MR models | `molecular imaging`, `preclinical imaging`, `image segmentation`, `machine learning` |
| [kinfitr](../recipes/kinfitr/build.yaml) | [mathesong/kinfitr](https://github.com/mathesong/kinfitr) | `MIT` | R package for reproducible PET pharmacokinetic modeling (compartmental, graphical, and reference-tissue models) | `molecular imaging`, `preclinical imaging`, `quantitative imaging`, `statistics` |
| [petfit](../recipes/petfit/build.yaml) | [mathesong/petfit](https://github.com/mathesong/petfit) | `MIT` | BIDS App and R Shiny suite built on `kinfitr` for interactive and automatic PET kinetic modeling (`regiondef`, `modelling_plasma`, `modelling_ref`) with HTML QC reports | `molecular imaging`, `preclinical imaging`, `quantitative imaging`, `bids apps` |

### Multimodal suites with PET support

| Recipe | PET / Molecular Imaging Role |
| --- | --- |
| [clinica](../recipes/clinica/build.yaml) | Clinical neuroimaging PET pipelines (`pet-linear`, `pet-volume`, `pet-surface`) |
| [freesurfer](../recipes/freesurfer/build.yaml) | Bundles PETSurfer (`gtmseg`, `mri_gtmpvc`, `mri_coreg`) |
| [spm12](../recipes/spm12/build.yaml), [spm12bi](../recipes/spm12bi/build.yaml), [spm25](../recipes/spm25/build.yaml) | Native PET/SPECT spatial normalization and voxel-wise statistics |
| [dcm2niix](../recipes/dcm2niix/build.yaml), [dicomtools](../recipes/dicomtools/build.yaml), [bidstools](../recipes/bidstools/build.yaml), [ezbids](../recipes/ezbids/build.yaml), [bidscoin](../recipes/bidscoin/build.yaml) | DICOM/ECAT/Interfile conversion (`dcm2niix`, `dcmtk`, `xmedcon`/`medcon`), Bruker conversion (`Bru2Nii` in `bidstools`), and BIDS curation (`ezbids` bundles `pypet2bids` with `dcm2niix4pet`) |
| [totalsegmentator](../recipes/totalsegmentator/build.yaml) | CT/MR whole-body organ segmentation commonly paired with PET/CT |
| [slicer](../recipes/slicer/build.yaml), [mipav](../recipes/mipav/build.yaml), [itksnap](../recipes/itksnap/build.yaml), [quantiphyse](../recipes/quantiphyse/build.yaml) | Visualization, multimodal overlay, ROI delineation, and quantitative analysis |
| [ants](../recipes/ants/build.yaml), [elastix](../recipes/elastix/build.yaml), [niftyreg](../recipes/niftyreg/build.yaml) | Multimodal PET-to-MR/CT registration |
| [root](../recipes/root/build.yaml) | CERN ROOT analysis framework (nuclear physics / OpenGATE outputs) |

### Preclinical / small-animal recipes

| Recipe | Role | Categories |
| --- | --- | --- |
| [aidamri](../recipes/aidamri/build.yaml) | Rodent MRI structural, diffusion, and functional processing | `rodent imaging`, `preclinical imaging`, `structural imaging`, `diffusion imaging`, `functional imaging` |
| [rabies](../recipes/rabies/build.yaml) | Rodent fMRI preprocessing, confound correction, and connectivity | `rodent imaging`, `preclinical imaging` |
| [samri](../recipes/samri/build.yaml) | Small-animal MRI BIDS preprocessing and workflows (includes `Bru2Nii`) | `rodent imaging`, `preclinical imaging`, `functional imaging`, `workflows` |
| [brkraw](../recipes/brkraw/build.yaml) | Bruker preclinical MRI raw-data conversion to NIfTI/JSON | `preclinical imaging`, `data organisation` |
| [dsistudio](../recipes/dsistudio/build.yaml) | Diffusion MRI tractography with small-animal / preclinical support | `diffusion imaging`, `preclinical imaging` |
| [bidstools](../recipes/bidstools/build.yaml) | BIDS conversion bundle including `Bru2Nii` for Bruker studies alongside `heudiconv`, `dcm2niix`, `dcmtk`, and `xmedcon` | `molecular imaging`, `preclinical imaging`, `data organisation` |

## Category Rollout

Two categories are included in `CATEGORIES` in `builder/validation.py`:

* `preclinical imaging` — broadens coverage beyond `rodent imaging` to include
  Bruker conversion (`brkraw`, `bidstools`), small-animal microPET/microCT
  (`amide`), preclinical CT/MR segmentation (`moosez`), kinetic modeling
  (`kinfitr`, `petfit`, `tpcclib`), phantoms, and non-rodent preclinical models.
  `rodent imaging` is retained for backwards compatibility.
* `molecular imaging` — gives PET, SPECT, nuclear medicine, and tracer kinetic
  modeling containers a dedicated discovery category alongside modality-agnostic
  categories such as `image reconstruction` and `quantitative imaging`.

Registry sync note: [NeuroContainers UI](https://github.com/neurodesk/neurocontainers-ui)
maintains the `CATEGORIES` map in `components/common.ts`, and `lib/zodSchema.ts`
derives its category enum from `Object.keys(CATEGORIES)`. Keep both entries
synced in `components/common.ts` so the web editor accepts recipes with these
categories.

## Multi-Domain Catalog Routing (`preclinical` and `molecular-imaging`)

NeuroContainers routes recipes to domain catalogs through `domains:` in each
`build.yaml`, validated against the
[apps-catalog `domains.json`](https://apps-catalog.scigetorg.workers.dev/domains.json)
registry. Both domains are registered in `scigetorg/apps-catalog` and actively
published:

* **`preclinical`** (registered in `scigetorg/apps-catalog#40`, tagged in
  `neurocontainers#3948` and `#3954`, published at
  `https://apps-catalog.scigetorg.workers.dev/preclinical/apps.json`) — **48 recipes**
  covering core PET/molecular imaging, small-animal/rodent MRI, and shared
  dual-use tools.
* **`molecular-imaging`** (registered in `scigetorg/apps-catalog#41`, tagged in
  `neurocontainers#3955`, published at
  `https://apps-catalog.scigetorg.workers.dev/molecular-imaging/apps.json`) — **43 recipes**
  covering core PET/molecular imaging and shared dual-use tools (excluding the
  5 rodent/small-animal MRI-only recipes).

Container launch metadata (`title`, `description`, `registry`, `defaultVersion`,
`containerName`, `volumeMount`, `defaultStorageMount`, `remoteUrl`,
`appJsonUrl`, `downloadUrls`) is intentionally deferred in
`scigetorg/apps-catalog` `domains.json` until dedicated desktop container images
are published (matching the `datascience` domain policy).

Keep brain-PET and shared neuroimaging tools tagged with `neuroimaging` as well
so Neurodesktop users retain seamless access to them.

### 1. Core PET & molecular imaging recipes (`neuroimaging`, `preclinical`, `molecular-imaging` — 14 recipes)

* `amide`, `clinica`, `freesurfer`, `kinfitr`, `moosez`, `petfit`, `petprep`,
  `petpvc`, `pypet2bids`, `spm12`, `spm12bi`, `spm25`, `stir`, `tpcclib`

### 2. Core preclinical / small-animal MRI recipes (`neuroimaging`, `preclinical` — 5 recipes)

* `aidamri`, `brkraw`, `dsistudio`, `rabies`, `samri`

### 3. Shared dual-use recipes (`neuroimaging`, `preclinical`, `molecular-imaging`, plus `microscopy` where applicable — 29 recipes)

* **DICOM, BIDS & data management:** `bidscoin`, `bidstools`, `datalad`, `dcm2niix`, `dicomtools`, `ezbids`, `globus`, `xnat`
* **Visualization & manual ROI tools:** `fiji`, `itksnap`, `mipav`, `napari`, `quantiphyse`, `slicer`, `voreen`
* **Registration & general image math:** `ants`, `cmtk`, `convert3d`, `elastix`, `niftyreg`, `niimath`
* **Organ & vessel segmentation:** `totalsegmentator`, `vesselvio`, `vmtk`
* **Physics, statistics & scripting:** `irkernel`, `jamovi`, `libreoffice`, `root`, `rstudio`
