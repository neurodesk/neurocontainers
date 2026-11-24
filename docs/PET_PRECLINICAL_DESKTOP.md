# PET & Preclinical Imaging Coverage and Desktop Plan

This document tracks Positron Emission Tomography (PET), molecular imaging, and
preclinical imaging support in NeuroContainers, including the category rollout
and the multi-domain tagging list for a future `preclinical` or
`molecular-imaging` desktop domain.

## Existing PET & Preclinical Coverage

### Dedicated PET recipes

| Recipe | Role | Existing Categories |
| --- | --- | --- |
| [petprep](../recipes/petprep/build.yaml) | BIDS App for PET preprocessing (motion correction, registration, PVC, TAC extraction) | `quantitative imaging`, `structural imaging` |
| [stir](../recipes/stir/build.yaml) | CPU PET/SPECT tomographic image reconstruction (`OSMAPOSL`, `FBP2D`, `FBP3DRP`) | `image reconstruction` |

### Multimodal suites with PET support

| Recipe | PET / Molecular Imaging Role |
| --- | --- |
| [clinica](../recipes/clinica/build.yaml) | Clinical neuroimaging PET pipelines (`pet-linear`, `pet-volume`, `pet-surface`) |
| [freesurfer](../recipes/freesurfer/build.yaml) | Bundles PETSurfer (`gtmseg`, `mri_gtmpvc`, `mri_coreg`) |
| [spm12](../recipes/spm12/build.yaml), [spm25](../recipes/spm25/build.yaml) | Native PET/SPECT spatial normalization and voxel-wise statistics |
| [dcm2niix](../recipes/dcm2niix/build.yaml), [dicomtools](../recipes/dicomtools/build.yaml), [bidstools](../recipes/bidstools/build.yaml), [ezbids](../recipes/ezbids/build.yaml), [bidscoin](../recipes/bidscoin/build.yaml) | DICOM/ECAT/Interfile conversion (`dcm2niix`, `dcmtk`, `xmedcon`/`medcon`), Bruker conversion (`Bru2Nii` in `bidstools`), and BIDS curation (`ezbids` already bundles `pypet2bids` with `dcm2niix4pet`) |
| [totalsegmentator](../recipes/totalsegmentator/build.yaml) | CT/MR whole-body organ segmentation commonly paired with PET/CT |
| [slicer](../recipes/slicer/build.yaml), [mipav](../recipes/mipav/build.yaml), [itksnap](../recipes/itksnap/build.yaml), [quantiphyse](../recipes/quantiphyse/build.yaml) | Visualization, multimodal overlay, ROI delineation, and quantitative analysis |
| [ants](../recipes/ants/build.yaml), [elastix](../recipes/elastix/build.yaml), [niftyreg](../recipes/niftyreg/build.yaml) | Multimodal PET-to-MR/CT registration |
| [root](../recipes/root/build.yaml) | CERN ROOT analysis framework (nuclear physics / OpenGATE outputs) |

### Existing preclinical / small-animal recipes

| Recipe | Role | Existing Categories |
| --- | --- | --- |
| [aidamri](../recipes/aidamri/build.yaml) | Rodent MRI structural, diffusion, and functional processing | `rodent imaging`, `structural imaging`, `diffusion imaging`, `functional imaging` |
| [rabies](../recipes/rabies/build.yaml) | Rodent fMRI preprocessing, confound correction, and connectivity | `rodent imaging` |
| [samri](../recipes/samri/build.yaml) | Small-animal MRI BIDS preprocessing and workflows (includes `Bru2Nii`) | `rodent imaging`, `functional imaging`, `workflows` |
| [brkraw](../recipes/brkraw/build.yaml) | Bruker preclinical MRI raw-data conversion to NIfTI/JSON | `data organisation` |
| [bidstools](../recipes/bidstools/build.yaml) | BIDS conversion bundle including `Bru2Nii` for Bruker studies alongside `heudiconv`, `dcm2niix`, `dcmtk`, and `xmedcon` | `data organisation` |

## First-Wave PET Container Additions

| Recipe | Upstream | License | Primary Role |
| --- | --- | --- | --- |
| `pypet2bids` | [openneuropet/PET2BIDS](https://github.com/openneuropet/PET2BIDS) (PyPI `pypet2bids`) | `MIT` | Standalone CLI/Python packaging for PET DICOM/ECAT + blood/injection metadata conversion to PET-BIDS (complements the older version bundled inside `ezbids`) |
| `petpvc` | [UCL/PETPVC](https://github.com/UCL/PETPVC) | `Apache-2.0` | Standalone partial volume correction CLI suite (GTM, RBV, Labbe, MG, IY, RL) |
| `tpcclib` | [vesoik/tpcclib](https://gitlab.utu.fi/vesoik/tpcclib) | `GPL-3.0-or-later` | Turku PET Centre C CLI suite for dynamic PET, TACs, input functions, and kinetic modeling |
| `amide` | [ferdymercury/amide](https://github.com/ferdymercury/amide) / Ubuntu `amide` | `GPL-2.0-or-later` | Multimodal clinical and preclinical microPET/microCT/MRI viewer and ROI/SUV analyzer |
| `moosez` | [ENHANCE-PET/MOOSE](https://github.com/ENHANCE-PET/MOOSE) (PyPI `moosez`) | `Apache-2.0` | nnU-Net multi-organ segmentation covering clinical whole-body PET/CT/MR models plus preclinical CT leg-muscle and preclinical multi-organ MR models |
| `kinfitr` | [mathesong/kinfitr](https://github.com/mathesong/kinfitr) + [mathesong/bloodstream](https://github.com/mathesong/bloodstream) | `MIT` | R pharmacokinetic modeling and arterial blood processing for dynamic PET / PET-BIDS |

## Category Rollout

Two categories are added to `CATEGORIES` in `builder/validation.py`:

* `preclinical imaging` — broadens coverage beyond `rodent imaging` to include
  Bruker conversion (`brkraw`, `bidstools`), small-animal microPET/microCT
  (`amide`), preclinical CT/MR segmentation (`moosez`), phantoms, and
  non-rodent preclinical models. `rodent imaging` is retained for backwards
  compatibility.
* `molecular imaging` — gives PET, SPECT, nuclear medicine, and tracer kinetic
  modeling containers a dedicated discovery category alongside modality-agnostic
  categories such as `image reconstruction` and `quantitative imaging`.

Registry sync note: [NeuroContainers UI](https://github.com/neurodesk/neurocontainers-ui)
maintains the `CATEGORIES` map in `components/common.ts`, and `lib/zodSchema.ts`
derives its category enum from `Object.keys(CATEGORIES)`. Add both entries to
`components/common.ts` (and verify the derived schema accepts them) before
opening recipes with the new categories in the web editor.

## Multi-Domain Tagging List for a Future Preclinical / Molecular Imaging Desktop

NeuroContainers routes recipes to domain desktops through `domains:` in each
`build.yaml`, validated against the
[apps-catalog `domains.json`](https://apps-catalog.scigetorg.workers.dev/domains.json)
registry. Adding a new domain requires:

1. Registering the domain (recommended ID: `preclinical` or `molecular-imaging`)
   in `scigetorg/apps-catalog` (`domains.json`) and publishing its desktop image.
2. Adding that domain ID to the `domains:` list of the recipes below (a
   domain-only PR updates catalog routing without triggering container rebuilds).

Keep brain-PET and shared neuroimaging tools tagged with `neuroimaging` as well
so Neurodesktop users retain seamless access to them.

### 1. Core PET & molecular imaging recipes (dual-tag with `neuroimaging`)

* Existing: `petprep`, `stir`, `clinica`, `freesurfer`, `spm12`, `spm25`
* First-wave additions: `pypet2bids`, `petpvc`, `tpcclib`, `amide`, `moosez`, `kinfitr`

### 2. Core preclinical / small-animal recipes (dual-tag with `neuroimaging`)

* `aidamri`, `brkraw`, `rabies`, `samri`

### 3. Shared dual-use recipes already in `neuroimaging` / `microscopy`

* **DICOM, BIDS & data management:** `bidscoin`, `bidstools`, `datalad`, `dcm2niix`, `dicomtools`, `ezbids`, `globus`, `xnat`
* **Visualization & manual ROI tools:** `fiji`, `itksnap`, `mipav`, `napari`, `quantiphyse`, `slicer`, `voreen`
* **Registration & general image math:** `ants`, `cmtk`, `convert3d`, `elastix`, `niftyreg`, `niimath`
* **Organ & vessel segmentation:** `totalsegmentator`, `vesselvio`, `vmtk`
* **Physics, statistics & scripting:** `irkernel`, `jamovi`, `libreoffice`, `root`, `rstudio`
