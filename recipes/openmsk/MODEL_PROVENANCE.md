# OpenMSK segmentation model provenance

Research date: 2026-09-02

This note traces the four model choices shown by the OpenMSK OpenRecon interface and the removed `acl_qdess_bone_july_2024` compatibility alias. It distinguishes facts recorded in code or model metadata from plausible but undocumented lineage.

## Short answer

| Menu label | What OpenMSK actually runs | Intended image and inference plane | Output |
| --- | --- | --- | --- |
| Goyal sagittal | `sagittal_best_model.h5`, loaded by `StanfordQDessBoneUNet2DSagittal` | DESS/qDESS knee MRI, two-echo root-sum-of-squares image, and sagittal 2D inference | Background plus nine knee tissues |
| Goyal coronal | `coronal_best_model.h5`, loaded by `StanfordQDessBoneUNet2DCoronal` | The input volume is reformatted and sliced coronally at 160 by 512 | The same nine tissues |
| Goyal axial | `axial_best_model.h5`, loaded by `StanfordQDessBoneUNet2DAxial` | The input volume is reformatted and sliced axially at 160 by 512 | The same nine tissues |
| nnU-Net knee | `Dataset500_KneeMRI`, `3d_fullres`, fold 1, `checkpoint_best.pth` | A single MRI volume, processed by a 3D full-resolution nnU-Net | The same nine tissues |

The plane in the three Goyal names is the **2D inference plane**, not a requirement that the scanner acquire three separate series. DOSMA receives one 3D DESS/qDESS volume, computes the two-echo root-sum-of-squares image when needed, reformats it to the class's target orientation, and runs the network slice by slice. [DOSMA model implementation](https://github.com/gattia/DOSMA/blob/bone_seg/dosma/models/stanford_qdess_bone.py) and [pinned KneePipeline dispatch](https://github.com/gattia/KneePipeline/blob/61144f23d9001950a70f77cb70d628e1883da86d/steps/segment.py)

## Exact files packaged by this recipe

The recipe downloads the four distinct weights from Anthony Gatti's Hugging Face account and installs them into KneePipeline. The URLs use the moving `main` branch, not a fixed Hugging Face revision. A future upstream replacement could therefore change the files without a recipe change. See [the local recipe](build.yaml) and the two upstream repositories, [`aagatti/dosma_bones`](https://huggingface.co/aagatti/dosma_bones/tree/main) and [`aagatti/nnunet_knee`](https://huggingface.co/aagatti/nnunet_knee/tree/main).

As of the research date, the DOSMA files are:

| Installed choice | Hugging Face file | SHA-256 | Upload history |
| --- | --- | --- | --- |
| Goyal sagittal | `sagittal_best_model.h5` | `3b772d469241f8addb48fe308cdf47bf62051101f8b948ecc35d2fc0388e3be5` | Added 2025-12-19 in [commit `ff5e0b5`](https://huggingface.co/aagatti/dosma_bones/commit/ff5e0b5d1bf8b850ee6258210e05418889d7133f) |
| Goyal coronal | `coronal_best_model.h5` | `bb63556a712255531da10e9342202571c1f3fd247e5756b7c0ed160af2ad47c2` | Added in the same 2025-12-19 commit |
| Goyal axial | `axial_best_model.h5` | `5f406eaf9aa67af62a63f2f22ba57abb0ca5acfd1d6428745d7df433016f3e3a` | Added in the same 2025-12-19 commit |

The historical `Goyal_Bone_Cart_July_2024_best_model.h5` file has the same SHA-256 as `sagittal_best_model.h5`. Their DOSMA wrappers both select sagittal orientation and a 512 by 512 input. OpenMSK therefore removed the older choice from the menu and no longer packages its duplicate file. Existing saved protocols can still send `acl_qdess_bone_july_2024`; the packaged configuration maps that identifier to `sagittal_best_model.h5`.

The nnU-Net choice installs four files from the 2025-11-07 [full-resolution model commit](https://huggingface.co/aagatti/nnunet_knee/commit/6ccfbd6c5756c8f203f8f0170305cfb0d7bb3221): `dataset.json`, `model_config.json`, `plans.json`, and the 816 MB `fold_1/checkpoint_best.pth`. Its checkpoint SHA-256 is `ced27b5ee8cd6edbea9a868e88fafb236d4eac039d07f2996089b39029a65d1a`. OpenMSK does not package the low-resolution stage or cascade checkpoint. It explicitly configures `type: fullres`, so this is the one-stage `3d_fullres` model, fold 1, despite the Hugging Face card's opening description of a cascade model. [Pinned inference implementation](https://github.com/gattia/nnunet_knee_inference/blob/45f1e51c335ff941f64b552c84b5e588513093fb/scripts/inference.py)

## Anatomy and labels

All four menu choices emit the same native label contract:

| Value | Structure |
| ---: | --- |
| 0 | Background |
| 1 | Patellar cartilage |
| 2 | Femoral cartilage |
| 3 | Medial tibial cartilage |
| 4 | Lateral tibial cartilage |
| 5 | Medial meniscus |
| 6 | Lateral meniscus |
| 7 | Femur bone |
| 8 | Tibia bone |
| 9 | Patella bone |

The contract appears in the DOSMA tissue order, the nnU-Net `dataset.json`, and OpenMSK's public label mapping. [DOSMA implementation](https://github.com/gattia/DOSMA/blob/bone_seg/dosma/models/stanford_qdess_bone.py), [nnU-Net dataset metadata](https://huggingface.co/aagatti/nnunet_knee/blob/main/models/Dataset500_KneeMRI/nnUNetTrainer__nnUNetResEncUNetMPlans__3d_fullres/dataset.json), and [local OpenMSK wrapper](openmsk.py)

## DOSMA DESS and Goyal models

### Model family

The three packaged `.h5` files are TensorFlow/Keras 2D segmentation networks. Inspection of the files themselves shows channels-first inputs, softmax outputs with ten channels including background, residual blocks, batch normalization, PReLU, dropout of 0.2, and deep-supervision branches. The sagittal file records Keras 2.11.0 and an input of `1 x 512 x 512`; the coronal and axial files record Keras 2.16.0 and inputs of `1 x 160 x 512`.

DOSMA whitens the image, predicts the slices, takes `argmax` over the ten output channels, keeps connected components, fills holes in labels 7 through 9, and restores the input orientation. Its class documentation says these models target quantitative double-echo steady-state knee scans and use the root-sum-of-squares of the two echoes. [DOSMA implementation](https://github.com/gattia/DOSMA/blob/bone_seg/dosma/models/stanford_qdess_bone.py)

### Documented training source for the July/sagittal model

The strongest publication match is Goyal et al., *Automating Imaging Biomarker Analysis for Knee Osteoarthritis Using an Open-Source MRI-Based Deep Learning Pipeline*. The paper describes a sagittal 2D Keras network with the same nine tissues, 512 by 512 input, deep supervision, batch normalization, dropout of 0.2, and per-tissue Dice loss. It trained on 347 scans:

- 176 standard DESS scans from Siemens 3T scanners at four sites
- 155 qDESS scans from one of two GE 3T MR750 scanners
- 16 qDESS scans from subjects with anterior cruciate ligament reconstruction, acquired on a Siemens 3T Magnetom

The authors used rotations within 6 degrees, translations within 20 percent, Adam, a batch size of 12, a learning rate of `10^-4.5`, and early stopping. They validated on a separate prospective set of 20 qDESS volumes from a GE 3T MR750. They also state that the model was optimized for sagittal fat-saturated gradient-echo images and that performance on other sequences remains untested. [Goyal et al. preprint and PDF](https://www.medrxiv.org/content/medrxiv/early/2025/02/23/2025.02.21.25322094.full.pdf), later published in *Osteoarthritis Imaging* as [DOI 10.1016/j.ostima.2025.100288](https://doi.org/10.1016/j.ostima.2025.100288)

The file name, architecture, label set, lead author, and KneePipeline citation all point to this paper. Still, neither the 21-byte Hugging Face model card nor the paper publishes the checkpoint hash. The public record does **not** prove that `Goyal_Bone_Cart_July_2024_best_model.h5` is the exact model evaluated in the paper. "July 2024" is a checkpoint name or vintage; Hugging Face did not receive the file until August 2024.

"Goyal" most likely refers to first author Ananya Goyal. Anthony Gatti owns the Hugging Face repository and is the last author of the paper. No first-party manifest explicitly explains the menu name.

### Coronal and axial uncertainty

The coronal and axial files use the same network family and labels. The DOSMA wrappers establish their slice planes and input sizes. The Hugging Face history says only "Add sagittal, coronal, axial orientation models." It gives no cohort, split, training procedure, performance, or citation for those two distinct checkpoints.

It is tempting to assume that the coronal and axial models came from reformatting the 347-volume Goyal training set, but no source confirms this. The paper explicitly describes and validates a sagittal model, not a three-plane ensemble. Treat the coronal and axial training provenance as undocumented until the maintainers provide a model card or training manifest.

## nnU-Net knee

This model is a later, separate PyTorch/nnU-Net v2 implementation. The packaged plans specify `dynamic_network_architectures.architectures.unet.ResidualEncoderUNet`, the `nnUNetResEncUNetMPlans` planner, 3D convolutions, Z-score normalization, a `56 x 192 x 160` full-resolution patch, target spacing near `0.800 x 0.313 x 0.313` mm, and a median source shape near `160 x 512 x 510`. [Packaged plans](https://huggingface.co/aagatti/nnunet_knee/blob/main/models/Dataset500_KneeMRI/nnUNetTrainer__nnUNetResEncUNetMPlans__3d_fullres/plans.json)

The packaged `dataset.json` says `numTraining: 342`, one MRI channel, and the nine-tissue label map. The Hugging Face card tags the dataset as `oai-zib`, but does not identify case IDs, releases, inclusion criteria, or a split manifest. [Dataset metadata](https://huggingface.co/aagatti/nnunet_knee/blob/main/models/Dataset500_KneeMRI/nnUNetTrainer__nnUNetResEncUNetMPlans__3d_fullres/dataset.json) and [model card](https://huggingface.co/aagatti/nnunet_knee)

The inference repository reports a mean Dice score of 0.921 for full-resolution fold 1 and 0.924 for the separate cascade. These are maintainer-reported numbers, not an independently verified evaluation tied to test case IDs. [Pinned inference README](https://github.com/gattia/nnunet_knee_inference/blob/45f1e51c335ff941f64b552c84b5e588513093fb/README.md) and [usage guide](https://github.com/gattia/nnunet_knee_inference/blob/45f1e51c335ff941f64b552c84b5e588513093fb/USAGE_GUIDE.md)

The nnU-Net provenance has two conflicts worth preserving:

1. The Hugging Face card foregrounds a two-stage cascade, while OpenMSK packages and runs only the one-stage full-resolution model.
2. The full-resolution directory's `model_config.json` says `stage: fullres` but also contains `configuration: 3d_lowres`. The latter appears stale because the directory, plans, and inference command all select `3d_fullres`.

The Goyal paper's training cohort has 347 images and describes a Keras 2D model. The nnU-Net metadata says 342 training images and supplies no subject manifest. The two numbers are close, but they are not enough to assert that nnU-Net was retrained on the Goyal cohort. Its exact training provenance remains undocumented.

## Practical interpretation

- The removed July model and Goyal sagittal are the same sagittal checkpoint under two names. Only Goyal sagittal remains in the menu and image.
- Goyal coronal and axial change the 2D slicing plane. They do not imply a coronal or axial acquisition protocol.
- All four DOSMA choices were designed for DESS/qDESS-style fat-saturated gradient-echo knee MRI. Applying them to unrelated contrasts is outside the published validation.
- nnU-Net knee is the only 3D model in the menu. OpenMSK runs the faster single-stage full-resolution fold, not the two-stage cascade advertised at the top of its model card.
- None of these weights has a proper versioned model card with case-level training and validation manifests. The recipe's moving Hugging Face URLs are also a reproducibility risk.
