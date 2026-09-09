# Full-body exam smoke test for MuscleMap v1.4

Research and execution date: 2026-08-27

## Outcome

The updated `musclemap:1.4.0` image completed offline, CPU-only whole-body
segmentation on five coronal MRI stations from the NLM Visible Human male. The
stations jointly cover the neck, chest, pelvis, knees, and ankles. All five
outputs passed structural and label-map validation.

This is a runtime and anatomical-coverage smoke test, not clinical accuracy
validation. The Visible Human data are regional cadaver acquisitions with
station-specific positioning and contrast, whereas MuscleMap's documented
whole-body workflow is aimed at in-vivo axial MRI. The pelvis and knee results
were correspondingly sparse.

## Why this exam was selected

The [NLM Visible Human Project](https://www.nlm.nih.gov/research/visible/)
provides complete, anatomically detailed male and female cadaver datasets for
medical education and for testing medical imaging algorithms. The
[Imaging Data Commons collection](https://portal.imaging.datacommons.cancer.gov/collections/nlm_visible_human_project/)
provides the project's MR and CT DICOM series without an account. The source
data are public domain.

No public living-subject MRI examined during this search contained strict
head-to-ankle coverage in a practical, directly downloadable NIfTI. The
Visible Human male therefore provided the strongest reproducible test that
every MuscleMap v1.4 whole-body region could be exercised in one subject.

Five T1 coronal series were selected from study `VHP-M`:

| Station | Series instance UID | DICOM files | Converted NIfTI shape | Spacing (mm) |
| --- | --- | ---: | --- | --- |
| Neck | `1.3.6.1.4.1.5962.1.3.2297.6.1672334394.26545` | 63 | `256 x 256 x 63` | `1.172 x 1.172 x 4.0` |
| Chest | `1.3.6.1.4.1.5962.1.3.2297.9.1672334394.26545` | 70 | `256 x 256 x 70` | `1.875 x 1.875 x 4.0` |
| Pelvis | `1.3.6.1.4.1.5962.1.3.2297.13.1672334394.26545` | 74 | `256 x 256 x 74` | `1.875 x 1.875 x 4.0` |
| Knee | `1.3.6.1.4.1.5962.1.3.2297.18.1672334394.26545` | 45 | `256 x 256 x 45` | `1.875 x 1.875 x 5.0` |
| Ankle | `1.3.6.1.4.1.5962.1.3.2297.21.1672334394.26545` | 56 | `256 x 256 x 56` | `1.875 x 1.875 x 4.0` |

The download contained 308 DICOM instances and occupied approximately 40 MB.
`dcm2niix` converted the five series to compressed NIfTI before inference.

## Container and execution

The exact generated NeuroContainers recipe built successfully as
`musclemap:1.4.0`:

```text
Image ID: sha256:bf49fcaf86907b133da0479176189b2d81f2c066fc02f5000920cbd4ac63e7f3
Image size: 6,757,731,199 bytes
Runtime network: disabled
Runtime compute: 8 CPUs, no GPU, 28 GiB memory limit
```

Before inference, the packaged verifier loaded the real whole-body model while
the container had no network access:

```bash
docker run --rm --network none musclemap:1.4.0 \
  python /opt/code/verify_musclemap_install.py --load-wholebody-model
```

It reported `MuscleMap installation verified`.

Neck and chest completed with automatic chunk sizing and 50 percent overlap.
The original command used one comma-separated input list:

```bash
docker run --rm --network none --memory 28g --cpus 8 \
  -v "$PWD/vhp-m-nifti:/input:ro" \
  -v "$PWD/vhp-m-output:/output" \
  musclemap:1.4.0 \
  mm_segment \
    --input_image /input/NECK_6.nii.gz,/input/T1_CORONAL_CHEST_9.nii.gz,/input/T1_CORONAL_PELVIS_13.nii.gz,/input/T1_CORONAL_KNEE_18.nii.gz,/input/T1_CORONAL_ANKLE_21.nii.gz \
    --output_dir /output \
    --region wholebody \
    --model_version 1.4 \
    --chunk_size auto \
    --overlap 50 \
    --use_GPU N
```

Automatic sizing selected 17 slices for the pelvis. The process was killed at
the 28 GiB limit during inverse resampling after neck and chest had completed.
The estimator treats the input's third axis as the inference slice axis, but
reorientation of these coronal images changes the dimensions that determine
the logits and post-processing peak. This edge case is not representative of
OpenRecon's axial input, but it matters for arbitrary command-line NIfTI use.

Pelvis, knee, and ankle were rerun successfully with explicit five-slice chunks:

```bash
docker run --rm --network none --memory 28g --cpus 8 \
  -v "$PWD/vhp-m-nifti:/input:ro" \
  -v "$PWD/vhp-m-output-retry:/output" \
  musclemap:1.4.0 \
  mm_segment \
    --input_image /input/T1_CORONAL_PELVIS_13.nii.gz,/input/T1_CORONAL_KNEE_18.nii.gz,/input/T1_CORONAL_ANKLE_21.nii.gz \
    --output_dir /output \
    --region wholebody \
    --model_version 1.4 \
    --chunk_size 5 \
    --overlap 0 \
    --use_GPU N
```

The retry completed in 44:26 wall time: pelvis inference took 17:40, knee
11:59, and ankle 14:16. Zero overlap was deliberately used to make the
CPU-only feasibility run tractable; it can introduce chunk-boundary seams and
is not the recommended quality setting.

## Output validation

For every station, an offline validation script loaded the source and mask with
Nibabel and asserted:

- identical voxel shape;
- numerically identical affine;
- `int16` mask datatype;
- nonzero foreground; and
- every observed value belongs to the packaged whole-body v1.4 JSON label map.

Results:

| Station | Foreground labels | Foreground voxels | Result |
| --- | ---: | ---: | --- |
| Neck | 30 | 147,957 | Pass |
| Chest | 36 | 82,597 | Pass |
| Pelvis | 20 | 545 | Pass, sparse |
| Knee | 26 | 3,029 | Pass, sparse |
| Ankle | 21 | 6,842 | Pass |

The union contained 64 of the 113 foreground label values represented by the
v1.4 model. Visual overlays were anatomically localized in all five stations,
but the sparse pelvis and knee masks reinforce that this out-of-distribution
cadaver exam must not be interpreted as an accuracy benchmark.

Artifacts are under `build/musclemap-e2e/`:

- source station montage: `vhp-m-stations.png`;
- segmentation overlay montage: `vhp-m-musclemap-overlay.png`;
- neck and chest masks: `vhp-m-output/`;
- pelvis, knee, and ankle masks: `vhp-m-output-retry/`.

## Living-subject cross-check

TotalSegmentator MRI v2 case `s0175` was also run as an in-vivo cross-check.
The official release contains 616 clinical MRI scans under CC BY-NC-SA 2.0.
[Official Zenodo record](https://zenodo.org/records/14710732),
[official TotalSegmentator repository](https://github.com/wasserth/TotalSegmentator)

The standalone NIfTI came from a mirror pinned to commit
`1afa2dfd85b57d41608c073ead6fd4b19520394f`:

```text
File:    s0175/mri.nii.gz
Size:    16,134,253 bytes
SHA-256: 0f311daa4f19d8b89c1380832332db11d5f32cf406d2b591ba6800e6433eb379
Shape:   320 x 240 x 360
Spacing: 1.2813 x 1.2813 x 3.0 mm
```

The updated image segmented it offline on CPU with automatic chunks and 50
percent overlap. Inference took 293 seconds (5:05 wall time). The output
matched the input shape and affine, used `int16`, contained 85 foreground
labels and 1,532,869 foreground voxels, and contained no unknown label values.

Although catalogued as whole-body and spanning 1,080 mm, visual inspection
showed that `s0175` ends at the upper thighs. It is a useful living-subject
runtime check, but it does not exercise the lower-leg regions.

## Other candidates screened

- All 393 standalone TotalSegmentator MRI mirror files were screened from
  their NIfTI headers. None provided verified strict head-to-ankle anatomy;
  the longest fields of view were `s0112` at 1,094.4 mm and `s0175` at
  1,080 mm.
- All 1,202 TotalSegmentator CT mirror files were likewise screened. The
  longest was 1,276.5 mm, but sampled long-volume candidates were still
  regional rather than head-to-ankle.
- CMB-MML patient `MSB-00268` explicitly describes a whole-body MRI and is
  de-identified, but the four public axial T2 series available for the chosen
  exam covered abdomen through lower legs and omitted the upper-body station.
  [IDC CMB-MML collection](https://portal.imaging.datacommons.cancer.gov/collections/cmb_mml/)
- The UK Biobank whole-body atlas contains population averages rather than one
  exam. [Atlas record](https://zenodo.org/records/13136891)
- The whole-body golden-angle dataset provides raw k-space rather than an image
  MuscleMap can consume directly. [Raw-data record](https://zenodo.org/records/45080)

## Reuse and privacy

The Visible Human test uses a public-domain cadaver dataset and therefore has
no living-subject privacy issue. TotalSegmentator `s0175` is a publicly released
pseudonymized clinical NIfTI under CC BY-NC-SA 2.0; the mirror is not the
publisher, so reproducible reuse should retain the pinned commit and checksum,
and redistribution should follow the official dataset license.
