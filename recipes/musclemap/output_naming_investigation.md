# MuscleMap Output Naming Investigation

## Artifact Alignment
- Failing scanner log: [composer_bug.log](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/composer_bug.log)
- Scanner image digest from the log: `bc5a69496ae919f6fdfe5a57a9102dd377d5d63d8fb197f1685f4f9899f45f3e`
- Decoded OpenRecon metadata from the same log reports `general.version = 1.3.11`.
- Local source in [build.yaml](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/build.yaml:1) is now `1.3.14`.
- There is no explicit `1.3.11` version bump in git history. The published `1.3.11` image appears to be an intermediate build between the `1.3.10` and `1.3.12` repo states.

## Relevant History
| Commit | Date | Intent | Fields affected |
| --- | --- | --- | --- |
| `ccb1fb9a` | 2026-03-11 | switch to volume-key Water-only processing | source volume selection, image typing, output image type defaults |
| `c0ff30cb` | 2026-04-03 | rename segmentation image type | `ImageTypeValue4`, `DicomImageType` |
| `9f888017` | 2026-04-06 | add source-dependent segmentation labels | `SeriesDescription`, `SequenceDescription`, `SequenceDescriptionAdditional` |
| `ad8f360b` | 2026-04-15 | create a new segmentation `image_series_index` | header `image_series_index`, copied header behavior |
| `42581608` | 2026-04-16 | collapse display naming to `Musclemap`, add identity logging | display label constants, minihead patching, output summaries |

## Failing Run Identity Contract

### Source Water series from the failing run
- `image_series_index = 4`
- `SequenceDescription = wholebody_t1_vibe_dixon_tra_mbh_dyn_W`
- `SeriesDescription = N/A` in Meta, so minihead is authoritative
- `SeriesNumberRangeNameUID = wholebody_t1_vibe_dixon_tra_mbh_dyn_W` from minihead
- `SeriesInstanceUID = 1.3.12.2.1107.5.2.63.213061.2026042110034866347945601` from minihead
- `ImageType = DERIVED\PRIMARY\DIXON\WATER`
- `ImageTypeValue3 = M` from minihead
- `ImageTypeValue4 = WATER` from minihead

This is visible in the latest `INPUT_VOLUME_SUMMARY` event in the log.

### Segmentation series from the failing run
- `image_series_index = 6`
- `SequenceDescription = Musclemap`
- `SeriesDescription = Musclemap`
- `SeriesNumberRangeNameUID = wholebody_t1_vibe_dixon_tra_mbh_dyn`
- `SeriesInstanceUID = 1.3.12.2.1107.5.2.63.213061.2026042110034866347945601`
- `ImageType = DERIVED\PRIMARY\M\MUSCLEMAP`
- `ImageTypeValue3 = M`
- `ImageTypeValue4 = Musclemap`
- `ImageComment` and `ImageComments` contain the segmentation label plus inline metrics payload
- `AnatomicalPartitionNo` and `ChronSliceNo` track the source slice order

This is visible in `MUSCLEMAP_OUTPUT_IDENTITY` events around `2026-04-21 17:07:36 UTC`.

### Metrics series from the failing run
- `image_series_index = 7`
- `SequenceDescription = MuscleMap_Metrics`
- `SeriesNumberRangeNameUID = wholebody_t1_vibe_dixon_tra_mbh_dyn_MuscleMap_Metrics`
- `ImageTypeValue4 = METRICS`
- Scanner created a distinct series UID for series number 7:
  `1.3.12.2.1107.5.2.63.213061.2026042110141194716368346`

This is visible in:
- `Created 1 MuscleMap metrics report image(s) in image_series_index=7`
- `Created new SeriesInstanceUID ... for series number 7`
- `SEND_BATCH_SUMMARY` at `2026-04-21 17:14:14 UTC`

## Per-Series Table From the Failing Run
| Output | Visible label | `image_series_index` | Grouping key | Series UID | Result |
| --- | --- | --- | --- | --- | --- |
| Water source | `wholebody_t1_vibe_dixon_tra_mbh_dyn_W` | 4 | `wholebody_t1_vibe_dixon_tra_mbh_dyn_W` | `...10034866347945601` | correct |
| Segmentation | `Musclemap` | 6 | `wholebody_t1_vibe_dixon_tra_mbh_dyn` | `...10034866347945601` | wrong |
| Metrics | `MuscleMap_Metrics` | 7 | `wholebody_t1_vibe_dixon_tra_mbh_dyn_MuscleMap_Metrics` | `...10141194716368346` | correct |

## Correlation To The Screenshot
- The screenshot shows the parent Dixon grouping plus a nested `Musclemap` folder and duplicate `wholebody_t1_vibe_dixon_tra_mbh_dyn <1>` / `_1` groups.
- That behavior matches the failing identity contract exactly:
  - segmentation changed the visible name to `Musclemap`
  - segmentation kept the grouping key on the parent Dixon name
  - segmentation reused the Water series UID instead of getting a segmentation-specific UID
- Metrics do not show the same nesting problem because metrics received both a distinct grouping key and a new series UID.

## Root Cause
The failing `1.3.11` image produced a split identity for segmentation:
- header series index changed to a new derived series
- visible description changed to `Musclemap`
- grouping and UID did not move with it

That creates an inconsistent series contract. Siemens Composer appears to use grouping and UID fields, not only visible descriptions, so segmentation gets attached to the parent Dixon series and then rendered as a duplicate or nested series.

The strongest evidence is that the failing segmentation series shares the exact same `SeriesInstanceUID` as the source Water series while exposing a different visible description.

## Current Source Comparison

### What is different in current `1.3.14` source
- Current code computes a dedicated segmentation grouping in [musclemap.py](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/musclemap.py:2035):
  - `segmentation_grouping = f"{source_parent_grouping}_{muscleMapDisplayLabel}"`
- Current code assigns that grouping into Meta in [musclemap.py](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/musclemap.py:2570):
  - `SeriesDescription = Musclemap`
  - `SequenceDescription = Musclemap`
  - `SeriesNumberRangeNameUID = segmentation_grouping`
- Current code also patches minihead `SequenceDescription` and `SeriesNumberRangeNameUID` in [_patch_ice_minihead](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/musclemap.py:598).

### What is still missing in current source
- Current source does not create or assign a new `SeriesInstanceUID` anywhere for segmentation or metrics. The file only reads or logs `SeriesInstanceUID`; it never generates one.
- Current source still changes `image_series_index` for segmentation in [musclemap.py](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/musclemap.py:2521), so series identity can still drift unless UID assignment is fixed in the same path.

### Conclusion about current source
- The exact `1.3.11` bug shown in the scanner log does not match current source anymore because current source now intends to use `..._Musclemap` as the grouping key.
- The current source still has an open identity risk because it does not assign a segmentation-specific `SeriesInstanceUID`.
- So the bug has shifted:
  - `1.3.11`: confirmed grouping mismatch plus UID reuse
  - current `1.3.14`: grouping likely improved, UID uniqueness still unresolved

## Fix Spec
Use one shared derived-series identity builder for both segmentation and metrics.

For segmentation, set these fields together in one code path:
- header:
  - new `image_series_index`
  - stable per-slice `image_index` and `slice`
- Meta:
  - `SeriesDescription = Musclemap`
  - `SequenceDescription = Musclemap`
  - `SeriesNumberRangeNameUID = <parent>_Musclemap`
  - `SeriesInstanceUID = <new segmentation UID>`
  - coherent `ImageType`, `ImageTypeValue3`, `ImageTypeValue4`, `DicomImageType`
  - desired `ImageComment` and `ImageComments`
- IceMiniHead:
  - same `SequenceDescription`
  - same `SeriesNumberRangeNameUID`
  - same `SeriesInstanceUID`
  - same image type tokens

For metrics, set these fields together:
- `SeriesDescription = MuscleMap_Metrics`
- `SequenceDescription = MuscleMap_Metrics`
- `SeriesNumberRangeNameUID = <parent>_MuscleMap_Metrics`
- `SeriesInstanceUID = <new metrics UID>`
- `ImageTypeValue4 = METRICS`

Required invariants:
- segmentation must never reuse the source Water `SeriesInstanceUID`
- metrics must never reuse either the source Water or segmentation `SeriesInstanceUID`
- header series index, Meta grouping, Meta UID, minihead grouping, and minihead UID must all describe the same derived series

## Scenario 2 Status
- A full local rerun against current source was not executed in this turn.
- Reason:
  - this workspace has no `/buildhostdirectory` sample scanner input mounted
  - local Python currently does not have `ismrmrd` or `mrdhelper` installed, which blocks a direct lightweight execution of `musclemap.py`
- Docker is available, so a follow-up runtime comparison is feasible once sample input data is mounted or copied into the environment.

## Reproducible Log Extraction
- Script added: [analyze_composer_output_naming.py](/Users/uqsbollm/github-repos/neurodesk/neurocontainers/recipes/musclemap/analyze_composer_output_naming.py)
- Example:

```bash
python3 recipes/musclemap/analyze_composer_output_naming.py recipes/musclemap/composer_bug.log
```

That script extracts:
- scanner image digest
- decoded OpenRecon metadata blob for MuscleMap
- latest Water input identity
- latest segmentation identity
- latest send-batch summary
- series-number and UID creation events
