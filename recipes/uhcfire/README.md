# UHC FIRE 1.1.0

Each sequence sends one inversion-time magnitude volume. The container saves
every TI to disk. Arrival of the configured trigger TI, initially 778 ms,
computes dSIR, lSIR and aSIR for every short/long pair in the acquisition set.
The calculation uses `make_dsir.py` and `rigid2.py` from the supplied example.

## Scanner configuration

Use identical settings on all sequences, except the current TI:

- Acquisition group: a unique name for this exam and acquisition set.
- Expected TIs: `300,500,778` initially. Enter a comma-separated list to use more TIs.
- TI that triggers processing: `778` ms.
- Current TI: leave empty to read the single TI from MRD sequence metadata, or
  enter this sequence's TI in milliseconds.
- Register inversion-time images: enabled by default, using the example's
  in-plane rigid registration independently for each pair.

The 300 ms and 500 ms sequences save their images and return the originals.
The 778 ms sequence saves its images, checks that all expected TIs are present,
and returns originals plus these derived series:

| TI pair (ms) | Outputs | Series indices |
| --- | --- | --- |
| 300 / 500 | dSIR, lSIR, aSIR | 101–103 |
| 300 / 778 | dSIR, lSIR, aSIR | 104–106 |
| 500 / 778 | dSIR, lSIR, aSIR | 107–109 |

Seven distinct TIs produce 21 pairs and 63 derived image series. Each series
name identifies the metric and TI pair. Pairs are ordered by increasing short
TI, then increasing long TI. Signed int16 output uses the example's factor of
10000. Inputs must share image geometry; registration corrects in-plane motion
but does not permit mismatched grids.

## Storage and invocation

Mount a persistent host directory at `/tmp/share/uhcfire`, writable by the
server user, or set `UHC_DATA_DIR` to another persistent mounted directory.
Use the same mount across all sequences. Container-local storage does not
survive container replacement. Do not use a home directory.

Start the server with `uhcfire`. For an MRD client, send `uhcfire` as the config
name and this JSON as additional config text:

```json
{"parameters":{"config":"uhcfire","group":"exam-unique-set-1","expectedtis":"300,500,778","triggerti":778,"ti":"","register":true}}
```

Send single-channel 2D MRD slices with unique slice counters. Waveforms are
ignored; non-magnitude images pass through. Raw k-space, packed 3D messages,
multi-channel images and repeated contrasts are rejected. TI matching allows
0.05 ms rounding error. Expected values must be separated by more than 0.1 ms.

MRD studyInstanceUID is required. A hash of study, frame-of-reference and patient
identifiers binds the acquisition group to an exam; mismatches are rejected.
Use a new group for each repeat set or reconstruction variant. Groups containing
1.0.0 pair-format data are rejected; acquire into a new group after upgrading.

Each TI is saved atomically, with its TI value and geometry. Per-group locks
protect concurrent reads and writes. Repeating a TI replaces that acquisition;
invalid geometry leaves existing data intact. If the trigger arrives before
all expected TIs, it is saved and processing fails with the missing TI list.
Acquire the missing TIs, then resend the trigger acquisition to compute.
Successful computation retains inputs. Remove the group's hashed directory
only when no acquisition is running and the data are no longer needed.

Local and container smoke tests cover two-, three- and seven-TI sets. Scanner
display, signed image scaling and motion correction quality still require
validation with representative scanner data. The supplied source archive
contains no license declaration; no third-party license has been inferred.
