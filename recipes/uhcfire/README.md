# UHC FIRE

This local application uses `make_dsir.py` and `rigid2.py` from the supplied
`26_11_UHC_Fire (2).zip`. The offline DICOM comparison and GUI scripts are not
needed by the server. The supplied archive contains no license declaration;
confirm redistribution terms before publishing its code or container.

Mount a persistent host directory at `/tmp/share/uhcfire`, writable by the
server user. Alternatively set `UHC_DATA_DIR` to a persistent mounted directory.
Container-local storage alone does not survive container replacement. Never
use a home directory. Keep the same mount across acquisitions and containers.

Start the server:

```sh
uhcfire
```

Send `uhcfire` as the MRD config name and this JSON as the additional config text:

```json
{"parameters":{"config":"uhcfire","group":"exam-unique-pair-1","shortti":300,"longti":500,"ti":300,"final":false,"register":true}}
```

Send one reconstructed magnitude volume per connection, as single-channel 2D
MRD slices with unique slice counters. Waveforms are ignored and non-magnitude images are passed through. Raw k-space, multi-channel images,
packed 3D image messages and repeated contrasts are rejected. Supply `ti` in
milliseconds, or leave it empty to use the single TI in MRD sequence metadata. TI matching
allows 0.05 ms rounding error and rejects ambiguous matches.

For the second acquisition, retain the group and pair, set `ti` to 500 and
`final` to true. Either TI may arrive last. Both TIs are configurable, with
`shortti < longti`. Original images are returned on every successful acquisition. Final processing
also returns dSIR, lSIR and aSIR as signed real-valued int16 series 101, 102 and 103, using the example's scale factor of 10000.
Registration defaults to the example's 2D rigid registration. Turn it off only
for already aligned inputs. Geometry must match even with registration enabled.

Use a new group for each exam, repeat pair or reconstruction variant. Never
reuse a group across patients. MRD `studyInformation.studyInstanceUID` is
required; a hash of the study, frame-of-reference and patient identifiers binds
the group to the exam. A different exam using that group is rejected. Within
an exam, the group still distinguishes repeated pairs and reconstruction variants. Repeating an acquisition replaces that TI atomically;
per-group file locks prevent partially read acquisitions. Geometry is checked
before replacement so rejected inputs leave saved data intact. A missing counterpart
fails final processing but retains the acquisition for retry. Successful
processing retains both inputs too. Delete the group's hashed directory only
when no acquisition is running and the data are no longer needed. This first
version has no automatic retention policy or scanner UI label.

The numerical implementation is preserved from the example. Scanner display,
image scaling, motion correction quality and host persistence must be validated
with representative scanner data before use. Local smoke tests use synthetic
MRD images and temporary disk storage.
