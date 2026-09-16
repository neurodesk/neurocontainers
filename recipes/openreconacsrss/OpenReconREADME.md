# ACS Fourier/RSS capture POC

`openreconacsrss` records calibration and imaging streams without filtering them.
With reconstruction enabled, it selects either MRD parallel-calibration flag,
places ACS lines at their declared PE coordinates on the encoded matrix, applies
the remaining inverse Fourier transforms, and returns RSS magnitude images.

- `kx-ky`: inverse transforms along readout and phase encoding.
- `x-ky`: inverse transform along phase encoding only. The readout dimension
  must already be spatial and its FOV/matrix must describe that spatial grid.

Missing outer PE lines are zero-filled. A contiguous central ACS region produces
a low-resolution image on the full encoded FOV; it is not a full-resolution
reconstruction. Separate slice, repetition, echo, phase, set, average, segment,
measurement, and encoding counters are grouped separately. Duplicate PE lines
within a group are rejected rather than silently overwritten or averaged.

## First scanner session

This is a research POC with runnable MRD-side code and synthetic tests. No scanner
software release, sequence, ICE tap, or DICOM return path has been verified yet.
The default `mode=capture` never guesses which Fourier transforms remain.
`inputdomain=unknown` and `preprocessed=false` deliberately block reconstruction.

1. Keep the normal ICE reconstruction enabled. Arrange a parallel export branch
   to FIRE with your scanner/sequence integration contact. The container cannot
   select an internal ICE functor or create that branch through its label.
2. Request calibration AND imaging data, including adjustment/reference scans.
   Export at the intended location after ramp-sampling regridding and EPI phase
   correction. Record the actual functor names and their ordering. Save a second
   capture from any alternative candidate tap rather than guessing equivalence.
3. For ACS, determine whether the stream contains `kx,ky` or `x,ky` samples and
   whether readout oversampling removal, readout reversal, coil compression,
   filtering, and Fourier transforms have already occurred. MRD calibration flags
   identify a data role, not the ICE processing history.
4. For SMS, request the individual single-band references and slice-collapsed
   imaging data before native slice-GRAPPA. Record physical slice order, packet
   mapping, multiband factor, CAIPI phase convention, in-plane acceleration,
   partial Fourier, reference reuse, coil compression basis, and phase correction
   applied to each stream. Include the standard reconstructed DICOM series.
5. Use a short phantom scan first. Confirm that capture-only, which can return
   zero images for raw-only input, completes in the parallel branch. Check that
   the normal image series still appears and that the capture directory persists.
6. Retain the whole session directory, server log, ICE configuration/export,
   protocol export, scanner software/FIRE versions, and reference DICOMs together.
   Fill in `scanner-session.example.json` before iterating. This manual context
   contains facts the MRD stream cannot establish.

The label requests a raw emitter, an image injector, adjustment data, and no
channel compression. These are requests, not evidence that a particular ICE tap
exports the required data. `can_process_adjustment_data=true` allows capture of
those data; it does not identify single-band references automatically.
There is no raw injector in the repository's OpenRecon label schema. This POC
returns a second image series; it does not inject separated acquisitions back
into a later ICE functor.

## Running and collecting captures

Build with `sf-build openreconacsrss`, then launch the standard server inside that image
or its OpenRecon packaging environment:

```sh
openreconacsrss -v -l /tmp/share/fire-poc/server.log
```

Select `config=openreconacsrss` and leave the other UI defaults unchanged. Mount a
persistent host directory at `/tmp/share/fire-poc`. Alternatively set
`FIRE_POC_CAPTURE_ROOT` to an existing writable shared location when launching
the server, and change the log path accordingly. Each connection creates a
UTC-time/application/UUID directory. Separate connections never share kernels.
Do not store runtime assets under `/home`.

Each session contains:

- `input.h5`: every acquisition with trajectory and original fixed header,
  every image with its attributes, and all waveforms. Images are grouped by
  source series, dimensions, and datatype to preserve mixed streams.
- `metadata.xml` and `config.json`: the metadata supplied to the application and
  the complete application configuration. XML serialization may normalize
  whitespace; the server owns the original wire-level header.
- `events.jsonl`: receive order and UTC receipt time, every fixed header field,
  decoded flag names, image attributes, dimensions, datatype, finite-value check,
  and maximum absolute sample value. User counters and user fields are included.
- `summary.json`: counts, flag totals, measurement UIDs, acquisition layouts,
  close-message observation, runtime/package versions, source hash, status, and
  error traceback. The summary updates every 100 messages and on completion.
- `reconstruction/`: only in opt-in reconstruction mode, derived `output.h5` and
  offline DICOM previews. Reconstruction failure preserves the input capture.

Capture writes incrementally and flushes each message. It does not keep the full
scan in RAM. There is no automatic deletion or size limit. Monitor available
space during long scans. The captured metadata and image attributes retain the
original subject information. A process crash can leave status `recording` and
an incomplete HDF5 file; this is not a completed capture.

`sendoriginal=true` returns each incoming MRD image unchanged and immediately.
Raw acquisitions are saved but are not sent through an image injector. Therefore
original scanner DICOMs require the native parallel ICE branch when the input
is raw-only. No synthetic reconstruction is labelled as an original image.

## Offline iteration

Copy `replay.example.json` and edit it only after checking the captured data.
Set `preprocessed=true` when regridding, phase correction, and canonical readout
polarity are confirmed. Set `inputdomain` explicitly. These are operator
assertions; the application cannot infer them from acquisition flags.

```sh
python /opt/code/python-ismrmrd-server/fire_poc.py /path/to/session \
  --app openreconacsrss --config /path/to/verified-config.json \
  --output /path/to/new-output --dicom
```

The same command runs from the repository with
`macros/fire_poc/fire_poc.py` and Python dependencies from the recipe. Output
must be a new directory. `--dicom` writes derived MR preview files with new
Study/Series/SOP UIDs, empty patient identity, encoded-grid geometry, and uint16
pixels plus a rescale slope. They are offline previews, not scanner-validated
clinical DICOMs. Scanner DICOM generation instead uses the returned MRD images
and the scanner's image injector. Confirm orientation, FOV, scaling, slice order,
series identity, and DICOM attributes against the phantom reference before use.

Both reconstruction modes run after the incoming session closes. They load the
acquisitions into memory for inspection and grouping, so start with short scans.
This is not yet a low-latency streaming reconstruction. A reconstruction error
can leave partial derived outputs; consult the status/traceback before using them.
The default derived MRD series index is 60000, configurable with `series` in JSON;
collision with an incoming original series is rejected.

## Tests and known scope

```sh
python macros/fire_poc/tests/smoke_fire_poc.py
```

The same synthetic capture, numerical reconstruction, and DICOM tests run from
`fulltest.yaml` inside each image. They prove MRD-side behavior, not scanner
compatibility. The shared implementation is `macros/fire_poc/fire_poc.py`.
The base image and four shared OpenRecon sources are pinned through recipe
variables and tracked by the repository's automatic update policy.

Only unsegmented 2D Cartesian reconstruction is implemented. Captures still save
unsupported inputs. Reconstruction rejects trajectories, reversed readouts,
asymmetric readout centers, repeated PE lines, and inconsistent geometry/coil
layouts. It does not perform ramp regridding, EPI phase correction, coil
compression, partial-Fourier completion, in-plane GRAPPA, or oversampling removal.
Headers must describe the grid at the export point, not a grid before ICE changed
it. No implicit flips or transpose conventions are used to imitate ICE.

## Sources and unresolved scanner contract

- [MRD acquisition headers and flags](https://ismrmrd.readthedocs.io/en/latest/mrd_raw_data.html)
  define calibration flags, encoding counters, readout centers, and geometry.
- [Python MRD server](https://github.com/astewartau/python-ismrmrd-server/tree/33362f2139701fbf5ea855808a325eb59b7db2ae)
  supplies the application's `process(connection, config, metadata)` interface.
- [Slice-GRAPPA reference implementation documentation](https://pygrappa.readthedocs.io/en/latest/slicegrappa.html)
  describes training from individual-slice measurements and simulated SMS sources.

These public interfaces do not establish the available ICE tap points on your
scanner. Exact placement, reference delivery across sessions, and parallel
native reconstruction remain scanner integration work. The first capture is
intended to make those questions answerable.
