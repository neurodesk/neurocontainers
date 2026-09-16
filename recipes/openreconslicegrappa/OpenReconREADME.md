# Slice-GRAPPA capture and reconstruction POC

`openreconslicegrappa` captures the complete stream and can train a conventional
slice-GRAPPA kernel from individual single-band references, then apply it to SMS
frames. It returns RSS images for the separated physical slices in a second
series. Reconstruction also saves `separated-*.npz` files containing complex
coil k-space in `[slice, coil, PE, RO]` order, physical slice IDs and positions,
and `kernel-*.npz` files containing fitted weights and training parameters.
These files support inspection; they are not an implemented kernel exchange
protocol or raw-data reinjection path. This is an image-output experiment, not an in-place replacement ICE functor.

One stateful application is sufficient if reference and SMS data reach the same
session. This POC waits until session end, so reference arrival order does not
matter. Kernels are reused for matching frames within that session. No kernel
state persists into a different patient, measurement, or connection.

Two ICE export points do not necessarily require two reconstruction applications.
Prefer routing both exports to one service/session if FIRE supports it. If the
scanner delivers adjustment data in a separate connection, this implementation
will capture it but will not join it to another session automatically. We first
need the captured identifiers and lifecycle to design that adapter.

If separate train/apply modules are required, use an explicit exchange protocol:
match measurement and calibration epoch, encoding, physical slice packet, coil
order/compression basis, sample grid, CAIPI convention, EPI correction state,
and geometry. Publish a complete versioned kernel atomically, acknowledge readiness,
then permit apply. Reject missing/stale/mismatched kernels, bound any waiting,
and discard state at measurement end or recalibration. A process-global array
or an unqualified shared `kernel.npy` cannot safely implement this. That transport
is deliberately not implemented before the scanner contract is known.

## Training and mapping

Single-band reference readouts must have `ACQ_IS_PARALLEL_CALIBRATION` and use
`idx.slice` for their physical slice IDs. Ordinary imaging readouts use `idx.slice`
for the SMS packet ID. This is an explicit adapter contract, not an assertion
about Siemens' native counter mapping. Combined calibration-and-imaging flags
are rejected for training because they do not prove the data are single-band.
Do not train from an already collapsed ACS region without per-slice targets.

The JSON-only `smsmap` parameter maps each packet to physical single-band slice
IDs and CAIPI shifts as fractions of the PE FOV. For example, the supplied
`replay.example.json` describes packet 0 as slices 0 and 1 with shifts 0 and 0.5.
It is illustrative and must be replaced with the captured scan's mapping.

The implemented convention multiplies each unshifted reference by
`exp(-2*pi*i*(ky-Ny//2)*shift)`, sums those phase-matched references into simulated
SMS sources, and fits a complex ridge least-squares kernel to per-slice targets.
It applies that kernel to SMS neighborhoods, removes the same per-slice phase
ramps, transforms, and combines coils by RSS. This assumes references do not
already contain the specified CAIPI modulation. Other CAIPI patterns and EPI
polarity-dependent models need an adapter after capture inspection.

`kernelsize` defaults to 3, giving a 3x3 kernel. `ridge` defaults to 0.001 and is
scaled by the mean diagonal of the source Gram matrix. Training uses only valid
neighborhoods in the contiguous ACS region. Application zero-pads neighborhoods
at the acquired grid boundary; inspect boundary artifacts in real data. This is
ordinary slice-GRAPPA, not split-slice/leak-block or dual-polarity slice-GRAPPA.

The initial reconstruction supports `kx-ky` and full in-plane sampling only.
It rejects undersampled SMS frames rather than pretending that slice separation
also fills missing PE lines. `x-ky`, in-plane acceleration, multiple reference
epochs, and separate calibration sessions are capture targets for the next
iteration. Calibration selection matches measurement, encoding, echo, phase,
and set; multiple reference frames for a physical slice are rejected. Coil
compression must remain disabled or use an externally verified identical basis.
Physical output geometry comes from the individual reference slice; dynamic
counters and timestamps come from the SMS frame.

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

Build with `sf-build openreconslicegrappa`, then launch the standard server inside that image
or its OpenRecon packaging environment:

```sh
openreconslicegrappa -v -l /tmp/share/fire-poc/server.log
```

Select `config=openreconslicegrappa` and leave the other UI defaults unchanged. Mount a
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
  --app openreconslicegrappa --config /path/to/verified-config.json \
  --output /path/to/new-output --dicom
```

The same command runs from the repository with
`shared/fire_poc/fire_poc.py` and Python dependencies from the recipe. Output
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
python shared/fire_poc/tests/smoke_fire_poc.py
```

The same synthetic capture, numerical reconstruction, and DICOM tests run from
`fulltest.yaml` inside each image. They prove MRD-side behavior, not scanner
compatibility. The shared implementation is `shared/fire_poc/fire_poc.py`.
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
