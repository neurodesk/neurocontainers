# ACS Fourier/RSS reconstruction

`acsrss` reconstructs ACS acquisitions in memory and sends magnitude
ISMRMRD images to the scanner's image injector for DICOM conversion. It writes no
raw capture, HDF5 output, or local DICOM files. Incoming original images are
forwarded by default. Keep the standard ICE branch enabled to produce the normal
image series when FIRE receives only raw acquisitions.

## FIRE development server

From the repository root, run `recipes/acsrss/start-docker.sh` with the repository
Python environment activated. The script builds ACSRSS and starts both ACSRSS and
an existing local Quickgrid image. Use `--no-build` to reuse the ACSRSS image.
For host ports 9004 and 9005, run:

```bash
ACSRSS_PORT=9004 QUICKGRID_PORT=9005 recipes/acsrss/start-docker.sh --no-build
```

ACSRSS starts directly with this command inside the container:

```bash
python /opt/code/python-ismrmrd-server/main.py -H 0.0.0.0 -p 9002 -d acsrss
```

Docker maps the selected host port to internal port 9002.

## Scanner settings

Select `config=acsrss`. The default `inputdomain=x-ky` expects ICE to have
already transformed readout. FIRE applies a centered, orthonormal forward FFT
(negative exponent) along phase encoding only, followed by root-sum-of-squares coil
combination. Select `kx-ky` explicitly for input where neither axis has been
transformed; that setting applies both RO and PE inverse transforms.

The PE sign for `x-ky` is selected for the current ICE tap to address the observed
top–bottom reversal. Readout samples and image direction vectors are unchanged.
Confirm orientation against the native scanner images after changing the tap.

The input must be Cartesian ACS after regridding and phase correction. The
application does not perform either operation. Calibration flags identify the
role of the data but cannot establish which ICE processing has occurred.

Parameterless adjustment connections use the same application and default PE-only
transform. The scanner logs showed that adjustment connections do not carry the
UI parameters; selecting PE-only in the UI therefore does not change those
connections. The packaged server default handles these connections without
requiring the launcher to supply an application name.

The label requests raw input, image output, adjustment data, and no channel
compression. The actual ICE tap and the scanner's handling of images returned
from adjustment connections still require scanner verification.

## Image output

Live scanner output is normalized per image to 0–4095 and sent as signed 16-bit
integers, with window center 2048 and width 4096. These are display intensities;
absolute signal differences between images are not preserved. Input magnitude,
unscaled RSS range, nonzero pixel count, and output range are logged for each
image. Zero-signal images remain zero and produce a warning. Offline replay
retains floating-point RSS values.

Both MRD parallel-calibration flags are accepted. Noise, phase-correction,
navigation, dummy, and surface-coil-correction acquisitions are excluded. Data
are grouped by measurement, encoding, slice, repetition, echo, phase, set, average,
with segment coverage resolved as described below. ACS samples remain at their declared positions on the encoded grid;
missing outer PE lines are zero-filled. The result is a low-resolution ACS image
with the encoded field of view.

At the end of each connection, the application sends RSS images using
`connection.send_image`, then sends the MRD close message. The derived series
index defaults to 60000. Original images retain their series and headers. A
collision with the derived series is rejected. Geometry is copied from the ACS
headers and encoded-space metadata. No acquisitions are written to disk.

A connection without calibration-flagged acquisitions returns no derived images
and logs the count. Errors are reported through ISMRMRD logging. Inspect the
scanner log for `Returned ... ACS RSS images via ISMRMRD` and verify that the
corresponding DICOM series appears with the expected orientation and field of
view. Synthetic TCP tests verify the returned MRD images; they cannot verify the
scanner's DICOM conversion.

## Scope and tests

Only 2D Cartesian data are supported. Disjoint multi-shot EPI segments are
reconstructed as one k-space. When every segment instead contains the same PE
line set, with each line occurring once per segment, each segment returns its own
RSS image with a distinct image index. Complex samples are never averaged across
these segments. Partial overlaps and duplicates within a segment are rejected.
ACS then needs at least two contiguous PE lines,
consistent geometry and channels, and a readout width, after discard samples,
that is a constant integer multiple of the encoded matrix. That multiple is the
vendor readout oversampling and is removed by cropping in image space, so
derived images always land on the encoded grid and match the header field of
view. One FIRE header compatibility case is also supported: when the received
readout already matches the smaller reconstruction width, encoded and
reconstruction RO matrix/FOV ratios agree, the width ratio is an integer, and
the PE/slice geometry is unchanged, the reconstruction RO width and FOV replace
the stale encoded RO geometry. A center matching the old RO midpoint (or its
preceding sample) is treated as stale and normalized to the new midpoint.
This assumes a complete, centered readout after oversampling removal; it does
not support partial readouts. The correction is logged and never changes the
selected input domain: use `x-ky` if ICE has already applied the readout FFT.
Reversed readouts, duplicate PE lines, trajectories, and
asymmetric k-space readout centers are rejected. There is no partial-Fourier
completion, in-plane GRAPPA, or slice-GRAPPA.
Start with a short phantom scan because ACS is buffered until the connection ends.

`fulltest.yaml` runs numerical tests and a real TCP test of the packaged server
with both parameterless adjustment and explicit application sessions. The TCP
test checks RSS pixels and image series, and asserts that no capture directory is
created. The implementation is this recipe's own `fire_poc.py`; this
application calls `process_acs`. The slice-GRAPPA recipe carries its own copy,
so the two containers release independently.
