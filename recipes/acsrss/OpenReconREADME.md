# ACS Fourier/RSS reconstruction

`acsrss` reconstructs ACS acquisitions in memory and sends magnitude
ISMRMRD images to the scanner's image injector for DICOM conversion. It writes no
raw capture, HDF5 output, or local DICOM files. Incoming original images are
forwarded by default. Keep the standard ICE branch enabled to produce the normal
image series when FIRE receives only raw acquisitions.

## Scanner settings

Select `config=acsrss`. The default `inputdomain=kx-ky` applies centered,
orthonormal inverse transforms along readout and phase encoding, followed by
root-sum-of-squares coil combination. Select `x-ky` only if ICE has already applied
the readout transform; that setting applies the PE inverse transform only.

The input must be Cartesian ACS after regridding and phase correction. The
application does not perform either operation. Calibration flags identify the
role of the data but cannot establish which ICE processing has occurred.

Parameterless adjustment connections use the same application and default RO+PE
transforms. The scanner logs showed that adjustment connections do not carry the
UI parameters; selecting PE-only in the UI therefore does not change those
connections. The packaged server default handles these connections without
requiring the launcher to supply an application name.

The label requests raw input, image output, adjustment data, and no channel
compression. The actual ICE tap and the scanner's handling of images returned
from adjustment connections still require scanner verification.

## Image output

Both MRD parallel-calibration flags are accepted. Noise, phase-correction,
navigation, dummy, and surface-coil-correction acquisitions are excluded. Data
are grouped by measurement, encoding, slice, repetition, echo, phase, set, average,
and segment. ACS samples remain at their declared positions on the encoded grid;
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

Only 2D Cartesian data are supported. Multi-shot EPI is handled: `idx.segment` marks
the shot that carried a line, not a separate frame, so a slice's segments are
reconstructed as one k-space. ACS then needs at least two contiguous PE lines,
consistent geometry and channels, and a readout width, after discard samples,
that is a constant integer multiple of the encoded matrix. That multiple is the
vendor readout oversampling and is removed by cropping in image space, so
derived images always land on the encoded grid and match the header field of
view. Reversed readouts, duplicate PE lines, trajectories, and
asymmetric k-space readout centers are rejected. There is no partial-Fourier
completion, in-plane GRAPPA, or slice-GRAPPA.
Start with a short phantom scan because ACS is buffered until the connection ends.

`fulltest.yaml` runs numerical tests and a real TCP test of the packaged server
with both parameterless adjustment and explicit application sessions. The TCP
test checks RSS pixels and image series, and asserts that no capture directory is
created. The implementation is this recipe's own `fire_poc.py`; this
application calls `process_acs`. The slice-GRAPPA recipe carries its own copy,
so the two containers release independently.
