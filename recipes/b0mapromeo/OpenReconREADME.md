# ROMEO B0 mapping

This OpenRecon image-to-image application computes a B0 field map in Hz from
reconstructed multi-echo GRE magnitude and phase images. It adapts the mask
preparation and ROMEO invocation in
[Theodore Brierre's starting pipeline](https://github.com/tbrierre/recon_me_b0map_romeo).
The upstream Julia Project and Manifest are pinned to a full source commit.
MATLAB scripts and FreeSurfer utilities are not installed. The Python adapter
uses the same threshold and mask cleanup. The upstream polynomial bias correction
does not change the mask it returns, and the upstream ROMEO invocation uses the
original magnitudes, so the adapter omits that unused corrected-magnitude output.

## Inputs and parameters

Send exactly one magnitude series and one corresponding phase series in one
connection. Echoes use MRD `contrast`; slices use their physical positions.
Single-channel 2D images and packed 3D volumes are supported. The adapter requires
at least two matching echoes and five regularly spaced slices. Mismatched echo
counts, duplicate/gapped slice positions, inconsistent geometry, repeated scans,
and multiple series pairs fail rather than produce a partial map. Separate
distortion-corrected and uncorrected series into separate jobs.

Echo times are in milliseconds. `echotimesms` can override the MRD
`sequenceParameters.TE`, for example `2.46,4.92,7.38`. Missing or incomplete echo
times are errors. Scanner label parameters are read from MRD user parameters;
JSON config parameters take precedence.

`phaseunits` defaults to `siemens`, unsigned 12-bit pixels in 0..4095 mapped by
`(2 * pixel - 4096) * pi / 4096`. Choose `signed` for already rescaled Siemens
counts in -4096..4096, or `radians` for wrapped radians in -pi..pi. MRD pixels
must already use the selected scale. The DICOM command handles the Siemens
enhanced-MR rescale slope 2/intercept -4096 explicitly. It also accepts classic
MR DICOM and sorts frames by echo time and physical slice position. There is no
observed-min/max phase scaling. ROMEO's additional phase rescaling is disabled.

Masking uses the first echo's magnitude, a threshold of 0.25 times the mean,
in-plane cross erosion, a radius-two-voxel spherical opening, the largest
26-connected component, and hole filling. This is a foreground mask, not an
anatomical brain extraction. `maxseeds` defaults to the upstream setting of 4000.

## Outputs

One derived B0 series keeps the input slice geometry. Scanner pixels are unsigned
integers centered on 2048. DICOM/MRD `RescaleSlope` and `RescaleIntercept`
recover Hz, with an adaptive scale to avoid clipping. `B0MapDisplayFormula`
records that conversion. Zero Hz outside the mask is stored as 2048. The initial
window is centered on zero with width 400 Hz. Quantization is at most 0.5 Hz
for maps whose absolute values fit below 2046 Hz; wider maps have coarser
quantization. A scanner display that ignores rescale metadata cannot be read as
Hz. Scanner round-trip behavior still needs verification on the target system.

`sendoriginal` defaults to false. Enable it to return original image copies before
the B0 series. The shared OpenRecon helpers assign fresh returned-series identity
and restamp storage metadata. Derived images also use the shared metadata helpers.
Acquisition, unwrapping, and fitting errors send an error message and close the
connection without a derived output.

## Local commands

```bash
b0mapromeo --dicom-dir /input/gre_pair --output-dir /output/b0
python3 /opt/code/python-ismrmrd-server/main.py -v -r -H=0.0.0.0 -p=9002
python3 /opt/code/python-ismrmrd-server/client.py -G dataset -o /output/result.h5 /input/images.h5 -c b0mapromeo
```

The DICOM command writes `b0_hz.nii`, `mask.nii`, `magnitude.nii`, `phase.nii`,
and ROMEO's unwrapped phase and diagnostic files. An existing nonempty output
directory is rejected. Patient DICOM tags and source filenames are not copied
into generated NIfTI headers. Image data and image geometry remain sensitive.
OpenRecon uses a private temporary directory per connection and removes it after
processing. Julia packages live under `/opt`; runtime needs no home directory or
package download.

Release smoke tests use generated synthetic data only. Private validation data
and its derived images must stay outside the repository and build context.

ROMEO citation: Dymerska et al., *Phase Unwrapping with a Rapid Opensource Minimum
Spanning TreE AlgOrithm (ROMEO)*, Magnetic Resonance in Medicine,
https://doi.org/10.1002/mrm.28563.

## Calibrated Cima.X shim settings

Shimming Toolbox 1.5 computes whole-volume static ABSOLUTE currents in A for a
Siemens MAGNETOM Cima.X 3T. No scanner calibration is bundled. Supply both
`shimcalibration` (a JSON file path) and `shimcurrenta` (a JSON current array or
object keyed by channel name) through MRD configuration/user parameters.
The baseline must be the actual currents used for this acquisition. It is never
inferred from DICOM or assumed to be zero. The CLI accepts the same inputs with
`--shim-calibration` and `--shim-current-a`.

The calibration JSON has these required fields:

| Field | Value |
| --- | --- |
| `scanner_model` | `"MAGNETOM Cima.X"` |
| `field_strength_t` | `3` |
| `profile_units` | `"Hz/A"` |
| `current_units` | `"A"` |
| `settings_mode` | `"absolute"` |
| `calibration_id` | Nonempty identity of the measured calibration |
| `channels` | Unique ordered channel names |
| `coil_profiles` | Path relative to this JSON, or absolute path, to a 4D NIfTI |
| `absolute_current_bounds_a` | One `[minimum, maximum]` pair per channel |
| `total_absolute_current_limit_a` | Optional nonnegative sum-of-absolute-currents limit |

A complete format example for a **synthetic two-channel test only** is below.
These profiles and limits do not describe a Cima.X scanner.

```json
{
  "scanner_model": "MAGNETOM Cima.X",
  "field_strength_t": 3,
  "profile_units": "Hz/A",
  "current_units": "A",
  "settings_mode": "absolute",
  "calibration_id": "synthetic-only",
  "channels": ["X", "Y"],
  "coil_profiles": "synthetic_profiles.nii",
  "absolute_current_bounds_a": [[-1, 1], [-1, 1]],
  "total_absolute_current_limit_a": 1
}
```

For example, an acquisition current argument `'{"X":0.1,"Y":-0.2}'` names two
channels. These are illustrative acquisition values, not Cima.X defaults or a
calibration. Obtain actual profiles, channel limits and acquisition currents from
the scanner's validated calibration. Profiles must already be registered to the
fieldmap's exact RAS voxel shape and affine. Each positive current increment must
add its signed profile in Hz/A. Unsupported units, models, dependent profiles,
missing baselines or invalid limits fail the job before shim settings are returned.

The fit minimizes masked spatial variance, allowing a free constant frequency
offset. It subtracts each profile's masked mean and the field's masked mean.
The optimization variable is absolute current. The measured field minus the
profile contribution of the acquisition currents is fitted with Toolbox's
constrained least-squares SLSQP optimizer. Predictions use the measured field plus the
profile contribution of the change in current. This follows the empirical-coil
constrained least-squares approach in Jason Stockmann's `perform_shim_quad.m`,
with an explicit signed profile convention, absolute-current baseline and free
frequency objective. The attachment's hardcoded regularization is not used.
The Toolbox 1.5 quadratic solver reverses asymmetric box constraints, so this
application uses SLSQP with the same squared-error objective and explicit current
constraints. Each returned solution is checked against the absolute limits.

Each returned slice has identical `ImageComment` and `ImageComments` containing
labeled ABSOLUTE A settings, calibration identity, and measured/predicted ROI
standard deviation in Hz. `B0ShimStatus` is `available`. Settings are recommendations
for scanner review; the application does not apply them to hardware. With neither
input configured, B0 reconstruction remains available and comments explicitly
state why shim settings are unavailable. Partial configuration fails the job.
The CLI writes `shim_settings.json` for either status and prints the same summary.

The container uses Ubuntu 24.04 to meet Toolbox's Python 3.11 minimum. Its source
is pinned to the 1.5 release commit. Packaging deliberately removes upstream's
`requirements_st-pinned.txt` before installation and uses its declared dependency
ranges, retaining the adapter's pinned NumPy, SciPy, nibabel and OpenRecon's
pydicom 3.0.1. This avoids replacing the validated reconstruction dependencies
with the upstream development lock. The build runs `pip check`, and release tests
exercise both real ROMEO reconstruction and the installed Toolbox optimizer.
