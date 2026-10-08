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
Acquisition, unwrapping, publication, and fitting errors send an error message and close the
connection without a derived output.

## Shared map for a target scan

Every successful server reconstruction publishes a persistent map bundle below
`/tmp/share/b0maps/<ID>/` before returning its B0 images. The bundle contains the
field in Hz, its validity mask, the patient-RAS voxel geometry in millimetres,
and a manifest identifying the source acquisition and any supplied shim inputs.
Known analytical models and measured calibration assets are saved with the map.
The map remains readable after the reconstruction's temporary files are removed.

`b0mapid` is the fourteenth GUI parameter. Leave it blank to generate an ID, or
enter an unused name of 1 to 64 ASCII characters, starting with a letter or digit
and containing only letters, digits, underscores, or hyphens.
Use an opaque scan name rather than patient details. Existing maps cannot be
overwritten. Each returned slice records `B0MapId` and the ID in both
`ImageComment` and `ImageComments`.

Run `shim_toolbox` on the target magnitude scan and enter that exact ID in its
**Shared B0 map ID** control. Both containers require the same host directory
mounted at `/tmp/share`. Creating the same directory inside separate containers
does not share the files. FIRE's share mount must remain persistent between the
two reconstructions. Runtime deployment can set `B0_MAP_STORE` to the same
alternative writable map-store path in both containers.

The target application resamples the saved map into the target scan's voxel
geometry, returns it to the scanner, and returns a second predicted B0 series
when valid shim inputs are available. The saved baseline always describes the
field-map acquisition. If those values were unknown when this map was acquired,
enter the actual field-map baseline, verified limits, and HFS magnet isocentre
in the target application's GUI later. A known saved baseline cannot be replaced
with the target scan's settings. Blank shim inputs still permit map export and
target-space display.

Maps publish as complete immutable bundles. Readers check their geometry,
units, and payload integrity. There is no latest-map fallback or automatic
deletion. Select the correct source acquisition and retain only the maps needed
for the study. Image values and geometry remain sensitive. Known source/target
identity mismatches are rejected. Missing identity metadata leaves pairing to
the operator's explicit map selection. Resampling does not register patient
motion or correct a changed physical coordinate frame.

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
OpenRecon removes its private reconstruction scratch directory after processing;
the shared map bundle remains. Julia packages live under `/opt`; runtime needs no home directory or
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

## Analytical native Cima.X estimates

When measured coil profiles are unavailable, the analytical mode fits ideal
Siemens first-order or first- and second-order fields. `X`, `Y`, and `Z` use
`uT/m`. `Z2`, `ZX`, `ZY`, `X2-Y2`, and `XY` use `uT/m^2`.
The result is an ideal field estimate, not a scanner calibration. No current
conversion or Cima.X hardware limits are bundled. The application does not apply
settings to the scanner.

### Direct HFS GUI inputs

For a confirmed head-first supine acquisition, enter the acquisition baseline,
absolute limits and explicit magnet isocentre in the OpenRecon GUI. These four
controls also accept MRD string user parameters. No analytical JSON file is
required. The existing calibration and model-file controls remain available.

| Parameter | Entry |
| --- | --- |
| `shimnativebaseline` | Actual native settings used to acquire the images |
| `shimnativelower` | Verified absolute lower native limits |
| `shimnativeupper` | Verified absolute upper native limits |
| `shimisocentrerasmm` | Explicit patient R,A,S coordinates of magnet isocentre, in mm |

Use three numbers in each native vector for `X,Y,Z`, or eight for
`X,Y,Z,Z2,ZX,ZY,X2-Y2,XY`. All three vectors must have the same count.
`X,Y,Z` use `uT/m`; the remaining channels use `uT/m^2`.
Separate numbers with commas or spaces. Scientific notation and precision
smaller than 0.1 are accepted. The isocentre always requires three numbers.

All four controls default to blank. Blank configured values permit MRD header
fallback; nonblank configuration values override the same header parameter.
Any direct input requires all four controls. An explicit `0,0,0` is supplied
data, not an empty value. Equal lower and upper limits fix a channel.
Baselines must lie within the supplied absolute limits. Missing values, unequal
counts, nonfinite numbers and empty comma components fail the job.
Direct inputs cannot be combined with measured calibration/current inputs or
analytical model-file/native-JSON inputs, including values from the MRD header.

These values are a complete **synthetic-only GUI example**, equivalent to the
HFS model-file example below. They are invented test values, not scanner limits
or acquisition settings to use on a real Cima.X.

```text
shimnativebaseline: .3,-.2,.1,2,-3,4,-5,6
shimnativelower: -1000,-1000,-1000,-1000,-1000,-1000,-1000,-1000
shimnativeupper: 1000,1000,1000,1000,1000,1000,1000,1000
shimisocentrerasmm: 5,-7,11
```

The direct HFS transform uses `R=diag(-1,1,-1)` and translation
`-R*p_iso`, so the entered patient-RAS isocentre maps to shim LAI zero.
The image centre and DICOM position never substitute for magnet isocentre.
The `entered-hfs-` configuration identity derives from the entered geometry,
orders and limits; it identifies supplied model data, not a hardware calibration.
The output records the actual transform, limits and acquisition baseline.
Direct and model-file inputs share model validation and the same optimizer.

### Explicit model-file inputs

The CLI requires both `--shim-analytical-model PATH` and
`--shim-native-settings JSON`. MRD uses `shimanalyticalmodel` and
`shimnativesettings`. Native settings must be a JSON object naming exactly the
selected channels. Supply the actual baseline for each acquisition separately.
The model file cannot contain a baseline. Measured and analytical inputs cannot
be combined. Empty UI strings permit MRD header values to supply these inputs.
All inputs empty retain the existing unavailable result. A partial mode fails.

The model requires exactly these keys. `configuration_id` is a nonempty identity
for supplied geometry and bounds. `scanner_model` is `MAGNETOM Cima.X`,
`field_strength_t` is `3`, and `orders` is either `[1]` or `[1,2]`.
`absolute_native_bounds` names exactly the selected channels, each with finite
ordered `[lower, upper]` values in its native unit. Equal bounds fix a channel.
There is no sum limit across channels with different units.

`patient_ras_mm_to_shim_lai_mm` is an explicit 4 by 4 homogeneous rigid transform
from the fieldmap's patient RAS coordinates in mm to actual Siemens shim LAI
coordinates in mm relative to magnet isocentre. Rotation must be orthogonal with
determinant +1, within `1e-6`. The last row must equal `[0,0,0,1]` within `1e-8`.
The transform must match the acquisition and its producer's coordinate origin.
DICOM ImagePositionPatient does not establish magnet isocentre. Conforming MRD
ImageHeader.position is the volume centre relative to isocentre, per the
[MRD image header specification](https://ismrmrd.readthedocs.io/en/stable/mrd_image_data.html#imageheader).
The pinned server's
[DICOM converter](https://github.com/astewartau/python-ismrmrd-server/blob/33362f2139701fbf5ea855808a325eb59b7db2ae/dicom2mrd.py#L167)
copies a corner ImagePositionPatient into MRD position instead, so validate the
producer's centre and origin conventions before using its geometry.
An explicit transform is mandatory for model-file inputs. Direct GUI inputs
construct the HFS transform from the required explicit isocentre.

Head-first alone does not establish supine orientation. Only after confirming
head-first supine and the corresponding patient axes can the rotation be
`R=diag(-1,1,-1)`. For a verified patient-RAS isocentre point `p_iso`, the
homogeneous transform is `T=[R,-R*p_iso;0,1]`. Other positions require their
verified rotation. Unknown isocentre is an error in configuration preparation.
Never substitute the image centre or infer table-position semantics.

The following model and command are a complete **synthetic-only example**.
It uses head-first supine rotation with an invented patient-RAS isocentre
`[5,-7,11]` mm. The isocentre, bounds, and baseline are invented for a test
acquisition. They do not describe a real scanner or patient.

```json
{
  "configuration_id": "synthetic-geometry-only",
  "scanner_model": "MAGNETOM Cima.X",
  "field_strength_t": 3,
  "orders": [1, 2],
  "patient_ras_mm_to_shim_lai_mm": [
    [-1, 0, 0, 5], [0, 1, 0, 7], [0, 0, -1, 11], [0, 0, 0, 1]
  ],
  "absolute_native_bounds": {
    "X": [-1000, 1000], "Y": [-1000, 1000], "Z": [-1000, 1000],
    "Z2": [-1000, 1000], "ZX": [-1000, 1000], "ZY": [-1000, 1000],
    "X2-Y2": [-1000, 1000], "XY": [-1000, 1000]
  }
}
```

```bash
b0mapromeo --dicom-dir /input/synthetic_gre --output-dir /output/synthetic_b0 \
  --shim-analytical-model /input/synthetic_model.json \
  --shim-native-settings '{"X":0.3,"Y":-0.2,"Z":0.1,"Z2":2,"ZX":-3,"ZY":4,"X2-Y2":-5,"XY":6}'
```

For actual acquisition values and limits, use the scanner's native shim panel
and validated scanner information. Toolbox's
[scanner constraint documentation](https://shimming-toolbox.org/en/latest/miscellaneous/constraint_file.html)
documents these commands on the scanner terminal:

```text
AdjValidate -shim -info -mp
AdjValidate -shim -info
```

The documented paired outputs establish scanner-specific conversion factors for
DICOM DAC values. DAC metadata must not be supplied directly as native units.
This adapter does not read or convert that metadata automatically.

The basis follows the pinned Toolbox 1.5 executable order
`X,Y,Z,Z2,ZX,ZY,X2-Y2,XY`. At shim LAI coordinates `(x,y,z)` in mm, the fields
are `gamma*1e-9*(x,y,z)` and
`gamma*1e-12*(z^2-(x^2+y^2)/2,2*z*x,2*z*y,x^2-y^2,2*x*y)`, with
`gamma=42577478.517832555 Hz/T`. Native column normalization conditions the
real SLSQP optimizer. A clipped least-squares start protects small second-order
sensitivities from its stopping tolerance. The fit allows a constant frequency
offset and uses absolute bounds. Fixed channels are removed before the rank
check; all-fixed configurations need no optimizer. Free channels without
independent spatial effects in the ROI fail, including undersized ROIs.
Solutions must remain finite, feasible, and no worse than the baseline's ROI
standard deviation within `1e-6*max(1,baseline_std_hz)`.

Predictions use the measured field plus ideal profiles times
`absolute-baseline`. Both image comment keys contain identical labeled
`ABSOLUTE ANALYTICAL ESTIMATE` settings with native units.
`B0ShimStatus` remains `available`. The CLI JSON records
`model_kind=siemens_analytical`, `validation=ideal_field_estimate`,
`basis_model_id=siemens-ideal-1st2nd-v1`, the actual transform, orders, bounds,
and per-channel name, unit, baseline, and absolute value. It contains no amp or
calibration fields. Existing measured-mode attributes and JSON remain unchanged.
Release tests verify ideal polynomial geometry and synthetic reconstruction;
real scanner field deviations and scanner round-trip behavior remain unvalidated.

For rotated MRD pixels, paired `ImageRowDir` and `ImageColumnDir` metadata
identify the actual pixel axes. The adapter uses these before acquisition header
directions. `ImageSliceDir` may supply the normal; otherwise the cross product
of row and column gives it. All axes must be finite, orthonormal,
and consistent across echoes. A flipped voxel grid may be left-handed. Derived orientation metadata preserves those pixel
axes while the acquisition header directions remain unchanged.
