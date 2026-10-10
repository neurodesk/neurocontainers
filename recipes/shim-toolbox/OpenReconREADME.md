# Shimming Toolbox target B0

This OpenRecon image application reads a field map saved by `b0mapromeo`,
resamples it into a target magnitude scan's voxel geometry, and returns B0
images to the scanner database. With complete shim configuration, it also returns
a predicted shimmed B0 series and absolute settings in both image comment fields.
The scanner receives images through the existing ISMRMRD image injector.

## Run the two scans

1. Mount the same persistent host share at `/tmp/share` in both containers.
2. Run `b0mapromeo` on the multi-echo GRE magnitude and phase images.
3. Read `B0MapId` from the returned B0 images or their image comments.
4. Set **Shared B0 map ID** to that exact value in `shim_toolbox`.
5. If the source shim settings were not saved, enter the actual field-map
   acquisition baseline, verified limits, and explicit HFS magnet isocentre.
6. Run `shim_toolbox` on one reconstructed magnitude target volume.

The source bundle is `/tmp/share/b0maps/<ID>/`. There is no latest-map fallback.
Map IDs select immutable acquisitions. Both applications can use `B0_MAP_STORE`
for an alternative shared store. Identical paths inside different unmounted
containers do not provide shared storage.

## Target images and geometry

Send one single-channel magnitude series with one contrast and no repetitions
or dynamic sets, cardiac phases, or separate averages. Single 2D slices, regularly spaced 2D stacks, and packed 3D
volumes are supported. The application sorts slices by physical position.
Mixed series, duplicate slices, irregular gaps, and inconsistent dimensions,
orientations, or fields of view fail the request.

`vox2ras` maps zero-based XYZ voxel centres to patient RAS millimetres.
The target grid comes from its MRD pixel directions, positions, dimensions,
and fields of view. Paired `ImageRowDir` and `ImageColumnDir` metadata take
precedence over acquisition header directions. Packed volumes use their physical
centre. A single plane retains its measured centre and uses its explicit
through-plane field of view. The image centre never supplies magnet isocentre.

The map uses linear interpolation at target positions through
`inverse(source_vox2ras) * target_vox2ras`. A sample is valid only when every
contributing source sample belongs to the saved validity mask and field of view.
The fit ROI is this valid coverage intersected with positive target magnitude.
Masked-out source zeros and samples outside the source field of view never
participate in the optimization. Partial coverage is reported in comments and
metadata. No supported target ROI fails the request.

Source and target must belong to the intended patient and unchanged physical
coordinate frame. Known identity mismatches fail. When identifiers are absent,
the operator's map selection establishes the intended pairing. The application
does not estimate motion, align anatomy, or correct table-coordinate changes.

## Shim configuration

The target container uses Shimming Toolbox's SLSQP optimizer. It has no MATLAB
backend selector. Native analytical estimates target a Siemens MAGNETOM Cima.X
3T with confirmed head-first supine positioning. No scanner calibration, native
limits, baseline values, or magnet isocentre are supplied by default.

Saved source acquisition settings remain authoritative. Leave all shim controls
blank to use complete saved inputs. If the map has no shim inputs, the GUI can
supply them later. These values describe the selected field-map acquisition,
including when they arrive as custom MRD user parameters. The application never
infers them from the target scan's hardware metadata. Explicit values that
conflict with a known source baseline fail the request.

The direct analytical GUI controls are:

| Parameter | Value |
| --- | --- |
| `shimnativebaseline` | Actual native baseline of the selected field map |
| `shimnativelower` | Verified absolute native lower limits |
| `shimnativeupper` | Verified absolute native upper limits |
| `shimisocentrerasmm` | Magnet isocentre R,A,S in the field map's patient coordinate frame, in mm |

Supply all four controls together. The native vectors each contain three values
in order `X,Y,Z`, or eight in order `X,Y,Z,Z2,ZX,ZY,X2-Y2,XY`. Their lengths must
match. `X,Y,Z` use `uT/m`. Second-order channels use `uT/m^2`. Separate numbers
with commas or spaces. The isocentre always has three explicit values.
Blank fields do not mean zero. Limits and baseline must be finite, and the
baseline must lie within the limits. Equal lower and upper limits fix a channel.

`shimanalyticalmodel` and `shimnativesettings` accept the existing analytical
model JSON and named native baseline instead of the four direct fields. Do not
combine the two input forms. Analytical profiles are evaluated at target patient
coordinates using the explicitly supplied source-to-shim transform. A free
constant frequency offset is allowed by the spatial-variance objective.

Physical currents in A require `shimcalibration` and `shimcurrenta` together.
Calibration contains signed `Hz/A` coil profiles, unique channel names, verified
absolute current limits, and an optional total absolute-current budget. Profiles
must already match the target shape and RAS affine exactly. The application does
not resample or infer the registration of measured coil profiles. Saved measured
profiles support matching target geometry. A different target grid requires
explicit compatible calibration; unsupported saved calibration geometry returns
the target B0 map with shim settings unavailable.

The calibration and analytical model schemas are documented in
[the B0 producer reference](https://github.com/neurodesk/neurocontainers/blob/main/recipes/b0mapromeo/OpenReconREADME.md).
The consumer has eleven GUI controls, including map selection and the existing
shim input forms. JSON configuration parameters override the same custom MRD
parameters. `sendoriginal` defaults to false.

## Returned images

The first derived series is the saved B0 field in the target scan's geometry.
The second is the predicted field after applying the computed setting change.
Both use the target slice anchors and distinct derived series and image
identities. The predicted field is
`resampled_field + target_profiles * (absolute_settings - field_map_baseline)`.
Its constant frequency offset is preserved.

The optimizer returns its own prediction. The application does not re-create
the polarity convention in its scanner adapter. Analytical predictions are
labelled ideal field estimates. These images predict a result; they are not a
new B0 measurement after changing the scanner settings. No settings are applied
to hardware.

Valid samples use unsigned stored pixels in `1..4095`, with a reversible Hz
`RescaleSlope` and `RescaleIntercept`. Unsupported samples use stored zero and
`PixelPaddingValue=0`. A valid numerical zero Hz uses the offset near 2048.
Do not interpret padding as measured zero Hz. The initial window is centred
on zero with width 400 Hz. Half a stored-pixel step is the quantization error
bound; larger field ranges require a coarser step.

Both `ImageComment` and `ImageComments` identify the selected map, valid ROI
coverage, and shim outcome. Available results include named absolute settings,
units, model identity, and predicted ROI standard deviation. Native analytical
settings remain native field units, not amperes.

With no shim configuration, the application returns only the target B0 series
with explicit unavailable comments. An unidentifiable ROI can also leave shims
unavailable, particularly for free channels on a single plane. It never emits
a duplicate field under a predicted-series name. Missing or corrupt maps,
invalid geometry, partial configuration, baseline conflicts, and unexpected
optimizer errors send a controlled error and close the connection.

## Runtime and verification

Run the ISMRMRD server with configuration `shim_toolbox`. The `shim_toolbox`
launcher reports the packaged version. Reconstruction runs through the MRD
server. Runtime libraries and code live under `/opt`; no home
directory or runtime package download is required.

Release tests use synthetic source and target scans. They exercise shared map
publication, target vox2ras, interpolation support, real Toolbox optimization,
prediction polarity, Hz pixel decoding, and both image comment keys. Actual
scanner database acceptance and padding/rescale display remain deployment
checks on the target scanner.
