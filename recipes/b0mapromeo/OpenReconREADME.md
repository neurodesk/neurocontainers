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
