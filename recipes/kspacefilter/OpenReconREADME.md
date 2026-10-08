# kspacefilter 0.1.0 experimental raw-return package

Research only. This package returns ISMRMRD acquisitions, message 1008, rather
than reconstructed images. Stock OpenRecon schema 1.1.0 and its image injector
do not support this raw-return contract. The package is built with an explicit
local experimental validation extension. Successful packaging does not prove
scanner compatibility. No native scanner SDK adapter is supplied.

## Build

Use the OpenRecon packager with the experimental extension, from its kspacefilter
recipe directory and an active Python virtual environment:

```sh
BUILD_PACKAGE_SELECTION=openrecon \
  /bin/bash ../build.sh --experimental-raw-return
```

Pin the exact source image in params.sh. The ordinary build rejects the honest
raw/raw label. The shared stock schema remains unchanged. Memory and CPU fields
are packaging requirements, not measured scanner qualification or performance
claims; qualify resources on the target system before use.

## Hello-world acquisition exchange

A custom ICE adapter must emit acquisitions over MRD TCP on port 9002 and receive
one acquisition per input, in order. For an initial unchanged sample exchange,
use this JSON configuration:

```json
{
  "parameters": {
    "config": "kspacefilter"
  }
}
```

Retain each original native acquisition header, replace only the returned sample
payload, and resume downstream reconstruction without recursive emission.
Send and receive concurrently to avoid backpressure deadlock. Early close or a
missing response fails the exchange; stop reinsertion. Text, images and waveforms
cannot replace native acquisition samples. This adapter is a required separate
implementation and has not been supplied or tested on a scanner.

## Retain the central readout

After the adapter verifies the tap precedes the readout FFT and holds full,
uniform, centered Cartesian raw k-space readouts, explicitly enable masking:

```json
{
  "parameters": {
    "config": "kspacefilter"
  },
  "kspacefilter": {
    "mode": "readout-edge-zero",
    "input_domain": "raw-uniform-cartesian-kspace",
    "measurement_role": "imaging"
  }
}
```

The existing service zeros floor(usable_samples/20) samples at each usable
readout end, for every coil. Rounding can retain slightly more than 90 percent.
It uses the actual readout width, including retained vendor oversampling.
The opt-in object belongs at the top level. Flat scanner UI parameters alone
cannot enable this mode. MRD metadata cannot establish the acquisition domain.
Adjustment and other unsupported acquisitions bypass filtering; integrated
reference datasets do not receive a uniform mask. Consult the recipe README
for the existing service's acquisition guards.

The recipe's wip_070_fire_kspacefilter.json contains this exact top-level opt-in.
For a FIRE bundle, select BUILD_PACKAGE_SELECTION=both; the packager copies this
JSON over generated defaults. The OpenRecon-only ZIP contains the Docker archive
and this PDF documentation; it does not export FIRE configuration files.
