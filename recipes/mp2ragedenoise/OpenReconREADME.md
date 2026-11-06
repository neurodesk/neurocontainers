# MP2RAGE UNI denoising

OpenRecon config `mp2ragedenoise` consumes three complete, co-registered magnitude
series in a single connection. Set `uniseries`, `inv1series`, and `inv2series` to
the MRD image_series_index values (defaults 0, 1, 2). Arrival order is irrelevant.
Repeated positions, missing contrasts, differing geometry and nonfinite pixels
fail with a server error and no output. Raw acquisitions and packed 3D MRD images
are not supported. Output is one float32 UNI-DEN series in UNI source geometry.

`noisefactor` defaults to 6. The algorithm estimates background from the last
11 voxels on the second and third axes of INV2. Scanner volumes are converted to
RAS first; the CLI uses the NIfTI array orientation unchanged, as the webapp does.
Thus equivalence requires the same voxel orientation, not only the same anatomy.
No registration, T1 mapping or B1 correction is performed.

```sh
mp2ragedenoise --uni MP2RAGE_UNI.nii.gz --inv1 MP2RAGE_INV1.nii.gz \
  --inv2 MP2RAGE_INV2.nii.gz --output UNI-DEN.nii.gz
```

## Provenance

The vendored `robust_combination` function is unchanged from Neurodesk/webapps
commit `af1bc1ea1a86353dd80d340b9ce353d52a9bf52f`,
`apps/easy-mp2rage/mp2rage_t1/denoise.py` (Python package 0.1.0,
webapp release 0.8.20261104). Unused B1 extension functions were omitted.
GPL-3.0-or-later; see LICENSE. This is a locally maintained adapter; updates to
its vendored algorithm require explicit parity verification. Shared OpenRecon
runtime sources are tracked by the recipe's automatic update policy.

## Verification

`example.json` records the webapp's Marques 7 T example URLs and SHA-256 hashes.
Download UNI, INV1 and INV2 and verify those hashes. To reproduce the independent
reference, check out the webapps commit above, copy `verify_denoise.rs` into
`apps/easy-mp2rage/crates/mp2rage-cli/examples/`, then run:

```sh
cargo run --release --manifest-path apps/easy-mp2rage/Cargo.toml \
  -p mp2rage-cli --example verify_denoise -- /path/to/example/
```

Run `verify_example.py /path/to/example` inside the built container, with
`PYTHONPATH=/opt/code/python-ismrmrd-server`. This checks exact voxel parity
against the Rust core used by the webapp, reconstructs shuffled MRD slice
messages from the real images, checks the returned pixels and positions, and
verifies that missing INV2 produces no output. Scanner hardware acceptance is
separate from this numerical and MRD interface verification.

The initial verification on the published example compared 6,858,280 voxels
against the independent Rust core with zero differing voxels. The MRD test also
checks RAS volume assembly against the input image before denoising.
