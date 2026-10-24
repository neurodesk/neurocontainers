# Discover and run container descriptors

Recipes can install Boutiques JSON under `/boutique`. The `sf-boutiques`
command extracts descriptors from a selected local SIF, validates them with
the official Boutiques schema and semantic validator, and writes a versioned
index binding the SIF SHA-256 to the extracted descriptors.

## Extract from a SIF

```bash
python3 -m pip install -e '.[boutiques]'
sf-boutiques /data/qsmxt.sif --output ./qsmxt-descriptors \
  --expected-version 10.0.0
cat qsmxt-descriptors/index.json
```

For a known artifact, add `--expected-sha256 HEX_DIGEST`. `--expected-version`
checks the declared tool version; it does not independently run the software.
The command rejects a wrong expected image hash or tool version, invalid
descriptors, missing descriptors, and existing output files. Use a new output
directory for another image. Extracted CVMFS sandbox directories are not SIF
files; obtain the release SIF to use this hash-bound extraction command.

Choose an image you trust before executing it. Discovery runs the image's `tar`
command to read descriptors. It does not publish descriptors to a central service.

## Run QSMxT outside Neurodesk

The recipe supplies a version/help descriptor and a minimal BIDS processing
descriptor. Pass the same SIF explicitly to Boutiques:

```bash
bosh exec launch --skip-data-collection --imagepath /data/qsmxt.sif \
  qsmxt-descriptors/qsmxt.json '{"action":"--version"}'
bosh exec launch --skip-data-collection --imagepath /data/qsmxt.sif \
  qsmxt-descriptors/qsmxt.json '{"action":"--help"}'
```

For processing, create an output directory and pass input/output binds.
The processing descriptor covers the default pipeline; use the CLI directly
for additional reconstruction options.

```bash
mkdir -p /data/qsm-results
bosh exec launch --skip-data-collection --imagepath /data/qsmxt.sif \
  --volumes /data/bids:/data/bids --volumes /data/qsm-results:/data/qsm-results \
  qsmxt-descriptors/qsmxt-run.json \
  '{"bids_dir":"/data/bids","output_dir":"/data/qsm-results"}'
```

Use paths without shell metacharacters when passing binds through Boutiques.
Verify the SIF and descriptor hashes against `index.json` if files have moved
or changed since extraction. Selecting a different `--imagepath` does not
automatically enforce the index hash.

## Run through Neurodesk modules

With Boutiques installed on the host, load the matching module and use the
extracted descriptor with `--no-container`. The module's launcher selects
the installed NeuroContainer:

```bash
ml qsmxt/10.0.0
bosh exec launch --skip-data-collection --no-container \
  qsmxt-descriptors/qsmxt.json '{"action":"--version"}'
```

MRIcron's descriptor accepts an optional image file and needs a graphical
desktop. The undeclared `[OPTIONS]` placeholder has been removed. Descriptor
validation does not test a GUI viewing session.

## Verify a complete processing invocation

From a repository checkout, install the host-side verifier dependencies and run
both descriptors against a built SIF:

```bash
python3 -m pip install -e '.[boutiques]' nibabel scipy
python3 -m workflows.verify_qsmxt_boutiques --image /data/qsmxt.sif \
  --version 10.0.0 --output /data/qsmxt-descriptor-check
```

The output directory must be new. Choose a path without shell metacharacters or
colons because Boutiques passes it as a container bind. The verifier extracts
and validates the actual image descriptors, checks the installed version, then
uses the default processing descriptor on a generated three-echo BIDS phantom.
It checks that a finite, nonzero susceptibility image preserves the input voxel
grid. It retains the phantom, results, Boutiques logs and `evidence.json` binding
the image, descriptor and output hashes. This checks command execution and
output geometry; it does not establish reconstruction accuracy on patient data.

For an installed native QSMxT executable, use `--native-descriptors DIRECTORY`
instead of `--image`. The directory must contain validated `qsmxt.json` and
`qsmxt-run.json` descriptors for the requested version. This mode runs the same
phantom and records `mode: native`; it supplies no evidence about SIF extraction
or container bindings. A new SIF release still needs the image-mode check.

### Verified release

On 2026-10-24, the image-mode verifier passed against the published
`quay.io/neurodesk/qsmxt:10.0.0_20261021` release using Apptainer 1.5.3 and
Boutiques 0.5.33. The resolved SIF artifact manifest was
`sha256:a4f37d703ef049a1bfbae9733b31bc0d1a61b39405f70015fcd0e2e493f15c56`;
the downloaded SIF SHA-256 was
`f54c74a2e27abf8fbc2b9e0ba2bcdd75854667a3a29ae6b6fbd54efe321780a8`.
Both extracted descriptors passed official validation, the version descriptor
reported QSMxT 10.0.0, and the processing descriptor completed the default
pipeline. The resulting susceptibility image had 5,226 nonzero voxels, all
values were finite, and its 32 × 32 × 24 grid and affine matched the phantom.
This verifies the default processing invocation for that release. MRIcron GUI
acceptance and wider descriptor adoption remain open.

## Adoption status

- QSMxT: version/help and default BIDS command descriptors, matched to the recipe's installed version.
- MRIcron: existing viewer descriptor repaired and schema-validated.
- Other recipes: add descriptors with a tested invocation and explicit input/output mapping.
- Shared search/publishing, MRIcron GUI acceptance and further recipe adoption remain follow-up work under #217.

See the [Boutiques executor documentation](https://boutiques.github.io/doc/_execute.html)
for invocation, container selection and bind options.
