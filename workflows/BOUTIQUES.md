# Discover and run container descriptors

Recipes can install Boutiques JSON under `/boutique`. The `sf-boutiques`
command extracts descriptors from a selected local SIF, validates them with
the official Boutiques schema and semantic validator, and writes a versioned
index binding the SIF SHA-256 to the extracted descriptors.

## Extract from a SIF

```bash
python3 -m pip install -e '.[boutiques]'
sf-boutiques /data/qsmxt.sif --output ./qsmxt-descriptors \
  --expected-version 9.22.0
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
ml qsmxt/9.22.0
bosh exec launch --skip-data-collection --no-container \
  qsmxt-descriptors/qsmxt.json '{"action":"--version"}'
```

MRIcron's descriptor accepts an optional image file and needs a graphical
desktop. The undeclared `[OPTIONS]` placeholder has been removed. Descriptor
validation does not test a GUI viewing session.

## Adoption status

- QSMxT: version/help and default BIDS command descriptors, matched to the recipe's installed version.
- MRIcron: existing viewer descriptor repaired and schema-validated.
- Other recipes: add descriptors with a tested invocation and explicit input/output mapping.
- Shared search/publishing and broader scientific processing acceptance remain follow-up work under #217.

See the [Boutiques executor documentation](https://boutiques.github.io/doc/_execute.html)
for invocation, container selection and bind options.
