# build3

`build3` is an isolated Python prototype for generating NeuroContainers
Dockerfiles without NeuroDocker or another external Dockerfile renderer.

From the repository root:

```bash
python -m build3 generate dcm2niix
python -m build3 stage dcm2niix
python -m build3 build dcm2niix --dry-run
python -m build3 test dcm2niix --dry-run
python -m build3 make dcm2niix --dry-run
```

To run the exhaustive one-to-one Dockerfile parity check, comparing the current
`builder/build.py generate` CLI against the `build3 generate` CLI for every
current recipe:

```bash
BUILD3_STRICT_DOCKERFILE_PARITY=1 \
  python -m pytest build3/tests/test_all_dockerfile_parity.py
```

For package development:

```bash
python -m pip install -e build3[test]
python -m pytest build3/tests
```
