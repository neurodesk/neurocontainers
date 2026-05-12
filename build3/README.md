# build3

`build3` is the active Python backend for generating NeuroContainers
Dockerfiles without NeuroDocker or another external Dockerfile renderer.

From the repository root:

```bash
python -m build3 generate dcm2niix
python -m build3 stage dcm2niix
python -m build3 build dcm2niix --dry-run
python -m build3 test dcm2niix --dry-run
python -m build3 make dcm2niix --dry-run
```

Legacy `python builder/build.py ...` entry points have been removed. Use
`python -m build3 <command> ...` for direct module invocation, or the installed
`sf-*` commands for day-to-day recipe work.

For package development:

```bash
python -m pip install -e build3[test]
python -m pytest build3/tests
```
