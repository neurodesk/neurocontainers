# CVMFS inventory scans

The weekly workflow is report-only. It never creates vulnerability issues, writes
CVMFS payloads, publishes containers or changes release metadata.

Provision a trusted runner with a read-only CVMFS mount, Python 3.10+, Syft, Grype
and Apptainer for SIF entries. Pin scanner executables in that runner's provision
configuration. Set repository variables `CVMFS_SCAN_RUNNER`, `CVMFS_SCAN_ROOT`,
`CVMFS_SCAN_INVENTORY`, `CVMFS_SCAN_SYFT_VERSION` and `CVMFS_SCAN_GRYPE_VERSION`.
The expected versions must match the scanners' JSON `version` fields exactly.
The scanner updates the Grype database before scanning and retains its status.
A failed database update is a scanner error for every enabled entry.
Then enable `CVMFS_SCAN_ENABLED=true`. This PR does not provision the host or
attest the deployed CVMFS inventory.

The inventory is an operator-generated JSON file outside this checkout. Include
all enabled published containers and disabled entries that need an explanation:

```json
{
  "revision": "cvmfs-revision-123",
  "containers": [
    {"identity": "niimath-1.0", "path": "containers/niimath/rootfs", "kind": "directory", "enabled": true},
    {"identity": "tool-2.0", "path": "containers/tool.simg", "kind": "sif", "enabled": true}
  ]
}
```

Paths resolve within the mount, including symlinks. Directory identities bind to
the supplied CVMFS revision; SIF entries also retain a SHA256 digest. Scans extract
SIFs into temporary storage and delete it after each entry. Reports must reside
outside the mount. Only explicit `enabled: true` entries execute scanners.

```bash
python -m workflows.cvmfs_scan --root /cvmfs/neurodesk.ardc.edu.au \
  --inventory /etc/neurodesk/enabled-containers.json --output /tmp/scan-reports \
  --syft-version 1.20.0 --grype-version 0.90.0 --dry-run
```

Use the installed pinned versions in place of the example values. Remove
`--dry-run` for a scan. A scanner crash, missing payload or version mismatch
produces an error, a retained diagnostic and exit code 1. Findings remain a
successful report. Each run retains coverage, SBOMs and findings for 90 days.
The operator must confirm enabled inventory coverage before treating this job
as deployed. Issue #61 remains open until that run is observed.
