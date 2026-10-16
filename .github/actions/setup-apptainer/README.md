# Apptainer setup

This action installs the requested release on native Linux AMD64 or ARM64.
The `apptainer-version` output is written only after `apptainer --version` and
the installed ELF architecture match the request and runner.

AMD64 uses the official Debian package. ARM64 builds the official source archive
with Go 1.25.1 through `actions/setup-go`, with two compiler jobs and no persistent
Go cache added by this action. It installs the non-setuid runtime and Ubuntu's
SquashFS, FUSE, fakeroot, and encryption helpers. On Ubuntu with restricted user
namespaces, it installs the upstream Apptainer AppArmor profile for the source
installation path. It does not disable the system's user namespace restrictions.

The [1.4.3 release](https://github.com/apptainer/apptainer/releases/tag/v1.4.3)
has no ARM64 Debian package. Its [installation instructions](https://github.com/apptainer/apptainer/blob/v1.4.3/INSTALL.md)
describe the source build and scoped AppArmor permission. Its
[Go module](https://github.com/apptainer/apptainer/blob/v1.4.3/go.mod) requires Go
1.23.6 or newer. Go 1.25.1 is also used by this repository's existing CI workflows.

Each download and cache hit is checked against the SHA-256 digest from the
requested official GitHub release's asset metadata. Releases without an asset
digest fail before installation. A partial or corrupt download never becomes a
cache entry. Build directories are unique and removed on success or failure.
The 1.4.3 artifacts verified when adding the ARM64 build were:

- `apptainer-1.4.3.tar.gz`, SHA-256 `dfb85b8ad48bd366245c7f6a1d0b56d2ce480cfdf18d7a64397098184b4ade90`.
- `apptainer_1.4.3_amd64.deb`, SHA-256 `c3c05120baab7ece2e81d40c1d1be63cc3fd1f5a25f379a4f2b2f99a0f6a1100`.

Run the boundary tests with `python -m pytest builder/tests/test_setup_apptainer.py`.
They execute the real Bash installer with network, package, and compiler commands
stubbed. They do not prove an ARM64 build works.

`test-setup-apptainer.yml` runs the action on native AMD64 and ARM64 runners when
this action or its tests change. Each job checks the version and builds and
executes a tiny local BusyBox SIF, without downloading a neuroimaging container.
To repeat that runtime check on a configured runner, install `busybox-static`
and run `bash .github/actions/setup-apptainer/smoke-test.sh` as the runner user.
