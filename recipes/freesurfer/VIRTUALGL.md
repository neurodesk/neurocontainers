# FreeView through VirtualGL

Investigation dated 2026-10-05. Scope: FreeSurfer 8.2.0 in NeuroContainers,
targeting standalone Apptainer/Singularity on HPC. Neurodesktop findings below
are background only; its nested runtime is outside this target.
GPU rendering has not been verified in this workspace. No GPU devices or
Apptainer/Singularity executable are available, and Docker daemon access fails.

## Recommended approach

Test VirtualGL's EGL backend inside the FreeSurfer app container. Keep the
desktop's existing X display for windows and use an EGL device for GPU rendering.
VirtualGL's EGL backend avoids a separate GPU-backed X server, but still needs
the desktop X display. Enumerate devices rather than assuming `egl0` identifies
the intended GPU. Use proxy transport for the local desktop/VNC session.
[VirtualGL guide](https://raw.githubusercontent.com/VirtualGL/virtualgl/main/doc/index.html#hd006004),
[device and transport settings](https://github.com/VirtualGL/virtualgl/blob/3.1.5/doc/advancedconfig.txt).

Run on a GPU compute node allocated through the site scheduler. Prefer a
site-supported VNC or Open OnDemand desktop on that same node, then launch
Apptainer directly there. A login-node desktop does not automatically give the
FreeView process access to a GPU on another node.

There is one container boundary in this target. Neurodesktop's VirGL wrapper and
its disabled automatic `--nv` setting do not control standalone HPC launches.

## What the reference image does

The [linked Dockerfile](https://github.com/pwighton/fs-docker/blob/master/freesurfer/Dockerfile.8.2.0-z2jh)
installs VirtualGL 3.1.5, sets `NVIDIA_DRIVER_CAPABILITIES=all`, and creates shell
aliases for `freeview` and `tkmeditfv` using `vglrun -d egl0` when `nvidia-smi`
succeeds. It packages the desktop and FreeSurfer together, expects the NVIDIA
Container Toolkit to inject runtime drivers, and uses the Ubuntu 24 FreeSurfer
package. Our recipe uses Ubuntu 22.04; the HPC target runs it directly through
the site's Apptainer/Singularity runtime.

The alias is insufficient for scripts and module wrappers. Running `nvidia-smi` proves driver management access, not EGL rendering.
Copying the image's environment setting also cannot make Apptainer inject
graphics libraries by itself.

## Current repository behavior

| Component | Observation | Implication |
| --- | --- | --- |
| [FreeSurfer recipe](build.yaml) | Ubuntu 22.04, FreeSurfer 8.2.0, VirtualGL 3.1.5, GL/EGL/GLVND and Qt packages | Rebuild the image to obtain the opt-in `freeview-vgl` launcher and diagnostics. |
| FreeView wrappers in the recipe | `freeview` calls `freeview_bin`, which calls `freeview_bin_real`; wrapper rebuilds `LD_LIBRARY_PATH`, removes MCR paths, and leaves `LD_PRELOAD` intact | VirtualGL injection appears possible. Preserve the Qt fixes and verify actual GPU-library resolution. |
| Library ordering | FreeSurfer Qt and system library directories precede inherited paths, including paths injected by Apptainer | Inspect loaded GL/EGL libraries. Driver-path changes must preserve FreeSurfer's Qt priority. |
| [FreeSurfer release tests](fulltest.yaml) | Package, launcher, diagnostic and preload checks do not require a GPU | GUI startup and accelerated rendering still need a GPU session. |

## Deployment requirements

For NVIDIA HPC nodes, launch the app container with `apptainer exec --nv`
or the site's Singularity equivalent. The container needs the allocated GPU
devices, GL/EGL driver libraries, and EGL vendor configuration. Inspect the
installed runtime's `nvliblist.conf` and host `ldconfig` results. Modern upstream
lists include NVIDIA EGL/GLX libraries and vendor JSON, but the deployed
runtime's list may differ.
[Apptainer GPU documentation](https://apptainer.org/docs/user/main/gpu.html),
[upstream library list](https://github.com/apptainer/apptainer/blob/main/etc/nvliblist.conf).

The reference's Docker setting `NVIDIA_DRIVER_CAPABILITIES=all` is not required
by standard Apptainer `--nv` and does not configure its injection list. It applies
to NVIDIA Container Toolkit deployments, where the default `compute,utility`
capabilities omit graphics.
[NVIDIA capability documentation](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/docker-specialized.html#driver-capabilities).

Use the GPU allocated by the scheduler. Do not assume `egl0` corresponds to that
allocation. Compare EGL enumeration with the permitted devices and GPU identity
in the job. CUDA visibility variables do not select VirtualGL's EGL device; use
its `-d` selector and respect the site's device isolation.
[VirtualGL device selection](https://github.com/VirtualGL/virtualgl/blob/3.1.5/doc/advancedconfig.txt).

Apptainer explicitly documents failures when bound host libraries and container
libc versions differ. `--nvccli` is another option, but is experimental and has
additional constraints; its capabilities come from the launch environment.
Do not use it as an assumed fix for libc incompatibility.
[Apptainer GPU documentation](https://apptainer.org/docs/user/main/gpu.html).

For Intel/AMD, first check VirtualGL EGL device enumeration and required
extensions with the actual driver. Bind the relevant `/dev/dri` devices and
provide matching Mesa drivers and device permissions. Apptainer's NVIDIA `--nv`
option is not the mechanism for this case. A GPU visible to VirGL does not by
itself establish compatibility with VirtualGL's EGL device backend.
[VirtualGL EGL configuration](https://raw.githubusercontent.com/VirtualGL/virtualgl/main/doc/index.html#hd006004).

All cases need a reachable `DISPLAY`, its X socket, and working X authentication
inside the app container. Bind the real `XAUTHORITY` file when required, including
when it is outside the usual home bind. Preserve user data paths through explicit
binds. Install VirtualGL runtime assets under `/opt/VirtualGL`, not under `/home`.

If EGL cannot work with the deployment's drivers, evaluate the GLX backend with
`vglrun -d :0`. This requires a GPU-backed X server, access to its socket and
authorization, and server configuration. The VNC desktop's display need not be
the GPU display. Follow the official configuration guide for that deployment.
[VirtualGL server configuration](https://raw.githubusercontent.com/VirtualGL/virtualgl/main/doc/index.html#hd006).

## First GPU experiment

These commands assume a candidate FreeSurfer image containing VirtualGL, an
scheduler-allocated NVIDIA GPU on the current compute node, and valid remote
session X access. Use a VNC/OnDemand session on that node for this first test.
They are a validation plan, not commands that work with today's stock image.

The updated recipe provides `freeview-vgl`, which invokes VirtualGL inside the
container and calls the existing FreeView wrappers. After rebuilding, the final
command below can use `freeview-vgl -v /data/volume.nii.gz` directly. Set
`--env VGL_DISPLAY=egl1` to select another allocated device or
`--env VGL_COMPRESS=jpeg` for a configured VirtualGL client transport. Defaults
are `egl0` and `proxy`; plain `freeview` retains its existing launch behavior.

1. In the allocated GPU job/session, check `nvidia-smi`, devices, EGL vendor JSON files, and
   `ldconfig -p` entries for EGL/NVIDIA libraries. Test X access with `xdpyinfo`.
2. Load the site's Apptainer/Singularity module if required and use that runtime
   directly for the first test. Confirm that data and X authentication reach
   the container.
3. Enumerate EGL devices inside the candidate app image, then verify the chosen
   device before starting FreeView.

```bash
# Set these to an actual candidate artifact and the desired mounted data.
image=/path/to/candidate-freesurfer.simg
runtime=apptainer  # Or singularity, according to the site installation.

"$runtime" exec --cleanenv --nv \
  --env "DISPLAY=$DISPLAY" \
  "$image" /opt/VirtualGL/bin/eglinfo -e

"$runtime" exec --cleanenv --nv \
  --env "DISPLAY=$DISPLAY" \
  "$image" /opt/VirtualGL/bin/eglinfo egl0

"$runtime" exec --cleanenv --nv \
  --env "DISPLAY=$DISPLAY" \
  "$image" /opt/VirtualGL/bin/vglrun -d egl0 -c proxy \
  /opt/VirtualGL/bin/glxspheres64

"$runtime" exec --cleanenv --nv \
  --env "DISPLAY=$DISPLAY" \
  --bind /path/to/data:/data \
  "$image" /opt/VirtualGL/bin/vglrun -d egl0 -c proxy \
  freeview -v /data/volume.nii.gz
```

Replace `egl0` using enumeration. Add X socket and Xauthority binds/environment
if the runtime defaults do not expose them. `--cleanenv` removes ordinary host
environment leakage, but explicit `APPTAINERENV_*`/`SINGULARITYENV_*` overrides
can still apply. Check and remove inherited VirGL settings for this experiment.

Record the actual renderer, driver, device, and resolved library paths. Check
that FreeView opens, loads a volume and a surface, and redraws its 3D view while
rotating. A version response, a forced Mesa version string, or successful
`nvidia-smi` alone cannot establish acceleration. Repeat through a module-loaded
command after direct execution works. For SSH X forwarding, proxy transport
can use the forwarded display, but responsiveness must be measured over the
actual connection. A VNC session on the GPU node keeps X image transfer local
and sends desktop updates to the viewer. Do not overwrite the session's
`DISPLAY` with `:0`; `-d egl0` selects the rendering device independently.

## Neurodesktop background

The desktop [GPU wrapper](../neurodesktop-lite/config/gpu/neurodesktop-container-runtime)
and [allowlist](../neurodesktop-lite/config/gpu/gpu-containers.tsv)
route enabled FreeSurfer 8.2.0 launches through VirGL settings. Its
[environment setup](../neurodesktop-lite/config/jupyter/environment_variables.sh)
disables automatic `--nv`, citing glibc errors on Ubuntu 24.04 hosts. Those
settings matter if this work later extends to Neurodesktop, but do not require
desktop changes for the standalone HPC target.

## Packaging and remaining verification

The recipe installs the declared VirtualGL 3.1.5 Debian package through
`{{ get_file("virtualgl_deb") }}`. Its SHA-256 was computed from the downloaded
asset and matched GitHub's published digest:
`df3f7788ce41b182a47c0d298e5cd6d2d63579522cb41825970b7726e825485e`.
[Release asset](https://github.com/VirtualGL/virtualgl/releases/tag/3.1.5).

The source policy keeps FreeSurfer as the container version driver and tracks
the pinned VirtualGL artifact digest as a dependency. This tracks changes to
the 3.1.5 artifact, not discovery of newer VirtualGL releases. A version upgrade
must update `virtualgl_version`, its digest source URL and SHA-256, and the
fulltest scalar together. Container version remains 8.2.0; the build date
distinguishes the new artifact.
[Update policies](../../workflows/AUTO_UPDATES.md).

`deploy.bins` exposes `freeview-vgl`, `vglrun`, and the VirtualGL diagnostics as
`vgl-eglinfo` and `vgl-glxspheres64`. The recipe's README includes direct HPC launch
commands. Release tests check the installed package and preload without a GPU.

Recipe validation, update-policy validation, Dockerfile generation, and staging
passed. The downloaded VirtualGL package also successfully preloaded through
the staged FreeView wrappers into a stand-in executable, preserving device and
transport overrides, arguments with spaces, and exit status. This does not
establish that the real FreeView binary renders correctly.

The next verification is a rebuilt image in the intended GPU allocation.
Confirm direct EGL rendering, real FreeView volume/surface interaction, and
module wrapper runtime options on the site's Transparent Singularity version.
