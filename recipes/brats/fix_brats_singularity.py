"""Make the brats singularity backend work in a read-only, daemonless image.

Four defects, found by container testing, each blocking on its own:

1. brats/constants.py resolves ADDITIONAL_FILES_FOLDER inside its own installed
   package. run_container() touches it for every algorithm -- 16 download
   Zenodo weights there, the other 50 mkdir a dummy in it -- and site-packages
   is read-only at run time, so all 66 algorithms died with Errno 30. It now
   honours $BRATS_ADDITIONAL_FILES_DIR and defaults under tempfile.gettempdir(),
   which follows TMPDIR into the same persistent cache as the sandboxes.

2. brats/core/singularity.py ran algorithms via spython with stream=True but no
   stream_type, which drains only stdout: once an algorithm filled its stderr
   pipe (3.7 MB observed), it blocked writing while brats blocked reading, and
   the job sat idle until walltime with the real error stuck in the pipe.
   stream_type="both" drains both.

3. run_container() pulled the multi-GB algorithm image before checking CPU
   compatibility, so a CPU-only user paid for the whole download and then got
   AlgorithmNotCPUCompatibleException. The check now runs first; it is a pure
   inspect-and-raise, so calling it early changes nothing else.

4. _get_docker_working_dir() asks the Docker daemon for the image's WorkingDir
   and returns None when there is no daemon -- always, here. Algorithms whose
   ENTRYPOINT is a relative path (brats23_africa_blackbean: "python3 mlcube.py")
   then start in the wrong directory and exit immediately. Apptainer discards
   the OCI WorkingDir when it converts an image to a sandbox (verified in
   apptainer 1.4.4's conveyorPacker_oci.go: only ENTRYPOINT/CMD/ENV/labels
   survive), so it cannot be recovered locally; brats-pull records it from the
   registry at pull time into <sandbox>/.brats_docker_workdir, and the fallback
   reads that.

Plus one cosmetic: the docker probe at import logged at ERROR, telling every
user to install docker on a platform where docker will never exist. It logs at
debug now, and the image sets LOGURU_LEVEL=INFO.
"""

import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def patch(path, old, new, must=1):
    src = path.read_text()
    if src.count(old) != must:
        raise SystemExit(
            f"expected {must} occurrence(s) of anchor in {path}, found "
            f"{src.count(old)}; upstream changed and this patch needs revisiting:"
            f"\n{old!r}"
        )
    path.write_text(src.replace(old, new, must))


constants = root / "brats" / "constants.py"
patch(
    constants,
    "from enum import Enum\nfrom pathlib import Path\n",
    "import os\nimport tempfile\nfrom enum import Enum\nfrom pathlib import Path\n",
)
patch(
    constants,
    'ADDITIONAL_FILES_FOLDER = DATA_DIR / "additional_files"',
    '# neurocontainers: never inside site-packages, which is read-only at run\n'
    '# time. Defaults under tempfile.gettempdir() so it follows TMPDIR into the\n'
    '# same persistent cache as the algorithm sandboxes.\n'
    'ADDITIONAL_FILES_FOLDER = Path(\n'
    '    os.environ.get("BRATS_ADDITIONAL_FILES_DIR")\n'
    '    or Path(tempfile.gettempdir()) / "brats_additional_files"\n'
    ')',
)

sing = root / "brats" / "core" / "singularity.py"
patch(
    sing,
    """        executor = Client.run(
            image,
            options=options,
            args=args,
            stream=True,
            bind=singularity_bindings,
        )""",
    """        executor = Client.run(
            image,
            options=options,
            args=args,
            stream=True,
            # neurocontainers: without this spython drains only stdout, so an
            # algorithm that fills its stderr pipe deadlocks against brats and
            # the job sits idle until walltime with the error stuck in the pipe.
            stream_type="both",
            bind=singularity_bindings,
        )""",
)
patch(
    sing,
    """    _log_algorithm_info(algorithm=algorithm)
    # ensure image is present, if not pull it
    image = _ensure_image(image=algorithm.run_args.docker_image)""",
    """    _log_algorithm_info(algorithm=algorithm)
    # neurocontainers: refuse CPU-incompatible algorithms BEFORE the multi-GB
    # pull. Upstream checks only after _ensure_image, so a CPU-only user paid
    # for the whole download and then got the exception. Pure inspect-and-raise;
    # the original call below still supplies the device requests.
    _handle_device_requests(
        algorithm=algorithm, cuda_devices=cuda_devices, force_cpu=force_cpu
    )
    # ensure image is present, if not pull it
    image = _ensure_image(image=algorithm.run_args.docker_image)""",
)
patch(
    sing,
    """    if docker_client is None:
        return None
    try:
        logger.debug(f"Inspecting image {image}")""",
    """    if docker_client is None:
        # neurocontainers: there is never a docker daemon here. Apptainer
        # discards the OCI WorkingDir when converting to a sandbox, so it
        # cannot be read locally; brats-pull records it from the registry at
        # pull time, next to the sandbox.
        marker = (
            Path(tempfile.gettempdir())
            / "brats_singularity_images"
            / image.replace(":", "_")
            / ".brats_docker_workdir"
        )
        try:
            recorded = marker.read_text().strip()
        except OSError:
            recorded = ""
        if recorded:
            logger.debug(f"Working directory from pull-time record: {recorded}")
            return Path(recorded)
        logger.warning(
            "No pull-time record of this image's working directory. Algorithms "
            "whose entrypoint is a relative path will exit immediately; re-run "
            "brats-pull on this image to record it."
        )
        return None
    try:
        logger.debug(f"Inspecting image {image}")""",
)

dock = root / "brats" / "core" / "docker.py"
patch(
    dock,
    '''    logger.error(
        f"Failed to connect to docker daemon. Please make sure docker is installed and running. Error: {e}"
    )''',
    '''    # neurocontainers: docker never exists on this platform, so an ERROR
    # telling the user to install it is noise on every single command.
    logger.debug(
        f"No docker daemon (expected here; the singularity backend is used). Error: {e}"
    )''',
)

print("brats singularity backend patched")
