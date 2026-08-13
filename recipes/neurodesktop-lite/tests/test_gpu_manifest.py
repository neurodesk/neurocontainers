import os
import subprocess
import tempfile
from pathlib import Path


WRAPPER = (
    Path(__file__).parents[1]
    / "config"
    / "gpu"
    / "neurodesktop-container-runtime"
).resolve()


def _run_wrapper(tmp_path: Path, container: str, *, enabled: bool) -> tuple[str, Path]:
    manifest = tmp_path / "gpu-containers.tsv"
    manifest.write_text("mrtrix3\t3.0.8\n", encoding="utf-8")

    real_runtime = tmp_path / "real-runtime"
    real_runtime.write_text(
        "#!/bin/sh\n"
        "printf 'driver=%s\\n' \"${APPTAINERENV_GALLIUM_DRIVER:-}\"\n"
        "printf 'socket=%s\\n' \"${APPTAINERENV_VTEST_SOCKET_NAME:-}\"\n"
        "printf 'gl=%s\\n' \"${APPTAINERENV_MESA_GL_VERSION_OVERRIDE:-}\"\n"
        "printf 'glsl=%s\\n' \"${APPTAINERENV_MESA_GLSL_VERSION_OVERRIDE:-}\"\n",
        encoding="utf-8",
    )
    real_runtime.chmod(0o755)

    socket_dir = Path(tempfile.mkdtemp(prefix="ndgpu-", dir="/tmp"))
    socket_path = socket_dir / "vtest"
    starter = tmp_path / "start-virgl"
    starter.write_text(
        "#!/bin/sh\n"
        "python3 -c 'import socket,sys; s=socket.socket(socket.AF_UNIX); "
        "s.bind(sys.argv[1]); s.close()' \"$NEURODESKTOP_VTEST_SOCKET\"\n",
        encoding="utf-8",
    )
    starter.chmod(0o755)

    apptainer = tmp_path / "apptainer"
    apptainer.symlink_to(WRAPPER)
    environment = {
        **os.environ,
        "NEURODESKTOP_APPTAINER_REAL": str(real_runtime),
        "NEURODESKTOP_GPU_MANIFEST": str(manifest),
        "NEURODESKTOP_VIRGL_STARTER": str(starter),
        "NEURODESKTOP_VTEST_SOCKET": str(socket_path),
        "NEURODESKTOP_GPU_MARKER": str(tmp_path / "disabled-marker"),
    }
    if enabled:
        environment["NEURODESKTOP_GPU_ACCELERATION"] = "1"

    result = subprocess.run(
        ["/bin/sh", str(apptainer), "exec", container, "true"],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return result.stdout, socket_path


def test_allowlisted_container_gets_scoped_virgl_environment(tmp_path: Path) -> None:
    output, socket_path = _run_wrapper(
        tmp_path, "mrtrix3_3.0.8_20260107.simg", enabled=True
    )

    assert "driver=virpipe\n" in output
    assert f"socket={socket_path}\n" in output
    assert "gl=4.1\n" in output
    assert "glsl=410\n" in output
    assert socket_path.is_socket()
    socket_path.unlink()
    socket_path.parent.rmdir()


def test_master_switch_off_does_not_start_or_inject_virgl(tmp_path: Path) -> None:
    output, socket_path = _run_wrapper(
        tmp_path, "mrtrix3_3.0.8_20260107.simg", enabled=False
    )

    assert "driver=virpipe\n" not in output
    assert not socket_path.exists()
    socket_path.parent.rmdir()


def test_unlisted_and_near_match_versions_do_not_get_virgl(tmp_path: Path) -> None:
    for index, container in enumerate(
        ("blender_5.0.1_20260701.simg", "mrtrix3_3.0.80_20260107.simg")
    ):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        output, socket_path = _run_wrapper(case_dir, container, enabled=True)
        assert "driver=virpipe\n" not in output
        assert not socket_path.exists()
        socket_path.parent.rmdir()
