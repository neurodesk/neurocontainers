"""Check preprocessing scheduling at the niimath process boundary."""

import shutil
import subprocess
import threading

import nibabel as nib
import numpy as np
import pytest

import ep3d_preproc


@pytest.mark.parametrize("n_procs", [1, 2, 3, 8])
@pytest.mark.parametrize("motion_fails", [False, True])
def test_motion_overlap_and_join(tmp_path, monkeypatch, n_procs, motion_fails):
    shape = (6, 5, 4, 2)
    affine = np.diag([0.4, 0.5, 0.6, 1])
    magnitude = tmp_path / "magnitude.nii"
    phase = tmp_path / "phase.nii"
    nib.save(nib.Nifti1Image(np.full(shape, 7, dtype=np.float32), affine), magnitude)
    nib.save(nib.Nifti1Image(np.zeros(shape, dtype=np.float32), affine), phase)
    entered = threading.Event()
    saved = threading.Event()
    finished = threading.Event()
    save_lock = threading.Lock()
    saved_components = set()
    original_save = ep3d_preproc._save
    calls = []

    def save(data, reference, path):
        if path.name in ("real.nii", "imag.nii") and n_procs > 1:
            assert entered.wait(5), "Motion did not overlap Cartesian preparation"
        original_save(data, reference, path)
        if path.name in ("real.nii", "imag.nii"):
            with save_lock:
                saved_components.add(path.name)
                if len(saved_components) == 2:
                    saved.set()

    def run(command, *, check, env):
        assert check
        calls.append(command)
        if "-moco" in command:
            assert int(env["OMP_NUM_THREADS"]) == max(
                1, n_procs - min(2, max(1, n_procs // 2))
            )
            entered.set()
            assert saved.wait(5), "Motion prevented Cartesian preparation"
            if motion_fails:
                raise subprocess.CalledProcessError(1, command)
            finished.set()
        else:
            assert "-resample" in command and finished.is_set()
            assert int(env["OMP_NUM_THREADS"]) == max(1, n_procs // min(n_procs, 2))
            shutil.copyfile(command[1], command[-1])

    monkeypatch.setattr(ep3d_preproc, "_save", save)
    monkeypatch.setattr(ep3d_preproc.subprocess, "run", run)
    work = tmp_path / "work"
    if motion_fails:
        with pytest.raises(subprocess.CalledProcessError):
            ep3d_preproc.preprocess(magnitude, phase, work, n_procs=n_procs)
        assert len(calls) == 1
    else:
        outputs = ep3d_preproc.preprocess(magnitude, phase, work, n_procs=n_procs)
        np.testing.assert_array_equal(nib.load(outputs["magnitude"]).get_fdata(), 7)
        np.testing.assert_array_equal(nib.load(outputs["phase"]).get_fdata(), 0)
        np.testing.assert_array_equal(
            nib.load(outputs["phase"]).affine, nib.load(phase).affine
        )
        assert len(calls) == 3
    assert not list(work.glob("motion-*"))
