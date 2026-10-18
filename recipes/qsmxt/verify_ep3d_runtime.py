"""Exercise dual-polarity EPI preprocessing and QSM without FSL or Nipype."""

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np
from scipy.ndimage import gaussian_filter, shift

sys.path.append("/opt/code/python-ismrmrd-server")
import ep3d_preproc
import qsmxt


def check_geometry_and_denoise(root, signal, truth, magnitude, core):
    from scipy.ndimage import affine_transform

    angle = np.deg2rad(20)
    rotation = np.array(
        [
            [np.cos(angle), -np.sin(angle), 0],
            [np.sin(angle), np.cos(angle), 0],
            [0, 0, 1],
        ]
    )
    affine = np.eye(4)
    affine[:3, :3] = rotation @ np.diag([1, 1.5, 2])
    affine[:3, 3] = [11, -9, 7]
    signal = signal.copy()
    motion_angle = np.deg2rad(2)
    world_rotation = np.array(
        [
            [1, 0, 0],
            [0, np.cos(motion_angle), -np.sin(motion_angle)],
            [0, np.sin(motion_angle), np.cos(motion_angle)],
        ]
    )
    voxel_rotation = np.linalg.solve(affine[:3, :3], world_rotation @ affine[:3, :3])
    center = (np.array(signal.shape[:3]) - 1) / 2
    offset = center - voxel_rotation @ center
    for repetition in (2, 3):
        signal[..., repetition] = affine_transform(
            signal[..., repetition].real, voxel_rotation, offset, order=3
        ) + 1j * affine_transform(
            signal[..., repetition].imag, voxel_rotation, offset, order=3
        )
    magnitude_path, phase_path = root / "magnitude.nii.gz", root / "phase.nii.gz"
    nib.save(nib.Nifti1Image(np.abs(signal).astype(np.float32), affine), magnitude_path)
    nib.save(nib.Nifti1Image(np.angle(signal).astype(np.float32), affine), phase_path)
    for denoise in (False, True):
        work = root / ("denoised" if denoise else "plain")
        outputs_path = root / f"{work.name}.json"
        command = [
            sys.executable,
            str(Path(ep3d_preproc.__file__)),
            str(magnitude_path),
            str(phase_path),
            str(work),
            str(outputs_path),
            "--n-procs",
            "2",
        ]
        if denoise:
            command.append("--denoise")
        subprocess.run(command, check=True)
        motion_parameters = np.loadtxt(work / "motion.1D")
        assert np.allclose(
            np.linalg.norm(motion_parameters[2:, :3], axis=1), 2, atol=0.4
        ), "The known two-degree rotation was not recovered"
        outputs = json.loads(outputs_path.read_text())
        output_magnitude = nib.load(outputs["magnitude"])
        output_phase = nib.load(outputs["phase"])
        for image in (output_magnitude, output_phase):
            assert image.shape == magnitude.shape
            assert np.allclose(image.affine, affine, rtol=0, atol=1e-5)
            assert np.allclose(image.header.get_zooms(), [1, 1.5, 2])
            assert np.isfinite(image.get_fdata()).all()
        averaged = output_magnitude.get_fdata() * np.exp(1j * output_phase.get_fdata())
        difference = averaged * np.conj(truth)
        phase_error = np.angle(difference[core])
        magnitude_error = np.median(
            np.abs(np.abs(averaged[core]) - magnitude[core]) / magnitude[core]
        )
        print(
            f"Oblique anisotropic grid, rotations, NLM={denoise}: "
            f"phase SD={phase_error.std():.4f}, magnitude error={magnitude_error:.4f}"
        )
        assert abs(np.angle(difference[core].sum())) < 0.1
        assert phase_error.std() < 0.08
        assert magnitude_error < 0.06
        assert not list(work.glob("motion-*")), (
            "Temporary Cartesian files were not removed"
        )


def main():
    for config in ("qsmxt_ep3d", "qsmxt_ep3d_denoise"):
        assert importlib.util.find_spec(config), config
    assert importlib.util.find_spec("nipype") is None, "Nipype must not be installed"
    for command in ("mcflirt", "flirt", "conda", "mamba", "micromamba"):
        assert shutil.which(command) is None, f"Unexpected dependency: {command}"

    rng = np.random.default_rng(0)
    shape = (48, 48, 32)
    x, y, z = np.meshgrid(*[np.linspace(-1, 1, n) for n in shape], indexing="ij")
    inside = x**2 / 0.8 + y**2 / 0.8 + z**2 / 0.7 < 1
    magnitude = np.where(
        inside, 1000 + 400 * gaussian_filter(rng.standard_normal(shape), 2), 0
    )
    tissue_phase = gaussian_filter(rng.standard_normal(shape), 2.5) / 0.05 * 0.4
    field_phase = 1.2 * x + 0.8 * z
    polarity_phase = 1.3 * (0.5 + y)
    truth = magnitude * np.exp(1j * (field_phase + tissue_phase + polarity_phase))
    motion = [(0, 0, 0), (0, 0, 0), (1.5, -1.0, 0), (1.5, -1.0, 0)]

    def mrd_volume(series, repetition, data_xyz, image_type):
        image = ismrmrd.Image.from_array(
            np.transpose(data_xyz, (2, 1, 0)).astype(np.float32), transpose=False
        )
        header = image.getHead()
        header.image_series_index = series
        header.image_index = repetition + 1
        header.image_type = image_type
        header.repetition = repetition
        for field, values in (
            ("matrix_size", shape),
            ("field_of_view", [float(n) for n in shape]),
            ("position", [0.0, 0.0, 0.0]),
            ("read_dir", [1.0, 0.0, 0.0]),
            ("phase_dir", [0.0, 1.0, 0.0]),
            ("slice_dir", [0.0, 0.0, 1.0]),
        ):
            for index, value in enumerate(values):
                getattr(header, field)[index] = value
        image.setHead(header)
        meta = ismrmrd.Meta()
        meta["SequenceDescription"] = (
            "ep3d_qsm" if image_type == ismrmrd.IMTYPE_MAGNITUDE else "ep3d_qsm_Pha"
        )
        meta["EchoNumber"] = "1"
        image.attribute_string = meta.serialize()
        return image

    images, volumes = [], []
    for repetition, offset in enumerate(motion):
        sign = 1 if repetition % 2 == 0 else -1
        signal = magnitude * np.exp(
            1j * (field_phase + tissue_phase + sign * polarity_phase)
        )
        signal = shift(signal.real, offset, order=3) + 1j * shift(
            signal.imag, offset, order=3
        )
        signal += 80 * (rng.standard_normal(shape) + 1j * rng.standard_normal(shape))
        volumes.append(signal)
        counts = np.round((np.angle(signal) + np.pi) / (2 * np.pi) * 4096) % 4096
        images.append(
            mrd_volume(1, repetition, np.abs(signal), ismrmrd.IMTYPE_MAGNITUDE)
        )
        images.append(mrd_volume(2, repetition, counts, ismrmrd.IMTYPE_PHASE))

    class Metadata:
        class acquisitionSystemInformation:
            systemFieldStrength_T = 7.0

        class measurementInformation:
            protocolName = "ep3d_qsm"

        class sequenceParameters:
            TE = [15.0]

    settings = qsmxt._settings_from_config(
        {"parameters": {"ep3ddualpolarity": "true"}}, Metadata()
    )
    with tempfile.TemporaryDirectory() as work:
        bids = Path(work) / "bids"
        result = qsmxt.write_bids_dataset(images, Metadata(), bids, settings)
        assert result["n_echoes"] == 1
        motion_parameters = np.loadtxt(
            bids.parent / "ep3d" / "preprocessing" / "motion.1D"
        )
        assert np.allclose(motion_parameters[:2, 3:], 0, atol=0.2)
        assert np.allclose(motion_parameters[2:, 3:], [0, 1.5, -1], atol=0.2), (
            "The known translation was not recovered"
        )
        averaged = nib.load(result["magnitude_paths"][0]).get_fdata() * np.exp(
            1j * nib.load(result["phase_paths"][0]).get_fdata()
        )
        core = inside & (x**2 / 0.8 + y**2 / 0.8 + z**2 / 0.7 < 0.6)
        difference = averaged * np.conj(truth)
        residual = np.angle(
            difference * np.exp(-1j * np.angle(difference[core].sum()))
        )[core]
        single = np.angle(volumes[0] * np.conj(truth))[core]
        naive = np.mean(volumes, axis=0)
        magnitude_error = np.median(
            np.abs(np.abs(averaged[core]) - magnitude[core]) / magnitude[core]
        )
        naive_error = np.median(
            np.abs(np.abs(naive[core]) - magnitude[core]) / magnitude[core]
        )
        print(
            f"tissue phase std {tissue_phase[core].std():.3f}, phase residual std {residual.std():.4f} rad (single volume {single.std():.4f}), "
            f"magnitude error {magnitude_error:.3f} (naive mean {naive_error:.3f})"
        )
        assert residual.std() < 0.7 * single.std()
        assert magnitude_error < 0.05 < naive_error

        output = Path(work) / "out"
        qsmxt._run_qsmxt(bids, output, result["settings"])
        chimaps = list(output.rglob("*_Chimap.nii*"))
        assert chimaps and np.isfinite(nib.load(chimaps[0]).get_fdata()).all()
    with tempfile.TemporaryDirectory() as geometry_work:
        check_geometry_and_denoise(
            Path(geometry_work), np.stack(volumes, axis=-1), truth, magnitude, core
        )
    print(
        "3D EPI niimath motion correction, phase matching, averaging, NLM and QSMxT verified"
    )


if __name__ == "__main__":
    main()
