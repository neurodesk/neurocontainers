"""Motion-correct, phase-match and complex-average a 4D magnitude/radian-phase pair.

niimath estimates motion on magnitude and resamples both Cartesian components.
Phase matching and optional ANTs NLM denoising follow Rüdiger Stirnberg's
ep3d-highres-preprocessing, packaged with the author's permission:
https://github.com/mrphysics-bonn/ep3d-highres-preprocessing/blob/
8062dc1a6e368f3d1c329c50dbea42c8fdc5c3e8/basic-preprocessing/complex_nifti_tools/nipype_helpers.py
"""

import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

import nibabel as nib
import numpy as np
from scipy.ndimage import gaussian_filter


def _save(data: np.ndarray, reference: nib.Nifti1Image, path: Path) -> None:
    image = nib.Nifti1Image(
        np.asarray(data, dtype=np.float32), reference.affine, reference.header
    )
    image.set_data_dtype(np.float32)
    nib.save(image, path)


def _phase_matched_average(signal: np.ndarray, *, n_procs: int = 1) -> np.ndarray:
    phase_difference = np.cumsum(
        np.angle(signal[..., 1:] * np.conj(signal[..., :-1])), axis=-1
    )
    sigma = (2, 2, 2, 0)
    with ThreadPoolExecutor(max_workers=min(n_procs, 2)) as executor:
        sine = executor.submit(gaussian_filter, np.sin(phase_difference), sigma)
        cosine = executor.submit(gaussian_filter, np.cos(phase_difference), sigma)
        correction = np.arctan2(sine.result(), cosine.result())
    signal[..., 1:] *= np.exp(-1j * correction)
    return signal.mean(axis=-1)


def _denoise_complex(
    signal: np.ndarray, reference: nib.Nifti1Image, work_dir: Path
) -> np.ndarray:
    import ants
    from skimage.restoration import unwrap_phase

    real_path = work_dir / "mean_real.nii"
    imag_path = work_dir / "mean_imag.nii"
    _save(signal.real, reference, real_path)
    _save(signal.imag, reference, imag_path)
    real_image = ants.image_read(str(real_path))
    imag_image = ants.image_read(str(imag_path))
    magnitude = real_image.new_image_like(np.abs(signal))
    phase_baseline = gaussian_filter(unwrap_phase(np.angle(signal)), 4)
    demodulated = signal * np.exp(-1j * phase_baseline)
    real = real_image.new_image_like(demodulated.real / 2 + 2048)
    imag = imag_image.new_image_like(demodulated.imag / 2 + 2048)
    mask = ants.get_mask(magnitude, low_thresh=magnitude.mean() / 5)
    real_denoised = ants.denoise_image(real, mask, noise_model="Gaussian").numpy()
    imag_denoised = ants.denoise_image(imag, mask, noise_model="Gaussian").numpy()
    denoised = (real_denoised * 2 - 4096) + 1j * (imag_denoised * 2 - 4096)
    return denoised * np.exp(1j * phase_baseline)


def preprocess(
    magnitude_path: Path,
    phase_path: Path,
    work_dir: Path,
    *,
    denoise: bool = False,
    n_procs: int = 1,
) -> dict[str, str]:
    """Write the averaged magnitude and phase, keeping repetition 0 as reference."""
    if n_procs < 1:
        raise ValueError("n_procs must be positive")
    magnitude_path = magnitude_path.resolve()
    phase_path = phase_path.resolve()
    magnitude_image = nib.load(magnitude_path)
    phase_image = nib.load(phase_path)
    if len(magnitude_image.shape) != 4 or magnitude_image.shape[-1] < 2:
        raise ValueError("3D EPI preprocessing requires at least two 3D repetitions")
    if magnitude_image.shape != phase_image.shape or not np.allclose(
        magnitude_image.affine, phase_image.affine, rtol=0, atol=1e-4
    ):
        raise ValueError("Magnitude and phase must have matching shapes and geometry")
    magnitude = magnitude_image.get_fdata(dtype=np.float32, caching="unchanged")
    phase = phase_image.get_fdata(dtype=np.float32, caching="unchanged")
    if not np.isfinite(magnitude).all() or not np.isfinite(phase).all():
        raise ValueError("Magnitude and phase must contain only finite values")

    work_dir = work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    motion_path = work_dir / "motion.1D"
    environment = dict(os.environ, OMP_NUM_THREADS=str(n_procs))
    preparation_workers = min(2, max(1, n_procs // 2))
    motion_environment = dict(
        environment, OMP_NUM_THREADS=str(max(1, n_procs - preparation_workers))
    )

    def estimate_motion() -> None:
        subprocess.run(
            [
                "niimath",
                str(magnitude_path),
                "-moco",
                "-ref",
                "0",
                "-estimate",
                str(motion_path),
            ],
            check=True,
            env=motion_environment,
        )

    with TemporaryDirectory(prefix="motion-", dir=work_dir) as temporary:
        temporary_dir = Path(temporary)
        real_path = temporary_dir / "real.nii"
        imag_path = temporary_dir / "imag.nii"
        real_corrected_path = temporary_dir / "real_mc.nii"
        imag_corrected_path = temporary_dir / "imag_mc.nii"
        with ThreadPoolExecutor(max_workers=1) as motion_executor:
            motion = (
                motion_executor.submit(estimate_motion) if n_procs > 1 else None
            )
            signal = magnitude * np.exp(1j * phase)
            with ThreadPoolExecutor(max_workers=preparation_workers) as executor:
                real_save = executor.submit(_save, signal.real, magnitude_image, real_path)
                imag_save = executor.submit(_save, signal.imag, magnitude_image, imag_path)
                real_save.result()
                imag_save.result()
            del magnitude, phase, signal
            if motion is None:
                estimate_motion()
            else:
                motion.result()
        resample_workers = min(n_procs, 2)
        resample_environment = dict(
            environment, OMP_NUM_THREADS=str(max(1, n_procs // resample_workers))
        )

        def resample(source: Path, destination: Path) -> None:
            subprocess.run(
                [
                    "niimath",
                    str(source),
                    "-gz",
                    "0",
                    "-resample",
                    str(magnitude_path),
                    "-hmc",
                    str(motion_path),
                    str(destination),
                ],
                check=True,
                env=resample_environment,
            )

        with ThreadPoolExecutor(max_workers=resample_workers) as executor:
            real_resample = executor.submit(resample, real_path, real_corrected_path)
            imag_resample = executor.submit(resample, imag_path, imag_corrected_path)
            real_resample.result()
            imag_resample.result()
        signal = (
            nib.load(real_corrected_path)
            .get_fdata(dtype=np.float32)
            .astype(np.complex64)
        )
        signal.imag = nib.load(imag_corrected_path).get_fdata(dtype=np.float32)
        averaged = _phase_matched_average(signal, n_procs=n_procs)
        if denoise:
            os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(n_procs)
            averaged = _denoise_complex(averaged, magnitude_image, temporary_dir)

    magnitude_output = work_dir / "average_magnitude.nii"
    phase_output = work_dir / "average_phase.nii"
    with ThreadPoolExecutor(max_workers=min(n_procs, 2)) as executor:
        magnitude_save = executor.submit(
            _save, np.abs(averaged), magnitude_image, magnitude_output
        )
        phase_save = executor.submit(
            _save, np.angle(averaged), magnitude_image, phase_output
        )
        magnitude_save.result()
        phase_save.result()
    return {"magnitude": str(magnitude_output), "phase": str(phase_output)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("magnitude", type=Path)
    parser.add_argument("phase", type=Path)
    parser.add_argument("work_dir", type=Path)
    parser.add_argument("outputs_json", type=Path)
    parser.add_argument("--denoise", action="store_true")
    parser.add_argument("--n-procs", type=int, default=1)
    args = parser.parse_args()
    outputs = preprocess(
        args.magnitude,
        args.phase,
        args.work_dir,
        denoise=args.denoise,
        n_procs=args.n_procs,
    )
    args.outputs_json.write_text(json.dumps(outputs))


if __name__ == "__main__":
    main()
