"""Prepare and preprocess repeated MRD magnitude and phase volumes."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np

import ep3d_preproc


@dataclass(frozen=True)
class PreprocessedSeries:
    magnitude_path: Path
    phase_path: Path
    magnitude_reference: list[ismrmrd.Image]
    phase_reference: list[ismrmrd.Image]


def phase_to_radians(phase_volume: np.ndarray, phase_wrap: float) -> np.ndarray:
    """Map scanner phase counts onto [-pi, pi] with ``phase_wrap`` counts per cycle.

    Radian input passes through. The period is fixed rather than inferred
    from the data range, so phase that does not span the full encoding keeps
    its scale.
    """
    phase = np.asarray(phase_volume, dtype=np.float32)
    if phase.size == 0:
        return phase
    data_min, data_max = phase.min(), phase.max()
    all_finite = np.isfinite(data_min) and np.isfinite(data_max)
    if not all_finite:
        finite = phase[np.isfinite(phase)]
        if finite.size == 0:
            return phase
        data_min, data_max = finite.min(), finite.max()
        del finite
    if -np.pi <= data_min and data_max <= np.pi:
        return phase
    span = float(data_max - data_min)
    if span > phase_wrap:
        raise ValueError(
            f"Phase spans {span:.6g} counts, more than one {phase_wrap:.6g}-count "
            "cycle; set phasewrap to the scanner's counts per 2*pi"
        )
    if (
        all_finite
        and 0 <= data_min
        and data_max <= np.iinfo(np.uint16).max
        and phase.size > int(data_max) + 1
    ):
        indices = phase.astype(np.uint16)
        if np.array_equal(phase, indices):
            counts = np.arange(int(data_max) + 1, dtype=np.float32)
            lookup = np.angle(
                np.exp(1j * counts * (2.0 * np.pi / phase_wrap))
            ).astype(np.float32)
            return lookup[indices]
    return np.angle(np.exp(1j * phase * (2.0 * np.pi / phase_wrap))).astype(np.float32)


def _repetition_groups(images: list[ismrmrd.Image]) -> dict[tuple, list[ismrmrd.Image]]:
    groups: dict[tuple, list[ismrmrd.Image]] = {}
    for image in sorted(images, key=lambda item: int(item.getHead().image_index)):
        header = image.getHead()
        key = (int(header.repetition), int(header.set), int(header.average))
        groups.setdefault(key, []).append(image)
    return dict(sorted(groups.items()))


def run_preprocessing(
    magnitude_path: Path,
    phase_path: Path,
    work_dir: Path,
    *,
    denoise: bool,
    n_procs: int,
) -> dict[str, str]:
    """Run the NIfTI pipeline in a subprocess to release its working memory."""
    outputs_path = work_dir / "outputs.json"
    command = [
        sys.executable,
        str(Path(ep3d_preproc.__file__)),
        str(magnitude_path),
        str(phase_path),
        str(work_dir / "preprocessing"),
        str(outputs_path),
        "--n-procs",
        str(n_procs),
    ]
    if denoise:
        command.append("--denoise")
    logging.info("Running 3D EPI preprocessing: %s", " ".join(command))
    started_at = time.monotonic()
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    logging.info(
        "3D EPI preprocessing result: returncode=%d elapsed_seconds=%.3f",
        result.returncode,
        time.monotonic() - started_at,
    )
    if result.stdout:
        logging.info("3D EPI preprocessing stdout:\n%s", result.stdout)
    if result.stderr:
        logging.info("3D EPI preprocessing stderr:\n%s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"3D EPI preprocessing failed with exit code {result.returncode}"
        )
    return json.loads(outputs_path.read_text())


def preprocess_images(
    magnitude_images: list[ismrmrd.Image],
    phase_images: list[ismrmrd.Image],
    work_dir: Path,
    *,
    read_volume: Callable[..., tuple[np.ndarray, np.ndarray]],
    phase_wrap: float = 4096.0,
    denoise: bool = False,
    n_procs: int = 1,
) -> PreprocessedSeries:
    """Return corrected NIfTI files and the first repetition's source images.

    ``read_volume(images, kind=...)`` supplies the caller's scanner geometry
    conversion. Repetition pairing, count scaling, and preprocessing live here.
    """
    magnitude_groups = _repetition_groups(magnitude_images)
    phase_groups = _repetition_groups(phase_images)
    if magnitude_groups.keys() != phase_groups.keys():
        raise ValueError(
            "3D EPI preprocessing needs one phase volume per magnitude volume; "
            "(repetition, set, average) keys differ: "
            f"magnitude={list(magnitude_groups)} phase={list(phase_groups)}"
        )
    if len(magnitude_groups) < 2:
        raise ValueError("3D EPI preprocessing needs at least two repetitions")

    magnitude_4d, phase_4d, affines = [], [], []
    for key, magnitude_group in magnitude_groups.items():
        magnitude, affine = read_volume(magnitude_group, kind="magnitude")
        phase, _ = read_volume(phase_groups[key], kind="phase")
        magnitude_4d.append(magnitude)
        phase_4d.append(phase)
        affines.append(affine)
    if len({volume.shape for volume in magnitude_4d + phase_4d}) != 1:
        raise ValueError(
            "3D EPI repetitions have different shapes: "
            f"{sorted({volume.shape for volume in magnitude_4d + phase_4d})}"
        )
    reference_affine = affines[0]
    logging.info(
        "3D EPI: %d repetitions of shape %s, denoise=%s",
        len(magnitude_4d),
        magnitude_4d[0].shape,
        denoise,
    )
    work_dir.mkdir(parents=True, exist_ok=True)
    magnitude_path = work_dir / "magnitude.nii"
    phase_path = work_dir / "phase.nii"
    nib.save(
        nib.Nifti1Image(np.stack(magnitude_4d, axis=-1), reference_affine),
        magnitude_path,
    )
    nib.save(
        nib.Nifti1Image(
            phase_to_radians(np.stack(phase_4d, axis=-1), phase_wrap),
            reference_affine,
        ),
        phase_path,
    )
    del magnitude_4d, phase_4d, magnitude, phase
    averaged = run_preprocessing(
        magnitude_path,
        phase_path,
        work_dir,
        denoise=denoise,
        n_procs=n_procs,
    )
    return PreprocessedSeries(
        Path(averaged["magnitude"]),
        Path(averaged["phase"]),
        next(iter(magnitude_groups.values())),
        next(iter(phase_groups.values())),
    )
