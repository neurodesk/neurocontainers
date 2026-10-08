from dataclasses import asdict, dataclass
import json
from pathlib import Path

import nibabel as nib
import numpy as np

from b0mapromeo_analytic import AnalyticalShimResult, _compute_analytical


@dataclass(frozen=True)
class ShimResult:
    status: str
    comment: str
    calibration_id: str | None = None
    channels: tuple[str, ...] = ()
    absolute_current_a: tuple[float, ...] = ()
    acquisition_current_a: tuple[float, ...] = ()
    measured_std_hz: float | None = None
    predicted_std_hz: float | None = None
    scanner_model: str = "MAGNETOM Cima.X"
    field_strength_t: int = 3
    settings_mode: str = "absolute"
    current_units: str = "A"
    profile_units: str = "Hz/A"

    def write(self, output_dir: Path) -> None:
        (output_dir / "shim_settings.json").write_text(
            json.dumps(asdict(self), indent=2, allow_nan=False) + "\n"
        )


def unavailable() -> ShimResult:
    return ShimResult(
        "unavailable",
        "MAGNETOM Cima.X 3T shim unavailable: calibrated Hz/A coil profiles "
        "and acquisition currents are required for ABSOLUTE A settings",
    )


def compute_shim(
    field: np.ndarray,
    mask: np.ndarray,
    affine: np.ndarray,
    calibration_path: str | Path | None = None,
    acquisition_current_a: str | list | dict | None = None,
    *,
    analytical_model_path: str | Path | None = None,
    acquisition_native: str | dict | None = None,
) -> ShimResult | AnalyticalShimResult:
    """Fit spatial variance with signed profiles; positive A adds profile Hz."""
    calibration_path, acquisition_current_a, analytical_model_path, acquisition_native = (
        None if isinstance(value, str) and value == "" else value
        for value in (calibration_path, acquisition_current_a,
                      analytical_model_path, acquisition_native)
    )
    measured = calibration_path is not None or acquisition_current_a is not None
    analytical = analytical_model_path is not None or acquisition_native is not None
    if measured and analytical:
        raise ValueError("Measured and analytical shim inputs cannot be combined")
    if analytical:
        if analytical_model_path is None or acquisition_native is None:
            raise ValueError("Analytical model and native acquisition settings must both be supplied")
        return _compute_analytical(field, mask, affine, analytical_model_path, acquisition_native)
    if not measured:
        return unavailable()
    if not calibration_path or acquisition_current_a is None:
        raise ValueError("Shim calibration and acquisition currents must both be supplied")
    path = Path(calibration_path)
    calibration = json.loads(path.read_text())
    if not isinstance(calibration, dict):
        raise ValueError("Shim calibration must be an object")
    expected = {
        "scanner_model": "MAGNETOM Cima.X",
        "field_strength_t": 3,
        "profile_units": "Hz/A",
        "current_units": "A",
        "settings_mode": "absolute",
    }
    for key, value in expected.items():
        if calibration.get(key) != value:
            raise ValueError(f"Shim calibration requires {key}={value}")
    identity = calibration.get("calibration_id")
    channels = calibration.get("channels")
    if not isinstance(identity, str) or not identity.strip():
        raise ValueError("A nonempty calibration_id is required")
    if (not isinstance(channels, list) or not channels
            or any(not isinstance(c, str) or not c.strip() for c in channels)
            or len(set(channels)) != len(channels)):
        raise ValueError("Unique nonempty channel labels are required")
    n = len(channels)
    bounds = np.asarray(calibration.get("absolute_current_bounds_a"), dtype=float)
    if (bounds.shape != (n, 2) or not np.isfinite(bounds).all()
            or np.any(bounds[:, 0] > bounds[:, 1])):
        raise ValueError("Finite ordered absolute current bounds are required per channel")
    limit = calibration.get("total_absolute_current_limit_a")
    if limit is not None and (not isinstance(limit, (int, float))
                              or not np.isfinite(limit) or limit < 0):
        raise ValueError("Total absolute current limit must be finite and nonnegative")
    raw = acquisition_current_a
    if isinstance(raw, str):
        raw = json.loads(raw)
    if isinstance(raw, dict):
        if set(raw) != set(channels):
            raise ValueError("Acquisition currents must name exactly the calibrated channels")
        raw = [raw[c] for c in channels]
    baseline = np.asarray(raw, dtype=float)
    if baseline.shape != (n,) or not np.isfinite(baseline).all():
        raise ValueError("Finite acquisition currents are required for every channel")

    def feasible(currents: np.ndarray) -> bool:
        return bool(
            np.all(currents >= bounds[:, 0] - 1e-7)
            and np.all(currents <= bounds[:, 1] + 1e-7)
            and (limit is None or np.abs(currents).sum() <= limit + 1e-7)
        )

    if not feasible(baseline):
        raise ValueError("Acquisition currents exceed calibrated absolute constraints")
    profile_name = calibration.get("coil_profiles")
    if not isinstance(profile_name, str) or not profile_name:
        raise ValueError("coil_profiles must identify a registered NIfTI file")
    profile_image = nib.load(path.parent / profile_name)
    profiles = profile_image.get_fdata(dtype=np.float64)
    field = np.asarray(field, dtype=float)
    mask = np.asarray(mask)
    affine = np.asarray(affine, dtype=float)
    if (field.ndim != 3 or mask.shape != field.shape or not np.isfinite(field).all()
            or not np.isfinite(mask).all() or not np.any(mask)
            or affine.shape != (4, 4) or not np.isfinite(affine).all()
            or abs(np.linalg.det(affine[:3, :3])) < 1e-12):
        raise ValueError("Finite field, nonempty mask and valid RAS affine are required")
    mask = mask != 0
    if (profiles.shape != field.shape + (n,) or not np.isfinite(profiles).all()
            or not np.allclose(profile_image.affine, affine, rtol=0, atol=1e-4)):
        raise ValueError("Coil profiles must match the fieldmap RAS shape and affine exactly")
    centered_profiles = profiles - profiles[mask].mean(axis=0)
    if np.linalg.matrix_rank(centered_profiles[mask]) != n:
        raise ValueError("Coil profiles must have independent spatial effects in the ROI")
    unshimmed = field - np.einsum("...c,c->...", profiles, baseline)
    unshimmed -= unshimmed[mask].mean()
    if limit == 0 or (np.all(unshimmed[mask] == 0) and feasible(np.zeros(n))):
        absolute = np.zeros(n)
    else:
        from shimmingtoolbox.coils.coil import Coil
        from shimmingtoolbox.optimizer.slsqp_optimizer import SlsqpOptimizer

        if np.all(unshimmed[mask] == 0):
            # Toolbox bypasses constraints for zero fields. Centered profiles
            # make a constant offset orthogonal to every channel, so adding
            # 1 Hz changes the objective by a constant without changing its fit.
            unshimmed = np.ones_like(unshimmed)
        coil = Coil(centered_profiles, affine, {
            "name": identity,
            "coef_channel_minmax": {"coil": bounds.tolist()},
            "coef_sum_max": limit,
        })
        absolute = np.asarray(SlsqpOptimizer(
            [coil], unshimmed, affine, opt_criteria="mse", reg_factor=0
        ).optimize(
            mask.astype(int), tuple(range(field.shape[2]))
        ), dtype=float)
    if absolute.shape != (n,) or not np.isfinite(absolute).all() or not feasible(absolute):
        raise ValueError("Optimizer returned invalid absolute currents")
    predicted = field + np.einsum("...c,c->...", profiles, absolute - baseline)
    before = float(np.std(field[mask]))
    after = float(np.std(predicted[mask]))
    labeled = ", ".join(f"{channel}={current:.9g}" for channel, current in zip(channels, absolute))
    comment = (f"MAGNETOM Cima.X 3T ABSOLUTE A: {labeled}; calibration={identity}; "
               f"predicted ROI std Hz {before:.6g} -> {after:.6g}")
    return ShimResult("available", comment, identity, tuple(channels),
                      tuple(absolute.tolist()), tuple(baseline.tolist()), before, after)
