from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from pathlib import Path

import numpy as np


DIRECT_NATIVE_FIELDS = (
    "shimnativebaseline",
    "shimnativelower",
    "shimnativeupper",
    "shimisocentrerasmm",
)


class NativeChannel(Enum):
    X = ("X", 1, "uT/m")
    Y = ("Y", 1, "uT/m")
    Z = ("Z", 1, "uT/m")
    Z2 = ("Z2", 2, "uT/m^2")
    ZX = ("ZX", 2, "uT/m^2")
    ZY = ("ZY", 2, "uT/m^2")
    X2_MINUS_Y2 = ("X2-Y2", 2, "uT/m^2")
    XY = ("XY", 2, "uT/m^2")

    @property
    def label(self) -> str:
        return self.value[0]

    @property
    def unit(self) -> str:
        return self.value[2]


@dataclass(frozen=True)
class NativeChannelEstimate:
    channel: NativeChannel
    baseline: float
    absolute: float
    bounds: tuple[float, float]

    def record(self) -> dict[str, object]:
        return {
            "name": self.channel.label,
            "unit": self.channel.unit,
            "baseline": self.baseline,
            "absolute": self.absolute,
            "bounds": self.bounds,
        }


@dataclass(frozen=True)
class _AnalyticalModel:
    configuration_id: str
    orders: tuple[int, ...]
    patient_ras_mm_to_shim_lai_mm: tuple[tuple[float, ...], ...]
    channels: tuple[NativeChannel, ...]
    absolute_native_bounds: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class AnalyticalShimResult:
    model: _AnalyticalModel
    settings: tuple[NativeChannelEstimate, ...]
    measured_std_hz: float
    predicted_std_hz: float
    predicted_field_hz: np.ndarray | None = field(default=None, repr=False, compare=False)
    status: str = "available"
    model_kind: str = "siemens_analytical"
    validation: str = "ideal_field_estimate"
    basis_model_id: str = "siemens-ideal-1st2nd-v1"

    @property
    def comment(self) -> str:
        labeled = ", ".join(
            f"{s.channel.label}={s.absolute:.9g} {s.channel.unit}"
            for s in self.settings
        )
        return (
            f"MAGNETOM Cima.X 3T ABSOLUTE ANALYTICAL ESTIMATE: {labeled}; "
            f"configuration={self.model.configuration_id}; ideal field estimate; "
            f"predicted ROI std Hz {self.measured_std_hz:.6g} -> "
            f"{self.predicted_std_hz:.6g}"
        )

    def write(self, output_dir: Path) -> None:
        record = {
            "status": self.status,
            "model_kind": self.model_kind,
            "validation": self.validation,
            "basis_model_id": self.basis_model_id,
            "configuration_id": self.model.configuration_id,
            "scanner_model": "MAGNETOM Cima.X",
            "field_strength_t": 3,
            "orders": self.model.orders,
            "settings_mode": "absolute",
            "patient_ras_mm_to_shim_lai_mm": self.model.patient_ras_mm_to_shim_lai_mm,
            "absolute_native_bounds": {
                c.label: b
                for c, b in zip(self.model.channels, self.model.absolute_native_bounds)
            },
            "settings": [s.record() for s in self.settings],
            "measured_std_hz": self.measured_std_hz,
            "predicted_std_hz": self.predicted_std_hz,
            "comment": self.comment,
        }
        (output_dir / "shim_settings.json").write_text(
            json.dumps(record, indent=2, allow_nan=False) + "\n"
        )


def _number(value: object) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not np.isfinite(value)
    ):
        raise ValueError("Native settings and bounds must be finite numbers")
    return float(value)


def _read_model(path: str | Path) -> _AnalyticalModel:
    return _validate_model(json.loads(Path(path).read_text()))


def _validate_model(raw: object) -> _AnalyticalModel:
    keys = {
        "configuration_id",
        "scanner_model",
        "field_strength_t",
        "orders",
        "patient_ras_mm_to_shim_lai_mm",
        "absolute_native_bounds",
    }
    if not isinstance(raw, dict) or set(raw) != keys:
        raise ValueError("Analytical model requires exactly the documented schema keys")
    if (
        raw["scanner_model"] != "MAGNETOM Cima.X"
        or isinstance(raw["field_strength_t"], bool)
        or raw["field_strength_t"] != 3
    ):
        raise ValueError("Analytical model requires MAGNETOM Cima.X at 3 T")
    identity = raw["configuration_id"]
    if not isinstance(identity, str) or not identity.strip():
        raise ValueError("A nonempty configuration_id is required")
    orders = raw["orders"]
    if orders not in ([1], [1, 2]) or any(type(o) is not int for o in orders):
        raise ValueError("Analytical orders must be [1] or [1, 2]")
    transform = np.asarray(raw["patient_ras_mm_to_shim_lai_mm"], dtype=float)
    if (
        transform.shape != (4, 4)
        or not np.isfinite(transform).all()
        or not np.allclose(transform[3], [0, 0, 0, 1], rtol=0, atol=1e-8)
        or not np.allclose(
            transform[:3, :3].T @ transform[:3, :3], np.eye(3), rtol=0, atol=1e-6
        )
        or not np.isclose(np.linalg.det(transform[:3, :3]), 1, rtol=0, atol=1e-6)
    ):
        raise ValueError(
            "Explicit proper rigid RAS mm to shim LAI mm transform required"
        )
    channels = tuple(c for c in NativeChannel if c.value[1] in orders)
    raw_bounds = raw["absolute_native_bounds"]
    if not isinstance(raw_bounds, dict) or set(raw_bounds) != {
        c.label for c in channels
    }:
        raise ValueError("Native bounds must name exactly the selected channels")
    bounds = []
    for c in channels:
        pair = raw_bounds[c.label]
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("Each native bound must be a lower/upper pair")
        lower, upper = map(_number, pair)
        if lower > upper:
            raise ValueError("Native bounds must be ordered")
        bounds.append((lower, upper))
    return _AnalyticalModel(
        identity, tuple(orders), tuple(map(tuple, transform)), channels, tuple(bounds)
    )


def _native_vector(value: object, name: str, lengths: tuple[int, ...]) -> list[float]:
    message = f"{name} requires {' or '.join(map(str, lengths))} finite numbers"
    if not isinstance(value, str):
        raise ValueError(message)
    if any(not part.strip() for part in value.split(",")):
        raise ValueError(message)
    try:
        numbers = [_number(float(v)) for v in value.replace(",", " ").split()]
    except ValueError as exc:
        raise ValueError(message) from exc
    if len(numbers) not in lengths:
        raise ValueError(message)
    return [v if v else 0.0 for v in numbers]


def direct_native_inputs(
    baseline: object, lower: object, upper: object, isocentre_ras_mm: object
) -> tuple[dict, dict[str, float]] | None:
    values = (baseline, lower, upper, isocentre_ras_mm)
    supplied = tuple(
        v is not None and not (isinstance(v, str) and not v.strip()) for v in values
    )
    if not any(supplied):
        return None
    for name, present in zip(DIRECT_NATIVE_FIELDS, supplied):
        if not present:
            raise ValueError(f"Direct HFS native inputs require {name}")
    baseline, lower, upper, isocentre = (
        _native_vector(value, name, (3,) if i == 3 else (3, 8))
        for i, (name, value) in enumerate(zip(DIRECT_NATIVE_FIELDS, values))
    )
    if not len(baseline) == len(lower) == len(upper):
        raise ValueError(
            "shimnativebaseline, shimnativelower and shimnativeupper must have matching lengths"
        )
    orders = [1] if len(baseline) == 3 else [1, 2]
    channels = tuple(c for c in NativeChannel if c.value[1] in orders)
    transform = np.eye(4)
    transform[:3, :3] = np.diag([-1, 1, -1])
    transform[:3, 3] = -transform[:3, :3] @ isocentre
    geometry = {
        "orders": orders,
        "patient_ras_mm_to_shim_lai_mm": transform.tolist(),
        "absolute_native_bounds": {
            c.label: [lo, hi] for c, lo, hi in zip(channels, lower, upper)
        },
    }
    identity = hashlib.sha256(
        json.dumps(
            geometry, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()
    model = {
        "configuration_id": f"entered-hfs-{identity}",
        "scanner_model": "MAGNETOM Cima.X",
        "field_strength_t": 3,
        **geometry,
    }
    return model, dict(zip((c.label for c in channels), baseline))


def _profiles(
    shape: tuple[int, ...], affine: np.ndarray, model: _AnalyticalModel
) -> np.ndarray:
    from shimmingtoolbox.coils.spher_harm_basis import siemens_basis

    voxels = np.indices(shape).reshape(3, -1)
    patient = affine[:3, :3] @ voxels + affine[:3, 3, None]
    transform = np.asarray(model.patient_ras_mm_to_shim_lai_mm)
    lai = transform[:3, :3] @ patient + transform[:3, 3, None]
    # siemens_basis applies these flips internally to its RAS input.
    ras = lai * np.array([-1, 1, -1])[:, None]
    return siemens_basis(*(v.reshape(shape) for v in ras), orders=model.orders)


def _compute_analytical(
    field: np.ndarray,
    mask: np.ndarray,
    affine: np.ndarray,
    model_path: str | Path | dict,
    acquisition_native: str | dict,
) -> AnalyticalShimResult:
    model = (
        _validate_model(model_path)
        if isinstance(model_path, dict)
        else _read_model(model_path)
    )
    raw = (
        json.loads(acquisition_native)
        if isinstance(acquisition_native, str)
        else acquisition_native
    )
    if not isinstance(raw, dict) or set(raw) != {c.label for c in model.channels}:
        raise ValueError(
            "Native acquisition settings must name exactly the selected channels"
        )
    baseline = np.array([_number(raw[c.label]) for c in model.channels])
    bounds = np.array(model.absolute_native_bounds)
    if np.any(baseline < bounds[:, 0]) or np.any(baseline > bounds[:, 1]):
        raise ValueError("Acquisition native settings exceed absolute bounds")
    field, mask, affine = (
        np.asarray(field, float),
        np.asarray(mask),
        np.asarray(affine, float),
    )
    if (
        field.ndim != 3
        or mask.shape != field.shape
        or not np.isfinite(field).all()
        or not np.isfinite(mask).all()
        or not np.any(mask)
        or affine.shape != (4, 4)
        or not np.isfinite(affine).all()
        or not np.allclose(affine[3], [0, 0, 0, 1], rtol=0, atol=1e-8)
        or abs(np.linalg.det(affine[:3, :3])) < 1e-12
    ):
        raise ValueError(
            "Finite field, nonempty mask and valid RAS affine are required"
        )
    mask = mask != 0
    profiles = _profiles(field.shape, affine, model)
    if not np.isfinite(profiles).all():
        raise ValueError("Analytical geometry produced nonfinite native fields")
    centered = profiles - profiles[mask].mean(axis=0)
    free = bounds[:, 0] != bounds[:, 1]
    absolute = bounds[:, 0].copy()
    baseline_field = profiles @ baseline
    fixed_field = profiles[..., ~free] @ absolute[~free]
    target = field - baseline_field + fixed_field
    target -= target[mask].mean()
    if np.any(free):
        from shimmingtoolbox.coils.coil import Coil
        from shimmingtoolbox.optimizer.slsqp_optimizer import SlsqpOptimizer

        scales = np.sqrt(np.mean(centered[mask][:, free] ** 2, axis=0))
        roundoff = 64 * np.finfo(float).eps * np.max(np.abs(profiles[mask][:, free]), axis=0)
        if np.any(scales <= roundoff) or not np.isfinite(scales).all():
            raise ValueError(
                "Free native channels lack independent spatial effects in ROI"
            )
        normalized = centered[..., free] / scales
        singular = np.linalg.svd(normalized[mask], compute_uv=False)
        tolerance = max(normalized[mask].shape) * np.finfo(float).eps * singular[0]
        if np.count_nonzero(singular > tolerance) != free.sum():
            raise ValueError(
                "Free native channels lack independent spatial effects in ROI"
            )
        if np.max(np.abs(target[mask])) <= 64 * np.finfo(float).eps * max(
            1,
            np.max(np.abs(field[mask])),
            np.max(np.abs(baseline_field[mask])),
            np.max(np.abs(fixed_field[mask])),
        ):
            # A constant adds only a constant objective term for centered profiles,
            # and avoids Toolbox's zero-field shortcut that bypasses constraints.
            feasible_zero = np.clip(np.zeros(free.sum()), bounds[free, 0], bounds[free, 1])
            physical_effect = centered[mask][:, free] @ feasible_zero
            seed_scale = float(np.sqrt(np.mean(physical_effect ** 2)))
            if seed_scale > 0:
                # Scale variables as well as the objective so tiny constrained
                # fields retain native coefficient accuracy at SLSQP tolerance.
                scales /= seed_scale
            target = np.ones_like(target)
        coil = Coil(
            normalized,
            affine,
            {
                "name": model.configuration_id,
                "coef_channel_minmax": {
                    "coil": (bounds[free] * scales[:, None]).tolist()
                },
                "coef_sum_max": None,
            },
        )
        optimizer = SlsqpOptimizer(
            [coil], target, affine, opt_criteria="mse", reg_factor=0
        )
        # Native quadratic effects can be tiny; a least-squares start avoids
        # losing physical accuracy to SLSQP's fixed objective stopping tolerance.
        start = np.linalg.lstsq(normalized[mask], -target[mask], rcond=None)[0]
        start = np.clip(start, bounds[free, 0] * scales, bounds[free, 1] * scales)
        # Toolbox's property setter needs coefficients as a second argument,
        # which ordinary property assignment cannot provide.
        type(optimizer).initial_guess_method.fset(optimizer, "set", start)
        scaled = optimizer.optimize(mask.astype(int), tuple(range(field.shape[2])))
        absolute[free] = np.asarray(scaled, float) / scales
    tolerance = 1e-7 * np.maximum(1, np.max(np.abs(bounds), axis=1))
    if (
        not np.isfinite(absolute).all()
        or np.any(absolute < bounds[:, 0] - tolerance)
        or np.any(absolute > bounds[:, 1] + tolerance)
    ):
        raise ValueError("Optimizer returned invalid absolute native settings")
    absolute = np.clip(absolute, bounds[:, 0], bounds[:, 1])
    predicted = field + profiles @ (absolute - baseline)
    before, after = float(np.std(field[mask])), float(np.std(predicted[mask]))
    if after > before + 1e-6 * max(1, before):
        raise ValueError(
            "Analytical estimate worsens the feasible acquisition baseline"
        )
    settings = tuple(
        NativeChannelEstimate(c, float(b), float(a), limits)
        for c, b, a, limits in zip(
            model.channels, baseline, absolute, model.absolute_native_bounds
        )
    )
    return AnalyticalShimResult(model, settings, before, after, predicted)
