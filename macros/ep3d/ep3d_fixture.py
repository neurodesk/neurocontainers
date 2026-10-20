"""Synthetic dual-polarity MRI shared by the 3D EPI runtime checks."""

from dataclasses import dataclass

import numpy as np
from scipy.ndimage import gaussian_filter, shift


@dataclass(frozen=True)
class Phantom:
    magnitude: np.ndarray
    tissue_phase: np.ndarray
    truth: np.ndarray
    core: np.ndarray
    signals: list[np.ndarray]


def dual_polarity_phantom() -> Phantom:
    """Generate four noisy repetitions, with a known translation in the last two."""
    rng = np.random.default_rng(0)
    shape = (48, 48, 32)
    x, y, z = np.meshgrid(*[np.linspace(-1, 1, n) for n in shape], indexing="ij")
    radius = x**2 / 0.8 + y**2 / 0.8 + z**2 / 0.7
    magnitude = np.where(
        radius < 1,
        1000 + 400 * gaussian_filter(rng.standard_normal(shape), 2),
        0,
    )
    tissue_phase = gaussian_filter(rng.standard_normal(shape), 2.5) / 0.05 * 0.4
    phase = 1.2 * x + 0.8 * z + tissue_phase
    polarity_phase = 1.3 * (0.5 + y)
    truth = magnitude * np.exp(1j * (phase + polarity_phase))
    signals = []
    for repetition, offset in enumerate([(0, 0, 0)] * 2 + [(1.5, -1.0, 0)] * 2):
        sign = 1 if repetition % 2 == 0 else -1
        signal = magnitude * np.exp(1j * (phase + sign * polarity_phase))
        signal = shift(signal.real, offset, order=3) + 1j * shift(
            signal.imag, offset, order=3
        )
        signal += 80 * (rng.standard_normal(shape) + 1j * rng.standard_normal(shape))
        signals.append(signal)
    return Phantom(magnitude, tissue_phase, truth, radius < 0.6, signals)
