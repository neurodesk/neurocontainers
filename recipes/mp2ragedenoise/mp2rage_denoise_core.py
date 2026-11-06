"""UNI background denoising and B1-map FOV extension.

Two small, optional preprocessing kernels that bring the Python package to
parity with the Rust/WASM core:

  * robust_combination  -> O'Brien (2014) robust UNI/background denoising, a
                           faithful port of RobustCombination.m. Removes the
                           salt-and-pepper background of the MP2RAGE UNI image
                           by regularising the two-inversion combination with a
                           noise term estimated from an INV2 corner slab.
  * extend_b1_fov       -> smooth low-order polynomial extrapolation of a
                           (relative) B1+ transmit field into brain voxels that
                           fell outside the measured B1-map field-of-view. Port
                           of crates/mp2rage-core/src/b1fill.rs.

References
----------
O'Brien et al., PLoS ONE 9:e99676 (2014)  -- robust background removal for UNI
"""
from __future__ import annotations
import numpy as np


# ============================================================================
# 1. Robust UNI combination  (port of RobustCombination.m, O'Brien 2014)
# ============================================================================
def robust_combination(uni, inv1, inv2, mf=6.0):
    """Denoise the MP2RAGE UNI image using the two inversion magnitudes.

    The UNI image is the noise-sensitive combination (INV1*INV2)/(INV1^2+INV2^2)
    of the two magnitude inversion images. O'Brien's robust combination adds a
    regularisation term beta to numerator and denominator so that low-signal
    background voxels collapse to the mid-grey level instead of random noise:

        robust = (INV1*INV2 - beta) / (INV1^2 + INV2^2 + 2*beta)

    beta is (mf * <INV2 corner>)^2, i.e. a multiple of the background noise
    estimated from an 11-voxel corner slab of INV2. Because Siemens stores the
    magnitude INV1/INV2 without sign, the correct sign of INV1 is recovered from
    the UNI value via the quadratic that links the (signed) INV1 to INV2 and the
    scaled UNI, then the root closer to the measured |INV1| is kept.

    Parameters
    ----------
    uni, inv1, inv2 : ndarray
        The MP2RAGE UNI image and the two inversion magnitude images, same shape.
    mf : float
        Regularisation / noise multiplication factor (O'Brien's default 6).

    Returns
    -------
    ndarray
        The denoised UNI. If UNI came in as a rescaled 0..4095 integer-valued
        image the output is rescaled back to 0..4095 (round half to even);
        otherwise the raw -0.5..0.5 combination is returned.
    """
    uni = uni.astype(float)
    inv1 = inv1.astype(float)
    inv2 = inv2.astype(float)

    # detect the 0..4095 Siemens-scaled UNI and map it to the model's -0.5..0.5
    maxv = uni.max()
    integer = uni.min() >= 0 and maxv >= 0.51
    u = (uni - maxv / 2.0) / maxv if integer else uni

    # sign-correct |INV1| using the sign of the (scaled) UNI, then pick the root
    # of the UNI quadratic that is closest to the measured magnitude
    inv1s = np.sign(u) * inv1
    with np.errstate(divide='ignore', invalid='ignore'):
        sq = np.sqrt(inv2 ** 2 - 4.0 * u ** 2 * inv2 ** 2)
        inv1pos = (-inv2 + sq) / (-2.0 * u)
        inv1neg = (-inv2 - sq) / (-2.0 * u)
    dpos = np.abs(inv1s - inv1pos)
    dneg = np.abs(inv1s - inv1neg)
    inv1final = inv1s.copy()
    inv1final = np.where(dpos > dneg, inv1neg, inv1final)
    inv1final = np.where(dpos <= dneg, inv1pos, inv1final)

    # noise floor from an 11-voxel corner slab of INV2 -> regularisation beta
    corner = inv2[:, -11:, -11:].mean()
    noise = mf * corner if corner != 0 else mf
    beta = noise ** 2

    robust = (inv1final * inv2 - beta) / (inv1final ** 2 + inv2 ** 2 + 2.0 * beta)
    return np.round(4095.0 * (robust + 0.5)) if integer else robust

