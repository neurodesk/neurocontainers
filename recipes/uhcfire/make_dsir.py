"""
make_dsir.py - Python port of make_dSIR.m

    dsir, lsir, asir = make_dsir(im1, im2, thresh=0.01, register=True,
                                 iters=100, Z=1)

Makes dSIR images from two IR images (or stack of images).
Performs a primitive registration and thresholding.

  - im1 is the short TI image [nx, ny, nz]
  - im2 is the long TI image  [nx, ny, nz]
  - thresh is a threshold as a fraction of max [0.01]
  - register is True/False to register images [True]
  - iters is the number of mask-filter iterations [100]

Returned images are scaled to int16 (x10000) for conversion to DICOM, with
MATLAB-style saturation (+-Inf / out of range values clip to the int16 limits).
"""
import numpy as np
from scipy import ndimage as ndi

from rigid2 import rigid2


def to_int16(x):
    """MATLAB-style int16(): round half away from zero, saturate, NaN -> 0."""
    x = np.nan_to_num(np.asarray(x, dtype=np.float64),
                      nan=0.0, posinf=32767.0, neginf=-32768.0)
    x = np.sign(x) * np.floor(np.abs(x) + 0.5)
    return np.clip(x, -32768, 32767).astype(np.int16)


def make_dsir(im1, im2, thresh=None, register=True, iters=None, Z=None):
    # ---- input validation
    im1 = np.asarray(im1)
    im2 = np.asarray(im2)
    if im1.shape != im2.shape:
        raise ValueError('size mismatch between im1 and im2')
    thresh = 0.01 if thresh is None else thresh
    iters = 100 if iters is None else iters
    Z = 1 if Z is None else Z

    squeeze_back = im1.ndim == 2
    if squeeze_back:
        im1, im2 = im1[..., None], im2[..., None]

    im1 = im1.astype(np.float32)
    im2 = im2.astype(np.float32)

    # ---- registration
    if register:
        im2, _ = rigid2(im1, im2)
        im2 = im2.astype(np.float32)

    # ---- create dsir | lsir | asir (division by zero gives inf/nan, like MATLAB)
    with np.errstate(divide='ignore', invalid='ignore'):
        add = im1 + im2
        sub = im1 - im2

        dsir = sub / add
        lsir = np.arctanh(dsir)

        R = im1 / im2
        p = 1
        q = 1
        asir = np.arctanh(np.minimum(np.abs(R / Z) ** p, np.abs(Z / R) ** q))

        if np.count_nonzero(R < 0):
            asir = -np.sign(R) * asir

    # ---- background removal (dodgy)
    # image mask
    mask = add > thresh * add.max()

    # tidy up (majority filter): 11x11 in-plane window, zero padded, more than
    # half of the 121 pixels set.  uniform_filter is the fast separable
    # equivalent of convn(mask, ones(11,11), 'same') > 121/2.
    maskf = mask.astype(np.float32)
    for _ in range(iters):
        maskf = (ndi.uniform_filter(maskf, size=(11, 11, 1),
                                    mode='constant', cval=0.0) > 0.5
                 ).astype(np.float32)
    mask = maskf > 0

    # a little dilation (6-connected 3x3x3 cross)
    se = ndi.generate_binary_structure(3, 1)
    mask = ndi.binary_dilation(mask, structure=se)

    # hole fill (slice by slice)
    for z in range(mask.shape[2]):
        mask[:, :, z] = ndi.binary_fill_holes(mask[:, :, z])

    # set background to lowest signal
    dsir[~mask] = -1
    lsir[~mask] = -np.inf
    asir[~mask] = -np.inf

    # ---- convert to integer
    out = [to_int16(10000 * a) for a in (dsir, lsir, asir)]
    if squeeze_back:
        out = [o[..., 0] for o in out]
    return tuple(out)
