"""
rigid2.py - 2D rigid-body registration by mutual information.

Python port of rigid2.m.

    im2_reg, delta = rigid2(im1, im2, n=None)

Aligns im2 with im1 using 2D rigid body registration
  - accepts a 2D image, or a stack of 2D images [nx, ny, ns]
  - n (optional) is the number of histogram bins (max 256)
  - im2_reg is returned registered (cubic interpolation, same dtype as im2)
  - delta is the shifts [dx, dy, zrot]

Ref: Lu et al (doi.org/10.1016/j.compmedimag.2007.12.001)

IMPORTANT - behaviour change vs rigid2.m
  In the MATLAB file the images are binned to uint8 (uim1/uim2) but those
  variables are then immediately overwritten with the raw images
  (uim1 = im1; uim2 = im2), so the *unbinned* data went into the histogram
  (which fails/misbehaves unless the data happen to be integers in 0..n-1).
  rigid3.m does it correctly. Here the binned images are used, as in rigid3.

Other notes: see rigid3.py (fminunc -> BFGS, interp2 'cubic' -> cubic spline,
histogram normalised, no GPU path).
"""
import numpy as np
from scipy.ndimage import map_coordinates
from scipy.optimize import minimize


# --------------------------------------------------------------------------
def rigid2(im1, im2, n=None, gtol=1e-6, maxiter=200, verbose=True):
    im1 = np.asarray(im1)
    im2 = np.asarray(im2)

    # ---- handle inputs
    if np.iscomplexobj(im1) or np.iscomplexobj(im2) \
            or not np.isfinite(im1).all() or not np.isfinite(im2).all():
        raise ValueError('im1 and im2 must be real valued and finite.')
    if im1.shape != im2.shape or im1.ndim < 2:
        raise ValueError('im1 and im2 must be 2d-3d arrays of the same size.')

    orig_shape, orig_dtype = im2.shape, im2.dtype
    if im1.ndim == 2:
        im1 = im1[..., None]
        im2 = im2[..., None]
    nx, ny, ns = im1.shape

    # Terrell-Scott rule (max 256)
    if n is None:
        n = int(min(np.ceil((2 * im1.size) ** (1 / 3)), 256))
    elif int(n) != n or n < 1 or n > 256:
        raise ValueError('number of histogram bins invalid.')

    # ---- convert to n bins
    b1 = _to_bins(im1, n)
    b2 = _to_bins(im2, n)

    # ---- calculate transforms: [dx dy zrot]
    cost = _make_cost(b1, b2, n)
    res = minimize(cost, np.zeros(3), jac=True, method='BFGS',
                   options={'gtol': gtol, 'maxiter': maxiter})
    delta = res.x
    if verbose:
        print('rigid2: ' + ' '.join(f'{d:+.1f}' for d in delta))

    # ---- apply transforms
    _, _, x2, y2, _, _ = _get_coords(nx, ny, delta)
    coords = np.stack([np.mod(x2, nx), np.mod(y2, ny)])   # wrap edges (0-based)

    out = np.empty(im2.shape, dtype=np.float64)
    for s in range(ns):
        curr = im2[..., s].astype(np.float64)
        S, L = curr.min(), curr.max()                     # preserve bounds
        curr = map_coordinates(curr, coords, order=3, mode='constant', cval=0.0)
        out[..., s] = np.clip(curr, S, L)

    if np.issubdtype(orig_dtype, np.integer):
        out = np.floor(out + 0.5)
    return out.astype(orig_dtype).reshape(orig_shape), delta


# --------------------------------------------------------------------------
def _to_bins(im, n):
    """Convert image to uint8 from 0 to n-1."""
    im = np.asarray(im, dtype=np.float64)
    im = im - im.min()
    mx = im.max()
    if mx == 0:
        raise ValueError('image is constant, cannot register.')
    return np.floor(im / mx * (n - 1) + 0.5).astype(np.uint8)


def _get_coords(nx, ny, delta):
    """Coordinates of im2 for the given [dx dy zrot]."""
    # coordinates of im1 (centred at 0 0)
    x1, y1 = np.meshgrid(np.arange(nx) - nx / 2, np.arange(ny) - ny / 2,
                         indexing='ij')
    sindz = np.sin(delta[2] * np.pi / 180)
    cosdz = np.cos(delta[2] * np.pi / 180)
    # coordinates of im2 (centred at nx/2 ny/2)
    x2 = nx / 2 + delta[0] + x1 * cosdz - y1 * sindz
    y2 = ny / 2 + delta[1] + x1 * sindz + y1 * cosdz
    return x1, y1, x2, y2, sindz, cosdz


# --------------------------------------------------------------------------
def _make_cost(im1, im2, n):
    """cost(delta) -> (fval, grad): negative mutual information by joint
    histogram estimation (hpv) and its analytic gradient."""
    nx, ny, ns = im1.shape
    a_n = im1.reshape(nx * ny, ns).ravel(order='F').astype(np.int64) * n
    im2f = im2.reshape(nx * ny, ns)

    def cost(delta):
        x1, y1, x2, y2, sindz, cosdz = _get_coords(nx, ny, delta)

        # joint histogram and partial derivatives [dx dy dzrot]
        h = np.zeros((n * n, 4))
        fx, fy = np.floor(x2), np.floor(y2)

        # convolution with 4x4 Hann window
        for i in range(-1, 3):
            ix = fx + i
            dx = x2 - ix
            ixm = np.mod(ix, nx).astype(np.int64) * ny
            cosdx, sindx = np.cos(np.pi * dx / 2), np.sin(np.pi * dx / 2)
            for j in range(-1, 3):
                iy = fy + j
                dy = y2 - iy
                iym = np.mod(iy, ny).astype(np.int64)
                cosdy, sindy = np.cos(np.pi * dy / 2), np.sin(np.pi * dy / 2)

                # histogram indices
                b = im2f[(ixm + iym).ravel(), :].ravel(order='F')
                index = a_n + b

                # function (k=0) and partial derivatives (k=1-3)
                f = [None] * 4
                f[0] = (1 + cosdx) * (1 + cosdy)                      # f
                f[1] = -(1 + cosdy) * sindx * np.pi / 2               # df/dx
                f[2] = -(1 + cosdx) * sindy * np.pi / 2               # df/dy
                f[3] = (-f[1] * (x1 * sindz + y1 * cosdz) * np.pi / 180
                        + f[2] * (x1 * cosdz - y1 * sindz) * np.pi / 180)  # df/dzrot

                for k in range(4):
                    w = np.tile(f[k].ravel(), ns)
                    h[:, k] += np.bincount(index, weights=w, minlength=n * n)

        h /= h[:, 0].sum()                       # normalise (see rigid3.py)
        h3 = h.reshape(n, n, 4)
        pA = h3.sum(axis=1)
        pB = h3.sum(axis=0)

        fval = _plogp(pA[:, 0]) + _plogp(pB[:, 0]) - _plogp(h[:, 0])
        grad = (_plogdp(pA[:, 0], pA[:, 1:]) + _plogdp(pB[:, 0], pB[:, 1:])
                - _plogdp(h[:, 0], h[:, 1:]))
        return float(fval), grad.astype(np.float64)

    return cost


def _plogp(p):
    p = np.where(p <= 0, 1.0, p)
    return np.sum(p * np.log(p))


def _plogdp(p, dp):
    p = np.where(p <= 0, np.exp(-1.0), p)
    return np.sum((1 + np.log(p))[:, None] * dp, axis=0)
