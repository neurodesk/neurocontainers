from concurrent.futures import ThreadPoolExecutor
import threading
import h5py
import numpy as np
import scipy.ndimage as ndi
import numba

# Optional: For a significant speed-up in the FFT calculation
try:
    import pyfftw
    pyfftw.interfaces.cache.enable()
    use_pyfftw = True
except ImportError:
    use_pyfftw = False

# --- Config ------------------------------------------------------------------
# Some of these config values should be adjustable in the .json file!
N           = 128
NUM_COILS   = 16
COIL_VARIANCE_RETENTION = 0.9 #.95 could be better, but .9 seems fine
COIL_COMBINE_MODE = 'AC'  # 'AC' for adaptive coil combine, 'SoS' for sum of squares
FOV_CM      = 22
OVERSAMP    = 2
DCF_ITER    = 5
MAX_WORKERS = 8
N4_SHRINK_FACTOR = 2
N4_MAX_ITERATIONS = [50, 50, 50, 50]
KB_KERNEL_WIDTH = 3.0 # 3 is a good trade off for 1H and 23Na
KB_BETA = np.pi * np.sqrt((KB_KERNEL_WIDTH / OVERSAMP * (OVERSAMP - 0.5))**2 - 0.8)
KB_I0_BETA = np.i0(KB_BETA)
KB_LUT_SIZE = 2048
KB_LUT_DISTANCES = np.linspace(0.0, 0.5 * KB_KERNEL_WIDTH, KB_LUT_SIZE, dtype=np.float64)
KB_LUT = (
    np.i0(KB_BETA * np.sqrt(np.maximum(0.0, 1.0 - (KB_LUT_DISTANCES / (0.5 * KB_KERNEL_WIDTH))**2)))
    / KB_I0_BETA
).astype(np.float64)

# --- Sharpness Control ---
FERMI_CUTOFF = .98 # Higher value = sharper image. (e.g., 0.9 to 1.0)
FERMI_WIDTH  = 0.05 # Smaller value = sharper transition.

# --- Memory Control ---
MAX_SIMULTANEOUS_GRIDS = 2
grid_semaphore = threading.Semaphore(MAX_SIMULTANEOUS_GRIDS)

TRAJ_FILE = '/Users/clicht/Data_7T/23Na/Sofia_20260317/23Na_n50_trajectory.h5'
#TRAJ_FILE = '/Users/clicht/Desktop/Python/n72_TPI_traj_new.h5'
#TRAJ_FILE = '/Users/clicht/Desktop/Python/n28_TPI_23Na.h5'
DATA_FILE = '/Users/clicht/Data_7T/23Na/Sofia_20260317/23Na_n50_TE50_7T_Sofia_1.h5'
#DATA_FILE ="/Users/clicht/Data_7T/For_Chiadika/10272025/TPI_1H_TE0_14.h5"
#DATA_FILE = '/Users/clicht/Data_3T/FIRE_sodiumNUFFT/23Na_FIRE_n28.h5'
#DATA_FILE = "/Users/clicht/Data_7T/23Na/20260616_23Na_MESTIM_Shailja/23Na_n28_7T_Shailja_TR55_TE05.h5"

def compress_coils_by_variance(coil_data, variance_retention=0.90, eps=1e-12):
    """Compress physical coils into virtual coils retaining target signal energy."""
    num_input_coils = coil_data.shape[0]
    if num_input_coils <= 1:
        return coil_data, np.array([1.0], dtype=np.float32), np.eye(num_input_coils, dtype=np.complex64)

    data_2d = coil_data.reshape(num_input_coils, -1)
    covariance = data_2d @ data_2d.conj().T
    covariance /= max(data_2d.shape[1], 1)

    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    sort_idx = np.argsort(eigenvalues)[::-1]
    eigenvalues = np.maximum(eigenvalues[sort_idx].real, 0.0)
    eigenvectors = eigenvectors[:, sort_idx]

    total_variance = eigenvalues.sum()
    if total_variance <= eps:
        print("Coil compression skipped: coil covariance has near-zero total variance.")
        return coil_data, np.ones(num_input_coils, dtype=np.float32), np.eye(num_input_coils, dtype=np.complex64)

    cumulative_variance = np.cumsum(eigenvalues) / total_variance
    variance_retention = np.clip(variance_retention, 0.0, 1.0)
    num_virtual_coils = np.searchsorted(cumulative_variance, variance_retention) + 1
    compression_matrix = eigenvectors[:, :num_virtual_coils].astype(np.complex64)
    compressed_data = np.einsum('cv,c...->v...', compression_matrix.conj(), coil_data, optimize=True)

    print(
        f"Coil compression: {num_input_coils} physical coils -> {num_virtual_coils} virtual coils "
        f"({100.0 * cumulative_variance[num_virtual_coils - 1]:.2f}% variance retained)"
    )
    print("Cumulative coil variance:", np.array2string(cumulative_variance[:num_virtual_coils], precision=4))
    return compressed_data.astype(np.complex64), cumulative_variance.astype(np.float32), compression_matrix

# --- Load data and trajectory ------------------------------------
print("Loading trajectory and data...")
with h5py.File(TRAJ_FILE, 'r') as f: k_1_cm_full = f['k'][...]
with h5py.File(DATA_FILE, 'r') as f:
    data_dset = f['data']
    if data_dset.ndim == 2:
        data_shape = data_dset.shape
        all_coil_data = data_dset[:data_shape[0], :data_shape[1]][None, ...].astype(np.complex64)
    elif data_dset.ndim == 3:
        actual_num_coils = min(NUM_COILS, data_dset.shape[0])
        data_shape = data_dset[0, :, :].shape
        all_coil_data = data_dset[:actual_num_coils, :data_shape[0], :data_shape[1]].astype(np.complex64)
    else:
        raise ValueError(f"Expected data to be 2D or 3D, got shape {data_dset.shape}")
#    all_coil_data = f['data'][:NUM_COILS, :data_shape[0], 0::2].astype(np.complex64)
NUM_COILS = all_coil_data.shape[0]
k_1_cm = k_1_cm_full[:data_shape[0], :data_shape[1]]

print("Loading trajectory and data done")
print(f"Loaded {NUM_COILS} channel(s) with data shape {data_shape}")

# --- THIS IS THE FIX ---
k_physical = k_1_cm.reshape(-1, 3) # Changed --1 back to -1

k_nyquist = 1.0 / (2.0 * FOV_CM / N)
k_norm_pre = (k_physical / k_nyquist / 2.0).astype(np.float32)

abs_k = np.linalg.norm(k_1_cm, axis=-1)
abs_k_norm = abs_k / np.nanmax(abs_k)
fermi_filter = 1.0 / (1.0 + np.exp((abs_k_norm - FERMI_CUTOFF) / FERMI_WIDTH))

def preprocess_physical_coil_kspace(coil_data):
    max_vals = np.abs(coil_data).max(axis=0)
    h1 = np.histogram(ndi.gaussian_filter1d(max_vals, 5), bins=40)
    most_common_max = h1[1][np.argmax(h1[0])]
    high_readout_vals = max_vals[max_vals >= 0.95 * most_common_max]
    high_readout_std = high_readout_vals.std() if high_readout_vals.size > 0 else max_vals.std()
    threshold = most_common_max - 3 * high_readout_std
    bad_readouts = np.where(max_vals < threshold)[0]

    data_weights = np.ones(coil_data.shape, dtype=np.uint8)
    if len(bad_readouts) > 0:
        data_weights[:, bad_readouts] = 0

    processed_data = coil_data * data_weights
    center_idx = processed_data.shape[1] // 2
    center_signal = np.abs(processed_data)[:, center_idx - 5:center_idx + 5].mean()
    if center_signal > 1e-8:
        processed_data *= 2.0 / center_signal

    return (processed_data * fermi_filter).astype(np.complex64)

print(f"Preprocessing {NUM_COILS} physical coils before compression...")
all_coil_data = np.stack(
    [preprocess_physical_coil_kspace(all_coil_data[k]) for k in range(NUM_COILS)],
    axis=0
).astype(np.complex64)

all_coil_data, coil_variance, coil_compression_matrix = compress_coils_by_variance(
    all_coil_data, variance_retention=COIL_VARIANCE_RETENTION
)
NUM_COILS = all_coil_data.shape[0]

print(f"Configuration: N={N}, Oversampling={OVERSAMP}, Fermi Cutoff={FERMI_CUTOFF}")
print(f"Kaiser-Bessel gridding: width={KB_KERNEL_WIDTH:.1f}, beta={KB_BETA:.3f}")
print(f"Using pyFFTW for acceleration: {use_pyfftw}")

# === Numba Gridding Kernels ==============================
@numba.jit(nopython=True, fastmath=True)
def _kaiser_bessel_weight_lut(distance, kernel_width, lut):
    half_width = 0.5 * kernel_width
    if distance > half_width:
        return 0.0
    lut_pos = distance / half_width * (len(lut) - 1)
    idx0 = int(np.floor(lut_pos))
    if idx0 >= len(lut) - 1:
        return lut[len(lut) - 1]
    frac = lut_pos - idx0
    return lut[idx0] * (1.0 - frac) + lut[idx0 + 1] * frac

@numba.jit(nopython=True)
def _grid_kb_complex(grid, k_coords_norm, weights, grid_size, oversamp, kernel_width, lut):
    og = grid_size * oversamp
    num_points = len(k_coords_norm)
    half_width = 0.5 * kernel_width
    for i in range(num_points):
        gx = (k_coords_norm[i, 0] + 0.5) * og - 0.5
        gy = (k_coords_norm[i, 1] + 0.5) * og - 0.5
        gz = (k_coords_norm[i, 2] + 0.5) * og - 0.5
        ix_min = int(np.ceil(gx - half_width)); ix_max = int(np.floor(gx + half_width))
        iy_min = int(np.ceil(gy - half_width)); iy_max = int(np.floor(gy + half_width))
        iz_min = int(np.ceil(gz - half_width)); iz_max = int(np.floor(gz + half_width))
        w = weights[i]
        for iz in range(iz_min, iz_max + 1):
            if 0 <= iz < og:
                wz = _kaiser_bessel_weight_lut(abs(gz - iz), kernel_width, lut)
                for iy in range(iy_min, iy_max + 1):
                    if 0 <= iy < og:
                        wy = _kaiser_bessel_weight_lut(abs(gy - iy), kernel_width, lut)
                        for ix in range(ix_min, ix_max + 1):
                            if 0 <= ix < og:
                                wx = _kaiser_bessel_weight_lut(abs(gx - ix), kernel_width, lut)
                                grid[iz, iy, ix] += w * wx * wy * wz

@numba.jit(nopython=True, fastmath=True)
def _grid_kb_real(grid, k_coords_norm, weights, grid_size, oversamp, kernel_width, lut):
    og = grid_size * oversamp
    num_points = len(k_coords_norm)
    half_width = 0.5 * kernel_width
    for i in range(num_points):
        gx = (k_coords_norm[i, 0] + 0.5) * og - 0.5
        gy = (k_coords_norm[i, 1] + 0.5) * og - 0.5
        gz = (k_coords_norm[i, 2] + 0.5) * og - 0.5
        ix_min = int(np.ceil(gx - half_width)); ix_max = int(np.floor(gx + half_width))
        iy_min = int(np.ceil(gy - half_width)); iy_max = int(np.floor(gy + half_width))
        iz_min = int(np.ceil(gz - half_width)); iz_max = int(np.floor(gz + half_width))
        w = weights[i]
        for iz in range(iz_min, iz_max + 1):
            if 0 <= iz < og:
                wz = _kaiser_bessel_weight_lut(abs(gz - iz), kernel_width, lut)
                for iy in range(iy_min, iy_max + 1):
                    if 0 <= iy < og:
                        wy = _kaiser_bessel_weight_lut(abs(gy - iy), kernel_width, lut)
                        for ix in range(ix_min, ix_max + 1):
                            if 0 <= ix < og:
                                wx = _kaiser_bessel_weight_lut(abs(gx - ix), kernel_width, lut)
                                grid[iz, iy, ix] += w * wx * wy * wz

@numba.jit(nopython=True, fastmath=True)
def _sample_kb(values, grid, k_coords_norm, grid_size, oversamp, kernel_width, lut):
    og = grid_size * oversamp
    num_points = len(k_coords_norm)
    half_width = 0.5 * kernel_width
    for i in range(num_points):
        gx = (k_coords_norm[i, 0] + 0.5) * og - 0.5
        gy = (k_coords_norm[i, 1] + 0.5) * og - 0.5
        gz = (k_coords_norm[i, 2] + 0.5) * og - 0.5
        ix_min = int(np.ceil(gx - half_width)); ix_max = int(np.floor(gx + half_width))
        iy_min = int(np.ceil(gy - half_width)); iy_max = int(np.floor(gy + half_width))
        iz_min = int(np.ceil(gz - half_width)); iz_max = int(np.floor(gz + half_width))
        val = 0.0
        for iz in range(iz_min, iz_max + 1):
            if 0 <= iz < og:
                wz = _kaiser_bessel_weight_lut(abs(gz - iz), kernel_width, lut)
                for iy in range(iy_min, iy_max + 1):
                    if 0 <= iy < og:
                        wy = _kaiser_bessel_weight_lut(abs(gy - iy), kernel_width, lut)
                        for ix in range(ix_min, ix_max + 1):
                            if 0 <= ix < og:
                                wx = _kaiser_bessel_weight_lut(abs(gx - ix), kernel_width, lut)
                                val += grid[iz, iy, ix] * wx * wy * wz
        values[i] = val

# === Main Functions ==============================================
def compute_dcf_kb(k_coords_norm, grid_size, oversamp, n_iter=10):
    og = grid_size * oversamp
    dcf = np.ones(len(k_coords_norm), dtype=np.float64)
    print(f"  DCF Iterations: ", end="")
    for i in range(n_iter):
        print(f"{i+1}...", end="", flush=True)
        wsum = np.zeros((og, og, og), dtype=np.float64)
        _grid_kb_real(wsum, k_coords_norm, dcf, grid_size, oversamp, KB_KERNEL_WIDTH, KB_LUT)
        sampled_density = np.zeros_like(dcf)
        _sample_kb(sampled_density, wsum, k_coords_norm, grid_size, oversamp, KB_KERNEL_WIDTH, KB_LUT)
        sampled_density[sampled_density < 1e-9] = 1.0
        dcf /= sampled_density
        dcf /= np.median(dcf)
    print(" Done.")
    return dcf.astype(np.float32)

def compute_deapodisation_kb(grid_size, oversamp):
    og = grid_size * oversamp
    kernel_1d = np.zeros(og, dtype=np.complex128)
    g_center = 0.5 * og - 0.5
    half_width = 0.5 * KB_KERNEL_WIDTH
    ix_min = int(np.ceil(g_center - half_width))
    ix_max = int(np.floor(g_center + half_width))
    for ix in range(ix_min, ix_max + 1):
        if 0 <= ix < og:
            distance = abs(g_center - ix)
            ratio = distance / half_width
            kernel_1d[ix] = np.i0(KB_BETA * np.sqrt(max(0.0, 1.0 - ratio * ratio))) / KB_I0_BETA

    response_1d = np.fft.ifftshift(np.fft.ifft(np.fft.ifftshift(kernel_1d))) * og
    start = (og - grid_size) // 2
    end = start + grid_size
    deapo_1d = np.abs(response_1d[start:end])
    deapo = np.multiply.outer(np.multiply.outer(deapo_1d, deapo_1d).ravel(), deapo_1d)
    deapo = deapo.reshape(grid_size, grid_size, grid_size)
    deapo /= np.nanmax(deapo)
    deapo = np.where(deapo < 1e-8, 1e-8, deapo)
    return deapo.astype(np.float32)

def regrid_3d_kb(kspace_data, k_coords_norm, grid_size, dcf, deapo, oversamp):
    og = grid_size * oversamp
    data_dcf = (kspace_data * dcf).astype(np.complex128)
    grid = np.zeros((og, og, og), dtype=np.complex128)
    _grid_kb_complex(grid, k_coords_norm, data_dcf, grid_size, oversamp, KB_KERNEL_WIDTH, KB_LUT)
    shifted_grid = np.fft.ifftshift(grid)
    if use_pyfftw:
        ifft_obj = pyfftw.builders.ifftn(shifted_grid, auto_align_input=True, auto_contiguous=True, threads=-1)
        transformed_grid = ifft_obj()
    else:
        transformed_grid = np.fft.ifftn(shifted_grid)
    img_os = np.fft.ifftshift(transformed_grid) * (og**3)
    start = (og - grid_size) // 2
    end = start + grid_size
    img = img_os[start:end, start:end, start:end].copy()
    img /= deapo
    return img.astype(np.complex64)


# --- Precomputation, Coil Processing, and Combination (Logic Unchanged) ---
print('Computing DCF...')
dcf_precomputed = compute_dcf_kb(k_norm_pre, grid_size=N, oversamp=OVERSAMP, n_iter=DCF_ITER)
print('Computing deapodisation...')
deapo_precomputed = compute_deapodisation_kb(N, OVERSAMP)
print('Ready for reconstruction.')

def process_coil(coil_k, coil_data):
    with grid_semaphore:
        img = regrid_3d_kb(
            coil_data.ravel(), k_norm_pre,
            grid_size=N, dcf=dcf_precomputed, deapo=deapo_precomputed,
            oversamp=OVERSAMP
        )
    return coil_k, img

NUFFT_all_coils = np.zeros([NUM_COILS, N, N, N], dtype=np.complex64)
print(f"Reconstructing {NUM_COILS} coils at {N}³ with {MAX_WORKERS} workers...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_coil, k, all_coil_data[k]) for k in range(NUM_COILS)]
    for i, future in enumerate(futures):
        k, img = future.result()
        NUFFT_all_coils[k] = img
        if (i + 1) % 4 == 0 or (i + 1) == NUM_COILS:
            print(f"Completed {i + 1}/{NUM_COILS} coils")

def estimate_sensitivities(img_coils, smooth_sigma=None, eps=1e-8):
    C, X, Y, Z = img_coils.shape
    if smooth_sigma is None: smooth_sigma = X / 32.0
    print(f"Using sensitivity smoothing sigma: {smooth_sigma:.1f} voxels")
    sens_smooth = np.zeros_like(img_coils, dtype=np.complex64)
    for c in range(C):
        sr = ndi.gaussian_filter(img_coils[c].real, smooth_sigma)
        si = ndi.gaussian_filter(img_coils[c].imag, smooth_sigma)
        sens_smooth[c] = sr + 1j * si
    rss = np.sqrt(np.sum(np.abs(sens_smooth) ** 2, axis=0)) + eps
    sens_smooth /= rss[None, ...]
    return sens_smooth.astype(np.complex64)

def combine_coils(img_coils, mode='AC'):
    mode_normalized = mode.strip().upper()
    if img_coils.shape[0] == 1:
        print("Single-channel data: skipping coil combination.")
        return np.abs(img_coils[0])

    if mode_normalized == 'SOS':
        print("Performing sum-of-squares coil combination...")
        return np.sqrt(np.sum(np.abs(img_coils) ** 2, axis=0)).astype(np.float32)

    if mode_normalized == 'AC':
        print("Estimating coil sensitivities...")
        sens_maps = estimate_sensitivities(img_coils)
        print("Performing adaptive coil combination...")
        img_combined = np.sum(np.conj(sens_maps) * img_coils, axis=0)
        return np.abs(img_combined).astype(np.float32)

    raise ValueError("COIL_COMBINE_MODE must be 'AC' or 'SoS'")

img_combined_AC = combine_coils(NUFFT_all_coils, mode=COIL_COMBINE_MODE)

print(f"\nFinal image shape: {img_combined_AC.shape}")
print(f"Coil combination mode: {COIL_COMBINE_MODE}")
print(f"Final voxel size: {FOV_CM/N:.3f} cm")
print("Reconstruction complete.")

# --- N4 Bias Field Correction -----------------------------------------------
import SimpleITK as sitk


def n4_bias_field_correct(image_xyz):
    image_zyx = image_xyz.swapaxes(0, 2).astype(np.float32)
    sitk_image = sitk.GetImageFromArray(image_zyx)

    mask = sitk.OtsuThreshold(sitk_image, 0, 1, 512)
    mask = sitk.BinaryMorphologicalClosing(mask, [9, 9, 9])
    mask = sitk.BinaryFillhole(mask)

    if N4_SHRINK_FACTOR > 1:
        shrink = [int(N4_SHRINK_FACTOR)] * sitk_image.GetDimension()
        correction_image = sitk.Shrink(sitk_image, shrink)
        correction_mask = sitk.Shrink(mask, shrink)
    else:
        correction_image = sitk_image
        correction_mask = mask

    corrector = sitk.N4BiasFieldCorrectionImageFilter()
    corrector.SetMaximumNumberOfIterations(N4_MAX_ITERATIONS)
    corrector.SetConvergenceThreshold(0.001)
    corrector.SetSplineOrder(3)
    corrector.SetWienerFilterNoise(0.1)
    corrector.SetBiasFieldFullWidthAtHalfMaximum(0.15) # this should also be in .json file to tune!

    corrector.Execute(correction_image, correction_mask)
    log_bias_field = corrector.GetLogBiasFieldAsImage(sitk_image)
    corrected_sitk = sitk_image / sitk.Exp(log_bias_field)
    corrected_xyz = sitk.GetArrayFromImage(corrected_sitk).swapaxes(0, 2)
    return corrected_xyz.astype(np.float32), corrected_sitk, mask


print(f"Running N4 bias field correction with shrink factor {N4_SHRINK_FACTOR}...")
img_combined_AC_n4, corrected_image, n4_mask = n4_bias_field_correct(img_combined_AC)
print("N4 bias field correction complete.")

# --- Compare Original vs N4-Corrected Image ---------------------------------
import matplotlib.pyplot as plt

axial_idx = N // 2 - 20
sagittal_idx = N // 2
coronal_idx = N // 2 - 10

img_original = np.abs(img_combined_AC).swapaxes(0, 2)
img_n4 = np.abs(img_combined_AC_n4).swapaxes(0, 2)

slice_pairs = [
    (img_original[:, :, axial_idx], img_n4[:, :, axial_idx], f"Axial (z={axial_idx})"),
    (img_original[sagittal_idx, :, :], img_n4[sagittal_idx, :, :], f"Sagittal (x={sagittal_idx})"),
    (img_original[:, coronal_idx, :], img_n4[:, coronal_idx, :], f"Coronal (y={coronal_idx})"),
]

fig, axes = plt.subplots(2, 3, figsize=(12, 7))
for col, (original_slice, n4_slice, title) in enumerate(slice_pairs):
    original_slice = np.rot90(original_slice)
    n4_slice = np.rot90(n4_slice)
    vmax = np.percentile(np.concatenate([original_slice.ravel(), n4_slice.ravel()]), 99.5)

    axes[0, col].imshow(original_slice, cmap='gray', vmin=0, vmax=vmax)
    axes[0, col].set_title(f"Original {title}")
    axes[1, col].imshow(n4_slice, cmap='gray', vmin=0, vmax=vmax)
    axes[1, col].set_title(f"N4 Corrected {title}")

for ax in axes.ravel():
    ax.axis('off')

plt.tight_layout()
plt.show()
