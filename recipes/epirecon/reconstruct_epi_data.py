#!/usr/bin/env python3
"""Standalone prototype for a fully-sampled Cartesian 3D EPI reconstruction.

Adapted from the TPI_gridding_N256_AC_coil_compression_kb.py workflow, but
Cartesian k-space needs neither a gridding kernel nor a density compensation
function: every readout already lands on a regular grid, so reconstruction is
a per-coil 3D FFT followed by root-sum-of-squares coil combination.
"""

from pathlib import Path

import ismrmrd
import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np

# --- Config ------------------------------------------------------------------
DATA_FILE = Path(__file__).parent / "local-test" / "input_data.h5"
GROUP_NAME = "dataset"
OUTPUT_NIFTI = Path(__file__).parent / "local-test" / "reconstructed_epi.nii.gz"

# Acquisitions carrying any of these flags are not imaging k-space lines (noise
# calibration, EPI ghost-correction navigators, dummy/feedback scans) and must
# be excluded when filling the k-space array.
NON_IMAGING_FLAGS = (
    ismrmrd.ACQ_IS_NOISE_MEASUREMENT,
    ismrmrd.ACQ_IS_PHASECORR_DATA,
    ismrmrd.ACQ_IS_NAVIGATION_DATA,
    ismrmrd.ACQ_IS_HPFEEDBACK_DATA,
    ismrmrd.ACQ_IS_RTFEEDBACK_DATA,
    ismrmrd.ACQ_IS_DUMMYSCAN_DATA,
)


def _is_imaging_acquisition(acquisition):
    return not any(acquisition.is_flag_set(flag) for flag in NON_IMAGING_FLAGS)


def load_kspace(data_file, group_name):
    """Read ISMRMRD acquisitions into a (coils, partitions, lines, samples) array.

    EPI zig-zags through k-space: alternate readouts are physically acquired
    back to front, and the scanner marks those with ACQ_IS_REVERSE. Placing
    them into the grid without reversing them first would scramble every
    other line of k-space, so that flag has to be honoured even though this
    is otherwise a plain Cartesian recon.
    """
    dataset = ismrmrd.Dataset(str(data_file), group_name, create_if_needed=False)
    try:
        header = ismrmrd.xsd.CreateFromDocument(dataset.read_xml_header())
        encoding = header.encoding[0]
        limits = encoding.encodingLimits
        num_lines = limits.kspace_encoding_step_1.maximum - limits.kspace_encoding_step_1.minimum + 1
        num_partitions = limits.kspace_encoding_step_2.maximum - limits.kspace_encoding_step_2.minimum + 1

        num_acquisitions = dataset.number_of_acquisitions()
        kspace = None
        filled = np.zeros((num_partitions, num_lines), dtype=bool)

        for acquisition_index in range(num_acquisitions):
            acquisition = dataset.read_acquisition(acquisition_index)
            if not _is_imaging_acquisition(acquisition):
                continue

            readout = np.asarray(acquisition.data, dtype=np.complex64)
            if acquisition.is_flag_set(ismrmrd.ACQ_IS_REVERSE):
                readout = readout[:, ::-1]

            if kspace is None:
                num_coils, num_samples = readout.shape
                kspace = np.zeros(
                    (num_coils, num_partitions, num_lines, num_samples),
                    dtype=np.complex64,
                )

            line = acquisition.idx.kspace_encode_step_1
            partition = acquisition.idx.kspace_encode_step_2
            kspace[:, partition, line, :] = readout
            filled[partition, line] = True
    finally:
        dataset.close()

    if kspace is None:
        raise ValueError(
            f"No imaging acquisitions found in {data_file}[{group_name}]. "
            "Every acquisition was flagged as noise/navigation/calibration data."
        )

    missing = np.count_nonzero(~filled)
    if missing:
        raise ValueError(
            f"K-space is not fully sampled: {missing} of {filled.size} "
            "(partition, line) positions were never acquired."
        )

    return kspace, encoding


def reconstruct_coil_images(kspace):
    """Per-coil 3D inverse FFT from Cartesian k-space to image space."""
    axes = (1, 2, 3)
    shifted = np.fft.ifftshift(kspace, axes=axes)
    images = np.fft.fftshift(np.fft.ifftn(shifted, axes=axes), axes=axes)
    return images.astype(np.complex64)


def crop_readout_oversampling(coil_images, recon_matrix_x):
    """Crop the readout axis back down from an oversampled acquisition matrix."""
    current_size = coil_images.shape[-1]
    if recon_matrix_x <= 0 or recon_matrix_x >= current_size:
        return coil_images

    start = (current_size - recon_matrix_x) // 2
    stop = start + recon_matrix_x
    return coil_images[..., start:stop]


def combine_coils_sos(coil_images):
    """Root-sum-of-squares coil combination."""
    return np.sqrt(np.sum(np.abs(coil_images) ** 2, axis=0)).astype(np.float32)


def _resolve_recon_matrix_x(encoding, num_samples):
    """Target readout size after removing acquisition oversampling.

    siemens_to_ismrmrd sets reconSpace.fieldOfView_mm.x correctly but leaves
    reconSpace.matrixSize.x at 0 for the (oversampled) readout axis, so that
    field can't be trusted on its own. Fall back to deriving the target size
    from the FOV ratio, which is exactly how much of the readout is real
    anatomy versus oversampled padding.
    """
    recon_matrix_x = int(encoding.reconSpace.matrixSize.x)
    if recon_matrix_x > 0:
        return recon_matrix_x

    encoded_fov_x = float(encoding.encodedSpace.fieldOfView_mm.x)
    recon_fov_x = float(encoding.reconSpace.fieldOfView_mm.x)
    if encoded_fov_x > 0 and recon_fov_x > 0:
        return round(num_samples * recon_fov_x / encoded_fov_x)

    return 0


def main():
    print(f"Loading {DATA_FILE} ...")
    kspace, encoding = load_kspace(DATA_FILE, GROUP_NAME)
    num_coils, num_partitions, num_lines, num_samples = kspace.shape
    print(
        f"Loaded k-space: {num_coils} coils, "
        f"{num_partitions} partitions x {num_lines} lines x {num_samples} samples"
    )

    print("Reconstructing with a per-coil 3D FFT...")
    coil_images = reconstruct_coil_images(kspace)

    recon_matrix_x = _resolve_recon_matrix_x(encoding, num_samples)
    print(f"Cropping readout axis from {num_samples} to {recon_matrix_x} samples")
    coil_images = crop_readout_oversampling(coil_images, recon_matrix_x)

    print("Combining coils with sum-of-squares...")
    image = combine_coils_sos(coil_images)
    print(f"Final image shape (partitions, lines, readout): {image.shape}")

    fov = encoding.reconSpace.fieldOfView_mm
    voxel_size_mm = (
        fov.x / image.shape[2] if image.shape[2] else 1.0,
        fov.y / image.shape[1] if image.shape[1] else 1.0,
        fov.z / image.shape[0] if image.shape[0] else 1.0,
    )
    affine = np.diag([*voxel_size_mm, 1.0])
    OUTPUT_NIFTI.parent.mkdir(parents=True, exist_ok=True)
    nib.save(nib.Nifti1Image(image.transpose(2, 1, 0), affine), str(OUTPUT_NIFTI))
    print(f"Saved {OUTPUT_NIFTI}")

    partition_mid, line_mid, readout_mid = (size // 2 for size in image.shape)
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    axes[0].imshow(image[partition_mid, :, :], cmap="gray")
    axes[0].set_title(f"Axial (partition={partition_mid})")
    axes[1].imshow(image[:, line_mid, :], cmap="gray")
    axes[1].set_title(f"Coronal (line={line_mid})")
    axes[2].imshow(image[:, :, readout_mid], cmap="gray")
    axes[2].set_title(f"Sagittal (readout={readout_mid})")
    for ax in axes:
        ax.axis("off")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
