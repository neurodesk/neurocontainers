#!/usr/bin/env python3

import argparse
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np


def _extract_component(image_data, component):
    array = np.asarray(image_data)
    if component == "complex":
        return np.asarray(array, dtype=np.complex64)
    if component == "real":
        return np.asarray(np.real(array), dtype=np.float32)
    if component == "imag":
        return np.asarray(np.imag(array), dtype=np.float32)
    return np.asarray(np.abs(array), dtype=np.float32)


def _to_canonical_volume(image, component):
    array = np.squeeze(_extract_component(image.data, component))

    if array.ndim == 0:
        return np.asarray(array.reshape((1, 1, 1)))

    if array.ndim == 1:
        return np.asarray(array[:, np.newaxis, np.newaxis])

    if array.ndim == 2:
        return np.asarray(array[:, :, np.newaxis])

    if array.ndim == 3:
        matrix_size = tuple(int(value) for value in image.getHead().matrix_size)
        if all(value > 0 for value in matrix_size):
            if array.shape == matrix_size:
                return np.asarray(array)
            if array.shape == matrix_size[::-1]:
                return np.asarray(array.transpose((2, 1, 0)))
        return np.asarray(array)

    raise ValueError(
        f"Unsupported ISMRMRD image data shape {np.asarray(image.data).shape} after squeezing to {array.shape}"
    )


def convert_mrd_to_nifti(input_path, output_path, group_name="dataset", image_series="image_0", component="magnitude"):
    dataset = ismrmrd.Dataset(str(input_path), group_name)
    try:
        image_count = dataset.number_of_images(image_series)
        if image_count == 0:
            raise RuntimeError(f"No images found in group '{group_name}' series '{image_series}'")

        first_image = dataset.read_image(image_series, 0)
        volume_blocks = [_to_canonical_volume(first_image, component)]

        for index in range(1, image_count):
            volume_blocks.append(
                _to_canonical_volume(
                    dataset.read_image(image_series, index),
                    component,
                )
            )

        nx, ny = volume_blocks[0].shape[:2]
        for block_index, block in enumerate(volume_blocks[1:], start=1):
            if block.shape[:2] != (nx, ny):
                raise ValueError(
                    f"Image {block_index} in series '{image_series}' has shape {block.shape}, "
                    f"expected matching in-plane shape {(nx, ny)}"
                )
        volume = np.concatenate(volume_blocks, axis=2)
        nz = volume.shape[2]

        fov = np.array([float(value) for value in first_image.field_of_view], dtype=np.float32)
        voxel_sizes = np.array(
            [
                fov[0] / max(nx, 1),
                fov[1] / max(ny, 1),
                fov[2] / max(nz, 1),
            ],
            dtype=np.float32,
        )

        head = first_image.getHead()
        read_dir = np.array([float(value) for value in head.read_dir], dtype=np.float32)
        phase_dir = np.array([float(value) for value in head.phase_dir], dtype=np.float32)
        slice_dir = np.array([float(value) for value in head.slice_dir], dtype=np.float32)
        slice_pos = np.array([float(value) for value in head.position], dtype=np.float32)

        affine = np.eye(4, dtype=np.float32)
        affine[:3, 0] = read_dir * voxel_sizes[0]
        affine[:3, 1] = phase_dir * voxel_sizes[1]
        affine[:3, 2] = slice_dir * voxel_sizes[2]
        affine[:3, 3] = slice_pos - 0.5 * (
            (nx - 1) * affine[:3, 0] + (ny - 1) * affine[:3, 1] + (nz - 1) * affine[:3, 2]
        )

        nifti = nib.Nifti1Image(volume, affine)
        nifti.header.set_data_dtype(volume.dtype)
        nifti.header.set_zooms(tuple(float(value) for value in voxel_sizes))
        nib.save(nifti, str(output_path))
    finally:
        dataset.close()

    print(
        f"Converted {input_path} [{image_series}] to {output_path} "
        f"with shape {volume.shape} and dtype {volume.dtype}"
    )


def main():
    parser = argparse.ArgumentParser(description="Convert ISMRMRD HDF5 image output to NIfTI.")
    parser.add_argument("-i", "--input", required=True, help="Input ISMRMRD HDF5 file")
    parser.add_argument("-o", "--output", required=True, help="Output NIfTI path")
    parser.add_argument("-g", "--group", default="dataset", help="ISMRMRD group name")
    parser.add_argument("-s", "--series", default="image_0", help="Image series name")
    parser.add_argument(
        "--component",
        choices=("magnitude", "complex", "real", "imag"),
        default="magnitude",
        help="Image component to write to NIfTI",
    )
    args = parser.parse_args()

    convert_mrd_to_nifti(
        input_path=Path(args.input),
        output_path=Path(args.output),
        group_name=args.group,
        image_series=args.series,
        component=args.component,
    )


if __name__ == "__main__":
    main()
