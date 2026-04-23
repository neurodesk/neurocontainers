#!/usr/bin/env python3

import argparse
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np


def _extract_component(image_data, component):
    array = np.asarray(image_data).squeeze()
    if component == "complex":
        return np.asarray(array, dtype=np.complex64)
    if component == "real":
        return np.asarray(np.real(array), dtype=np.float32)
    if component == "imag":
        return np.asarray(np.imag(array), dtype=np.float32)
    return np.asarray(np.abs(array), dtype=np.float32)


def convert_mrd_to_nifti(input_path, output_path, group_name="dataset", image_series="image_0", component="magnitude"):
    dataset = ismrmrd.Dataset(str(input_path), group_name)
    try:
        image_count = dataset.number_of_images(image_series)
        if image_count == 0:
            raise RuntimeError(f"No images found in group '{group_name}' series '{image_series}'")

        first_image = dataset.read_image(image_series, 0)
        first_slice = _extract_component(first_image.data, component)
        nx, ny = first_slice.shape
        volume = np.zeros((nx, ny, image_count), dtype=first_slice.dtype)
        volume[:, :, 0] = first_slice

        for index in range(1, image_count):
            volume[:, :, index] = _extract_component(
                dataset.read_image(image_series, index).data,
                component,
            )

        fov = np.array([float(value) for value in first_image.field_of_view], dtype=np.float32)
        voxel_sizes = np.array(
            [
                fov[0] / max(nx, 1),
                fov[1] / max(ny, 1),
                fov[2] / max(image_count, 1),
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
            (nx - 1) * affine[:3, 0] + (ny - 1) * affine[:3, 1]
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
