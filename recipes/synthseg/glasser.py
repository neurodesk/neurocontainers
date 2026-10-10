"""Approximate native-volume Glasser parcels from the ICBM2009a atlas."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import nibabel as nib
import numpy as np


def _volume(path: str | Path) -> tuple[nib.Nifti1Image, np.ndarray]:
    image = nib.load(str(path))
    data = np.asarray(image.dataobj)
    if data.ndim != 3 or not np.isfinite(data).all():
        raise ValueError(f"Expected a finite 3D volume: {path}")
    if (
        not np.isfinite(image.affine).all()
        or abs(np.linalg.det(image.affine[:3, :3])) < 1e-8
    ):
        raise ValueError(f"Invalid physical geometry: {path}")
    return image, data


def _same_grid(first: nib.Nifti1Image, second: nib.Nifti1Image) -> bool:
    return first.shape == second.shape and np.allclose(
        first.affine, second.affine, atol=1e-5, rtol=0,
    )


def _labels(data: np.ndarray, maximum: int) -> None:
    if (
        np.any(data < 0) or np.any(data > maximum)
        or not np.allclose(data, np.rint(data), atol=1e-4, rtol=0)
    ):
        raise ValueError(f"Expected integral atlas labels in 0..{maximum}")
    if not np.any(data):
        raise ValueError("Atlas contains no foreground labels")


def _save_labels(data: np.ndarray, source: nib.Nifti1Image, output: str | Path) -> None:
    result = nib.Nifti1Image(data.astype(np.int16), source.affine, source.header.copy())
    result.set_data_dtype(np.int16)
    result.header.set_slope_inter(1, 0)
    result.set_qform(source.get_qform(), int(source.header['qform_code']))
    result.set_sform(source.get_sform(), int(source.header['sform_code']))
    nib.save(result, str(output))


def prepare(
    atlas: str | Path, names: str | Path, template: str | Path,
    mask: str | Path, destination: str | Path,
) -> None:
    """Split shared IDs by physical RAS x; assign labelled x=0 to the left."""
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    image, labels = _volume(atlas)
    reference, _ = _volume(template)
    brain, foreground = _volume(mask)
    _labels(labels, 180)
    if not _same_grid(image, reference) or not _same_grid(reference, brain):
        raise ValueError("Atlas, ICBM2009a T1 and mask must share their physical grid")
    if not np.any(foreground):
        raise ValueError("Template brain mask is empty")
    coordinates = np.indices(labels.shape, dtype=np.float32)
    physical_x = sum(
        image.affine[0, axis] * coordinates[axis] for axis in range(3)
    ) + image.affine[0, 3]
    split = np.rint(labels).astype(np.int16)
    split[(labels > 0) & (physical_x > 0)] += 180
    _save_labels(split, image, destination / 'atlas.nii.gz')
    entries = [line.split() for line in Path(names).read_text().splitlines() if line.strip()]
    if (
        [int(entry[0]) for entry in entries] != list(range(1, 181))
        or any(not entry[1].startswith('L_') for entry in entries)
    ):
        raise ValueError("Expected the published 180 left parcel names")
    table = ['0 Background']
    table += [f'{index} {name}' for index, name in entries]
    table += [f'{int(index) + 180} R_{name[2:]}' for index, name in entries]
    (destination / 'labels.txt').write_text('\n'.join(table) + '\n')
    nib.save(reference, str(destination / 'template.nii.gz'))
    nib.save(brain, str(destination / 'mask.nii.gz'))


def register_glasser(
    scan: str | Path, segmentation: str | Path, output: str | Path,
    *, threads: int, assets_dir: str | Path = '/opt/glasser',
) -> None:
    """Write labels 0..360 on the exact source grid, or raise without an output.

    A child process isolates ITK's thread environment from the scanner server.
    """
    if not isinstance(threads, int) or threads < 1:
        raise ValueError("Registration threads must be a positive integer")
    environment = os.environ.copy()
    environment['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = str(threads)
    environment['OMP_NUM_THREADS'] = str(threads)
    environment['ANTS_RANDOM_SEED'] = '1729'
    subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), 'register', str(scan),
         str(segmentation), str(output), '--assets-dir', str(assets_dir)],
        env=environment, check=True,
    )


def _register(
    scan: str | Path, segmentation: str | Path, output: str | Path, assets_dir: str | Path,
) -> None:
    import ants

    ants.config.set_ants_deterministic(on=False, seed_value=1729)

    source, intensity = _volume(scan)
    segmented, tissue = _volume(segmentation)
    if not _same_grid(source, segmented):
        raise ValueError("SynthSeg segmentation must share the exact scan grid")
    if np.any(tissue < 0) or not np.array_equal(tissue, np.rint(tissue)):
        raise ValueError("SynthSeg labels must be nonnegative integers")
    cortex = (
        np.isin(tissue, [3, 42])
        | ((tissue >= 1000) & (tissue <= 1035))
        | ((tissue >= 2000) & (tissue <= 2035))
    )
    if not np.any(cortex) or np.ptp(intensity[tissue > 0]) <= 0:
        raise ValueError("Scan needs cortical foreground and nonconstant brain intensities")
    assets = Path(assets_dir)
    atlas_image, atlas_data = _volume(assets / 'atlas.nii.gz')
    template_image, _ = _volume(assets / 'template.nii.gz')
    mask_image, mask_data = _volume(assets / 'mask.nii.gz')
    _labels(atlas_data, 360)
    if (
        not _same_grid(atlas_image, template_image)
        or not _same_grid(template_image, mask_image) or not np.any(mask_data)
    ):
        raise ValueError("Packaged atlas, template and brain mask geometry disagree")
    with tempfile.TemporaryDirectory(prefix='glasser-') as workspace:
        # ITK prefers qform while nibabel prefers sform. Use the authoritative
        # nibabel affine for working files and retain both original forms below.
        fixed_path = Path(workspace) / 'fixed.nii.gz'
        working = nib.Nifti1Image(intensity.astype(np.float32), source.affine)
        working.set_qform(source.affine, 1)
        working.set_sform(source.affine, 1)
        nib.save(working, fixed_path)
        fixed = ants.image_read(str(fixed_path))
        moving = ants.image_read(str(assets / 'template.nii.gz'))
        fixed_mask = fixed.new_image_like((tissue > 0).astype(np.float32))
        moving_mask = ants.image_read(str(assets / 'mask.nii.gz'))
        affine = ants.registration(
            fixed=fixed * fixed_mask, moving=moving * moving_mask,
            type_of_transform='Affine', mask=fixed_mask, moving_mask=moving_mask,
            mask_all_stages=True, aff_metric='mattes',
            aff_iterations=(100, 60, 20, 0), aff_shrink_factors=(8, 4, 2, 1),
            aff_smoothing_sigmas=(3, 2, 1, 0),
            outprefix=str(Path(workspace) / 'affine'),
        )
        registration = ants.registration(
            fixed=fixed * fixed_mask, moving=moving * moving_mask,
            type_of_transform='SyNOnly', initial_transform=affine['fwdtransforms'],
            mask=fixed_mask, moving_mask=moving_mask, syn_metric='mattes',
            reg_iterations=(40, 20, 0),
            outprefix=str(Path(workspace) / 'deformable'),
        )
        warped = ants.apply_transforms(
            fixed=fixed, moving=ants.image_read(str(assets / 'atlas.nii.gz')),
            transformlist=registration['fwdtransforms'], interpolator='nearestNeighbor',
        )
        labels = warped.numpy()
        if labels.shape != source.shape:
            raise ValueError("Registered atlas does not match source dimensions")
        _labels(labels, 360)
        if not set(np.unique(labels)).issubset(set(np.unique(atlas_data)) | {0}):
            raise ValueError("Registration introduced unknown parcel IDs")
        labels[~cortex] = 0
        _labels(labels, 360)
        staged = Path(workspace) / 'labels.nii.gz'
        _save_labels(labels, source, staged)
        check, _ = _volume(staged)
        if not _same_grid(source, check):
            raise ValueError("Saved atlas geometry differs from the scan")
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(staged, output)


def _main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    preparation = commands.add_parser('prepare')
    for name in ('atlas', 'names', 'template', 'mask', 'destination'):
        preparation.add_argument(name)
    registration = commands.add_parser('register')
    for name in ('scan', 'segmentation', 'output'):
        registration.add_argument(name)
    registration.add_argument('--assets-dir', default='/opt/glasser')
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.atlas, args.names, args.template, args.mask, args.destination)
    else:
        _register(args.scan, args.segmentation, args.output, args.assets_dir)


if __name__ == '__main__':
    _main()
