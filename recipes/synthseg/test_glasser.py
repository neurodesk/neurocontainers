import importlib.util
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest

spec = importlib.util.spec_from_file_location('glasser', Path(__file__).with_name('glasser.py'))
glasser = importlib.util.module_from_spec(spec)
spec.loader.exec_module(glasser)


def _save(path, data, affine):
    image = nib.Nifti1Image(data, affine)
    image.set_qform(affine, 1)
    sform = affine.copy()
    image.set_sform(sform, 4)
    nib.save(image, path)
    return image


def test_preparation_uses_full_physical_x_and_midline_policy(tmp_path):
    affine = np.eye(4)
    affine[0] = [0, -2, 0, 2]
    affine[1] = [1, 0, 0, 0]
    labels = np.ones((2, 3, 2), dtype=np.int16)
    _save(tmp_path / 'atlas.nii.gz', labels, affine)
    _save(tmp_path / 'template.nii.gz', labels.astype(np.float32), affine)
    _save(tmp_path / 'mask.nii.gz', labels, affine)
    names = tmp_path / 'names.txt'
    names.write_text(''.join(f'{index} L_Parcel{index}_ROI\n' for index in range(1, 181)))
    glasser.prepare(tmp_path / 'atlas.nii.gz', names, tmp_path / 'template.nii.gz', tmp_path / 'mask.nii.gz', tmp_path / 'prepared')
    output = nib.load(tmp_path / 'prepared/atlas.nii.gz').get_fdata()
    assert np.all(output[:, 0, :] == 181)
    assert np.all(output[:, 1:, :] == 1)
    assert '181 R_Parcel1_ROI' in (tmp_path / 'prepared/labels.txt').read_text()


@pytest.mark.parametrize('invert', [False, True])
def test_real_registration_improves_translated_oblique_phantom(tmp_path, invert):
    pytest.importorskip('ants')
    from scipy.ndimage import shift

    grid = np.indices((40, 40, 40)).astype(float)
    x, y, z = grid
    brain = ((x - 20) / 13) ** 2 + ((y - 20) / 12) ** 2 + ((z - 20) / 11) ** 2 < 1
    intensity = (20 + x + 0.4 * y + 110 * np.exp(-((x - 15)**2 + (y - 16)**2 + (z - 18)**2) / 20)
                 + 65 * np.exp(-((x - 24)**2 + (y - 22)**2 + (z - 23)**2) / 12)) * brain
    labels = np.where(brain, np.where(x < 20, 7, 189), 0).astype(np.int16)
    angle = 0.27
    affine = np.array([[np.cos(angle), -1.3*np.sin(angle), 0, -20],
                       [np.sin(angle), 1.3*np.cos(angle), 0, -30],
                       [0, 0, 1.8, -35], [0, 0, 0, 1]])
    assets = tmp_path / 'assets'
    assets.mkdir()
    _save(assets / 'template.nii.gz', intensity.astype(np.float32), affine)
    _save(assets / 'mask.nii.gz', brain.astype(np.int16), affine)
    _save(assets / 'atlas.nii.gz', labels, affine)
    displacement = (4, -3, 2)
    shifted_brain = shift(brain.astype(np.int16), displacement, order=0)
    target = shift(labels, displacement, order=0)
    scan = shift(intensity, displacement, order=0)
    if invert:
        scan = np.where(shifted_brain, 200 - scan, 0)
    source = _save(tmp_path / 'scan.nii.gz', scan.astype(np.float32), affine)
    distinct_qform = affine.copy()
    distinct_qform[0, 3] += 0.25
    source.set_qform(distinct_qform, 1)
    nib.save(source, tmp_path / 'scan.nii.gz')
    tissue = shifted_brain.astype(np.int16) * 3
    if invert:
        tissue = np.where(target == 7, 1001, np.where(target == 189, 2001, 0)).astype(np.int16)
    _save(tmp_path / 'seg.nii.gz', tissue, affine)
    output_path = tmp_path / 'registered.nii.gz'
    glasser.register_glasser(tmp_path / 'scan.nii.gz', tmp_path / 'seg.nii.gz', output_path, threads=1, assets_dir=assets)
    output = nib.load(output_path)
    actual = np.asarray(output.dataobj)
    def accuracy(data):
        return np.count_nonzero((data == target) & (target > 0)) / np.count_nonzero(target)
    assert accuracy(actual) > 0.75
    assert accuracy(actual) > accuracy(labels) + 0.15
    assert set(np.unique(actual)) == {0, 7, 189}
    assert actual.dtype == np.int16
    assert not np.any(actual[shifted_brain == 0])
    np.testing.assert_array_equal(output.affine, source.affine)
    np.testing.assert_array_equal(output.get_qform(), source.get_qform())
    np.testing.assert_array_equal(output.get_sform(), source.get_sform())
    assert int(output.header['sform_code']) == 4
    assert int(output.header['qform_code']) == 1


def test_failed_registration_does_not_create_output(tmp_path):
    pytest.importorskip('ants')
    affine = np.eye(4)
    _save(tmp_path / 'scan.nii.gz', np.ones((4, 4, 4), np.float32), affine)
    other = affine.copy()
    other[0, 3] = 4
    _save(tmp_path / 'seg.nii.gz', np.full((4, 4, 4), 3, np.int16), other)
    with pytest.raises(ValueError, match='exact scan grid'):
        glasser._register(tmp_path / 'scan.nii.gz', tmp_path / 'seg.nii.gz', tmp_path / 'failed.nii.gz', tmp_path)
    assert not (tmp_path / 'failed.nii.gz').exists()
