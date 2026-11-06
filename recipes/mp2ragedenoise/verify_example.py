"""Verify CLI pixels against the webapp core and round-trip the real example via MRD.

Run inside the container with the checksum-verified webapp example directory as argument.
The independent Rust core output must be named webapp-reference.nii.gz.
"""
from pathlib import Path
import sys
import numpy as np
import nibabel as nib
import ismrmrd
from mp2ragedenoise import denoise, process


def verify(directory):
    inputs = [nib.load(directory / f'MP2RAGE_{role}.nii.gz') for role in ('UNI', 'INV1', 'INV2')]
    result = denoise(*(im.get_fdata() for im in inputs))
    reference = nib.load(directory / 'webapp-reference.nii.gz').get_fdata()
    np.testing.assert_array_equal(result, reference)
    print(f'Webapp Rust parity: {result.size} voxels, maximum absolute error 0')
    images = []
    canonical = [nib.as_closest_canonical(im) for im in inputs]
    for series, im in enumerate(canonical):
        data = im.get_fdata()
        nx, ny, nz = data.shape
        affine = im.affine.copy()
        affine[:2] *= -1  # RAS to scanner LPS
        spacing = np.linalg.norm(affine[:3, :3], axis=0)
        read, phase, axis = (affine[:3, i] / spacing[i] for i in range(3))
        for z in range(nz):
            image = ismrmrd.Image.from_array(data[:, :, z].T.astype(np.float32), transpose=False)
            h = image.getHead()
            h.image_type = ismrmrd.IMTYPE_MAGNITUDE
            h.image_series_index = series
            h.slice = z
            h.read_dir = tuple(read)
            h.phase_dir = tuple(phase)
            h.slice_dir = tuple(axis)
            h.position = tuple((affine @ np.array([(nx-1)/2, (ny-1)/2, z, 1]))[:3])
            h.field_of_view = (nx*spacing[0], ny*spacing[1], spacing[2])
            image.setHead(h)
            images.append(image)
    class Connection:
        def __init__(self, source):
            self.source, self.outputs, self.errors, self.closed = source, [], [], False
        def __iter__(self):
            return iter(self.source)
        def send_image(self, images):
            self.outputs.extend(images)
        def send_logging(self, level, message):
            self.errors.append(message)
        def send_close(self):
            self.closed = True
    from mp2ragedenoise import SourceVolume
    for series, im in enumerate(canonical):
        v = SourceVolume.from_images([x for x in images if x.image_series_index == series])
        np.testing.assert_allclose(v.voxels, im.get_fdata(), rtol=1e-6)
    connection = Connection(list(reversed(images)))
    process(connection, 'mp2ragedenoise', None)
    assert not connection.errors, connection.errors
    assert connection.closed
    actual = np.stack([im.data[0, 0].T for im in connection.outputs], axis=2)
    expected = denoise(*(im.get_fdata() for im in canonical))
    np.testing.assert_array_equal(actual, expected)
    for output, source in zip(connection.outputs, images[:len(connection.outputs)]):
        np.testing.assert_allclose(output.position, source.position)
        assert output.image_series_index not in (0, 1, 2)
    missing = Connection([im for im in images if im.image_series_index != 2])
    process(missing, 'mp2ragedenoise', None)
    assert missing.closed and missing.errors and not missing.outputs
    print('MRD round-trip matches canonical reference; missing INV2 rejected')


if __name__ == '__main__':
    verify(Path(sys.argv[1]))
