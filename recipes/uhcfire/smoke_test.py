"""Exercise separate acquisition calls against persisted data and real MRD images."""
import os
import tempfile

import ismrmrd
import numpy as np

from uhcfire import reconstruct


def images(value):
    result = []
    for z in range(2):
        image = ismrmrd.Image.from_array(np.full((1, 1, 64, 64), value, np.float32), transpose=False)
        header = image.getHead()
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        header.slice = z
        header.position = (0, 0, z)
        image.setHead(header)
        result.append(image)
    return result


def main():
    with tempfile.TemporaryDirectory() as directory:
        os.environ['UHC_DATA_DIR'] = directory
        settings = dict(group='exam-a', shortti=250, longti=600, ti=250, register=False)
        assert reconstruct(images(3), settings, None) == []
        settings.update(ti=600, final=True)
        output = reconstruct(images(1), settings, None)
        assert len(output) == 6
        assert output[0].data.dtype == np.int16
        assert output[0].data[0, 0, 32, 32] == 5000
        assert {im.getHead().image_series_index for im in output} == {101, 102, 103}
        assert np.array_equal(output[0].data, reconstruct(images(1), settings, None)[0].data)
        settings['group'] = 'exam-b'
        try:
            reconstruct(images(1), settings, None)
        except ValueError as error:
            assert 'missing' in str(error)
        else:
            raise AssertionError('Different exam reused saved data')
        settings['group'] = 'exam-a'
        shifted = images(1)
        head = shifted[0].getHead()
        head.position = (10, 0, 0)
        shifted[0].setHead(head)
        try:
            reconstruct(shifted, settings, None)
        except ValueError as error:
            assert 'geometry' in str(error)
        else:
            raise AssertionError('Geometry mismatch accepted')
    print('UHC persistence and reconstruction passed')


if __name__ == '__main__':
    main()
