"""Exercise separate acquisition calls against persisted data and real MRD images."""
import os
import hashlib
from itertools import combinations
from pathlib import Path
import tempfile
from types import SimpleNamespace

import ismrmrd
import numpy as np

from uhcfire import process, reconstruct

META = SimpleNamespace(studyInformation=SimpleNamespace(studyInstanceUID="1.2.3"),
                       sequenceParameters=SimpleNamespace(TI=[250]))


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
        settings = dict(group='exam-a', ti=250, register=False, expectedtis='250,600', triggerti=600)
        assert reconstruct(images(3), settings, META) == []
        settings.update(ti=600)
        output = reconstruct(images(1), settings, META)
        assert len(output) == 6
        assert output[0].data.dtype == np.int16
        assert output[0].data[0, 0, 32, 32] == 5000
        assert {im.getHead().image_series_index for im in output} == {101, 102, 103}
        assert np.array_equal(output[0].data, reconstruct(images(1), settings, META)[0].data)
        settings['group'] = 'exam-b'
        try:
            reconstruct(images(1), settings, META)
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
            reconstruct(shifted, settings, META)
        except ValueError as error:
            assert 'geometry' in str(error)
        else:
            raise AssertionError('Geometry mismatch accepted')
        # A rejected replacement must leave the previous valid acquisition intact.
        assert np.array_equal(output[0].data, reconstruct(images(1), settings, META)[0].data)
        other_exam = SimpleNamespace(studyInformation=SimpleNamespace(studyInstanceUID='9.8.7'))
        try:
            reconstruct(images(1), settings, other_exam)
        except ValueError as error:
            assert 'exam' in str(error)
        else:
            raise AssertionError('Mixed exams accepted')
        settings.update(group='handler', ti='')
        source = images(3)
        head = source[0].getHead()
        head.image_type = 0
        source[0].setHead(head)
        phase = images(1)[0]
        head = phase.getHead()
        head.image_type = ismrmrd.IMTYPE_PHASE
        phase.setHead(head)
        waveform = ismrmrd.Waveform.from_array(np.zeros((1, 10), np.uint32))

        class Connection:
            def __init__(self, items):
                self.items, self.sent, self.errors, self.closed = items, [], [], False

            def __iter__(self):
                return iter(self.items + [None])

            def send_image(self, image):
                self.sent.append(image)

            def send_logging(self, level, message):
                self.errors.append(message)

            def send_close(self):
                self.closed = True

        connection = Connection([waveform] + source + [phase])
        process(connection, {'parameters': settings}, META)
        assert connection.closed and not connection.errors
        assert connection.sent == source + [phase]
        settings.update(ti=600)
        connection = Connection(images(1))
        process(connection, {'parameters': settings}, META)
        assert connection.closed and not connection.errors
        assert len(connection.sent) == 8
        assert connection.sent[2].data[0, 0, 32, 32] == 5000
        for expected in ('300,500,778', '100,200,300,400,500,600,778'):
            settings.update(group='set-' + expected, expectedtis=expected, triggerti=778)
            values = list(map(float, expected.split(',')))
            pairs = list(combinations(values, 2))
            for ti in values:
                settings['ti'] = ti
                result = reconstruct(images(ti), settings, META)
                assert len(result) == (6 * len(pairs) if ti == 778 else 0)
            for pair_index, (short, long) in enumerate(pairs):
                dsir = result[pair_index * 6]
                assert dsir.data[0, 0, 32, 32] == round(10000 * (short - long) / (short + long))
                meta = ismrmrd.Meta.deserialize(dsir.attribute_string)
                assert meta['SequenceDescriptionAdditional'] == f'dSIR {short:g}/{long:g} ms'
            assert len({image.getHead().image_series_index for image in result}) == 3 * len(pairs)
            folder = Path(directory) / hashlib.sha256(settings['group'].encode()).hexdigest()
            assert len(list(folder.glob('ti-*.npz'))) == len(expected.split(','))
        settings.update(group='early-trigger', expectedtis='250,600,778', ti=778)
        try:
            reconstruct(images(1), settings, META)
        except ValueError as error:
            assert 'missing' in str(error)
        else:
            raise AssertionError('Trigger processed an incomplete acquisition set')
    print('UHC persistence and reconstruction passed')


if __name__ == '__main__':
    main()
