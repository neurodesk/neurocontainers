import csv
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

import ismrmrd
import nibabel as nib
import numpy as np

sys.path.insert(0, '/opt/code/python-ismrmrd-server')
import brainqcnet as adapter


class Connection:
    def __init__(self, images):
        self.images = images
        self.outputs = []
        self.errors = []
        self.closed = False

    def __iter__(self):
        return iter(self.images)

    def send_image(self, images):
        self.outputs.append(images if isinstance(images, list) else [images])

    def send_logging(self, level, message):
        self.errors.append(message)

    def send_close(self):
        self.closed = True


def source(z, series=7, repetition=0):
    image = ismrmrd.Image.from_array(np.arange(20, dtype=np.float32).reshape(4, 5) + 10 * z, transpose=False)
    header = image.getHead()
    angle = np.pi / 6
    header.read_dir[:] = [np.cos(angle), np.sin(angle), 0]
    header.phase_dir[:] = [-np.sin(angle), np.cos(angle), 0]
    header.slice_dir[:] = [0, 0, 1]
    header.position[:] = [10, 20, 30 + z * 3]
    header.field_of_view[:] = [10, 6, 3]
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_series_index = series
    header.image_index = z + 1
    header.slice = z
    header.repetition = repetition
    image.setHead(header)
    meta = ismrmrd.Meta()
    meta['SeriesDescription'] = f't1_mprage_{series}'
    image.attribute_string = meta.serialize()
    return image


def fake_app(mode, seen):
    def run(command, **kwargs):
        if mode == 'failure':
            return subprocess.CompletedProcess(command, 3, 'model failed')
        bids, out = Path(command[1]), Path(command[2])
        seen.append((nib.load(bids / 'sub-scan/anat/sub-scan_T1w.nii.gz'), command))
        if mode != 'missing':
            result = out / 'sub-scan/ses-single_session/single_run'
            result.mkdir(parents=True)
            proba = {'valid': '0.83', 'invalid': '1.7'}[mode]
            with (result / 'tot_df.csv').open('w', newline='') as handle:
                writer = csv.writer(handle)
                writer.writerow(['subid', 'proba', 't', 'slices', 'axes', 'x_proba', 'y_proba', 'z_proba', 'pred'])
                writer.writerow(['sub-scan', proba, '12.5', '[]', '[]', '0.7', '0.9', '0.8', '1'])
        return subprocess.CompletedProcess(command, 0, '')
    return run


def assert_failure(images, config, text, mode='valid'):
    connection = Connection(images)
    with patch.object(adapter.subprocess, 'run', fake_app(mode, [])):
        adapter.process(connection, config, None)
    assert connection.closed
    assert connection.errors and text in connection.errors[0], connection.errors
    assert not connection.outputs


def verify_adapter():
    assert adapter.Settings.parse({'parameters': {'predmethod': 'mean'}}).predmethod == 'mean'
    volume = adapter.SourceVolume.from_images([source(2), source(0), source(1)])
    assert volume.voxels.shape == (5, 4, 3)
    assert [image.slice for image in volume.images] == [0, 1, 2]
    plane = np.asarray(volume.images[0].data[0, 0])
    np.testing.assert_array_equal(volume.voxels[:, :, 0], plane.T[:, ::-1])
    for i, j, k in [(0, 0, 0), (4, 3, 2), (2, 1, 1)]:
        header = volume.images[k].getHead()
        row = 3 - j
        lps = (np.asarray(header.position) + (i - 2) * 2 * np.asarray(header.read_dir)
               + (row - 1.5) * 1.5 * np.asarray(header.phase_dir))
        np.testing.assert_allclose(volume.affine @ [i, j, k, 1], [-lps[0], -lps[1], lps[2], 1], atol=1e-5)

    seen = []
    images = [source(z) for z in [2, 0, 1]] + [source(z, 8) for z in [1, 0, 2]] + [source(z, 7, 1) for z in [0, 1, 2]]
    connection = Connection(images)
    with patch.object(adapter.subprocess, 'run', fake_app('valid', seen)):
        adapter.process(connection, {'parameters': {'predmethod': 'median'}}, None)
    assert connection.closed and not connection.errors, connection.errors
    assert len(seen) == 3 and all('median' in command for _, command in seen)
    assert len(connection.outputs) == 6
    indices = [batch[0].image_series_index for batch in connection.outputs]
    assert len(set(indices)) == 6 and not set(indices) & {7, 8}
    for originals, report in zip(connection.outputs[::2], connection.outputs[1::2]):
        assert len(originals) == 3 and len(report) == 1
        for output in originals:
            meta = ismrmrd.Meta.deserialize(output.attribute_string)
            assert meta['BrainQCNetVerdict'] == 'FAIL', meta['BrainQCNetVerdict']
            assert meta['SequenceDescriptionAdditional'] == 'BrainQCNet_FAIL', meta['SequenceDescriptionAdditional']
            assert meta['SeriesDescription'].startswith('t1_mprage_'), meta['SeriesDescription']
            assert 'artifact probability 0.83' in meta['ImageComments']
        np.testing.assert_array_equal(np.sort([o.getHead().position[2] for o in originals]), [30, 33, 36])
        report_meta = ismrmrd.Meta.deserialize(report[0].attribute_string)
        assert report_meta['SeriesDescription'].endswith('-brainqcnet-report'), report_meta['SeriesDescription']
        assert report_meta['BrainQCNetProbability'] == '0.83'
        assert np.asarray(report[0].data).max() > 0

    connection = Connection([source(0), source(1)])
    with patch.object(adapter.subprocess, 'run', fake_app('valid', [])):
        adapter.process(connection, {'sendoriginal': False}, None)
    assert not connection.errors and len(connection.outputs) == 1

    assert_failure([source(0), source(0)], {}, 'duplicate')
    assert_failure([source(0), source(1), source(3)], {}, 'regular volume')
    bad = source(0)
    bad.data[0, 0, 0, 0] = np.nan
    assert_failure([bad], {}, 'finite')
    assert_failure([object()], {}, 'raw acquisitions')
    assert_failure([source(0)], {'predmethod': 'max'}, 'predmethod')
    assert_failure([source(0)], {'sendoriginal': 'false'}, 'boolean')
    assert_failure([source(0)], {'threshold': 0.4}, 'Unknown')
    assert_failure([source(0), source(1)], {}, 'exit 3', 'failure')
    assert_failure([source(0), source(1)], {}, 'tot_df.csv', 'missing')
    assert_failure([source(0), source(1)], {}, 'invalid probability', 'invalid')
    print('MRD adapter geometry, grouping, output and errors passed')


def verify_model():
    with tempfile.TemporaryDirectory(prefix='brainqcnet-model-') as directory:
        work = Path(directory)
        anat = work / 'bids/sub-01/anat'
        anat.mkdir(parents=True)
        grid = np.indices((48, 56, 52), dtype=np.float32)
        centre = np.array([24, 28, 26], dtype=np.float32)[:, None, None, None]
        radius = np.sqrt((((grid - centre) / [[[[20]]], [[[24]]], [[[22]]]]) ** 2).sum(axis=0))
        head = np.where(radius < 1, 400 + 200 * np.cos(grid[0] / 3) * np.sin(grid[1] / 4), 0)
        nib.save(nib.Nifti1Image(head.astype(np.int16), np.diag([1.0, 1.0, 1.0, 1.0])),
                 anat / 'sub-01_T1w.nii.gz')
        environment = dict(os.environ, OMP_NUM_THREADS='2')
        subprocess.run(['brainqcnet', 'bids', 'out', 'participant'], cwd=work, env=environment, check=True)
        subprocess.run(['brainqcnet', 'bids', 'out', 'group'], cwd=work, env=environment, check=True)
        run = work / 'out/sub-01/ses-single_session/single_run'
        with (run / 'tot_df.csv').open(newline='') as handle:
            row = next(csv.DictReader(handle))
        assert 0 <= float(row['proba']) <= 1 and row['pred'] in ('0', '1'), row
        with (run / 'table.csv').open(newline='') as handle:
            axes = {line['axis'] for line in csv.DictReader(handle)}
        assert axes == {'x', 'y', 'z'}, axes
        assert (work / 'out/group_results.csv').read_text().count('sub-01') == 1
        assert not any((run / axis).exists() for axis in 'xyz')
    print('BrainQCNet ResNet152 checkpoint scored a synthetic T1w volume')


if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] not in ('adapter', 'model'):
        raise SystemExit('Usage: verify_runtime.py adapter|model')
    {'adapter': verify_adapter, 'model': verify_model}[sys.argv[1]]()
