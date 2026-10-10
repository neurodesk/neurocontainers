from contextlib import contextmanager
import json
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
import vessynth as adapter


class Connection:
    def __init__(self, images):
        self.images = images
        self.outputs = []
        self.errors = []
        self.closed = False

    def __iter__(self):
        return iter(self.images)

    def send_image(self, images):
        self.outputs.extend(images if isinstance(images, list) else [images])

    def send_logging(self, level, message):
        self.errors.append(message)

    def send_close(self):
        self.closed = True


def source(z, series=7, repetition=0):
    image = ismrmrd.Image.from_array(np.arange(20, dtype=np.float32).reshape(4, 5) + z, transpose=False)
    header = image.getHead()
    angle = np.pi / 6
    read = np.array([np.cos(angle), np.sin(angle), 0])
    phase = np.array([-np.sin(angle), np.cos(angle), 0])
    header.read_dir[:] = read
    header.phase_dir[:] = phase
    header.slice_dir[:] = [0, 0, 1]
    header.position[:] = [10, 20, 30 + z * 3]
    header.field_of_view[:] = [10, 6, 3]
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_series_index = series
    header.image_index = z + 1
    header.slice = z
    header.repetition = repetition
    image.setHead(header)
    return image


@contextmanager
def deterministic_inference():
    previous = adapter.infer
    adapter.infer = lambda volume, settings: np.linspace(0, 1, volume.voxels.size, dtype=np.float32).reshape(volume.voxels.shape)
    try:
        yield
    finally:
        adapter.infer = previous


def assert_failure(images, config, text):
    connection = Connection(images)
    adapter.process(connection, config, None)
    assert connection.closed
    assert connection.errors and text in connection.errors[0], connection.errors
    assert not connection.outputs


def verify_inference_boundary(volume):
    settings = adapter.Settings(patch_size=32, step_size=16)
    def result(mode):
        def run(command, **kwargs):
            if mode == "failure":
                return subprocess.CompletedProcess(command, 9, "checkpoint failed")
            work = Path(command[command.index("-o") + 1])
            pixels = np.full(volume.voxels.shape, 0.4, dtype=np.float32)
            affine = volume.affine.copy()
            if mode == "affine":
                affine[0, 3] += 1
            if mode == "shape":
                pixels = pixels[:-1]
            if mode != "missing":
                nib.save(nib.Nifti1Image(pixels, affine), work / "source_vessels_prob.nii.gz")
            return subprocess.CompletedProcess(command, 0, "")
        return run
    with patch.object(adapter.subprocess, 'run', result('valid')):
        np.testing.assert_allclose(adapter.infer(volume, settings), 0.4)
    for mode, message in [('failure', 'exit 9'), ('affine', 'affine'), ('shape', 'shape'), ('missing', 'probability volume')]:
        with patch.object(adapter.subprocess, 'run', result(mode)):
            try:
                adapter.infer(volume, settings)
            except (ValueError, RuntimeError) as error:
                assert message in str(error), error
            else:
                raise AssertionError(f"Inference accepted invalid {mode}")


def verify_adapter():
    assert adapter.Settings.parse({'threshold': '0.3'}).threshold == 0.3
    images = [source(2), source(0), source(1)]
    volume = adapter.SourceVolume.from_images(images)
    assert volume.voxels.shape == (5, 4, 3)
    verify_inference_boundary(volume)
    assert [image.slice for image in volume.images] == [0, 1, 2]
    first = volume.images[0].getHead()
    expected = np.asarray(first.position) - np.asarray(first.read_dir) * 4 - np.asarray(first.phase_dir) * 2.25
    expected[:2] *= -1
    np.testing.assert_allclose(volume.affine[:3, 3], expected, atol=1e-5)
    np.testing.assert_allclose(np.linalg.norm(volume.affine[:3, :3], axis=0), [2, 1.5, 3], atol=1e-5)
    all_images = images + [source(z, 8) for z in [1, 2, 0]] + [source(z, 7, 1) for z in [2, 0, 1]]
    with deterministic_inference():
        connection = Connection(all_images)
        adapter.process(connection, {'parameters': {'output': 'both', 'patchsize': 32, 'stepsize': 16}}, None)
        assert connection.closed and not connection.errors, connection.errors
        assert len(connection.outputs) == 27
        series = {}
        for image in connection.outputs:
            series.setdefault(image.image_series_index, []).append(image)
        assert len(series) == 9
        assert not set(series) & {7, 8}
        batches = list(series.values())
        for offset in range(0, 9, 3):
            original, binary, probability = batches[offset:offset + 3]
            for old, mask, prob in zip(original, binary, probability):
                assert mask.data.dtype == np.uint16 and prob.data.dtype == np.float32
                np.testing.assert_array_equal(mask.data, prob.data > 0.3)
                for field in ('position', 'read_dir', 'phase_dir', 'field_of_view', 'matrix_size'):
                    np.testing.assert_allclose(getattr(mask.getHead(), field), getattr(old.getHead(), field))
                mask_meta = ismrmrd.Meta.deserialize(mask.attribute_string)
                prob_meta = ismrmrd.Meta.deserialize(prob.attribute_string)
                assert mask_meta['SeriesInstanceUID'] != prob_meta['SeriesInstanceUID']
                assert 'probability' in prob_meta['SeriesDescription']
                assert prob_meta['DataRole'] == 'Image'
                assert mask_meta['Keep_image_geometry'] == '1'
        assert_failure([source(0), source(0)], {}, 'duplicate')
        assert_failure([source(0), source(1), source(3)], {}, 'regular volume')
        bad = source(0)
        bad.data[0, 0, 0, 0] = np.nan
        assert_failure([bad], {}, 'finite')
        bad = source(0)
        header = bad.getHead()
        header.read_dir[:] = [0, 0, 0]
        bad.setHead(header)
        assert_failure([bad], {}, 'orthonormal')
        assert_failure([object()], {}, 'raw acquisitions')
        assert_failure([source(0)], {'modality': 'LSFM'}, 'modality')
        assert_failure([source(0)], {'sendoriginal': 'false'}, 'booleans')
        assert_failure([source(0)], {'patchsize': 16}, 'patch_size')
        assert_failure([source(0)], {'stepsize': 129}, 'step_size')
        assert_failure([source(0)], {'stepsize': 31}, 'divide patch_size')
        assert_failure([source(0)], {'threshold': float('nan')}, 'threshold')
    print('MRD adapter geometry, grouping, output and errors passed')


def verify_model():
    import torch
    sys.path.insert(0, '/opt/VesSynth')
    from utils.networks import SegNet

    models = Path('/opt/VesSynth/models')
    for name, prefix in [('TOF', 'TOF'), ('T2star', 'T2star'), ('HiPCT', 'HiPCT'), ('OCT', 'OCT'), ('fibers', 'axons')]:
        backbone = json.loads((models / f'segnet_model_{name}.json').read_text())
        weights = sorted((models / 'weights').glob(f'{prefix}_model*'))
        assert len(weights) == 1, weights
        model = SegNet(ndim=3, in_channels=1, out_channels=1,
                       init_kernel_size=3, final_activation='Sigmoid',
                       backbone='UNet', kwargs_backbone=backbone)
        checkpoint = torch.load(weights[0], map_location='cpu', weights_only=True)
        state = checkpoint.get('model_state_dict', checkpoint.get('model_state_dict_segnet'))
        assert state is not None, f'Model state dict missing for {name}'
        model.load_state_dict(state, strict=True)
        del model, checkpoint, state
        print(f'{name} official checkpoint matches packaged network')
    with tempfile.TemporaryDirectory(prefix='vessynth-model-') as directory:
        work = Path(directory)
        grid = np.indices((8, 9, 7), dtype=np.float32)
        voxels = grid[0] + grid[1] * 0.5 + np.sin(grid[2]) + 1
        affine = np.array([[1.2, 0, 0, 12], [0, 1.5, 0, -7], [0, 0, 2.1, 25], [0, 0, 0, 1]])
        nib.save(nib.Nifti1Image(voxels, affine), work / 'input.nii.gz')
        environment = dict(os.environ, CUDA_VISIBLE_DEVICES='', OMP_NUM_THREADS='2')
        subprocess.run(['vessynth', '-i', 'input.nii.gz', '-o', 'results', '-mod', 'TOF',
                        '-t', '0.3', '--patch_size', '32', '--step_size', '16'],
                       cwd=work, env=environment, check=True)
        probability = nib.load(work / 'results/input_vessels_prob.nii.gz')
        binary_paths = list((work / 'results').glob('input_vessels_binary_th_*.nii.gz'))
        assert len(binary_paths) == 1
        binary = nib.load(binary_paths[0])
        assert probability.shape == voxels.shape and binary.shape == voxels.shape
        np.testing.assert_allclose(probability.affine, affine, atol=1e-5)
        np.testing.assert_allclose(binary.affine, affine, atol=1e-5)
        values = probability.get_fdata()
        assert np.isfinite(values).all() and values.min() >= 0 and values.max() <= 1 + 1e-5
        np.testing.assert_array_equal(binary.get_fdata(), values > 0.3)
    print('Official TOF checkpoint CPU inference and native geometry passed')


if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] not in ('adapter', 'model'):
        raise SystemExit('Usage: verify_runtime.py adapter|model')
    {'adapter': verify_adapter, 'model': verify_model}[sys.argv[1]]()
