"""Verify MRD products with cached real inference and registration outputs."""
import argparse
import gc
from pathlib import Path
import shutil
import sys

sys.path.insert(0, '/opt/code/python-ismrmrd-server')

import ismrmrd
import nibabel as nib
import numpy as np
import glasser
import synthseg


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('source', type=Path)
parser.add_argument('assets', type=Path)
args = parser.parse_args()
assets = args.assets
source = nib.load(args.source)
pixels = np.asarray(source.dataobj)
spacing = np.linalg.norm(source.affine[:3, :3], axis=0)
lps = np.array([-1.0, -1.0, 1.0])
images = []
for index in range(source.shape[2]):
    image = ismrmrd.Image.from_array(pixels[:, :, index].T[None, None], transpose=False)
    head = image.getHead()
    head.field_of_view = (source.shape[0] * spacing[0], source.shape[1] * spacing[1], spacing[2])
    head.position = tuple((source.affine[:3, 3] + index * source.affine[:3, 2]) * lps)
    head.read_dir = tuple(source.affine[:3, 0] / spacing[0] * lps)
    head.phase_dir = tuple(source.affine[:3, 1] / spacing[1] * lps)
    head.slice_dir = tuple(source.affine[:3, 2] / spacing[2] * lps)
    head.image_series_index = 8
    head.image_index = index + 1
    head.slice = index
    image.setHead(head)
    meta = ismrmrd.Meta()
    for key, value in {
        'SeriesDescription': 't1w',
        'SequenceDescription': 't1w',
        'ProtocolName': 't1w',
        'SeriesNumberRangeNameUID': 'source',
        'SeriesInstanceUID': '1.2.3',
        'SOPInstanceUID': f'1.2.3.{index + 1}',
        'ImageType': 'ORIGINAL\\PRIMARY\\M\\ND',
        'DicomImageType': 'ORIGINAL\\PRIMARY\\M\\ND',
        'ImageTypeValue3': 'M',
        'ImageTypeValue4': 'ND',
    }.items():
        meta[key] = value
    image.attribute_string = meta.serialize()
    images.append(image)
images.reverse()

synthseg.debugFolder = str(assets / 'stream-probe')
inferences = []
registrations = []


def inference(command: list[str], output_dir: Path) -> None:
    inferences.append(command)
    generated_input = nib.load(command[command.index('--i') + 1])
    assert generated_input.shape == source.shape
    np.testing.assert_allclose(generated_input.affine, source.affine, atol=1e-3)
    np.testing.assert_array_equal(np.asarray(generated_input.dataobj), pixels)
    main_output = command[command.index('--o') + 1]
    if '--parc' in command:
        shutil.copyfile(assets / 't1w_parc.nii.gz', main_output)
        base_output = command[command.index('--base-output') + 1]
        shutil.copyfile(assets / 't1w_parc_base.nii.gz', base_output)
    else:
        shutil.copyfile(assets / 't1w_seg.nii.gz', main_output)


def registration(scan: Path, segmentation: Path, output: Path, *, threads: int) -> None:
    assert Path(segmentation).name == 'input_synthseg.nii.gz'
    tissue = np.asarray(nib.load(segmentation).dataobj)
    assert not np.any(tissue >= 1000), np.unique(tissue)
    registrations.append((scan, segmentation, output))
    shutil.copyfile(assets / 't1w_glasser_approx.nii.gz', output)


class Connection:
    def __init__(self):
        self.batches = []

    def send_image(self, batch):
        if isinstance(batch, ismrmrd.Image):
            batch = [batch]
        self.batches.append(list(batch))


synthseg._run_synthseg_command = inference
glasser.register_glasser = registration
choices = {
    'none': {'synthseg'},
    'desikan': {'synthseg', 'desikan'},
    'glasser': {'synthseg', 'glasser_approx'},
    'both': {'synthseg', 'desikan', 'glasser_approx'},
}
checks = 0
for header in (False, True):
    for choice, expected_products in choices.items():
        config = {'parameters': dict(
            synthseg.OPENRECON_DEFAULTS,
            ssparc=choice, sendoriginal=True, sssegmentheader=header,
            ssreslicesagittal=True, ssreslicecoronal=True,
        )}
        before = len(inferences)
        outputs = synthseg.process_image(images, None, config, None)
        assert len(inferences) == before + 1
        connection = Connection()
        synthseg._send_images_by_series(connection, outputs, 'both segmentation and parcellation probe')
        sent = [image for batch in connection.batches for image in batch]
        assert len(sent) == len(outputs)
        for batch in connection.batches:
            assert len({int(image.getHead().image_series_index) for image in batch}) == 1
        products = {}
        all_series = {}
        uids = set()
        for image in sent:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            series = int(image.getHead().image_series_index)
            assert series > 8
            all_series.setdefault(series, []).append(image)
            uid = meta['SOPInstanceUID']
            assert uid not in uids
            uids.add(uid)
            product = meta.get('LabelProduct')
            if product:
                assert meta['LUTFileName'] == 'MicroDeltaHotMetal.pal'
                if not meta.get('SynthSegReformatOrientation'):
                    products.setdefault(product, []).append(image)
        assert set(products) == expected_products, (choice, products.keys())
        reference = {
            'synthseg': 't1w_parc_base.nii.gz' if choice in ('desikan', 'both') else 't1w_seg.nii.gz',
            'desikan': 't1w_parc.nii.gz',
            'glasser_approx': 't1w_glasser_approx.nii.gz',
        }
        for product, stack in products.items():
            assert len(stack) == source.shape[2], (product, len(stack))
            actual = np.stack([image.data[0, 0].T for image in stack], axis=2)
            wanted = np.asarray(nib.load(assets / reference[product]).dataobj)
            np.testing.assert_array_equal(actual, wanted)
            for index, image in enumerate(stack):
                np.testing.assert_allclose(image.getHead().position, images[-index - 1].getHead().position)
        expected_series = 1 + 3 * (2 if 'desikan' in expected_products else 1) + int('glasser_approx' in expected_products)
        assert len(all_series) == expected_series, (choice, len(all_series), expected_series)
        first_series = next(iter(all_series.values()))
        assert len(first_series) == len(images)
        for index, image in enumerate(first_series):
            np.testing.assert_array_equal(image.data, images[-index - 1].data)
            assert 'LUTFileName' not in ismrmrd.Meta.deserialize(image.attribute_string)
        checks += 1
        print(f'PASS {choice} header={header}: products={sorted(products)}, series={len(all_series)}, sent images={len(sent)}', flush=True)
        del outputs, sent, products, all_series, first_series, stack, connection
        gc.collect()
print(f'{checks} actual MRD sender checks passed with real scan, network labels and registered atlas', flush=True)
