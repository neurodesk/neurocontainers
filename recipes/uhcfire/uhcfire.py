"""Persist separate FIRE magnitude acquisitions and compute paired UHC images."""

import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import tempfile
import traceback
import uuid

import ismrmrd
import numpy as np

from make_dsir import make_dsir


def flag(value):
    if value in (True, 'true', 'True', 1):
        return True
    if value in (False, 'false', 'False', 0):
        return False
    raise ValueError('Expected a boolean parameter')


def positive(value):
    value = float(value)
    if not np.isfinite(value) or value <= 0:
        raise ValueError('Inversion times must be positive finite milliseconds')
    return value


def geometry(images):
    return np.asarray([
        [*im.getHead().position, *im.getHead().read_dir,
         *im.getHead().phase_dir, *im.getHead().slice_dir,
         *im.getHead().field_of_view] for im in images
    ], dtype=np.float64)


def exam_identity(metadata):
    study = getattr(getattr(metadata, 'studyInformation', None), 'studyInstanceUID', None)
    if not study:
        raise ValueError('MRD studyInstanceUID is required to isolate exams')
    frame = getattr(getattr(metadata, 'measurementInformation', None), 'frameOfReferenceUID', None)
    patient = getattr(getattr(metadata, 'subjectInformation', None), 'patientID', None)
    return hashlib.sha256(json.dumps([study, frame, patient]).encode()).hexdigest()


def atomic_write(path, writer):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        temporary = Path(tmp.name)
        try:
            writer(tmp)
            tmp.flush()
            os.fsync(tmp.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def reconstruct(images, settings, metadata):
    identity = exam_identity(metadata)
    final = flag(settings.get('final', False))
    register = flag(settings.get('register', True))
    group = str(settings.get('group', '')).strip()
    if not group:
        raise ValueError('Set group to a unique identifier for this exam and TI pair')
    short = positive(settings.get('shortti', 300))
    long = positive(settings.get('longti', 500))
    if short >= long:
        raise ValueError('shortti must be less than longti')
    ti = settings.get('ti')
    if ti is None or ti == '':
        values = getattr(getattr(metadata, 'sequenceParameters', None), 'TI', [])
        if len(values) != 1:
            raise ValueError('Provide ti in milliseconds or exactly one metadata TI')
        ti = values[0]
    ti = positive(ti)
    matches = [value for value in (short, long) if abs(ti - value) <= 0.05]
    if len(matches) != 1:
        raise ValueError('Current TI must match exactly one configured TI within 0.05 ms')
    ti = matches[0]
    if not images or any(im.getHead().image_type not in (0, ismrmrd.IMTYPE_MAGNITUDE)
                         for im in images):
        raise ValueError('Supply one magnitude volume per acquisition')
    images = sorted(images, key=lambda im: int(im.getHead().slice))
    if len({int(im.getHead().slice) for im in images}) != len(images):
        raise ValueError('Repeated slices: supply only one magnitude volume')
    if any(im.data.shape[0] != 1 or im.data.shape[1] != 1 for im in images):
        raise ValueError('Expected single-channel 2D MRD slices')
    data = np.stack([im.data[0, 0] for im in images], axis=-1).astype(np.float32)
    if not np.isfinite(data).all() or np.any(data < 0):
        raise ValueError('Magnitude data must be finite and nonnegative')
    geom = geometry(images)
    root = Path(os.environ.get('UHC_DATA_DIR', '/tmp/share/uhcfire'))
    key = hashlib.sha256(group.encode()).hexdigest()
    folder = root / key
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Serialize acquisition replacement and final reads across server processes.
    with (folder / 'lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest = json.dumps({'shortti': short, 'longti': long, 'exam': identity}, sort_keys=True)
        manifest_path = folder / 'pair.json'
        if manifest_path.exists() and manifest_path.read_text() != manifest:
            raise ValueError('This group already belongs to a different exam or TI pair')
        paths = [folder / name for name in ('short.npz', 'long.npz')]
        for path in paths:
            if path.exists():
                with np.load(path, allow_pickle=False) as saved:
                    if (saved['data'].shape != data.shape
                            or saved['geometry'].shape != geom.shape
                            or not np.allclose(saved['geometry'], geom, atol=1e-4, rtol=0)):
                        raise ValueError('TI acquisitions have mismatched image geometry')
        if not manifest_path.exists():
            atomic_write(manifest_path, lambda stream: stream.write(manifest.encode()))
        target = folder / ('short.npz' if ti == short else 'long.npz')
        atomic_write(target, lambda stream: np.savez(stream, data=data, geometry=geom))
        if not final:
            return []
        if not all(path.exists() for path in paths):
            raise ValueError('Final acquisition saved, but the other TI is missing')
        arrays = []
        for path in paths:
            with np.load(path, allow_pickle=False) as saved:
                if (saved['data'].shape != data.shape
                        or saved['geometry'].shape != geom.shape
                        or not np.allclose(saved['geometry'], geom, atol=1e-4, rtol=0)):
                    raise ValueError('TI acquisitions have mismatched image geometry')
                arrays.append(saved['data'].copy())
        results = make_dsir(*arrays, register=register)
    output = []
    for series, (name, volume) in enumerate(zip(('dSIR', 'lSIR', 'aSIR'), results), 101):
        uid = '2.25.' + str(uuid.uuid4().int)
        for index, source in enumerate(images):
            image = ismrmrd.Image.from_array(volume[:, :, index][None, None], transpose=False)
            head = source.getHead()
            head.data_type = image.getHead().data_type
            head.image_type = ismrmrd.IMTYPE_REAL
            head.image_series_index = series
            head.image_index = index + 1
            image.setHead(head)
            meta = ismrmrd.Meta()
            meta['DataRole'] = 'Image'
            meta['ImageProcessingHistory'] = ['UHC', name]
            meta['SequenceDescriptionAdditional'] = f'{name} {short:g}/{long:g} ms'
            meta['SeriesInstanceUID'] = uid
            meta['Keep_image_geometry'] = 1
            meta['WindowCenter'] = '0'
            meta['WindowWidth'] = '20000'
            image.attribute_string = meta.serialize()
            output.append(image)
    return output


def process(connection, config, metadata):
    try:
        if isinstance(config, str):
            config = json.loads(config) if config.lstrip().startswith('{') else {}
        settings = config.get('parameters', config)
        images = []
        originals = []
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Waveform):
                continue
            if not isinstance(item, ismrmrd.Image):
                raise ValueError('UHC requires reconstructed MRD image input')
            originals.append(item)
            if item.getHead().image_type in (0, ismrmrd.IMTYPE_MAGNITUDE):
                images.append(item)
        output = reconstruct(images, settings, metadata)
        for image in originals + output:
            connection.send_image(image)
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(3, message)
    finally:
        connection.send_close()
