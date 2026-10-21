"""OpenRecon (MRD image-in/image-out) module for SeedSeg prostate gold-marker detection.

Receives the reconstructed T1 magnitude images of one series, runs the SeedSeg v2
pipeline (``seedseg_onnx.py`` from the SeedSeg repository: N4, nnU-Net ensemble with
ONNX Runtime, top-K candidates) and returns the images with the outlines of the K
detected markers drawn in. Per-marker mean probability, position and the count flag
(fewer candidates than implanted markers) are written to the image metadata and the
series description.

Parameters (OpenReconLabel.json): ``markers`` (implant count K, default 3),
``network1``..``network5`` (which of the 5 networks to use; the probability maps of the
selected networks are averaged; default all 5, the configuration evaluated on independent
patients), ``mirroring`` (test-time mirroring, default off: the ensemble without
mirroring gave the same top-3 as mirrored nnU-Net on 34/34 parity cases) and
``cpuonenetwork`` (default on: without a usable GPU only the first selected network runs).
Environment overrides: SEEDSEG_DEVICE (cpu|cuda), SEEDSEG_MIRROR (0|1), SEEDSEG_MODELS,
SEEDSEG_SRC.
"""
import base64
import json
import logging
import os
import sys
import time
import traceback

import ismrmrd
import numpy as np
from scipy.ndimage import binary_dilation, binary_fill_holes
from skimage.segmentation import find_boundaries

import constants
import mrdhelper

sys.path.insert(0, os.environ.get('SEEDSEG_SRC', '/opt/seedseg/scripts/deploy'))
import seedseg_onnx  # noqa: E402

MODEL_DIR = os.environ.get('SEEDSEG_MODELS', '/opt/models')
_model_cache = {}


def get_model(folds, device, mirror):
    key = (tuple(folds), device, mirror)
    if key not in _model_cache:
        _model_cache.clear()
        _model_cache[key] = seedseg_onnx.SeedSegONNX(MODEL_DIR, folds, mirror=mirror, device=device, log=logging.info)
    return _model_cache[key]


def default_device():
    """cuda when ONNX Runtime has the CUDA provider and a GPU is visible, else cpu."""
    import onnxruntime as ort
    try:
        ort.preload_dlls()
    except Exception:
        pass
    if 'CUDAExecutionProvider' not in ort.get_available_providers():
        return 'cpu'
    try:
        import ctypes
        n = ctypes.c_int()
        ok = ctypes.CDLL('libcudart.so.12').cudaGetDeviceCount(ctypes.byref(n)) == 0 and n.value > 0
    except Exception:
        ok = False
    return 'cuda' if ok else 'cpu'


def process(connection, config, metadata):
    logging.info("Config: \n%s", config)
    try:
        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
    except Exception as e:
        logging.warning("Failed to parse metadata properly: %s", str(e))

    currentSeries = 0
    imgGroup = []
    try:
        for item in connection:
            if isinstance(item, ismrmrd.Image):
                if item.image_series_index != currentSeries:
                    currentSeries = item.image_series_index
                    if imgGroup:
                        connection.send_image(process_image(imgGroup, connection, config, metadata))
                    imgGroup = []
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imgGroup.append(item)
                else:
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta['Keep_image_geometry'] = 1
                    item.attribute_string = tmpMeta.serialize()
                    connection.send_image(item)
            elif isinstance(item, (ismrmrd.Acquisition, ismrmrd.Waveform)):
                continue  # image-in/image-out only
            elif item is None:
                break
            else:
                logging.error("Unsupported data type %s", type(item).__name__)
        if imgGroup:
            connection.send_image(process_image(imgGroup, connection, config, metadata))
    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
    finally:
        connection.send_close()


def slice_order(images):
    """Indices of `images` sorted along the slice normal (read x phase), as GDCM/SimpleITK
    sort a DICOM series; this gives the voxel layout of the training NIfTIs."""
    n = np.cross(np.array(images[0].read_dir), np.array(images[0].phase_dir))
    return np.argsort([float(np.dot(np.array(im.position), n)) for im in images], kind='stable')


def to_volume(images):
    """MRD images -> (z, y, x) volume in the training (dcm2niix) layout: z = slices sorted
    along the normal, y = rows bottom-up, x = columns. Returns (vol, order, spacing_zyx)."""
    order = slice_order(images)
    sl = [np.asarray(images[i].data, np.float64).reshape(images[i].data.shape[-2:]) for i in order]  # [row, col]
    vol = np.stack(sl)[:, ::-1, :]
    h = images[0].getHead()
    fov, mat = np.array(h.field_of_view[:], float), np.array(h.matrix_size[:], float)
    rows, cols = vol.shape[1:]
    # MRD field_of_view/matrix_size are (read/x=cols, phase/y=rows, slice)
    vx = fov[0] / (mat[0] if mat[0] == cols else cols)
    vy = fov[1] / (mat[1] if mat[1] == rows else rows)
    if len(images) > 1:
        pos = np.array([images[i].position for i in order])
        dz = float(np.median(np.linalg.norm(np.diff(pos, axis=0), axis=1)))
    else:
        dz = float(fov[2])
    return vol, order, np.array([dz, vy, vx])


def marker_outline(mask):
    """Outline of the dilated, hole-filled marker mask ([image, row, col])."""
    m = binary_dilation(mask > 0, structure=np.ones((3, 3, 3)))
    m = binary_fill_holes(m)
    return find_boundaries(m, mode='outer')


def run_seedseg(images, k=3, folds=('0', '1', '2', '3', '4'), device='cpu', mirror=False):
    """Detection on one series. Returns (det, mask [image,row,col] with value = rank, summary)."""
    vol, order, spacing = to_volume(images)
    logging.info("Volume (z,y,x) %s, spacing %s mm", vol.shape, np.round(spacing, 4))
    xyz = seedseg_onnx.n4_correct_xyz(vol.T)  # N4 on the NIfTI-order array, as for training
    model = get_model(folds, device, mirror)
    probs = model.predict(xyz.T, spacing_zyx=spacing)
    thr = model.meta['candidates']['threshold']
    p = probs[model.meta['marker_class']]  # (z, y, x)
    det = seedseg_onnx.detect(p, spacing, k=k, threshold=thr)
    m = seedseg_onnx.marker_mask(p, det, thr)  # (z, y, x)
    mask = np.zeros((len(images),) + vol.shape[1:], np.uint8)
    mask[order] = m[:, ::-1, :]
    summary = []
    for c in det['markers']:
        z, y, x = c['centroid_vox']
        summary.append(dict(rank=c['rank'], mean_prob=round(c['mean_prob'], 4), image=int(order[int(round(z))]),
                            row=round(vol.shape[1] - 1 - y, 1), col=round(x, 1), volume_mm3=round(c['volume_mm3'], 1)))
    return det, mask, summary


def process_image(images, connection, config, metadata):
    if not images:
        return []
    t0 = time.time()
    k = mrdhelper.get_json_config_param(config, 'markers', default=3, type='int')
    folds = [str(i) for i in range(5)
             if mrdhelper.get_json_config_param(config, f'network{i + 1}', default=True, type='bool')]
    if not folds:
        logging.warning("SeedSeg: no network selected; using all 5")
        folds = [str(i) for i in range(5)]
    mirror = mrdhelper.get_json_config_param(config, 'mirroring', default=False, type='bool')
    if os.environ.get('SEEDSEG_MIRROR') is not None:
        mirror = os.environ['SEEDSEG_MIRROR'] == '1'
    device = os.environ.get('SEEDSEG_DEVICE') or default_device()
    if device == 'cuda' and 'CUDAExecutionProvider' not in get_model(folds, device, mirror).providers:
        logging.warning("SeedSeg: a GPU was found but ONNX Runtime could not use it (CUDA/cuDNN libraries); "
                        "running on CPU")
        device = 'cpu'
    if device == 'cpu' and len(folds) > 1 and mrdhelper.get_json_config_param(
            config, 'cpuonenetwork', default=True, type='bool'):
        logging.info("SeedSeg: no GPU; using only network %d of the %d selected", int(folds[0]) + 1, len(folds))
        folds = folds[:1]
    logging.info("SeedSeg: %d images, K=%d, folds=%s, mirroring=%s, device=%s", len(images), k, folds, mirror, device)

    head = [img.getHead() for img in images]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in images]
    det, mask, summary = run_seedseg(images, k, folds, device, mirror)
    flag = det['count_flag']
    desc = f"SeedSeg {len(det['markers'])}of{k}" + ("_CHECK" if flag else "")
    logging.info("SeedSeg: %s, %d candidates, %s, %.1f s", desc, det['n_candidates'], summary, time.time() - t0)

    outline = marker_outline(mask)
    BitsStored = 12
    maxVal = 2 ** BitsStored - 1
    data = np.stack([np.asarray(img.data, np.float64).reshape(img.data.shape[-2:]) for img in images])
    data *= maxVal / max(data.max(), 1e-12)
    data[outline] = maxVal
    data = np.around(data).astype(np.int16)

    info = json.dumps(dict(k=k, count_flag=bool(flag), n_candidates=det['n_candidates'], markers=summary,
                           device=device, networks=[int(f) + 1 for f in folds], mirroring=bool(mirror)))
    comment = desc + ': ' + '; '.join(f"#{s['rank']} p={s['mean_prob']:.3f}" for s in summary)
    imagesOut = []
    for i in range(len(images)):
        im = ismrmrd.Image.from_array(data[i][None, None], transpose=False)  # [cha z y x]
        oldHeader = head[i]
        oldHeader.data_type = im.data_type
        im.setHead(oldHeader)
        tmpMeta = meta[i]
        tmpMeta['DataRole'] = 'Image'
        tmpMeta['ImageProcessingHistory'] = ['PYTHON', 'SEEDSEG']
        tmpMeta['WindowCenter'] = str((maxVal + 1) / 2)
        tmpMeta['WindowWidth'] = str(maxVal + 1)
        tmpMeta['SequenceDescriptionAdditional'] = desc
        tmpMeta['ImageComments'] = comment
        tmpMeta['SeedSegMarkers'] = base64.b64encode(info.encode()).decode()
        tmpMeta['Keep_image_geometry'] = 1
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(v) for v in oldHeader.read_dir]
        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(v) for v in oldHeader.phase_dir]
        im.attribute_string = tmpMeta.serialize()
        imagesOut.append(im)
    return imagesOut
