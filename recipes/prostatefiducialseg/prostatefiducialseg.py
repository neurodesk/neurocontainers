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


# Free GPU memory needed (MiB), measured peaks plus margin: all selected networks loaded at once
# (5 networks: 11.4 GiB) or one network at a time (2.7 GiB, same output, about 2 s slower)
GPU_MIB_ALL_LOADED = 13000
GPU_MIB_ONE_LOADED = 3500


def get_model(folds, device, mirror, lazy=False):
    key = (tuple(folds), device, mirror, lazy)
    if key not in _model_cache:
        _model_cache.clear()
        # one window per run: batching the 8 mirrored copies needs several GB more GPU memory
        _model_cache[key] = seedseg_onnx.SeedSegONNX(MODEL_DIR, folds, mirror=mirror, device=device, batch_size=1,
                                                     lazy=lazy, log=logging.info)
    return _model_cache[key]


def gpu_free_mib():
    """Free memory (MiB) of the visible GPU, or None when ONNX Runtime cannot use CUDA or there is no GPU."""
    import onnxruntime as ort
    try:
        ort.preload_dlls()
    except Exception:
        pass
    if 'CUDAExecutionProvider' not in ort.get_available_providers():
        return None
    try:
        import ctypes
        cudart = ctypes.CDLL('libcudart.so.12')
        n, free, total = ctypes.c_int(), ctypes.c_size_t(), ctypes.c_size_t()
        if cudart.cudaGetDeviceCount(ctypes.byref(n)) != 0 or n.value < 1:
            return None
        if cudart.cudaMemGetInfo(ctypes.byref(free), ctypes.byref(total)) != 0:
            return None
        return free.value // 2 ** 20
    except Exception:
        return None


def host_available_mib():
    """Memory available to this container (MiB): the smaller of MemAvailable and the cgroup limit."""
    vals = []
    try:
        for line in open('/proc/meminfo'):
            if line.startswith('MemAvailable:'):
                vals.append(int(line.split()[1]) // 1024)
    except OSError:
        pass
    paths = ['/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes']
    try:  # this process's own cgroup (nested under Slurm, Kubernetes, ...)
        for line in open('/proc/self/cgroup'):
            hier, ctrl, path = line.strip().split(':', 2)
            parts = path.strip('/').split('/') if path.strip('/') else []
            for i in range(len(parts), 0, -1):  # the limit may be set on any ancestor
                sub = '/' + '/'.join(parts[:i])
                if hier == '0':
                    paths.append(f'/sys/fs/cgroup{sub}/memory.max')
                elif 'memory' in ctrl.split(','):
                    paths.append(f'/sys/fs/cgroup/memory{sub}/memory.limit_in_bytes')
    except (OSError, ValueError):
        pass
    for f in paths:
        try:
            v = open(f).read().strip()
            if v.isdigit() and int(v) < 2 ** 60:
                vals.append(int(v) // 2 ** 20)
        except OSError:
            pass
    return min(vals) if vals else None


def choose_device(n_networks):
    """(device, lazy, reason): GPU with all networks loaded when there is room, else one network at a
    time, else CPU."""
    if os.environ.get('SEEDSEG_DEVICE'):
        return os.environ['SEEDSEG_DEVICE'], False, 'SEEDSEG_DEVICE'
    free = gpu_free_mib()
    if free is None:
        # on CPU too, one network loaded at a time: all 5 at once need more than 10 GB of memory
        return 'cpu', n_networks > 1, 'no usable GPU'
    if n_networks > 1 and free >= GPU_MIB_ALL_LOADED:
        return 'cuda', False, f'GPU {free} MiB free'
    if free >= GPU_MIB_ONE_LOADED:
        lazy = n_networks > 1
        return 'cuda', lazy, f'GPU {free} MiB free' + (', one network loaded at a time' if lazy else '')
    return 'cpu', n_networks > 1, f'GPU has only {free} MiB free (needs {GPU_MIB_ONE_LOADED})'


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


def slice_positions(images):
    """Position of each image along the slice normal (read x phase), in mm."""
    n = np.cross(np.array(images[0].read_dir), np.array(images[0].phase_dir))
    return np.array([float(np.dot(np.array(im.position), n)) for im in images])


def split_volumes(images, tol=1e-3):
    """Indices of `images` grouped into volumes with one image per slice position.

    A series can hold several images at each position (repetitions, averages, contrasts or
    sets the scanner did not combine); the k-th image arriving at a position goes to volume k.
    When all images share one position (positions not filled in), the series is one volume."""
    z = slice_positions(images)
    if len(images) > 1 and np.ptp(z) <= tol:
        return [list(range(len(images)))]
    seen, groups = [], []
    for i, zi in enumerate(z):
        k = sum(abs(zi - zj) <= tol for zj in seen)
        seen.append(zi)
        if k == len(groups):
            groups.append([])
        groups[k].append(i)
    return groups


def slice_spacing(images, z):
    """Slice spacing (mm): median step between the sorted slice positions, else the image
    header's slice thickness (field_of_view[2] / matrix_size[2])."""
    steps = np.diff(np.sort(z))
    steps = steps[steps > 1e-3]
    if len(steps):
        return float(np.median(steps)), 'slice positions'
    h = images[0].getHead()
    return float(h.field_of_view[2]) / max(int(h.matrix_size[2]), 1), 'header slice thickness'


def to_volume(images):
    """MRD images of one volume -> (z, y, x) volume in the training (dcm2niix) layout: z = slices
    sorted along the normal (as GDCM/SimpleITK sort a DICOM series), y = rows bottom-up,
    x = columns. Returns (vol, order, spacing_zyx); images[order[j]] is slice j."""
    z = slice_positions(images)
    if len(images) > 1 and np.ptp(z) <= 1e-3:
        logging.warning("SeedSeg: all %d images have the same slice position; using the image header slice "
                        "index (or arrival order) and slice thickness", len(images))
        order = np.argsort([im.slice for im in images], kind='stable')
    else:
        order = np.argsort(z, kind='stable')
    sl = [np.asarray(images[i].data, np.float64).reshape(images[i].data.shape[-2:]) for i in order]  # [row, col]
    vol = np.stack(sl)[:, ::-1, :]
    h = images[0].getHead()
    fov, mat = np.array(h.field_of_view[:], float), np.array(h.matrix_size[:], float)
    rows, cols = vol.shape[1:]
    # MRD field_of_view/matrix_size are (read/x=cols, phase/y=rows, slice)
    vx = fov[0] / (mat[0] if mat[0] == cols else cols)
    vy = fov[1] / (mat[1] if mat[1] == rows else rows)
    dz, source = slice_spacing(images, z)
    spacing = np.array([dz, vy, vx])
    logging.info("SeedSeg: slice spacing %.4f mm from the %s", dz, source)
    if not (np.all(np.isfinite(spacing)) and np.all(spacing > 0)):
        raise ValueError(f"SeedSeg: invalid voxel spacing {spacing} mm (z, y, x) from the image headers "
                         f"(field_of_view {fov}, matrix_size {mat})")
    return vol, order, spacing


def marker_outline(mask):
    """Outline of the dilated, hole-filled marker mask ([image, row, col])."""
    m = binary_dilation(mask > 0, structure=np.ones((3, 3, 3)))
    m = binary_fill_holes(m)
    return find_boundaries(m, mode='outer')


def run_seedseg(images, k=3, folds=('0', '1', '2', '3', '4'), device='cpu', mirror=False, lazy=False):
    """Detection on one volume. Returns (det, mask [image,row,col] with value = rank, order, summary)."""
    vol, order, spacing = to_volume(images)
    logging.info("Volume (z,y,x) %s, spacing %s mm", vol.shape, np.round(spacing, 4))
    xyz = seedseg_onnx.n4_correct_xyz(vol.T)  # N4 on the NIfTI-order array, as for training
    model = get_model(folds, device, mirror, lazy)
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
    return det, mask, order, summary


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
    host = host_available_mib()
    if host is not None and host < 4000:
        logging.warning("SeedSeg: only %d MiB of memory available; at least 4000 MiB is recommended", host)
    device, lazy, reason = choose_device(len(folds))
    logging.info("SeedSeg: device %s (%s); host memory available %s MiB", device, reason, host)
    if device == 'cuda' and 'CUDAExecutionProvider' not in get_model(folds, device, mirror, lazy).providers:
        logging.warning("SeedSeg: a GPU was found but ONNX Runtime could not use it (CUDA/cuDNN libraries); "
                        "running on CPU")
        device, lazy, reason = 'cpu', len(folds) > 1, 'CUDA provider failed'
    if device == 'cpu' and len(folds) > 1 and mrdhelper.get_json_config_param(
            config, 'cpuonenetwork', default=True, type='bool'):
        logging.info("SeedSeg: no GPU; using only network %d of the %d selected", int(folds[0]) + 1, len(folds))
        folds = folds[:1]
        lazy = False
    logging.info("SeedSeg: %d images, K=%d, folds=%s, mirroring=%s, device=%s", len(images), k, folds, mirror, device)

    head = [img.getHead() for img in images]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in images]
    groups = split_volumes(images)
    if len(groups) > 1:
        logging.info("SeedSeg: %d images at %d slice positions; processing %d volumes separately",
                     len(images), len(groups[0]), len(groups))
    outline = np.zeros((len(images),) + np.asarray(images[0].data).shape[-2:], bool)
    desc, comment, info = [None] * len(images), [None] * len(images), [None] * len(images)
    for g in groups:
        det, m, order, summary = run_seedseg([images[i] for i in g], k, folds, device, mirror, lazy)
        for sm in summary:
            sm['image'] = g[sm['image']]
        o = np.zeros(m.shape, bool)
        o[order] = marker_outline(m[order])  # outline in slice order, not arrival order
        outline[g] = o
        flag = det['count_flag']
        d = f"SeedSeg {len(det['markers'])}of{k}" + ("_CHECK" if flag else "")
        logging.info("SeedSeg: %s, %d candidates, %s, %.1f s", d, det['n_candidates'], summary, time.time() - t0)
        c = d + ': ' + '; '.join(f"#{sm['rank']} p={sm['mean_prob']:.3f}" for sm in summary)
        j = json.dumps(dict(k=k, count_flag=bool(flag), n_candidates=det['n_candidates'], markers=summary,
                            device=device, device_reason=reason, networks=[int(f) + 1 for f in folds],
                            mirroring=bool(mirror)))
        for i in g:
            desc[i], comment[i], info[i] = d, c, j

    BitsStored = 12
    maxVal = 2 ** BitsStored - 1
    data = np.stack([np.asarray(img.data, np.float64).reshape(img.data.shape[-2:]) for img in images])
    data *= maxVal / max(data.max(), 1e-12)
    data[outline] = maxVal
    data = np.around(data).astype(np.int16)

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
        tmpMeta['SequenceDescriptionAdditional'] = desc[i]
        tmpMeta['ImageComments'] = comment[i]
        tmpMeta['SeedSegMarkers'] = base64.b64encode(info[i].encode()).decode()
        tmpMeta['Keep_image_geometry'] = 1
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(v) for v in oldHeader.read_dir]
        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(v) for v in oldHeader.phase_dir]
        im.attribute_string = tmpMeta.serialize()
        imagesOut.append(im)
    return imagesOut
