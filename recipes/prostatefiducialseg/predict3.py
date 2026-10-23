#!/usr/bin/env python3
"""SeedSeg v2 gold-marker detection on a T1 NIfTI (command-line entry point of the container).

Runs seedseg_onnx.py from the SeedSeg repository (N4, nnU-Net fold ensemble with ONNX
Runtime, top-K candidates) and writes to the output directory:
  marker_prob.nii.gz          marker probability (mean of the fold softmax maps)
  top{K}_markers.nii.gz       detected markers, voxel value = rank (1..K)
  detections.json             per-marker mean probability, centroid (voxel, world mm), count flag
"""
import argparse
import json
import os
from pathlib import Path
import sys

import nibabel as nib
import numpy as np

sys.path.insert(0, os.environ.get('SEEDSEG_SRC', '/opt/seedseg/scripts/deploy'))
import seedseg_onnx  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-i', '--input', required=True, help='T1 NIfTI (not bias-corrected unless --no-n4)')
    parser.add_argument('-o', '--output_dir', required=True)
    parser.add_argument('-m', '--models', default=os.environ.get('SEEDSEG_MODELS', '/opt/models'),
                        help='directory with seedseg-v2.json and the fold ONNX files')
    parser.add_argument('--n-markers', type=int, default=3, help='implant count K')
    parser.add_argument('--threshold', type=float, default=0.1)
    parser.add_argument('--folds', nargs='+', help='fold subset (default: all five)')
    parser.add_argument('--mirror', action='store_true', help='test-time mirroring (8x slower; default off)')
    parser.add_argument('--no-n4', action='store_true', help='input is already N4-corrected')
    parser.add_argument('--device', default='auto', choices=['auto', 'cpu', 'cuda'])
    parser.add_argument('--threads', type=int)
    args = parser.parse_args()
    if args.n_markers < 1 or not 0 <= args.threshold <= 1:
        parser.error('n-markers must be positive and threshold in [0, 1]')
    device = args.device
    if device == 'auto':
        import onnxruntime as ort
        device = 'cuda' if 'CUDAExecutionProvider' in ort.get_available_providers() else 'cpu'
    # one window per run and one network loaded at a time (about 3 GB of GPU or host memory)
    model = seedseg_onnx.SeedSegONNX(args.models, args.folds, mirror=args.mirror, device=device,
                                     threads=args.threads, batch_size=1, lazy=True)
    det, pm, img = seedseg_onnx.run_nifti(model, args.input, n4=not args.no_n4, k=args.n_markers,
                                          threshold=args.threshold)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    hdr = img.header.copy()
    hdr.set_data_dtype(np.float32)
    hdr['scl_slope'], hdr['scl_inter'] = 1, 0
    nib.save(nib.Nifti1Image(pm.astype(np.float32), img.affine, hdr), out / 'marker_prob.nii.gz')
    hdr.set_data_dtype(np.uint8)
    nib.save(nib.Nifti1Image(seedseg_onnx.marker_mask(pm, det, args.threshold), img.affine, hdr),
             out / f'top{args.n_markers}_markers.nii.gz')
    det.update(folds=model.folds, mirror=bool(model.mirror_axes), providers=model.providers)
    (out / 'detections.json').write_text(json.dumps(det, indent=1) + '\n')
    flag = ' (fewer candidates than markers: review)' if det['count_flag'] else ''
    print(f"{len(det['markers'])}/{args.n_markers} markers{flag}; outputs saved to {out}")


if __name__ == '__main__':
    main()
