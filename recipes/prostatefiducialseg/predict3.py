#!/usr/bin/env python3
"""Run published SeedSeg ONNX models with the OpenRecon predict3 interface."""

import argparse
import glob
from pathlib import Path
import sys

import nibabel as nib
import numpy as np
import onnxruntime as ort
from scipy.special import softmax
import torch
import torchio as tio

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts" / "inference"))
from consensus_inference import find_nearest_compatible_size, select_top_n_markers


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-m", "--models", nargs="+", required=True,
                        help="Published ONNX model paths or wildcard patterns")
    parser.add_argument("-o", "--output_dir", required=True)
    parser.add_argument("--n-markers", type=int, default=3)
    parser.add_argument("--threshold", type=float, default=0.1)
    parser.add_argument("--device", default="cuda:0" if torch.cuda.is_available() else "cpu")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    if args.n_markers < 1 or args.threads < 1 or not 0 <= args.threshold <= 1:
        parser.error("markers and threads must be positive; threshold must be in [0, 1]")
    model_paths = set()
    for pattern in args.models:
        matches = glob.glob(pattern)
        if not matches:
            parser.error(f"No models match {pattern}")
        model_paths.update(matches)

    device = torch.device(args.device)
    providers = ["CPUExecutionProvider"]
    if device.type == "cuda":
        ort.preload_dlls()
        if "CUDAExecutionProvider" not in ort.get_available_providers():
            parser.error("This installation does not provide CUDA inference; use --device cpu")
        providers.insert(0, ("CUDAExecutionProvider", {"device_id": device.index or 0}))
    elif device.type != "cpu":
        parser.error("device must be cpu or cuda[:index]")

    image = nib.load(args.input)
    if len(image.shape) != 3 or not np.isfinite(image.get_fdata()).all():
        parser.error("Input must be a finite 3D NIfTI volume")
    shape = find_nearest_compatible_size(image.shape)
    sample = tio.ZNormalization()(tio.CropOrPad(shape)(tio.ScalarImage(args.input)))
    inputs = sample.data.unsqueeze(0).numpy()
    options = ort.SessionOptions()
    options.intra_op_num_threads = args.threads
    probabilities = np.zeros((3, *shape), dtype=np.float32)
    for model_path in sorted(model_paths):
        session = ort.InferenceSession(model_path, options, providers=providers)
        if device.type == "cuda" and "CUDAExecutionProvider" not in session.get_providers():
            raise RuntimeError("CUDA provider failed to initialize")
        logits = session.run(None, {session.get_inputs()[0].name: inputs})[0]
        if logits.shape != (1, 3, *shape) or not np.isfinite(logits).all():
            raise ValueError(f"Unexpected SeedSeg output from {model_path}: {logits.shape}")
        probabilities += softmax(logits[0], axis=0) / len(model_paths)
        del session
        print(f"Processed {Path(model_path).name}", flush=True)

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)

    def save(data: np.ndarray, name: str) -> None:
        # TorchIO reverses its own asymmetric padding for odd image dimensions.
        restored = tio.CropOrPad(image.shape)(
            tio.ScalarImage(tensor=data[np.newaxis], affine=sample.affine)
        ).data[0].numpy()
        header = image.header.copy()
        header.set_data_dtype(data.dtype)
        nib.save(nib.Nifti1Image(restored, image.affine, header), output / name)

    for index, probability in enumerate(probabilities):
        save(probability, f"consensus_prob_class{index}.nii.gz")
    seeds = select_top_n_markers(probabilities[1], n_markers=args.n_markers,
                               threshold=args.threshold)
    save(seeds, f"consensus_top{args.n_markers}_seeds.nii.gz")
    save(probabilities.argmax(axis=0).astype(np.uint8), "consensus_argmax_segmentation.nii.gz")
    print(f"Processed {len(model_paths)} models; outputs saved to {output}")


if __name__ == "__main__":
    main()
