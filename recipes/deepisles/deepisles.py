"""Command-line entry point for the DeepISLES stroke lesion ensemble.

Upstream ships only scripts/predict.py, which runs a hardcoded sample case,
and main.py, which assumes the /app layout of the upstream Docker image. This
wraps IslesEnsemble.predict_ensemble for arbitrary inputs and makes it run
from a read-only container on machines without a GPU:

* The ensemble writes scratch files into its own source tree (SEALS keeps its
  nnU-Net raw data and fold predictions under src/SEALS, and team outputs go
  to <ensemble_path>/output_teams), so each run executes from a writable copy
  of src/ in a temporary directory. The weights stay where they were built.
* Skull stripping always asks HD-BET for a GPU; on CPU it is told otherwise.
* The NVAUTO team model only runs on CUDA, so the full FLAIR ensemble
  requires a GPU. Without one, --fast (SEALS only) must be requested.
"""

import argparse
import os
import shutil
import sys
import tempfile
from typing import Any

DEEPISLES_ROOT = "/opt/DeepIsles"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="deepisles",
        description="Segment ischemic stroke lesions with the DeepISLES ensemble.",
    )
    parser.add_argument("--dwi", required=True, help="DWI image (b=1000), NIfTI")
    parser.add_argument("--adc", required=True, help="ADC map, NIfTI")
    parser.add_argument("--flair", help="FLAIR image, NIfTI (optional)")
    parser.add_argument("-o", "--output", required=True, help="output directory")
    parser.add_argument("--fast", action="store_true",
                        help="run only the ISLES'22 winning model (SEALS); required without a GPU when --flair is given")
    parser.add_argument("--skull-strip", action="store_true",
                        help="skull strip the inputs with HD-BET first")
    parser.add_argument("--save-team-outputs", action="store_true",
                        help="also write each team's individual prediction")
    parser.add_argument("--results-mni", action="store_true",
                        help="also write results in MNI space (requires --flair)")
    parser.add_argument("--no-parallel", action="store_true",
                        help="run the team models one after another (always the case on CPU)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    for flag, path in (("--dwi", args.dwi), ("--adc", args.adc), ("--flair", args.flair)):
        if path is not None and not os.path.isfile(path):
            sys.exit(f"deepisles: {flag} {path}: no such file")

    if args.results_mni and not args.flair:
        sys.exit("deepisles: --results-mni requires --flair")

    output = os.path.abspath(args.output)
    for name in ("lesion_msk.nii.gz", "output_screenshot.png", "output_teams", "output_mni"):
        if os.path.lexists(os.path.join(output, name)):
            sys.exit(f"deepisles: {output} already contains results; choose a fresh output directory")

    import torch
    gpu = torch.cuda.is_available()
    if not gpu and args.flair and not args.fast:
        sys.exit("deepisles: no CUDA GPU found. The full ensemble with FLAIR needs a GPU "
                 "(the NVAUTO model is CUDA-only); rerun with --fast to use SEALS alone.")

    os.makedirs(output, exist_ok=True)

    workdir = tempfile.mkdtemp(prefix="deepisles-")
    try:
        shutil.copytree(os.path.join(DEEPISLES_ROOT, "src"), os.path.join(workdir, "src"),
                        symlinks=True, ignore=shutil.ignore_patterns("__pycache__"))
        os.symlink(os.path.join(DEEPISLES_ROOT, "data"), os.path.join(workdir, "data"))

        sys.path.insert(0, workdir)
        import src.isles22_ensemble as ensemble
        import src.utils as utils

        # extract_brain is imported by name and always called with gpu=1.
        def extract_brain(*a: Any, **kw: Any) -> Any:
            kw["gpu"] = gpu
            return utils.extract_brain(*a, **kw)
        ensemble.extract_brain = extract_brain

        ensemble.IslesEnsemble().predict_ensemble(
            ensemble_path=workdir,
            weights_dir=os.path.join(DEEPISLES_ROOT, "weights"),
            input_dwi_path=os.path.abspath(args.dwi),
            input_adc_path=os.path.abspath(args.adc),
            input_flair_path=os.path.abspath(args.flair) if args.flair else None,
            output_path=output,
            skull_strip=args.skull_strip,
            fast=args.fast,
            save_team_outputs=args.save_team_outputs,
            results_mni=args.results_mni,
            parallelize=gpu and not args.no_parallel,
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    mask = os.path.join(output, "lesion_msk.nii.gz")
    if not os.path.isfile(mask):
        sys.exit(f"deepisles: no lesion mask was written to {mask}")


if __name__ == "__main__":
    main()
