"""Verify installed model files, default SCT assets, and CPU dependencies."""

import argparse
import json
import os
import subprocess
import sys
from importlib.metadata import distributions
from pathlib import Path

from sct_model_profile import load_tasks, PROFILE_PATH

# The sibling OpenRecon wrapper has the same name as the installed SCT package.
sys.path = [path for path in sys.path if Path(path).resolve() != Path(__file__).resolve().parent]


def verify_inference(data: Path, output: Path) -> None:
    import nibabel as nib
    import numpy as np

    data, output = data.resolve(), output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    def run(*args):
        subprocess.run([str(arg) for arg in args], cwd=output, check=True)

    def check(path, source, *, nonempty=False):
        image, original = nib.load(path), nib.load(source)
        values = image.get_fdata()
        assert image.shape == original.shape, path
        assert np.allclose(image.affine, original.affine, atol=1e-3), path
        assert np.isfinite(values).all() and (values >= 0).all(), path
        if nonempty:
            assert np.any(values > 0), f"Empty result: {path}"

    t2 = data / "t2/t2.nii.gz"
    t2s = data / "t2s/t2s_uncropped.nii.gz"
    axial = output / "t2_axial.nii.gz"
    run("sct_resample", "-i", t2, "-o", axial, "-mm", "0.8x3x0.8", "-x", "spline")
    cases = (
        ("spinalcord", t2, ("",)),
        ("graymatter", t2s, ("",)),
        ("lesion_sci_t2", data / "t2/t2_fake_lesion.nii.gz", ("_sc_seg", "_lesion_seg")),
        ("lesion_ms_axial_t2", axial, ("_sc_seg", "_lesion_seg")),
        # SCT's upstream smoke test uses T2 as a dummy MP2RAGE input.
        ("lesion_ms_mp2rage", t2, ("",)),
    )
    for task, source, suffixes in cases:
        destination = output / f"{task}.nii.gz"
        run("sct_deepseg", task, "-i", source, "-o", destination)
        for suffix in suffixes:
            check(output / f"{task}{suffix}.nii.gz", source,
                  nonempty=task in ("spinalcord", "graymatter") or suffix == "_sc_seg")
    legacy_gm = output / "legacy_gm.nii.gz"
    run("sct_deepseg_gm", "-i", t2s, "-o", legacy_gm)
    check(legacy_gm, t2s, nonempty=True)
    labels = output / "vertebrae"
    run("sct_label_vertebrae", "-i", t2, "-s", output / "spinalcord.nii.gz",
        "-c", "t2", "-initfile", data / "t2/init_label_vertebrae.txt", "-ofolder", labels)
    check(labels / "spinalcord_labeled.nii.gz", t2, nonempty=True)
    run("sct_process_segmentation", "-i", output / "spinalcord.nii.gz",
        "-o", output / "csa.csv")
    assert (output / "csa.csv").stat().st_size > 0
    print("Verified CPU inference, legacy gray matter, initialized vertebral labelling, and cord area")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, help="Extracted SCT testing data; enables real inference")
    parser.add_argument("--output", type=Path, default=Path("sct-smoke-output"))
    args = parser.parse_args()
    import torch
    from spinalcordtoolbox.deepseg import models
    from spinalcordtoolbox.download import DATASET_DICT

    profile = json.loads(PROFILE_PATH.read_text())["profile"]
    tasks = load_tasks()
    for task in tasks:
        for model in models.TASKS[task]["models"]:
            paths = models.find_model_folder_paths(models.folder(model))
            assert paths and models.is_valid(paths), f"Missing or invalid model: {model}"
    for name, dataset in DATASET_DICT.items():
        if dataset.get("default"):
            path = Path(dataset["default_location"])
            assert path.is_dir() and any(path.rglob("*")), f"Missing SCT asset: {name}"
    pam50 = Path(os.environ["SCT_DIR"]) / "data/PAM50/template"
    for name in ("PAM50_t2.nii.gz", "PAM50_cord.nii.gz", "PAM50_levels.nii.gz"):
        assert (pam50 / name).is_file(), f"Missing vertebral labelling asset: {name}"
    if profile == "lite":
        assert torch.version.cuda is None and torch.version.hip is None
        gpu_packages = [
            dist.metadata["Name"] for dist in distributions()
            if dist.metadata["Name"].lower().startswith(("nvidia-", "cuda-", "cupy", "tensorrt"))
            or dist.metadata["Name"].lower() == "onnxruntime-gpu"
        ]
        assert not gpu_packages, f"GPU packages installed: {gpu_packages}"
        gpu_libraries = [
            path for path in Path(os.environ["SCT_DIR"]).rglob("*.so*")
            if path.name.startswith(("libtorch_cuda", "libcudart", "libcublas", "libcudnn", "libcufft", "libnccl"))
        ]
        assert not gpu_libraries, f"GPU libraries installed: {gpu_libraries}"
        installed = Path(models.folder("spinalcord")).parent
        allowed = {model for task in tasks for model in models.TASKS[task]["models"]}
        extras = {path.name for path in installed.iterdir() if path.is_dir()} - allowed
        assert not extras, f"Unexpected deepseg model directories: {extras}"
    print(f"Verified {profile} profile: {', '.join(tasks)} and standard SCT assets")
    if args.data:
        verify_inference(args.data, args.output)


if __name__ == "__main__":
    main()
