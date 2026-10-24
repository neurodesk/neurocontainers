"""Run the QSMxT Boutiques descriptors on a synthetic BIDS phantom."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
from pathlib import Path

import numpy as np

from builder.boutiques import extract, file_digest


def verify(output: Path, version: str, *, image: Path | None,
           descriptors: Path | None) -> dict:
    from boutiques.validator import validate_descriptor

    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    if image:
        image = image.resolve(strict=True)
        descriptors = output / "descriptors"
        extract(image, descriptors, expected_version=version)
    assert descriptors is not None
    descriptors = descriptors.resolve(strict=True)
    for name in ("qsmxt", "qsmxt-run"):
        descriptor = json.loads((descriptors / f"{name}.json").read_text())
        validate_descriptor(descriptor)
        if descriptor["tool-version"] != version:
            raise ValueError(f"{name} descriptor does not match version {version}")

    source = Path(__file__).resolve().parents[1] / "recipes/qsmxt/verify_v10_runtime.py"
    spec = importlib.util.spec_from_file_location("qsmxt_phantom", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    bids, _, affine = module.fixture(output / "fixture")
    results = output / "results"
    results.mkdir()
    launcher = ["bosh", "exec", "launch", "--skip-data-collection"]
    if image:
        launcher += ["--imagepath", str(image), "--volumes", f"{output}:{output}"]
    else:
        launcher += ["--no-container"]

    def launch(name: str, invocation: dict) -> str:
        result = subprocess.run(
            [*launcher, str(descriptors / f"{name}.json"), json.dumps(invocation)],
            text=True, capture_output=True,
        )
        (output / f"{name}.log").write_text(result.stdout + result.stderr)
        result.check_returncode()
        return result.stdout + result.stderr

    version_output = launch("qsmxt", {"action": "--version"})
    if f"qsmxt {version}" not in version_output:
        raise ValueError("Installed QSMxT does not report the requested version")
    launch("qsmxt-run", {"bids_dir": str(bids), "output_dir": str(results)})
    data, reconstructed = module.primary(results)
    if data.shape != (32, 32, 24) or not np.allclose(reconstructed.affine, affine):
        raise ValueError("Reconstruction changed the phantom's voxel grid")
    artifact = next(results.glob("derivatives/qsmxt/sub-01/anat/*_Chimap.nii*"))
    evidence = {
        "version": version,
        "mode": "sif" if image else "native",
        "image_sha256": file_digest(image) if image else None,
        "descriptor_sha256": file_digest(descriptors / "qsmxt-run.json"),
        "output_sha256": file_digest(artifact),
        "output_shape": list(data.shape),
        "nonzero_voxels": int(np.count_nonzero(data)),
        "finite": bool(np.isfinite(data).all()),
    }
    (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--image", type=Path, help="Built QSMxT SIF to extract and execute")
    mode.add_argument("--native-descriptors", type=Path,
                      help="Validate descriptors against qsmxt on PATH, without a SIF")
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True,
                        help="New directory for the phantom, results and evidence")
    args = parser.parse_args()
    evidence = verify(args.output, args.version, image=args.image,
                      descriptors=args.native_descriptors)
    print(json.dumps(evidence))


if __name__ == "__main__":
    main()
