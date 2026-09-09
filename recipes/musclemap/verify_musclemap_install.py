#!/usr/bin/env python3
"""Verify the immutable MuscleMap assets installed in the container."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any


MODEL_SPECS = {
    "wholebody": {
        "filenames": (
            "contrast_agnostic_wholebody_model.pth",
            "contrast_agnostic_wholebody_model.json",
        ),
        "out_channels": 114,
        "num_res_units": 2,
        "label_count": 113,
    },
    "abdomen": {
        "filenames": (
            "contrast_agnostic_abdomen_model.pth",
            "contrast_agnostic_abdomen_model.json",
        ),
        "out_channels": 9,
        "num_res_units": 2,
        "label_count": 8,
    },
    "forearm": {
        "filenames": (
            "contrast_agnostic_forearm_model.pth",
            "contrast_agnostic_forearm_model.json",
        ),
        "out_channels": 6,
        "num_res_units": 1,
        "label_count": 5,
    },
    "leg": {
        "filenames": (
            "contrast_agnostic_leg_model.pth",
            "contrast_agnostic_leg_model.json",
        ),
        "out_channels": 15,
        "num_res_units": 2,
        "label_count": 14,
    },
    "pelvis": {
        "filenames": (
            "contrast_agnostic_pelvis_model.pth",
            "contrast_agnostic_pelvis_model.json",
        ),
        "out_channels": 14,
        "num_res_units": 2,
        "label_count": 13,
    },
    "thigh": {
        "filenames": (
            "contrast_agnostic_thigh_model.pth",
            "contrast_agnostic_thigh_model.json",
        ),
        "out_channels": 29,
        "num_res_units": 2,
        "label_count": 28,
    },
}

TEMPLATE_FILENAMES = ('abdomen_template.nii.gz', 'abdomen_template_dseg.nii.gz', 'abdomen_template_dseg_label-1.nii.gz', 'abdomen_template_dseg_label-2.nii.gz', 'abdomen_template_dseg_label-3.nii.gz', 'abdomen_template_dseg_label-4.nii.gz', 'abdomen_template_dseg_label-5.nii.gz', 'abdomen_template_dseg_label-6.nii.gz', 'abdomen_template_dseg_label-7.nii.gz', 'abdomen_template_dseg_label-8.nii.gz')


def md5sum(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise AssertionError(f"required environment variable is unset: {name}")
    return value


def record_checksums(root: Path, region: str) -> dict[str, str]:
    record = json.loads((root / "records" / (region + ".json")).read_text())
    checksums = {}
    for file in record["files"]:
        algorithm, separator, checksum = file["checksum"].partition(":")
        if algorithm != "md5" or not separator or len(checksum) != 32:
            raise AssertionError("unsupported checksum in published model record")
        checksums[file["key"]] = checksum
    return checksums


def verify_file(path: Path, expected_checksum: str) -> None:
    if not path.is_file():
        raise AssertionError(f"missing installed asset: {path}")
    actual_checksum = md5sum(path)
    if actual_checksum != expected_checksum:
        raise AssertionError(
            f"checksum mismatch for {path}: {actual_checksum} != {expected_checksum}"
        )


def verify_model(
    root: Path,
    region: str,
    spec: dict[str, Any],
    version: str,
) -> tuple[Path, dict[str, Any]]:
    model_dir = root / "scripts" / "models" / region / f"v{version}"
    weight_name, config_name = spec["filenames"]
    weight_path = model_dir / weight_name
    config_path = model_dir / config_name
    checksums = record_checksums(root, region)
    verify_file(weight_path, checksums[weight_name])
    verify_file(config_path, checksums[config_name])

    config = json.loads(config_path.read_text(encoding="utf-8"))
    model = config["model"]
    labels = config["labels"]
    expected = {
        "version": version,
        "out_channels": spec["out_channels"],
        "num_res_units": spec["num_res_units"],
    }
    actual = {key: model[key] for key in expected}
    actual["version"] = str(actual["version"])
    if actual != expected:
        raise AssertionError(f"unexpected {region} model metadata: {actual} != {expected}")
    if len(labels) != spec["label_count"]:
        raise AssertionError(
            f"unexpected {region} label count: {len(labels)} != {spec['label_count']}"
        )
    return weight_path, config


def load_wholebody_model(weight_path: Path, config: dict[str, Any]) -> None:
    import torch
    from monai.networks.layers import Norm
    from monai.networks.nets import UNet

    model_config = config["model"]
    norms = {"instance": Norm.INSTANCE}
    model = UNet(
        spatial_dims=model_config["spatial_dims"],
        in_channels=model_config["in_channels"],
        out_channels=model_config["out_channels"],
        channels=tuple(model_config["channels"]),
        act=model_config["act"],
        strides=tuple(model_config["strides"]),
        num_res_units=model_config["num_res_units"],
        norm=norms[model_config["norm"]],
    )
    state = torch.load(weight_path, map_location="cpu", weights_only=True)
    model.load_state_dict(state)


def verify_install(
    root: Path,
    load_model: bool,
    verify_runtime_defaults: bool = True,
) -> None:
    wholebody_version = require_environment("MUSCLEMAP_WHOLEBODY_MODEL_VERSION")
    regional_versions = {region: require_environment("MUSCLEMAP_" + region.upper() + "_MODEL_VERSION") for region in MODEL_SPECS if region != "wholebody"}
    software_version = require_environment("MUSCLEMAP_SOFTWARE_VERSION")

    installed_version = (root / "version.txt").read_text(encoding="utf-8").strip()
    if installed_version != software_version:
        raise AssertionError(
            f"unexpected MuscleMap software version: {installed_version} != {software_version}"
        )

    wholebody_weight = None
    wholebody_config = None
    for region, spec in MODEL_SPECS.items():
        version = wholebody_version if region == "wholebody" else regional_versions[region]
        weight_path, config = verify_model(root, region, spec, version)
        if region == "wholebody":
            wholebody_weight = weight_path
            wholebody_config = config

    if wholebody_weight is None or wholebody_config is None:
        raise AssertionError("whole-body model specification was not verified")
    labels = wholebody_config["labels"]
    values = {entry["value"] for entry in labels}
    if len(values) != 113 or max(values) != 8222:
        raise AssertionError("whole-body v1.4 label values are incomplete")

    template_dir = root / "scripts" / "templates" / "abdomen"
    checksums = record_checksums(root, "abdomen_template")
    for filename in TEMPLATE_FILENAMES:
        verify_file(template_dir / filename, checksums[filename])

    if verify_runtime_defaults:
        sys.path.insert(0, str(root))
        mm_util = importlib.import_module("scripts.mm_util")
        for region in MODEL_SPECS:
            expected_version = wholebody_version if region == "wholebody" else regional_versions[region]
            actual_version = mm_util._resolve_container_model_version(region, "latest")
            if actual_version != expected_version:
                raise AssertionError(
                    f"container default mismatch for {region}: {actual_version} != {expected_version}"
                )

    if load_model:
        load_wholebody_model(wholebody_weight, wholebody_config)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/MuscleMap"))
    parser.add_argument(
        "--load-wholebody-model",
        action="store_true",
        help="Instantiate the v1.4 MONAI network and load its state dictionary.",
    )
    parser.add_argument(
        "--skip-runtime-defaults",
        action="store_true",
        help="Skip importing mm_util; intended only for host-side cache verification.",
    )
    args = parser.parse_args()
    verify_install(
        args.root,
        args.load_wholebody_model,
        verify_runtime_defaults=not args.skip_runtime_defaults,
    )
    print("MuscleMap installation verified")


if __name__ == "__main__":
    main()
