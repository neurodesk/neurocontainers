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
        "checksums": (
            "910b722aeb641c380404c99ec6d1af97",
            "b586ac488b2e40a4e8624a9a1c52d6b5",
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
        "checksums": (
            "25d777a05b00a8106bec6acb034c212c",
            "35e0efb873497ad063fee30fdd4ea69b",
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
        "checksums": (
            "aee13b1f942328cf787679a83fb88137",
            "f80881b5372b0bba403f2f94f1a68e0b",
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
        "checksums": (
            "a293a5fed25a2b298c9fe58a0bfda5f1",
            "9da4d3ae4857a015f615649646e34bd1",
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
        "checksums": (
            "1a085c3c1cee45e8d48120c8805d28f2",
            "c8cba0118c5dfa5f870ff138187c1c55",
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
        "checksums": (
            "4e570b2cab9125dfe53cf20b86398581",
            "1f820178021a866103e901d7bfa0af68",
        ),
        "out_channels": 29,
        "num_res_units": 2,
        "label_count": 28,
    },
}

TEMPLATE_CHECKSUMS = {
    "abdomen_template.nii.gz": "c2c7828b0bbbe2f5dcd78bc3454be573",
    "abdomen_template_dseg.nii.gz": "ca685cd35cf8212b52abca177571455c",
    "abdomen_template_dseg_label-1.nii.gz": "7e712776154de9d4e7c1317f53193ca1",
    "abdomen_template_dseg_label-2.nii.gz": "89281562711639604b8bd9ebacc06f52",
    "abdomen_template_dseg_label-3.nii.gz": "b8d91d68d12f1166c70529e7e60ca620",
    "abdomen_template_dseg_label-4.nii.gz": "7350c21220e42f9837f0527681f1a97a",
    "abdomen_template_dseg_label-5.nii.gz": "66342aaf7ed11ee733c1aa86a4a7f71f",
    "abdomen_template_dseg_label-6.nii.gz": "d852b3cc640af8f6c413fbd0556629f6",
    "abdomen_template_dseg_label-7.nii.gz": "1fa85278c89cd3913357d42fc34ea943",
    "abdomen_template_dseg_label-8.nii.gz": "f8bde003ca64c8198ac513780068347b",
}


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
    verify_file(weight_path, spec["checksums"][0])
    verify_file(config_path, spec["checksums"][1])

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
    regional_version = require_environment("MUSCLEMAP_REGIONAL_MODEL_VERSION")
    software_version = require_environment("MUSCLEMAP_SOFTWARE_VERSION")

    installed_version = (root / "version.txt").read_text(encoding="utf-8").strip()
    if installed_version != software_version:
        raise AssertionError(
            f"unexpected MuscleMap software version: {installed_version} != {software_version}"
        )

    wholebody_weight = None
    wholebody_config = None
    for region, spec in MODEL_SPECS.items():
        version = wholebody_version if region == "wholebody" else regional_version
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
    for filename, checksum in TEMPLATE_CHECKSUMS.items():
        verify_file(template_dir / filename, checksum)

    if verify_runtime_defaults:
        sys.path.insert(0, str(root))
        mm_util = importlib.import_module("scripts.mm_util")
        for region in MODEL_SPECS:
            expected_version = wholebody_version if region == "wholebody" else regional_version
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
