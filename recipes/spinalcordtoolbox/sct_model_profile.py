"""Select installed SCT tasks and the matching OpenRecon menu."""

import argparse
import json
import subprocess
from pathlib import Path

LITE_TASKS = (
    "spinalcord",
    "graymatter",
    "lesion_ms_axial_t2",
    "lesion_ms_mp2rage",
    "lesion_sci_t2",
)
FULL_TASKS = LITE_TASKS + (
    "sc_epi",
    "sc_lumbar_t2",
    "gm_sc_7t_t2star",
    "gm_wm_exvivo_t2",
    "tumor_t2",
    "rootlets",
    "sc_canal_t2",
)
PROFILES = {"lite": LITE_TASKS, "full": FULL_TASKS}
PROFILE_PATH = Path(__file__).with_name("sct_model_profile.json")


def load_tasks(path: Path = PROFILE_PATH) -> tuple[str, ...]:
    return PROFILES[json.loads(path.read_text())["profile"]]


def configure(profile: str, label_path: Path, *, gpu: bool = False) -> None:
    if profile == "lite" and gpu:
        raise ValueError("The lite profile requires CPU-only SCT")
    tasks = PROFILES[profile]
    label = json.loads(label_path.read_text())
    allowed = {f"sct_deepseg_{task}" for task in tasks} | {
        "sct_label_vertebrae", "sct_spinalcord_area", "sct_bundle_t2s_gm",
    }
    analysis = next(p for p in label["parameters"] if p["id"] == "analysis")
    analysis["values"] = [v for v in analysis["values"] if v["id"] in allowed]
    if {v["id"] for v in analysis["values"]} != allowed:
        raise ValueError("OpenRecon label is missing supported analyses")
    if profile == "lite":
        label["general"]["id"] = "spinalcordtoolbox_lite"
        for field in ("name", "information"):
            label["general"][field]["en"] = "spinalcordtoolbox_lite"
    label_path.write_text(json.dumps(label, indent=2) + "\n")
    manifest = label_path.with_name("sct_model_profile.json")
    manifest.write_text(json.dumps({"profile": profile}) + "\n")
    manifest.chmod(0o444)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=PROFILES)
    parser.add_argument("--label", type=Path)
    parser.add_argument("--install", action="store_true")
    parser.add_argument("--gpu", action="store_true")
    args = parser.parse_args()
    if args.profile == "lite" and args.gpu:
        parser.error("The lite profile requires CPU-only SCT")
    if args.install:
        for task in PROFILES[args.profile]:
            subprocess.run(["sct_deepseg", task, "-install"], check=True)
    if args.label:
        configure(args.profile, args.label, gpu=args.gpu)


if __name__ == "__main__":
    main()
