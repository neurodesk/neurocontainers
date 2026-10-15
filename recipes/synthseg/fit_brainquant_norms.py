"""Fit the SynthSeg brain volume reference that brainquant.py charts against.

Maintainer tool, not installed in the container. Reproduce the shipped
brainquant_norms.json from the IXI T1 images
(https://brain-development.org/ixi-dataset/, CC BY-SA 3.0). CPU SynthSeg on
the full IXI field of view needs more than 9 GB of RAM, and its --crop box is
centred on the field of view, which clips the occipital lobe of the
off-centre IXI heads. So each image is first trimmed to its SynthStrip brain
box plus a margin, which SynthSeg then segments whole.
For every IXI-T1/<subject>.nii.gz:

    mri_synthstrip -i IXI-T1/<subject>.nii.gz -m mask/<subject>.nii.gz
    python fit_brainquant_norms.py trim IXI-T1/<subject>.nii.gz mask/<subject>.nii.gz trimmed/<subject>.nii.gz
    mri_synthseg --i trimmed/<subject>.nii.gz --o seg/<subject>_synthseg.nii.gz \
        --vol vol/<subject>.csv --fast --cpu

then, with the per-subject CSVs concatenated under one header:

    python fit_brainquant_norms.py assemble ixi_synthseg_vol.csv IXI.xls brainquant_ixi_volumes.csv
    python fit_brainquant_norms.py fit brainquant_ixi_volumes.csv brainquant_norms.json

trim needs nibabel, assemble needs pandas and xlrd, fit needs numpy and scipy.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path

import numpy as np
from scipy import optimize, stats

SCHEMA = "neurodesk.synthseg.brainquant.norms/1"
ICV_COLUMN = "total intracranial"
AGE_CENTER = 50.0
AGE_SCALE = 10.0
REFERENCE_ICV_MM3 = 1500000.0
SEXES = ("female", "male", "all")

BILATERAL_TISSUE = (
    "cerebral white matter", "cerebral cortex", "cerebellum white matter", "cerebellum cortex",
    "thalamus", "caudate", "putamen", "pallidum", "hippocampus", "amygdala", "accumbens area", "ventral DC",
)
STRUCTURES = (
    ("hippocampus_left", "Left hippocampus", ["left hippocampus"]),
    ("hippocampus_right", "Right hippocampus", ["right hippocampus"]),
    ("amygdala_left", "Left amygdala", ["left amygdala"]),
    ("amygdala_right", "Right amygdala", ["right amygdala"]),
    ("thalamus_left", "Left thalamus", ["left thalamus"]),
    ("thalamus_right", "Right thalamus", ["right thalamus"]),
    ("lateral_ventricles", "Lateral ventricles", [
        "left lateral ventricle", "left inferior lateral ventricle",
        "right lateral ventricle", "right inferior lateral ventricle",
    ]),
    ("cortical_grey_matter", "Cortical grey matter", ["left cerebral cortex", "right cerebral cortex"]),
    ("cerebral_white_matter", "Cerebral white matter", [
        "left cerebral white matter", "right cerebral white matter",
    ]),
    ("whole_brain", "Whole brain", [
        f"{side} {name}" for side in ("left", "right") for name in BILATERAL_TISSUE
    ] + ["brain-stem"]),
)
TRIM_MARGIN_MM = 15.0
SUBJECT_PATTERN = re.compile(r"IXI(?P<id>\d+)-(?P<site>[A-Za-z]+)-\d+-T1")
SYNTHSEG_SETTINGS = "SynthSeg 2.0, fast mode, FreeSurfer 8.2.0 mri_synthseg --vol"
COHORTS = {
    "ixi": {
        "cohort": "IXI",
        "url": "https://brain-development.org/ixi-dataset/",
        "license": "CC-BY-SA-3.0",
        "field_strength": "1.5 T and 3 T",
        "synthseg": SYNTHSEG_SETTINGS,
    },
    "ahead": {
        "cohort": "AHEAD",
        "url": "https://doi.org/10.21942/uva.10007840",
        "license": "CC-BY-4.0",
        "field_strength": "7 T",
        "synthseg": f"{SYNTHSEG_SETTINGS} on skull-stripped images, corrected to whole-head volumes",
    },
}
# AHEAD publishes age only in these bins; each subject takes the bin midpoint.
AHEAD_AGE_BINS = {"18-30": (18, 30), "31-40": (31, 40), "41-50": (41, 50),
                  "51-60": (51, 60), "61-70": (61, 70), "71-80": (71, 80)}


def trim(image_path: Path, mask_path: Path, out_path: Path) -> None:
    """Cut the image to the brain-mask box plus a margin, keeping intensities and voxel grid."""
    import nibabel as nib

    image = nib.load(str(image_path))
    occupied = np.argwhere(np.asarray(nib.load(str(mask_path)).dataobj) > 0)
    margin = np.ceil(TRIM_MARGIN_MM / np.asarray(image.header.get_zooms()[:3])).astype(int)
    low = np.maximum(occupied.min(0) - margin, 0)
    high = np.minimum(occupied.max(0) + margin + 1, image.shape[:3])
    nib.save(image.slicer[low[0]:high[0], low[1]:high[1], low[2]:high[2]], str(out_path))


def assemble(volumes_csv: Path, demographics_xls: Path, out_csv: Path) -> None:
    import pandas as pd

    volumes = pd.read_csv(volumes_csv)
    demographics = pd.read_excel(demographics_xls).rename(
        columns={"IXI_ID": "ixi_id", "SEX_ID (1=m, 2=f)": "sex_id", "AGE": "age"}
    )[["ixi_id", "sex_id", "age"]]
    # IXI.xls repeats some subjects; keep an ID only when its rows agree.
    demographics = demographics.dropna().drop_duplicates()
    demographics = demographics[~demographics.ixi_id.duplicated(keep=False)].set_index("ixi_id")

    rows = []
    for record in volumes.to_dict("records"):
        match = SUBJECT_PATTERN.fullmatch(str(record["subject"]))
        ixi_id = int(match["id"])
        row = {"subject": record["subject"], "site": match["site"], "sex": "", "age": "", "exclude": ""}
        if ixi_id not in demographics.index:
            row["exclude"] = "no unique age and sex in IXI.xls"
        else:
            row["sex"] = {1: "male", 2: "female"}[int(demographics.at[ixi_id, "sex_id"])]
            row["age"] = round(float(demographics.at[ixi_id, "age"]), 2)
        row.update({name: record[name] for name in volumes.columns if name != "subject"})
        rows.append(row)

    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    excluded = sum(1 for row in rows if row["exclude"])
    print(f"assembled {len(rows)} subjects, {excluded} excluded")


def assemble_ahead(volumes_csv: Path, participants_csv: Path, out_csv: Path) -> None:
    """Join AHEAD SynthSeg volumes with participants.csv; age is the bin midpoint."""
    with participants_csv.open(newline="") as handle:
        participants = {row["ScanName"]: row for row in csv.DictReader(handle)}
    with volumes_csv.open(newline="") as handle:
        records = list(csv.DictReader(handle))
    rows = []
    for record in records:
        participant = participants[record["subject"]]
        low, high = AHEAD_AGE_BINS[participant["Group"]]
        rows.append({
            "subject": record["subject"], "site": "AHEAD",
            "sex": {"f": "female", "m": "male"}[participant["Sex"]],
            "age": (low + high + 1) / 2, "age_bin": participant["Group"], "exclude": "",
            **{name: record[name] for name in record if name != "subject"},
        })
    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"assembled {len(rows)} AHEAD subjects")


def strip_correction(whole_csv: Path, stripped_csv: Path, out_json: Path) -> None:
    """Per-column log ratio of whole-head to skull-stripped SynthSeg volumes of the same scans."""
    def read(path: Path) -> dict[str, dict[str, float]]:
        with path.open(newline="") as handle:
            return {row.pop("subject"): {k: float(v) for k, v in row.items()} for row in csv.DictReader(handle)}
    whole, stripped = read(whole_csv), read(stripped_csv)
    subjects = sorted(whole.keys() & stripped.keys())
    columns = [column for column in whole[subjects[0]] if column in stripped[subjects[0]]]
    correction = {}
    for column in columns:
        ratio = np.log([whole[s][column] / stripped[s][column] for s in subjects])
        correction[column] = {"log_ratio": float(ratio.mean()), "sd": float(ratio.std(ddof=1))}
    out_json.write_text(json.dumps({"subjects": subjects, "columns": correction}, indent=2) + "\n")
    for column, values in correction.items():
        print(f"{column:34} whole/stripped {math.exp(values['log_ratio']):.4f}  sd {values['sd']:.4f}")


def _fit_model(age: np.ndarray, log_volume: np.ndarray, log_icv_ratio: np.ndarray, site: np.ndarray) -> dict:
    a = (age - AGE_CENTER) / AGE_SCALE
    sites = sorted(set(site))
    # Sum-to-zero site contrasts make the intercept the unweighted mean over
    # scanners, so the shipped curve is not tied to any single IXI site.
    contrasts = np.column_stack([
        (site == name).astype(float) - (site == sites[-1]).astype(float) for name in sites[:-1]
    ]) if len(sites) > 1 else np.empty((len(a), 0))
    design = np.column_stack([np.ones_like(a), a, a * a, log_icv_ratio, contrasts])
    beta, *_ = np.linalg.lstsq(design, log_volume, rcond=None)
    residual_sd = float(np.std(log_volume - design @ beta))

    sigma_design = np.column_stack([np.ones_like(a), a])

    def negative_log_likelihood(params: np.ndarray) -> tuple[float, np.ndarray]:
        residual = log_volume - design @ params[:design.shape[1]]
        log_sigma = sigma_design @ params[-2:]
        precision = np.exp(-2.0 * log_sigma)
        value = float(np.sum(log_sigma + 0.5 * residual * residual * precision))
        gradient = np.concatenate([
            -design.T @ (residual * precision),
            sigma_design.T @ (1.0 - residual * residual * precision),
        ])
        return value, gradient

    start = np.concatenate([beta, [math.log(residual_sd), 0.0]])
    result = optimize.minimize(negative_log_likelihood, start, jac=True, method="BFGS")
    # BFGS reports precision loss when the line search stalls at the optimum,
    # so judge convergence by the gradient instead of its status flag.
    gradient_norm = float(np.max(np.abs(result.jac)))
    if gradient_norm > 1e-2:
        raise RuntimeError(f"maximum likelihood fit did not converge: {result.message} (|grad|={gradient_norm:.2g})")
    params = result.x
    return {
        "mu": [float(value) for value in params[:3]],
        "icv_slope": float(params[3]),
        "log_sigma": [float(params[-2]), float(params[-1])],
    }


def _z_scores(model: dict, age: np.ndarray, log_volume: np.ndarray, log_icv_ratio: np.ndarray) -> np.ndarray:
    a = (age - AGE_CENTER) / AGE_SCALE
    b0, b1, b2 = model["mu"]
    adjusted = log_volume - model["icv_slope"] * log_icv_ratio
    sigma = np.exp(model["log_sigma"][0] + model["log_sigma"][1] * a)
    return (adjusted - (b0 + b1 * a + b2 * a * a)) / sigma


def fit(table_csv: Path, out_json: Path, cohort: str = "ixi", correction_json: Path | None = None) -> None:
    with table_csv.open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if not row["exclude"]]
    if correction_json is not None:
        # Scale skull-stripped volumes to what SynthSeg reports on the whole head.
        correction = json.loads(correction_json.read_text())["columns"]
        for row in rows:
            for column, values in correction.items():
                row[column] = str(float(row[column]) * math.exp(values["log_ratio"]))
    age = np.array([float(row["age"]) for row in rows])
    sex = np.array([row["sex"] for row in rows])
    site = np.array([row["site"] for row in rows])
    log_icv_ratio = np.log(np.array([float(row[ICV_COLUMN]) for row in rows]) / REFERENCE_ICV_MM3)

    structures = []
    print(f"{'structure':24} {'sex':6} {'n':>4} {'in 5-95':>8} {'young':>6} {'mid':>6} {'old':>6}")
    for structure_id, title, columns in STRUCTURES:
        log_volume = np.log(np.array([sum(float(row[column]) for column in columns) for row in rows]))
        models = {}
        for sex_key in SEXES:
            keep = np.ones(len(rows), bool) if sex_key == "all" else sex == sex_key
            model = _fit_model(age[keep], log_volume[keep], log_icv_ratio[keep], site[keep])
            models[sex_key] = model
            inside = np.abs(_z_scores(model, age[keep], log_volume[keep], log_icv_ratio[keep])) <= stats.norm.ppf(0.95)
            tertiles = np.array_split(np.argsort(age[keep]), 3)
            print(
                f"{structure_id:24} {sex_key:6} {int(keep.sum()):4d} {inside.mean():8.3f} "
                + " ".join(f"{inside[part].mean():6.3f}" for part in tertiles)
            )
        structures.append({"id": structure_id, "title": title, "columns": columns, "models": models})

    norms = {
        "schema": SCHEMA,
        "reference": {
            **{key: COHORTS[cohort][key] for key in ("cohort", "url", "license", "field_strength")},
            "subjects": len(rows),
            "female": int((sex == "female").sum()),
            "male": int((sex == "male").sum()),
            "sites": sorted(set(site)),
            "age_min": round(float(age.min()), 1),
            "age_max": round(float(age.max()), 1),
            "synthseg": COHORTS[cohort]["synthseg"],
        },
        "age_center": AGE_CENTER,
        "age_scale": AGE_SCALE,
        "icv_column": ICV_COLUMN,
        "reference_icv_mm3": REFERENCE_ICV_MM3,
        "structures": structures,
    }
    out_json.write_text(json.dumps(norms, indent=2) + "\n")
    print(f"wrote {out_json} from {len(rows)} subjects")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    trim_parser = commands.add_parser("trim")
    for name in ("image", "mask", "out"):
        trim_parser.add_argument(name, type=Path)
    assemble_parser = commands.add_parser("assemble")
    for name in ("volumes_csv", "demographics_xls", "out_csv"):
        assemble_parser.add_argument(name, type=Path)
    ahead_parser = commands.add_parser("assemble-ahead")
    for name in ("volumes_csv", "participants_csv", "out_csv"):
        ahead_parser.add_argument(name, type=Path)
    correction_parser = commands.add_parser("strip-correction")
    for name in ("whole_csv", "stripped_csv", "out_json"):
        correction_parser.add_argument(name, type=Path)
    fit_parser = commands.add_parser("fit")
    fit_parser.add_argument("table_csv", type=Path)
    fit_parser.add_argument("out_json", type=Path)
    fit_parser.add_argument("--cohort", choices=sorted(COHORTS), default="ixi")
    fit_parser.add_argument("--strip-correction", type=Path)
    args = parser.parse_args()
    if args.command == "trim":
        trim(args.image, args.mask, args.out)
    elif args.command == "assemble":
        assemble(args.volumes_csv, args.demographics_xls, args.out_csv)
    elif args.command == "assemble-ahead":
        assemble_ahead(args.volumes_csv, args.participants_csv, args.out_csv)
    elif args.command == "strip-correction":
        strip_correction(args.whole_csv, args.stripped_csv, args.out_json)
    else:
        fit(args.table_csv, args.out_json, args.cohort, args.strip_correction)


if __name__ == "__main__":
    main()
