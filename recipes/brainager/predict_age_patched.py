#!/usr/bin/env python3
import sys, os, subprocess, shutil, argparse

def check_r_dependencies(packages, rscript_bin):
    """Ensure required R packages are installed"""
    for pkg in packages:
        print(f"[INFO] Checking for R package: {pkg}")
        try:
            subprocess.run(
                [rscript_bin, "-e",
                 f"if (!require('{pkg}', quietly=TRUE)) install.packages('{pkg}', repos='https://cloud.r-project.org')"],
                check=True
            )
        except subprocess.CalledProcessError:
            sys.exit(f"ERROR: Failed to install or load R package '{pkg}'")

def predict_new_data_gm_wm_csf(tempdir, brainager_dir=None, subjname="T1w.nii"):
    """
    Run brainageR's predict_new_data_gm_wm_csf.R on smwc* files in tempdir.

    Args:
        tempdir (str): sandbox directory with smwc1<subjname>, smwc2<subjname>, smwc3<subjname>
        brainager_dir (str, optional): root of the brainageR repo.
            Defaults to the folder containing this script.
        subjname (str, optional): postfix of the smwc* files (default: "T1w.nii")
    """

    if brainager_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        brainager_dir = script_dir

    # check Rscript (case-insensitive)
    print(f"[DEBUG] PATH={os.environ.get('PATH')}")
    print(f"[DEBUG] which Rscript={shutil.which('Rscript')}")
    print(f"[DEBUG] which rscript={shutil.which('rscript')}")

    rscript_bin = shutil.which("Rscript") or shutil.which("rscript")
    if not rscript_bin:
        sys.exit("ERROR: !Rscript not found in PATH — please install R")

    # ensure required R packages
    # required = ["caret", "kernlab", "e1071", "optparse", "RNifti"]
    # check_r_dependencies(required, rscript_bin)

    # locate required files
    smwc1_nii = os.path.join(tempdir, f"smwc1{subjname}")
    smwc2_nii = os.path.join(tempdir, f"smwc2{subjname}")
    smwc3_nii = os.path.join(tempdir, f"smwc3{subjname}")

    rscript = os.path.join(brainager_dir, "predict_new_data_gm_wm_csf.R")
    model   = os.path.join(brainager_dir, "GPR_model_gm_wm_csf.RData")
    if not os.path.exists(rscript):
        sys.exit(f"ERROR: R script not found: {rscript}")
    if not os.path.exists(model):
        sys.exit(f"ERROR: Model file not found: {model}")

    output_csv = os.path.join(tempdir, "brainage_prediction.csv")

    cmd = [
        rscript_bin, rscript,
        brainager_dir,
        smwc1_nii, smwc2_nii, smwc3_nii,
        model,
        output_csv
    ]
    print(f"[INFO] Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"[INFO] Prediction written to: {output_csv}")

    # --- overlay png ---
    if shutil.which("slices"):
        subj_stem = os.path.splitext(subjname)[0]
        overlay_png = os.path.join(brainager_dir, f"{subj_stem}.png")
        try:
            subprocess.run(
                ["slices", smwc1_nii, smwc2_nii, "-o", overlay_png],
                check=True
            )
            print(f"[INFO] Overlay saved to: {overlay_png}")
        except subprocess.CalledProcessError:
            print("[WARN] 'slices' command failed to produce overlay")
    else:
        print("[INFO] 'slices' not found in PATH — skipping overlay")

    return output_csv

def main():
    parser = argparse.ArgumentParser(description="Run brainageR prediction on segmented images")
    parser.add_argument("tempdir", help="Sandbox directory with smwc* files")
    parser.add_argument(
        "--subjname",
        default="T1w.nii",
        help="Postfix for smwc1/2/3 images (default: T1w.nii)"
    )
    args = parser.parse_args()

    tempdir = os.path.abspath(args.tempdir)
    if not os.path.isdir(tempdir):
        sys.exit(f"ERROR: Not a directory: {tempdir}")

    predict_new_data_gm_wm_csf(tempdir, subjname=args.subjname)

if __name__ == "__main__":
    main()
