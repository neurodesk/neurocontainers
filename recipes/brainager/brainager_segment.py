#!/usr/bin/env python3
import sys, os, subprocess, shutil, gzip, csv, argparse, fcntl
from pathlib import Path

def clean_stem(p: Path) -> str:
    stem = p.name
    if stem.endswith(".nii.gz"):
        stem = stem[:-7]
    elif stem.endswith(".nii"):
        stem = stem[:-4]
    return stem

def gunzip_nii_gz(src, dst):
    """Uncompress .nii.gz -> .nii"""
    with gzip.open(src, 'rb') as f_in, open(dst, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def append_to_shared_csv(out_csv, stem, prediction_csv):
    """Append results from prediction_csv into a shared out_csv with file lock"""
    with open(prediction_csv, newline="") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        row = next(reader)
        row[0] = stem

    file_exists = os.path.exists(out_csv)
    with open(out_csv, "a+", newline="") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)
        fcntl.flock(f, fcntl.LOCK_UN)

def main():
    parser = argparse.ArgumentParser(description="BrainageR segmentation + prediction wrapper")
    parser.add_argument("t1", help="Input T1 image (.nii or .nii.gz)")
    parser.add_argument("outdir", nargs="?", default=None, help="Output directory (default: script dir)")
    parser.add_argument("--delete-temp", action="store_true", help="Delete temporary folder (default: keep)")
    args = parser.parse_args()

    if shutil.which("run_spm12.sh") is None:
        sys.exit("ERROR: 'run_spm12.sh' not found. 'ml spm12'\n")

    # Input T1
    input_t1 = os.path.abspath(args.t1)
    if not os.path.exists(input_t1):
        sys.exit(f"ERROR: T1 file not found: {input_t1}")

    stem = clean_stem(Path(input_t1))

    # Output root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_root = os.path.abspath(args.outdir) if args.outdir else script_dir
    os.makedirs(out_root, exist_ok=True)

    # Locate batch template and templates folder
    batch_template = os.path.join(script_dir, "brainager_batch.m")
    if not os.path.exists(batch_template):
        sys.exit(f"ERROR: brainager_batch.m not found in {script_dir}")
    templates_src = os.path.join(script_dir, "templates")
    if not os.path.isdir(templates_src):
        sys.exit(f"ERROR: templates folder not found in {script_dir}")

    # Workdir is out_root/stem
    workdir = os.path.join(out_root, stem)
    os.makedirs(workdir, exist_ok=True)

    # Copy and patch batch file
    batch_copy = os.path.join(workdir, "brainager_batch.m")
    with open(batch_template, "r") as f_in, open(batch_copy, "w") as f_out:
        for line in f_in:
            if line.strip().startswith("t1 ="):
                f_out.write(f"t1 = './{stem}.nii';\n")
            elif line.strip().startswith("template_dir ="):
                f_out.write(f"template_dir = '{templates_src}/';\n")
            else:
                f_out.write(line)

    # Prepare T1 file as stem.nii
    t1_dst = os.path.join(workdir, f"{stem}.nii")
    if input_t1.endswith(".nii.gz"):
        gunzip_nii_gz(input_t1, t1_dst)
    elif input_t1.endswith(".nii"):
        shutil.copy2(input_t1, t1_dst)
    else:
        sys.exit("ERROR: Input must be .nii or .nii.gz")

    print(f"[INFO] Working directory: {workdir}")
    print(f"[INFO] Running SPM12 with T1: {t1_dst}")

    # Run spm12 standalone
    cmd = ["run_spm12.sh", "/opt/mcr/v97", "script", batch_copy]
    subprocess.run(cmd, check=True, cwd=workdir)

    print(f"[INFO] SPM12 finished, now running prediction")

    # Call predict_age.py with subjname
    predict_script = os.path.join(script_dir, "predict_age.py")
    subprocess.run([sys.executable, predict_script, workdir, "--subjname", f"{stem}.nii"], check=True)

    # Append results to shared CSV
    prediction_csv = os.path.join(workdir, "brainage_prediction.csv")
    if not os.path.exists(prediction_csv):
        sys.exit("ERROR: Prediction CSV not found")

    out_csv = os.path.join(out_root, "brainage_prediction.csv")
    append_to_shared_csv(out_csv, stem, prediction_csv)

    print(f"[INFO] Appended result for {stem} to {out_csv}")

    if args.delete_temp:
        shutil.rmtree(workdir)
        print(f"[INFO] Deleted temp folder: {workdir}")
    else:
        print(f"[INFO] Temp folder kept: {workdir}")

if __name__ == "__main__":
    main()
