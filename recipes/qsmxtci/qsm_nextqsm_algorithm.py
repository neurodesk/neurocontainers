#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import tempfile
import shutil

def parse_arguments():
    parser = argparse.ArgumentParser(description='QSM reconstruction using NeXtQSM deep learning')
    parser.add_argument('output_file', help='Output QSM map file path')
    parser.add_argument('--mag_files', nargs='+', required=True, help='Multi-echo magnitude image files')
    parser.add_argument('--phase_files', nargs='+', required=True, help='Multi-echo phase image files')
    parser.add_argument('--echo_times', type=float, nargs='+', required=True, help='Echo times in seconds')
    parser.add_argument('--field_strength', type=float, required=True, help='Magnetic field strength in Tesla')
    parser.add_argument('--mask_file', required=True, help='Brain mask file')
    return parser.parse_args()

def main():
    print("[INFO] Starting QSM-NeXtQSM algorithm...")

    # Parse command line arguments
    args = parse_arguments()
    output_file = args.output_file
    mag_files = args.mag_files
    phase_files = args.phase_files
    echo_times = args.echo_times
    field_strength = args.field_strength
    mask_file = args.mask_file

    print(f"[INFO] Number of echoes: {len(mag_files)}")
    print(f"[INFO] Magnetic field strength: {field_strength}T")
    print(f"[INFO] Echo times: {echo_times}")

    # Validate inputs
    if len(mag_files) != len(phase_files):
        print(f"[ERROR] Number of magnitude files ({len(mag_files)}) must match number of phase files ({len(phase_files)})")
        sys.exit(1)
    if len(mag_files) != len(echo_times):
        print(f"[ERROR] Number of image files ({len(mag_files)}) must match number of echo times ({len(echo_times)})")
        sys.exit(1)

    # Verify all input files exist
    all_files = mag_files + phase_files + [mask_file]
    for file_path in all_files:
        if not os.path.exists(file_path):
            print(f"[ERROR] Input file does not exist: {file_path}")
            sys.exit(1)

    # Create temporary BIDS structure for QSMxT
    with tempfile.TemporaryDirectory() as temp_dir:
        print("[INFO] Creating temporary BIDS structure...")
        bids_temp = os.path.join(temp_dir, 'bids_temp')
        anat_dir = os.path.join(bids_temp, 'sub-01', 'anat')
        os.makedirs(anat_dir, exist_ok=True)

        # Copy files to temporary BIDS structure
        print("[INFO] Copying files to temporary BIDS structure...")
        for i, (mag_file, phase_file) in enumerate(zip(mag_files, phase_files)):
            echo_num = i + 1
            mag_dest = os.path.join(anat_dir, f'sub-01_part-mag_echo-{echo_num}_MEGRE.nii.gz')
            phase_dest = os.path.join(anat_dir, f'sub-01_part-phase_echo-{echo_num}_MEGRE.nii.gz')

            print(f"[INFO] Copying echo {echo_num} files...")
            shutil.copy2(mag_file, mag_dest)
            shutil.copy2(phase_file, phase_dest)

        # Create temporary output directory
        output_temp = os.path.join(temp_dir, 'output_temp')
        os.makedirs(output_temp, exist_ok=True)

        # Run QSMxT with NeXtQSM
        print("[INFO] Running QSMxT with NeXtQSM...")
        qsmxt_cmd = [
            'qsmxt', bids_temp, output_temp,
            '--premade', 'nextqsm',
            '--subjects', 'sub-01',
            '--auto_yes'
        ]

        print(f"[INFO] Executing: {' '.join(qsmxt_cmd)}")
        try:
            result = subprocess.run(qsmxt_cmd, capture_output=True, text=True, check=True)
            print("[INFO] QSMxT completed successfully")
            if result.stdout:
                print("[INFO] QSMxT stdout:", result.stdout[-500:])  # Last 500 chars
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] QSMxT failed with return code {e.returncode}")
            print(f"[ERROR] stderr: {e.stderr}")
            print(f"[ERROR] stdout: {e.stdout}")
            sys.exit(1)

        # Find and copy result to final output
        chi_files = []
        for root, dirs, files in os.walk(output_temp):
            for file in files:
                if 'chi' in file.lower() and file.endswith(('.nii', '.nii.gz')):
                    chi_files.append(os.path.join(root, file))

        if not chi_files:
            print("[ERROR] No QSM output files found!")
            print("[INFO] Output directory contents:")
            for root, dirs, files in os.walk(output_temp):
                for file in files:
                    print(f"  {os.path.join(root, file)}")
            sys.exit(1)

        # Use the first chi file found
        result_file = chi_files[0]
        print(f"[INFO] Copying result from: {result_file}")
        print(f"[INFO] Copying result to: {output_file}")

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        shutil.copy2(result_file, output_file)

    print("[INFO] QSM-NeXtQSM algorithm completed successfully!")

if __name__ == "__main__":
    main()