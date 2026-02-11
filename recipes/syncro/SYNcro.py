#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from scipy.ndimage import distance_transform_edt
from scipy.ndimage import gaussian_filter
import ants
import argparse
import logging
import nibabel as nib
import numpy as np
import os
import time
import platform
import re
import shutil
import subprocess
import sys
import importlib.metadata
from packaging import version
import tempfile
import errno
import gzip

def safe_basename(fname: str) -> str:
    """Strip NIfTI extensions and sanitize for filesystem use."""
    base = fname
    if base.endswith(".nii.gz"):
        base = base[:-7]
    elif base.endswith(".nii"):
        base = base[:-4]
    base = Path(base).name  # drop directories
    # Replace anything not alphanumeric, dash, underscore with "_"
    return re.sub(r'[^A-Za-z0-9._-]', '_', base)

def get_file_directory(filename):
    """
    Get the directory of a file.

    Parameters:
        filename (str): Path to the file.

    Returns:
        str: Directory of the file.
    """
    if os.path.isabs(filename):
        return os.path.dirname(filename)
    else:
        return os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(filename)))

def add_prefix(prefix, filename, directory = ""):
    """
    Add a prefix to a filename and return the full path with an optional new directory.

    Parameters:
        prefix (str): Prefix to prepend to the base filename.
        filename (str): Full path to the original file.
        directory (str): Optional target directory for the new filename. 
                         If empty, uses the directory of the input file.

    Returns:
        str: Full path to the new filename with the prefix, in the specified or original directory.
    """
    _, file_name = os.path.split(filename)
    if directory == "":
        directory = get_file_directory(filename)
    new_file_name = os.path.join(directory, f"{prefix}{file_name}")
    return new_file_name

def nifti_min_max_binary(filename):
    """
    Compute the minimum and maximum voxel values in a NIfTI image,
    and determine whether the image is binary (contains only 0 and 1).

    Parameters:
        filename (str): Path to the NIfTI image.

    Returns:
        tuple: (min_val, max_val, is_binary)
            - min_val (float): Minimum voxel value
            - max_val (float): Maximum voxel value
            - is_binary (bool): True if image contains only 0 and 1, else False
    """
    img = nib.load(filename)
    data = img.get_fdata()
    min_val = np.nanmin(data)
    max_val = np.nanmax(data)
    is_binary = np.array_equal(np.unique(data), [min_val, max_val])
    return min_val, max_val, is_binary

def do_ants(template_path, input_path, directory="", others_path=[]):
    """
    Register an input image to a template using ANTs and apply the same transform
    to additional images with appropriate interpolation.

    Parameters:
        template_path (str): Path to the fixed image (template).
        input_path (str): Path to the moving image (to be registered).
        directory (str): Optional target directory for the output image(s).
                         If empty, uses the directory of the input file.
        others_path (list of str): Paths to additional images aligned with the moving image
                                   to be transformed using the same registration.

    Returns:
        tuple:
            - warped_input_path (str): Path to the warped moving image (with 'w' prefix).
            - warped_others (list of str): Paths to the warped additional images.
    """
    fixed = ants.image_read(template_path)
    moving = ants.image_read(input_path)
    if directory == "":
        directory = get_file_directory(input_path)
    # Use all available CPU threads (or set a specific number)
    os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(os.cpu_count())

    # Perform registration
    reg = ants.registration(fixed=fixed, moving=moving, type_of_transform='SyN')
    
    # Save warped moving image
    warped_moving = reg['warpedmovout']
    warped_input_path = add_prefix("w", input_path, directory)
    warped_moving.to_file(warped_input_path)

    # Apply transform to others
    warped_others = []
    for other_path in others_path:
        other_img = ants.image_read(other_path)
        min_val, max_val, is_binary = nifti_min_max_binary(other_path)
        interp = 'nearestNeighbor' if is_binary else 'linear'
        logging.info(f"{os.path.basename(other_path)}: min={min_val}, max={max_val}, interp={interp}")
        warped_other = ants.apply_transforms(
            fixed=fixed,
            moving=other_img,
            transformlist=reg['fwdtransforms'],
            interpolator=interp,
            defaultvalue=min_val
        )
        warped_other_path = add_prefix("w", other_path, directory)
        warped_other.to_file(warped_other_path)
        warped_others.append(warped_other_path)

    return warped_input_path, warped_others

def get_num_threads():
    cpu_count = os.cpu_count() or 1  # fallback to 1 if None
    return max(1, cpu_count - 1)

def do_synthsr(input_path, output_path, is_gpu=False, is_ct=False):
    # Pick executable: mri_synthsr → py_synthsr → error
    # exe = shutil.which("py_synthsr")    
    exe = shutil.which("mri_synthsr")
    if exe is None:
        exe = shutil.which("py_synthsr")
    if exe is None:
        sys.exit("Error: Could not find 'mri_synthsr' or 'py_synthsr'. Please install:\n"
                 "  https://github.com/neurolabusc/py_synthsr")
    
    cmd = [exe, '--i', str(input_path), '--o', str(output_path)]
    if not is_gpu:
        cmd += ['--cpu', '--threads', str(get_num_threads())]
    if is_ct:
        cmd.append('--ct')
    logging.info('Running: %s', ' '.join(cmd))
    subprocess.run(cmd, check=True)
    # --- Post-check for negative values ---
    if os.path.exists(output_path):
        img = nib.load(output_path)
        data = img.get_fdata(dtype=np.float32)  # always float for check
        if np.any(data < 0):
            sys.exit(
                f"Error: Output image {output_path} contains negative voxel values. "
                "This suggests an old or faulty version of SynthSR. "
                "T1-weighted magnitude images should never have negative values."
            )


def do_synthstrip(input_path, out_path):
    """
    Run synthstrip CLI from Python.

    Parameters:
    - input_path (str): Path to input NIfTI file
    - out_path (str): Path to save output segmentation
    """
    cmd = ["mri_synthstrip", "-i", input_path, "-o", out_path]

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"do_synthstrip failed: {e}")

def do_brainchop(input_path, out_path, is_ct=False, model="mindgrab"):
    """
    Run brainchop CLI from Python.

    Parameters:
    - input_path (str): Path to input NIfTI file
    - out_path (str): Path to save output segmentation
    - is_ct (bool): If True, adds the --ct flag (default: False)
    - model (str): Model name to use (default: 'mindgrab')
    """
    cmd = ["brainchop", "-m", model, "-i", input_path, "-o", out_path]
    if is_ct:
        cmd.append("--ct")
    try:
        subprocess.run(cmd, check=True)
        if not os.path.exists(out_path) and os.path.exists(out_path + ".gz"):
            gz_path = out_path + ".gz"
            with gzip.open(gz_path, "rb") as f_in, open(out_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"brainchop failed: {e}")

def check_nifti(file_list):
    """
    Check if a list of NIfTI images have the same shape and orientation.

    Parameters:
        file_list (list): List of NIfTI image paths.

    Returns:
        bool: True if all images have the same shape and orientation, False otherwise.
    """
    for fnm in file_list:
        if not os.path.isfile(fnm):
            raise EnvironmentError(f"Unable to find input: {fnm}")
        if not fnm.lower().endswith(('.nii', '.nii.gz')):
            raise EnvironmentError(f"Requires NIfTI format image: {fnm}")
    reference_img = nib.load(file_list[0])
    if len(file_list) < 2:
        return True
    ref_shape = reference_img.shape[:3]
    ref_qform = reference_img.get_qform()
    ref_sform = reference_img.get_sform()
    tolerance = 0.01
    for file_path in file_list[1:]:
        current_img = nib.load(file_path)
        if current_img.shape[:3] != ref_shape:
            logging.warning(f"Dimensions of '{file_list[0]}' differ from '{file_path}': {ref_shape} != {current_img.shape[:3]}")
            return False
        curr_qform = current_img.get_qform()
        curr_sform = current_img.get_sform()
        if not np.allclose(curr_sform, ref_sform, atol=tolerance):
            logging.warning(f"SForm of '{file_list[0]}' differs from '{file_path}'.")
            logging.warning(f"{ref_sform}\n!=\n{curr_sform}")
            return False
        if not np.allclose(curr_qform, ref_qform, atol=tolerance):
            logging.warning(f"QForm of '{file_list[0]}' differs from '{file_path}'.")
            logging.warning(
                f"QForm differs between '{file_list[0]}' and '{file_path}'.\n"
                f"hint: if the sforms agree but qforms differ, you can fix consistency by running:\n"
                f"  fslorient -copysform2qform <image>"
            )
            return False
    return True


import nibabel as nib
import numpy as np
import logging

def binarize_nifti(input_filename):
    """
    Binarize a NIfTI image using the midpoint between min and max
    of finite voxel values as the threshold.

    Parameters:
        input_filename (str): Path to the input NIfTI image.
    """
    img = nib.load(input_filename)
    data = img.get_fdata()

    # Remove non-finite values
    finite_data = data[np.isfinite(data)]
    if finite_data.size == 0:
        logging.warning(f"No finite values in {input_filename}, skipping.")
        return

    vmin = np.min(finite_data)
    vmax = np.max(finite_data)

    if vmax == vmin:
        logging.warning(f"No variability in {input_filename}, skipping.")
        return

    threshold = (vmin + vmax) / 2.0
    logging.info(f"Binarizing {input_filename} using threshold {threshold}")

    binary_data = (data >= threshold).astype(np.uint8)

    # Force correct dtype
    img.header.set_data_dtype(np.uint8)
    binary_img = nib.Nifti1Image(binary_data, img.affine, img.header)
    nib.save(binary_img, input_filename)


def dilate_smooth_nifti(input_path, output_path = "", dilate_vox=0, fwhm_mm=3):
    """
    Dilate and/or smooth a binary NIfTI image in-place.
    
    Parameters:
        input_path (str): Path to the binary input NIfTI image.
        output_path (str): Optional path to save the modified output. Defaults to overwriting the input.
        dilate_vox (int): Radius in voxels to dilate binary regions (default: 0 = no dilation).
        fwhm_mm (float): Full-width at half maximum for Gaussian smoothing (default: 3 mm).
    """
    if output_path == "":
        output_path = input_path
    img = nib.load(input_path)
    data = img.get_fdata()

    if len(np.unique(data)) != 2:
        if len(np.unique(data)) == 1:
            logging.warning(f"Unary image has no variation (expected binary 0s and 1s): {input_path}")
        else:
            raise ValueError(f"Input image must be binary (contain only 0s and 1s): {input_path}")
    logging.info(f"dilation {dilate_vox} smooth {fwhm_mm}mm: {input_path}")
    if dilate_vox > 0:
        dist_transform = distance_transform_edt(1 - data)
        data = (dist_transform <= dilate_vox).astype(np.float32)

    if fwhm_mm > 0:
        voxel_sizes = np.sqrt(np.sum(img.affine[:3, :3] ** 2, axis=0))
        sigma = fwhm_mm / (2 * np.sqrt(2 * np.log(2)))
        data = gaussian_filter(data, sigma / voxel_sizes)

    dilated_img = nib.Nifti1Image(data, img.affine)
    nib.save(dilated_img, output_path)

def is_cuda_installed():
    try:
        # Execute the nvcc command to get CUDA version
        nvcc_version = subprocess.check_output(["nvcc", "--version"]).decode("utf-8")
        return True, nvcc_version
    except FileNotFoundError:
        # If nvcc command is not found, CUDA is likely not installed
        return False, "CUDA is not installed."

def copy_nifti(outdir, file_list):
    """
    Copy NIfTI files to a specified directory.

    Parameters:
        outdir (str): Directory to copy the files to.
        file_list (list): List of NIfTI image paths.

    Returns:
        list: List of copied and potentially compressed file paths.
    """
    Path(outdir).mkdir(parents=True, exist_ok=True)
    output_file_list = []
    for fnm in file_list:
        fnmout = os.path.join(outdir, Path(fnm).name)
        shutil.copy(fnm, fnmout)
        output_file_list.append(fnmout)
    return output_file_list

def _rmtree_onerror(func, path, exc_info):
    # Ignore "No such file or directory" errors during rmtree
    if exc_info[1].errno == errno.ENOENT:
        return
    raise

def normalize(fnms, outdir = "", log_level = 'silent', is_gpu = False, is_bet = False, is_ct = False):
    """
    Normalize and warp NIfTI images to a standard space using SynthSR, mindgrab, and ANTs.

    Parameters:
        fnms (list of str): List of NIfTI file paths.
                            The first image is the primary scan (best resolution).
                            Subsequent images (e.g., lesion or pathology masks) will be co-registered.
        outdir (str): Output directory. If empty, uses the same directory as the input.
        is_gpu (bool): If True, use GPU acceleration (requires CUDA and SynthSR GPU support).
        is_bet (bool): If True, assumes the input is already brain extracted (currently unused).
        is_ct (bool): If True, treat the image as a CT scan (e.g., skip SynthSR, adjust Brainchop/ANTs).
                      If False, the function attempts to auto-detect CT based on intensity (< -500).

    Returns:
        tuple:
            warped_input (str): Path to the warped synthetic T1-weighted image in template space.
            warped_others (list of str): Paths to the warped versions of the additional input images.
    """
    log_levels = {
        'silent': logging.WARNING,
        'verbose': logging.INFO,
        'debug': logging.DEBUG
    }
    outdir = os.path.abspath(outdir)
    logging.basicConfig(level=log_levels[log_level], format='%(levelname)s - %(message)s')

    # find template
    templatebase = 'MNI152_T1_1mm_brain.nii.gz'
    script_folder = os.path.dirname(os.path.abspath(__file__))
    template = os.path.join(script_folder, templatebase)
    if not os.path.isfile(template):
        fsldir = os.environ.get('FSLDIR')
        if fsldir is not None:
            template = os.path.join(fsldir, 'data', 'standard', templatebase)
        if not os.path.isfile(template):
            raise EnvironmentError(f"Template required: {template}")

    if is_gpu:
        is_gpu = is_cuda_installed()[0]

    pth = get_file_directory(fnms[0])
    if outdir == "":
        outdir = pth
    if not os.access(outdir, os.W_OK):
        raise EnvironmentError(f"Write access required: {outdir}")
    if not check_nifti(fnms):
        sys.exit("Error: NIfTI files must have the same shape and orientation.")

    # --- Create a unique temp directory safely ---
    tmpdir = tempfile.mkdtemp(prefix="SYNcro_", dir=outdir)

    try:
        fnms = copy_nifti(tmpdir, fnms)
        are_binary = [nifti_min_max_binary(f)[2] for f in fnms]

        if not is_ct:
            min_val, _, _ = nifti_min_max_binary(fnms[0])
            is_ct = min_val < -500
            if is_ct:
                logging.info(f"Auto-detected CT input based on min={min_val}")

        for fname, is_binary in zip(fnms, are_binary):
            if is_binary:
                dilate_smooth_nifti(fname)  # in-place with default parameters

        # synthesize a T1w scan
        t1fnm = add_prefix('t1', fnms[0], tmpdir)
        do_synthsr(fnms[0], t1fnm, is_gpu, is_ct)

        # brain extract
        bt1fnm = add_prefix('b', t1fnm)
        if shutil.which("mri_synthstrip"):
          do_synthstrip(t1fnm, bt1fnm)
        else:
          import brainchop
          # check brainchop mindgrab requirement
          req_ver = "0.1.18"
          try:
              bc_ver = importlib.metadata.version("brainchop")
          except importlib.metadata.PackageNotFoundError:
              raise EnvironmentError("brainchop required: pip install brainchop")
          if version.parse(bc_ver) < version.parse(req_ver):
              raise EnvironmentError(f"brainchop {req_ver}+ required (found {bc_ver})")

          do_brainchop(t1fnm, bt1fnm)

        # warp to template, apply warp to all images
        warped_input, warped_others = do_ants(template, bt1fnm, outdir, fnms)
        for fname, is_binary in zip(warped_others, are_binary):
            if is_binary:
                binarize_nifti(fname)

        return warped_input, warped_others

    finally:
        # Robust cleanup
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir, onerror=_rmtree_onerror)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Normalize NIfTI images with lesion maps.')
    parser.add_argument('fnms', metavar='N', type=str, nargs='+',
                        help='NIfTI images: first is anatomical (required), second (optional) lesion map, third (optional) pathological')
    parser.add_argument('--force-gpu', dest='force_gpu',
                        choices=['true', 'false', 'auto'], default='auto',
                        help='Force GPU usage (true), disable GPU (false), or auto-detect (auto, default)')
    parser.add_argument('-b', '--bet', dest='is_bet', default=False, action='store_true',
                        help='images are already brain extracted (default: False)')
    parser.add_argument('-c', '--ct', dest='is_ct', default=False, action='store_true',
                        help='images are CT scans (default: False)')
    parser.add_argument('-d', '--directory', dest='directory', type=str, default='',
                        help='output directory (default: same as input)')
    parser.add_argument('--log', dest='log_level',
                        choices=['silent', 'verbose', 'debug'], default='verbose',
                        help='Set log level: silent (default), verbose, or debug')
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s 0.5.20250505')

    args = parser.parse_args()

    # Resolve is_gpu from force_gpu
    if args.force_gpu == 'true':
        is_gpu = True
    elif args.force_gpu == 'false':
        is_gpu = False
    else:  # auto
        is_gpu = platform.system() == 'Linux' and is_cuda_installed()[0]

    start_time = time.perf_counter()
    normalize(args.fnms, args.directory, args.log_level, is_gpu, args.is_bet, args.is_ct)
    logging.info('SYNcro time: %d ms', round((time.perf_counter() - start_time) * 1000))


