#!/opt/brainstemnavigator/bin/python
import argparse
from pathlib import Path

import nibabel as nib
import numpy as np

parser = argparse.ArgumentParser(description="Inspect user-supplied Brainstem Navigator atlas images. Download v1.0 yourself from NITRC after accepting its agreement.")
parser.add_argument("atlas", type=Path, help="Mounted directory containing your extracted atlas")
args = parser.parse_args()
if not args.atlas.is_dir():
    parser.error("atlas must be an existing directory")
images = sorted(set(args.atlas.rglob("*.nii")) | set(args.atlas.rglob("*.nii.gz")))
if not images:
    parser.error("atlas contains no NIfTI images")
for path in images:
    image = nib.load(path)
    data = np.asanyarray(image.dataobj)
    if image.ndim < 3 or not np.isfinite(data).all() or not np.isfinite(image.affine).all():
        parser.error(f"invalid volume or affine in {path}")
    print(f"{path.relative_to(args.atlas)} shape={image.shape} voxel_size={image.header.get_zooms()}")
print(f"Validated {len(images)} NIfTI images")
