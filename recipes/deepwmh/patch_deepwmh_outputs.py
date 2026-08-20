"""Fix two silent output defects in DeepWMH itself.

1. save_nifti() casts data to float32 but reuses the caller's header, which
   for the segmentation stages declares an on-disk dtype of uint8. nibabel
   then stores 255 and sets scl_slope=1/255, so the declared final mask
   (002_Segmentations/003_postproc_fov) reads back as {0, 1.00000006} and
   `mask == 1` silently selects nothing, while code reading raw integers sees
   255. The raw stage of the same run writes a true {0, 1}, so the encoding is
   not even self-consistent. Nothing errors and the mask looks correct in a
   viewer, which makes this the most likely defect to quietly corrupt an
   analysis.

2. nii_as_gif() writes previews through imageio 2.13.2, pinned by DeepWMH's
   setup.py. For several input geometries the result could not be read by any
   decoder tried -- including imageio's own reader inside this same image --
   while deepwmh still exited 0. The frames themselves are fine, so they are
   collected and written with Pillow, which is already installed as an imageio
   dependency. The imageio pin is left alone: later releases changed the GIF
   duration unit, which would silently alter the animation speed.
"""

import pathlib
import sys

root = pathlib.Path(sys.argv[1])

# -- 1. save_nifti ---------------------------------------------------------
data_io = root / "deepwmh/utilities/data_io.py"
source = data_io.read_text()
old_save = (
    "def save_nifti(data, header, path):\n"
    "    nib.save(nib.nifti1.Nifti1Image(data.astype('float32'), None, header=header),path)\n"
)
new_save = (
    "def save_nifti(data, header, path):\n"
    "    # neurocontainers: keep the header's dtype in step with the data.\n"
    "    # Reusing a uint8 header for float32 data makes nibabel rescale, storing\n"
    "    # 255 with scl_slope=1/255, so `mask == 1` finds nothing downstream.\n"
    "    data = data.astype('float32')\n"
    "    header = header.copy()\n"
    "    header.set_data_dtype(data.dtype)\n"
    "    nib.save(nib.nifti1.Nifti1Image(data, None, header=header), path)\n"
)
if source.count(old_save) != 1:
    raise SystemExit(
        "expected exactly one save_nifti definition in %s, found %d; upstream "
        "changed and this patch needs revisiting" % (data_io, source.count(old_save))
    )
data_io.write_text(source.replace(old_save, new_save, 1))
print("patched %s" % data_io)

# -- 2. GIF writer ---------------------------------------------------------
preview = root / "deepwmh/utilities/nii_preview.py"
source = preview.read_text()
old_writer = "    with imageio.get_writer(outpath, mode='I', duration=dt) as writer:\n"
new_writer = "    with _neurocontainers_gif_writer(outpath, dt) as writer:\n"
if source.count(old_writer) != 1:
    raise SystemExit(
        "expected exactly one imageio.get_writer call in %s, found %d; upstream "
        "changed and this patch needs revisiting" % (preview, source.count(old_writer))
    )
source = source.replace(old_writer, new_writer, 1)

shim = '''

class _neurocontainers_gif_writer(object):
    """Collect frames and write the GIF with Pillow.

    Same interface as the imageio writer it replaces: a context manager with
    append_data(). See patch_deepwmh_outputs.py for why.
    """

    def __init__(self, path, duration):
        self.path = path
        self.duration = duration
        self.frames = []

    def append_data(self, frame):
        self.frames.append(np.asarray(frame, dtype='uint8'))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None or not self.frames:
            return False
        from PIL import Image
        images = [Image.fromarray(frame) for frame in self.frames]
        images[0].save(
            self.path, save_all=True, append_images=images[1:],
            duration=max(int(round(self.duration * 1000)), 20), loop=0)
        return False
'''
preview.write_text(source + shim)
print("patched %s" % preview)
