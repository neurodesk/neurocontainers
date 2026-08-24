"""Let imageio read GIFs whose palette holds fewer than 256 colours.

imageio's Pillow plugin decides whether a palettised image is grayscale with

    palette = np.asarray(pil_image.getpalette()).reshape((256, 3))

which was safe while Pillow padded getpalette() out to 256 entries. Pillow 9.2
changed it to return only the colours actually used, so reading any GIF with a
smaller palette raises

    ValueError: cannot reshape array of size 24 into shape (256,3)

imageio 2.13.2 (Feb 2022) predates that change, and DeepWMH's setup.py pins it.

This is not cosmetic. Real FLAIR previews routinely quantise to 8-128 colours,
and testing on three clinical T2-FLAIR subjects found two of the three previews
unreadable this way -- valid GIFs that ImageMagick and a current Pillow decode
without complaint, which the container's own reader could not open. DeepWMH's
try_load_gif() therefore returned False for them, and both predict.py and
DCNN_multistage.py gate preview generation on `try_load_gif(out_gif) == False`,
so every rerun regenerated exactly the previews that fail.

Upstream imageio fixed this by reshaping to (-1, 3). Only that line is
corrected rather than bumping the pin, because later imageio releases also
changed the GIF duration unit, which would silently alter the animation speed.
"""

import pathlib

import imageio.plugins

OLD = "np.asarray(pil_image.getpalette()).reshape((256, 3))"
NEW = "np.asarray(pil_image.getpalette()).reshape((-1, 3))"

# Located through the interpreter that will actually import it, rather than a
# hardcoded site-packages path that goes stale with the Python or env version.
plugins = pathlib.Path(imageio.plugins.__file__).parent

patched = 0
for target in sorted(plugins.glob("*.py")):
    source = target.read_text()
    count = source.count(OLD)
    if count == 0:
        continue
    if count != 1:
        raise SystemExit(
            "expected at most one 256-entry palette reshape in %s, found %d; "
            "imageio changed and this patch needs revisiting" % (target, count)
        )
    target.write_text(source.replace(OLD, NEW, 1))
    print("patched %s" % target)
    patched += 1

if patched == 0:
    raise SystemExit(
        "no 256-entry palette reshape found under %s; either imageio fixed this "
        "upstream and the patch can be dropped, or the code moved. Either way "
        "this needs revisiting rather than silently doing nothing." % plugins
    )
