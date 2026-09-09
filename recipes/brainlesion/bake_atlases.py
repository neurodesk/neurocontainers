"""Bake the registration atlases into the image.

`AtlasCentricPreprocessor` -- what the `preprocessor` command builds -- defaults
to `Atlas.BRATS_SRI24` and calls `fetch_atlases()` to get it. That downloads
into `brainles_preprocessing/registration/atlases`, inside the installed
package, which is read-only once the image is built, so the command failed
within seconds of starting with its own default settings:

    OSError: [Errno 30] Read-only file system:
    '.../site-packages/brainles_preprocessing/registration/atlases'

This is the same defect the $BLS_WEIGHTS_DIR redirect fixes for gliomoda, petu,
brainles_aurora and brainles_hd_bet. The atlases are not weights and were not
covered by it. They are baked in rather than redirected because they are ~92 MB
for the whole record and the SRI24 template is what makes this bundle able to
produce the atlas-space input that gliomoda, petu and AURORA all require -- so
the preprocessing entry point works offline, as the standalone
brainles-preprocessing container already does.

The layout matters as much as the files. ZenodoRecord globs for
"<record_id>_v*.*.*" inside the target directory and treats a bare directory of
files as "nothing downloaded yet", so the files go into
<target>/<record_id>_v<version>/ exactly as _build_folder_path would.

Files are fetched individually rather than through upstream's fetch, which
requests Zenodo's files-archive endpoint: that endpoint is unreliable for these
records, while the per-file API works.
"""

import json
import pathlib
import shutil
import zipfile
from urllib.request import urlopen

from brainles_preprocessing.utils import zenodo as z


def bake(record_id, target_dir, label):
    target_dir = pathlib.Path(target_dir)
    with urlopen(f"https://zenodo.org/api/records/{record_id}") as r:
        record = json.loads(r.read().decode())
    version = record["metadata"]["version"]
    # Same name ZenodoRecord._build_folder_path() would produce, so
    # fetch_atlases() finds this copy instead of trying to download.
    folder = target_dir / f"{record_id}_v{version}"
    folder.mkdir(parents=True, exist_ok=True)

    files = record.get("files", [])
    if not files:
        raise SystemExit(f"{label}: record {record_id} lists no files")
    for entry in files:
        name = entry["key"]
        out = folder / name
        if out.exists():
            continue
        print(f"{label}: downloading {name}", flush=True)
        with urlopen(entry["links"]["self"]) as resp, open(out, "wb") as fh:
            shutil.copyfileobj(resp, fh)
        if name.endswith(".zip"):
            with zipfile.ZipFile(out) as zf:
                zf.extractall(folder)
            out.unlink()
    print(f"{label}: ready at {folder}", flush=True)


bake(z.ATLASES_RECORD_ID, z.ATLASES_FOLDER, "atlases")
