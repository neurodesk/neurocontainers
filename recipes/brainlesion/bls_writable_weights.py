"""Make the bundled deep-learning components able to fetch weights at run time.

Two independent problems are fixed here, both of which stop every DL component
in this image from running.

1. Each component resolves its weights cache to a directory inside its own
   installed package, which lives on the read-only squashfs. The first thing
   inference does is create that directory, so it dies with
   "OSError: [Errno 30] Read-only file system". Worse, gliomoda and petu call
   sys.exit() rather than raising, so a calling pipeline cannot catch it.

2. gliomoda, petu and brainles_aurora all download by requesting Zenodo's
   files-archive endpoint, which now returns HTTP 400. So even with a writable
   directory the download fails. Their per-file endpoints do work.

Weights are deliberately NOT baked into the image: together they are several GB
and most users need one component. They are fetched on first use into
$BLS_WEIGHTS_DIR, which defaults to a writable path and should be pointed at
persistent storage so the download happens once.

Each replacement is appended to the module so it shadows the original, and each
target is asserted to exist first, so an upstream layout change fails the build
here rather than silently leaving the defect in place.
"""

import pathlib
import sys
from importlib.util import find_spec

MARKER = "brainlesion recipe: writable weights + working zenodo endpoint"

SHARED = '''

# --- {marker} ---
import os as _bls_os
import pathlib as _bls_pathlib


def _bls_weights_root(component):
    """Writable weights directory, created on demand."""
    base = _bls_os.environ.get("BLS_WEIGHTS_DIR")
    if not base:
        base = _bls_os.path.join(
            _bls_os.environ.get("TMPDIR", "/tmp"), "brainlesion-weights")
    root = _bls_pathlib.Path(base) / component
    root.mkdir(parents=True, exist_ok=True)
    return root


def _bls_fetch_record(record_id, dest):
    """Download every file in a Zenodo record into dest, unpacking archives.

    Upstream requests links.archive, which returns HTTP 400. The per-file
    endpoints still work, so the files are fetched individually.
    """
    import json as _json
    import shutil as _shutil
    import zipfile as _zipfile
    from urllib.request import urlopen as _urlopen

    with _urlopen(f"https://zenodo.org/api/records/{{record_id}}") as r:
        record = _json.loads(r.read().decode())
    version = record.get("metadata", {{}}).get("version", "unknown")
    target = _bls_pathlib.Path(dest) / f"weights_v{{version}}"
    if target.exists() and any(target.iterdir()):
        return target
    target.mkdir(parents=True, exist_ok=True)

    for entry in record.get("files", []):
        name = entry["key"]
        url = entry["links"]["self"]
        out = target / name
        print(f"downloading {{name}} ...", flush=True)
        with _urlopen(url) as resp, open(out, "wb") as fh:
            _shutil.copyfileobj(resp, fh)
        if name.endswith(".zip"):
            with _zipfile.ZipFile(out) as zf:
                zf.extractall(target)
            out.unlink()
        # .tar files are left as they are: AURORA loads them by filename.
    # a zip may itself contain a zip, as upstream also handles
    for leftover in list(target.glob("*.zip")):
        with _zipfile.ZipFile(leftover) as zf:
            zf.extractall(target)
        leftover.unlink()
    return target
'''


def patch(module_path: pathlib.Path, addition: str) -> None:
    src = module_path.read_text()
    if MARKER in src:
        return
    module_path.write_text(src + addition)


def package_dir(name: str) -> pathlib.Path:
    spec = find_spec(name)
    if spec is None or not spec.submodule_search_locations:
        raise SystemExit(f"{name} is not installed")
    return pathlib.Path(next(iter(spec.submodule_search_locations)))


def main() -> int:
    shared = SHARED.format(marker=MARKER)

    # --- gliomoda and petu: identical shape ---
    for name, fn in (("gliomoda", "check_weights_path"), ("petu", "check_weights_path")):
        pkg = package_dir(name)
        mod = pkg / "weights.py"
        if not mod.is_file():
            raise SystemExit(f"{name}: expected {mod}, layout changed")
        const = (pkg / "constants.py").read_text()
        if "ZENODO_RECORD_URL" not in const:
            raise SystemExit(f"{name}: ZENODO_RECORD_URL missing, layout changed")
        record = const.split("ZENODO_RECORD_URL")[1].split("records/")[1].split('"')[0]
        patch(mod, shared + f'''

def {fn}():
    """Resolve weights in a writable directory, downloading on first use."""
    root = _bls_weights_root("{name}")
    existing = sorted(root.glob("weights_v*"))
    if existing:
        return existing[-1]
    return _bls_fetch_record("{record}", root)
''')
        print(f"patched {{}}".format(mod))

    # --- brainles_aurora: different entry point and file layout ---
    pkg = package_dir("brainles_aurora")
    mod = pkg / "utils" / "weights.py"
    if not mod.is_file():
        raise SystemExit(f"brainles_aurora: expected {mod}, layout changed")
    src = mod.read_text()
    if "ZENODO_RECORD_URL" not in src:
        raise SystemExit("brainles_aurora: ZENODO_RECORD_URL missing, layout changed")
    record = src.split("ZENODO_RECORD_URL")[1].split("records/")[1].split('"')[0]
    patch(mod, shared + f'''

def check_model_weights():
    """Resolve weights in a writable directory, downloading on first use.

    AURORA loads "<mode>_<selection>.tar" straight out of this directory, so the
    tar files are stored as downloaded rather than unpacked.
    """
    root = _bls_weights_root("brainles_aurora")
    existing = sorted(root.glob("weights_v*"))
    if existing:
        return existing[-1]
    return _bls_fetch_record("{record}", root)
''')
    print(f"patched {mod}")

    # --- HD-BET: direct URLs already work, only the directory is unwritable ---
    pkg = package_dir("brainles_hd_bet")
    mod = pkg / "utils.py"
    if not mod.is_file():
        raise SystemExit("brainles_hd_bet: expected utils.py, layout changed")
    src = mod.read_text()
    needle = 'folder_with_parameter_files = Path(os.path.join(file_abspath, "model_weights"))'
    if needle not in src:
        raise SystemExit("brainles_hd_bet: weights folder line changed, review this patch")
    replacement = (
        f'# {MARKER}: the package directory is read-only at run time\n'
        'folder_with_parameter_files = Path(\n'
        '    os.environ.get("BLS_WEIGHTS_DIR")\n'
        '    or os.path.join(os.environ.get("TMPDIR", "/tmp"), "brainlesion-weights")\n'
        ') / "brainles_hd_bet" / "model_weights"'
    )
    mod.write_text(src.replace(needle, replacement, 1))
    print(f"patched {mod}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
