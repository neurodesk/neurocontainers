#!/usr/bin/env python3
import sys
from pathlib import Path


HELPER = '''import os
import re
from pathlib import Path


def _safe_identity():
    identity = os.environ.get("USER") or str(os.getuid())
    return re.sub(r"[^A-Za-z0-9_.-]", "_", identity)


def _candidate_dirs():
    override = os.environ.get("FUNCTIONNECTOME_CONFIG_DIR")
    if override:
        yield Path(override)

    xdg_config_home = os.environ.get("XDG_CONFIG_HOME")
    if xdg_config_home:
        yield Path(xdg_config_home) / "Functionnectome"

    yield Path("/neurodesktop-storage") / ".functionnectome"

    tmp_root = Path(os.environ.get("TMPDIR") or "/tmp")
    yield tmp_root / f"functionnectome-{_safe_identity()}"
    yield Path("/tmp") / f"functionnectome-{_safe_identity()}"


def _is_writable(directory):
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".neurodesk-write-test"
        probe.write_text("", encoding="utf-8")
        probe.unlink()
    except OSError:
        return False
    return True


def priors_paths_json():
    for directory in _candidate_dirs():
        if _is_writable(directory):
            return str(directory / "priors_paths.json")
    raise RuntimeError("No writable Functionnectome configuration directory found.")
'''


def replace_once(path, old, new):
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise RuntimeError(f"Expected text not found in {path}: {old!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: neurodesk_runtime_patch.py <functionnectome-venv>")

    venv = Path(sys.argv[1])
    matches = sorted((venv / "lib").glob("python*/site-packages/Functionnectome"))
    if len(matches) != 1:
        raise RuntimeError(f"Expected one Functionnectome package, found {matches!r}")

    package_dir = matches[0]
    (package_dir / "neurodesk_runtime.py").write_text(HELPER, encoding="utf-8")

    replace_once(
        package_dir / "functionnectome_GUI.py",
        "import Functionnectome.functionnectome as fun\n"
        "from Functionnectome.functionnectome import PRIORS_H5  # , PRIORS_URL, PRIORS_ZIP\n",
        "import Functionnectome.functionnectome as fun\n"
        "from Functionnectome import neurodesk_runtime\n"
        "from Functionnectome.functionnectome import PRIORS_H5  # , PRIORS_URL, PRIORS_ZIP\n",
    )
    replace_once(
        package_dir / "functionnectome_GUI.py",
        "        pkgPath = os.path.dirname(__file__)\n"
        "        jsonPath = os.path.join(pkgPath, \"priors_paths.json\")\n",
        "        jsonPath = neurodesk_runtime.priors_paths_json()\n",
    )

    replace_once(
        package_dir / "functionnectome.py",
        "import os\n",
        "import os\n"
        "from Functionnectome import neurodesk_runtime\n",
    )
    replace_once(
        package_dir / "functionnectome.py",
        "        pkgPath = os.path.dirname(__file__)\n"
        "        jsonPath = os.path.join(pkgPath, \"priors_paths.json\")\n",
        "        jsonPath = neurodesk_runtime.priors_paths_json()\n",
    )

    replace_once(
        package_dir / "quickDisco.py",
        "import Functionnectome.functionnectome as fun\n"
        "from Functionnectome.functionnectome import PRIORS_H5\n",
        "import Functionnectome.functionnectome as fun\n"
        "from Functionnectome import neurodesk_runtime\n"
        "from Functionnectome.functionnectome import PRIORS_H5\n",
    )
    replace_once(
        package_dir / "quickDisco.py",
        "    pkgPath = os.path.dirname(__file__)\n"
        "    jsonPath = os.path.join(pkgPath, \"priors_paths.json\")\n",
        "    jsonPath = neurodesk_runtime.priors_paths_json()\n",
    )


if __name__ == "__main__":
    main()
