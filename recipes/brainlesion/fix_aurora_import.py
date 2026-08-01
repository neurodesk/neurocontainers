from importlib.util import find_spec
from pathlib import Path


package_spec = find_spec("brainles_aurora")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_aurora is not installed")

weights_path = (
    Path(next(iter(package_spec.submodule_search_locations))) / "utils" / "weights.py"
)
source = weights_path.read_text()
old_import = (
    "from brainles_aurora.inferer.constants import WEIGHTS_DIR_PATTERN"
)
replacement = 'WEIGHTS_DIR_PATTERN = "weights_v*.*.*"'

if old_import not in source:
    raise RuntimeError(
        "brainles_aurora import layout changed; review the circular-import fix"
    )

weights_path.write_text(source.replace(old_import, replacement, 1))
