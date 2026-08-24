"""Break the circular import between brainles_aurora.utils and .inferer.

`brainles_aurora/utils/weights.py` imports WEIGHTS_DIR_PATTERN from
`brainles_aurora.inferer.constants`, while `brainles_aurora/inferer/model.py`
imports `check_model_weights` from `brainles_aurora.utils`. The two packages
therefore import each other, and every import order that reaches `utils` before
`inferer` dies:

    from brainles_aurora.utils import check_model_weights
    ImportError: cannot import name 'check_model_weights' from partially
    initialized module 'brainles_aurora.utils' (most likely due to a circular
    import)

Three of the six plausible ways to reach the weights helper fail this way, and
the error reads like a broken installation rather than an import-order rule.
This recipe hit it too: the weights module had to be located by path, because
importing it to find its location was not reliable.

WEIGHTS_DIR_PATTERN is a constant string, so defining it in weights.py removes
the cycle without changing any behaviour. Its value is read from constants.py
first, so if upstream ever changes it the build fails here instead of shipping
a stale copy.
"""

from importlib.util import find_spec
from pathlib import Path

from brainles_aurora.inferer.constants import WEIGHTS_DIR_PATTERN

package_spec = find_spec("brainles_aurora")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_aurora is not installed")

weights_path = (
    Path(next(iter(package_spec.submodule_search_locations))) / "utils" / "weights.py"
)
source = weights_path.read_text()

old_import = "from brainles_aurora.inferer.constants import WEIGHTS_DIR_PATTERN"
replacement = (
    "# Inlined by the neurocontainers recipe: importing this constant from\n"
    "# brainles_aurora.inferer makes utils and inferer import each other, so\n"
    "# any import reaching utils first fails with a circular ImportError.\n"
    f"WEIGHTS_DIR_PATTERN = {WEIGHTS_DIR_PATTERN!r}"
)

if old_import not in source:
    raise RuntimeError(
        "brainles_aurora import layout changed; review the circular-import fix"
    )

weights_path.write_text(source.replace(old_import, replacement, 1))
print(f"circular import removed; WEIGHTS_DIR_PATTERN inlined as {WEIGHTS_DIR_PATTERN!r}")
