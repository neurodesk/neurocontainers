"""Make the `hd-bet` console script startable.

brainles_hd_bet 0.0.11's hd_bet.py imports maybe_mkdir_p from
brainles_hd_bet.utils, which defines subfiles but not maybe_mkdir_p, so every
invocation of the console script -- `--help` included -- dies on ImportError
before parsing a single argument. The blast radius is exactly that module:
brainles_hd_bet.run and brainles_hd_bet.utils import cleanly, so the library
route used by HDBetExtractor is unaffected; only the exported command is dead.

The function's single call site (hd_bet.py) creates the output directory when
the input is a directory, so the missing helper is the usual one-liner:
os.makedirs(directory, exist_ok=True). utils.py already imports os at module
level, which is why appending the definition is sufficient.

Kept as a patch rather than a fork, same reasoning as fix_preprocessor_cli.py:
it is one function, and pinning a fork would strand the recipe on this release.
"""

from importlib.util import find_spec
from pathlib import Path

package_spec = find_spec("brainles_hd_bet")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_hd_bet is not installed")

utils_path = Path(next(iter(package_spec.submodule_search_locations))) / "utils.py"
source = utils_path.read_text()

if "def maybe_mkdir_p" in source:
    raise RuntimeError(
        "brainles_hd_bet.utils now defines maybe_mkdir_p; drop this patch"
    )
if "import os" not in source:
    raise RuntimeError(
        "brainles_hd_bet.utils no longer imports os; review this patch"
    )

utils_path.write_text(
    source
    + "\n\ndef maybe_mkdir_p(directory):\n"
    + "    os.makedirs(directory, exist_ok=True)\n"
)
print("hd-bet CLI patched")
