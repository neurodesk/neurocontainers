"""Make the `preprocessor` console script usable.

Two defects in brainles_preprocessing 0.6.10's cli.py, both hit before any
work is done:

1. `output_dir` is annotated `str | Path`. Typer cannot build a parameter from
   a union type and raises at import, so every invocation fails -- including
   `preprocessor --help`. Narrowing it to `Path` is what upstream does
   elsewhere in the same signature, and the body already uses it as a Path
   (`output_dir / "temp"`).

2. `input_atlas` defaults to the string "SRI24 BraTS atlas", which is a label,
   not a path. The body guards with `if input_atlas is not None`, so that
   default is passed straight through as `atlas_image_path` and registration
   fails on a missing file. `None` lets the guard fall through to the
   preprocessor's own bundled atlas, which is what the label describes.

Kept as a patch rather than a fork: it is two annotations, and pinning a fork
would strand the recipe on this release.
"""

from importlib.util import find_spec
from pathlib import Path

package_spec = find_spec("brainles_preprocessing")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_preprocessing is not installed")

cli_path = Path(next(iter(package_spec.submodule_search_locations))) / "cli.py"
source = cli_path.read_text()

replacements = [
    (
        """output_dir: Annotated[
        str | Path,
""",
        """output_dir: Annotated[
        Path,
""",
    ),
    (
        """    ] = "SRI24 BraTS atlas",
""",
        """    ] = None,
""",
    ),
]

for old, new in replacements:
    if old not in source:
        raise RuntimeError(
            f"brainles_preprocessing CLI layout changed; review this patch:\n{old!r}"
        )
    source = source.replace(old, new, 1)

cli_path.write_text(source)
print("preprocessor CLI patched")
