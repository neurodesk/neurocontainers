from importlib.util import find_spec
from pathlib import Path


package_spec = find_spec("brainles_preprocessing")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_preprocessing is not installed")

cli_path = Path(next(iter(package_spec.submodule_search_locations))) / "cli.py"
source = cli_path.read_text()
replacements = [
    # Typer cannot build a parameter from a union type, so cli.py raises at
    # import and every invocation fails, including --help.
    (
        """output_dir: Annotated[
        str | Path,
""",
        """output_dir: Annotated[
        Path,
""",
    ),
    # input_atlas defaults to a label rather than a path, and the body only
    # guards with `if input_atlas is not None`, so the default was passed
    # through as atlas_image_path and registration failed on a missing file.
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
