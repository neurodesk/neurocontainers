from importlib.util import find_spec
from pathlib import Path


package_spec = find_spec("brainles_preprocessing")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_preprocessing is not installed")

cli_path = Path(next(iter(package_spec.submodule_search_locations))) / "cli.py"
source = cli_path.read_text()
old_annotation = """output_dir: Annotated[
        str | Path,
"""
replacement = """output_dir: Annotated[
        Path,
"""

if old_annotation not in source:
    raise RuntimeError(
        "brainles_preprocessing CLI layout changed; review the Typer fix"
    )

cli_path.write_text(source.replace(old_annotation, replacement, 1))
