"""Make the `preprocessor` console script usable.

Three defects in brainles_preprocessing 0.6.10's cli.py. The first two are hit
before any work is done; the third only shows up on a CPU node, where it is the
difference between a run that finishes and one that does not.

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

3. Brain extraction is nailed to HD-BET's heaviest profile. The CLI builds
   `AtlasCentricPreprocessor` without a brain extractor, so it falls back to
   `HDBetExtractor()`, and `Modality.extract_brain_region` calls `extract()`
   without `mode` or `do_tta` -- leaving the defaults, a five-fold ensemble
   with test-time mirroring. On a GPU-less node that is hours per volume with
   no way to ask for anything cheaper: a measured run completed
   co-registration, atlas registration, atlas correction and normalisation,
   then sat in brain extraction until a 90-minute timeout killed it. `--bet-
   mode fast --no-bet-tta` makes the same command finish. Upstream's defaults
   are kept, so a run that says nothing about brain extraction still produces
   what the standalone module produces.

Kept as patches rather than a fork: pinning a fork would strand the recipe on
this release. Every anchor is asserted to exist first, so an upstream change
fails the build here instead of silently leaving the defect in place.
"""

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
    # A brain extractor whose profile the CLI can choose. extract() is called
    # by Modality with only the paths, so the mode and TTA settings have to
    # ride on the extractor instance rather than be passed at the call site.
    (
        """from brainles_preprocessing.preprocessor import AtlasCentricPreprocessor
""",
        """from brainles_preprocessing.preprocessor import AtlasCentricPreprocessor
from brainles_preprocessing.brain_extraction.brain_extractor import HDBetExtractor


class _ProfiledHDBetExtractor(HDBetExtractor):
    \"\"\"HD-BET with a caller-chosen speed profile.

    Added by the neurocontainers recipe. Upstream's default -- a five-fold
    ensemble with test-time mirroring -- is hours per volume on a CPU-only
    node, and the CLI exposed no way to ask for the single-fold profile.
    \"\"\"

    def __init__(self, mode="accurate", do_tta=True):
        self._bet_mode = mode
        self._bet_do_tta = do_tta

    def extract(self, *args, **kwargs):
        kwargs.setdefault("mode", self._bet_mode)
        kwargs.setdefault("do_tta", self._bet_do_tta)
        return super().extract(*args, **kwargs)
""",
    ),
    # The two flags themselves, inserted ahead of --version so they appear
    # with the other real options.
    (
        """    version: Annotated[
        Optional[bool],
""",
        """    bet_mode: Annotated[
        str,
        typer.Option(
            "--bet-mode",
            help="HD-BET profile: 'accurate' (five-fold ensemble, upstream's "
            "default) or 'fast' (single fold). 'fast' is what makes this "
            "command finish on a node without a GPU.",
        ),
    ] = "accurate",
    bet_tta: Annotated[
        bool,
        typer.Option(
            "--bet-tta/--no-bet-tta",
            help="HD-BET test-time augmentation by mirroring along all axes. "
            "On by default, as upstream has it; --no-bet-tta is markedly "
            "faster on CPU.",
        ),
    ] = True,
    version: Annotated[
        Optional[bool],
""",
    ),
    # And the wiring. Without this the flags would parse and do nothing.
    (
        """        temp_folder=output_dir / "temp",
        **optional_kwargs,
    )
""",
        """        temp_folder=output_dir / "temp",
        brain_extractor=_ProfiledHDBetExtractor(mode=bet_mode, do_tta=bet_tta),
        **optional_kwargs,
    )
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
