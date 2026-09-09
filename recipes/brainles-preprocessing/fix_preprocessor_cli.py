"""Make the `preprocessor` console script usable.

Three defects in brainles_preprocessing 0.6.10's cli.py. The first two are hit
before any work is done; the third is hit after two and a half minutes of it:

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

3. The CLI passes no `brain_extractor`, so the preprocessor substitutes
   `HDBetExtractor()` and its `extract` defaults: `mode="accurate"`,
   `device=0`, `do_tta=True`. That is a five-fold ensemble with test-time
   mirroring -- 40 forward passes. On a CPU-only node a measured run reached
   brain extraction in 157 s and then produced no mask in the remaining
   1643 s, while `hd-bet -mode fast -device cpu -tta 0` completed the same
   extraction in 934 s. The one-shot command this container advertises could
   not express what the image's other command does in 15 minutes, and had no
   flag to ask for it.

   Upstream's `extract` already accepts all three, so nothing new is
   implemented here: a subclass supplies the caller's choices as defaults and
   the CLI grows three options to set them. Upstream's own defaults are
   unchanged, so a GPU run behaves exactly as before.

Kept as a patch rather than a fork: it is two annotations and one extractor,
and pinning a fork would strand the recipe on this release.
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
    # Defect 3, part one: an extractor that carries the caller's profile.
    (
        """from brainles_preprocessing.preprocessor import AtlasCentricPreprocessor


def version_callback(value: bool):
""",
        """from brainles_preprocessing.preprocessor import AtlasCentricPreprocessor
from brainles_preprocessing.brain_extraction.brain_extractor import HDBetExtractor


class _ProfiledHDBetExtractor(HDBetExtractor):
    \"\"\"HD-BET with a caller-chosen speed profile.

    Added by the neurocontainers recipe. Upstream's extract() defaults --
    mode="accurate", device=0, do_tta=True -- are 40 forward passes, and the
    CLI passed no extractor at all, so the preprocessor substituted exactly
    those. They are kept as the defaults here so a GPU run is unchanged; the
    point is that a CPU user can now ask for something else.

    Both values are validated here rather than left to fail later. Upstream
    checks the mode inside extract(), which on this pipeline is 157 s after the
    command was accepted, and never checks the device at all: a non-numeric,
    non-"cpu" string reaches net.cuda() and dies there.
    \"\"\"

    def __init__(self, mode="accurate", device=0, do_tta=True):
        if mode not in ("fast", "accurate"):
            raise typer.BadParameter(
                "--bet-mode must be 'fast' or 'accurate', not %r" % mode
            )
        device = str(device)
        if device != "cpu" and not device.isdigit():
            raise typer.BadParameter(
                "--bet-device must be 'cpu' or a CUDA device index such as "
                "'0', not %r" % device
            )
        self._bet_mode = mode
        # HD-BET wants an int device id or the string "cpu". Typer hands over a
        # string either way, and net.cuda("0") raises on a node with a GPU.
        self._bet_device = device if device == "cpu" else int(device)
        self._bet_do_tta = do_tta

    def extract(self, *args, **kwargs):
        kwargs.setdefault("mode", self._bet_mode)
        kwargs.setdefault("device", self._bet_device)
        kwargs.setdefault("do_tta", self._bet_do_tta)
        return super().extract(*args, **kwargs)


def version_callback(value: bool):
""",
    ),
    # Defect 3, part two: the flags, ahead of --version so they appear with
    # the other real options rather than after the eager one.
    (
        """    version: Annotated[
        Optional[bool],
""",
        """    bet_mode: Annotated[
        str,
        typer.Option(
            "--bet-mode",
            help="HD-BET profile: 'accurate' (five-fold ensemble, upstream's "
            "default) or 'fast' (single fold). 'fast' is most of what makes "
            "this command finish on a node without a GPU.",
        ),
    ] = "accurate",
    bet_device: Annotated[
        str,
        typer.Option(
            "--bet-device",
            help="Device for HD-BET: 'cpu', or a CUDA device index such as "
            "'0' (upstream's default). HD-BET falls back to CPU on its own "
            "when no GPU is present, so this is for choosing between GPUs or "
            "forcing CPU on a machine that has one.",
        ),
    ] = "0",
    bet_tta: Annotated[
        bool,
        typer.Option(
            "--bet-tta/--no-bet-tta",
            help="HD-BET test-time augmentation by mirroring along all axes. "
            "On by default, as upstream has it; --no-bet-tta drops the eight "
            "passes per fold and is the rest of what makes a CPU run finish.",
        ),
    ] = True,
    version: Annotated[
        Optional[bool],
""",
    ),
    # Defect 3, part three: the wiring. Without this the flags parse and do
    # nothing, which is the failure mode the fulltest checks for.
    (
        """        temp_folder=output_dir / "temp",
        **optional_kwargs,
    )
""",
        """        temp_folder=output_dir / "temp",
        brain_extractor=_ProfiledHDBetExtractor(
            mode=bet_mode, device=bet_device, do_tta=bet_tta
        ),
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
