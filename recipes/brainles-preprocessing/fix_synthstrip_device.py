"""Make SynthStrip pick a device instead of assuming a GPU.

`SynthStripExtractor.extract()` defaults to `device="cuda"`. On a CPU-only
node -- which is what Neurodesk mostly runs -- the first call raises before any
work happens, from `model.to(device)` inside `_setup_model`:

    RuntimeError: Found no NVIDIA driver on your system. Please check that you
    have an NVIDIA GPU and installed a driver from http://www.nvidia.com/...

Not a slow path and not a warning: a traceback on the first call. It lands
squarely on this recipe, which goes out of its way to make SynthStrip usable --
installing the optional `nipreps-synthstrip` extra and baking the weights into
the image -- and then hands a CPU user a crash the first time they call it. The
CPU path itself is fine; the only thing missing was asking for it.

This is the same defect class the recipe already fixed for the other bundled
extractor. `fix_preprocessor_cli.py` added `--bet-mode/--bet-device/--bet-tta`
because HD-BET's GPU defaults do not finish on CPU. SynthStrip's failure mode
is harsher: HD-BET falls back to CPU on its own, slowly, while SynthStrip
raises.

Only the unspecified case changes, and only where the current behaviour is a
guaranteed crash:

  - an explicit `device=` from the caller is returned untouched, string or
    `torch.device`, so nothing that names a device behaves differently;
  - a host with a GPU still resolves to cuda, so a GPU run is unchanged;
  - a host without one resolves to cpu instead of raising.

The resolution is a named method rather than three inline lines so that the
GPU branch is obvious by inspection, and so the build and the fulltest can
assert it without loading the model.
"""

from importlib.util import find_spec
from pathlib import Path

package_spec = find_spec("brainles_preprocessing")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_preprocessing is not installed")

module_path = (
    Path(next(iter(package_spec.submodule_search_locations)))
    / "brain_extraction"
    / "synthstrip.py"
)
source = module_path.read_text()

replacements = [
    # The default itself. None means "decide", which is what upstream's "cuda"
    # was pretending to be.
    (
        """        device: Union[torch.device, str] = "cuda",
""",
        """        device: Optional[Union[torch.device, str]] = None,
""",
    ),
    # The docstring, so help() does not describe the old default.
    (
        """            device (Union[torch.device, str], optional): Device to use for computation. Defaults to "cuda".
""",
        """            device (Union[torch.device, str], optional): Device to use for computation. Defaults to CUDA when a GPU is present, otherwise CPU.
""",
    ),
    # The call site.
    (
        """        device = torch.device(device) if isinstance(device, str) else device
        model = self._setup_model(device=device)
""",
        """        device = self._resolve_device(device)
        model = self._setup_model(device=device)
""",
    ),
    # And the resolution, placed just before the method that consumes it.
    (
        """    def _setup_model(self, device: torch.device) -> StripModel:
""",
        '''    @staticmethod
    def _resolve_device(
        device: Optional[Union[torch.device, str]],
    ) -> torch.device:
        """Choose a device when the caller did not name one.

        Added by the neurocontainers recipe. Upstream defaulted to "cuda",
        which on a machine without an NVIDIA driver raises RuntimeError from
        model.to(device) before any work happens. A caller who names a device
        still gets exactly that device, and a host with a GPU still gets cuda.
        """
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        return torch.device(device) if isinstance(device, str) else device

    def _setup_model(self, device: torch.device) -> StripModel:
''',
    ),
]

for old, new in replacements:
    if old not in source:
        raise RuntimeError(
            f"brainles_preprocessing SynthStrip layout changed; review this patch:\n{old!r}"
        )
    source = source.replace(old, new, 1)

module_path.write_text(source)
print("synthstrip device default now follows the hardware")
