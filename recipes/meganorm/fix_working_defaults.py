"""Make MEGaNorm's shipped configuration able to complete a run.

The container test drove seven configurations through the pipeline and every
one of them failed, producing no features at all. None of the failures needed
unusual settings: each is a default in `meganorm.utils.IO.Config` that cannot
work as shipped. This patches the defaults in the installed package so the
documented invocation reaches the end, and leaves every one of them
overridable in the user's own config.

1. `apply_gedai=True` aborts every run after ~33 s.

       TypeError: Gedai.__init__() got an unexpected keyword argument
       'epoch_size_in_cycles'

   `preprocess.py` calls `Gedai(...)` with `epoch_size_in_cycles`,
   `signal_type`, `highpass_cutoff` and
   `preliminary_broadband_noise_multiplier`. No published gedai accepts them:
   0.1.0 takes three wavelet arguments, 0.2.0 and 0.3.0 take none at all, and
   MEGaNorm's requirement is an unpinned `gedai>=0.1.0`. There is no version to
   pin that makes this call work, so GEDAI is off by default. Turning it back
   on is one line in a config, for whenever upstream's two halves agree again.

2. `eeg_flat_threshold` equals `eeg_var_threshold` (both 40e-6), and they are
   passed straight to `mne.Epochs` as `reject` (drop above) and `flat` (drop
   below). Every epoch is on one side or the other of 40 µV, so every epoch is
   dropped:

       Exception: All epochs were rejected. ... Retained: 0 (0.0%)

   This is arithmetic, not a property of any particular recording. The MEG
   pairs are separated by a factor of 500; the EEG flat value looks like a copy
   of the reject value and is set three orders of magnitude below it here.

3. `ica_method="fastica"` cannot run, because `preprocess.py` hardcodes
   `fit_params=dict(extended=True)` -- accepted by infomax, rejected by
   scikit-learn's FastICA:

       TypeError: FastICA.__init__() got an unexpected keyword argument
       'extended'

   The code's own comment ("bc icalabel is trained with this") says infomax was
   what it meant, and the infomax path completes and produces a full feature
   table. So infomax is the default.

4. `parametrization_method="irasa"` produces nothing usable. Every channel's
   fit came back with a negative R-squared (-2.9 to -40.4, i.e. worse than a
   horizontal line) and was discarded by the `min_r_squared=0.9` gate, leaving
   a (1, 0) feature table. FOOOF on the identical preprocessed data produced
   the full 42 features, so the default is FOOOF until the IRASA branch is
   fixed upstream.

5. A run that discards every channel reports success. With all channels gone
   the pipeline still writes an 11-byte CSV, logs "feature extraction ...
   complete" and exits 0 -- so a whole cohort can produce empty files and clean
   exit statuses from every job, and nothing short of opening them reveals it.
   The all-epochs-rejected path already raises; this makes the all-channels
   path do the same.

6. Four of the ten default band ratios use Delta, which the default
   `freq_bands` does not define, and `feature_extract` silently skips a ratio
   whose bands are missing. Users asking for the shipped defaults got six ratio
   features instead of ten, Delta/Theta -- a standard dementia EEG marker --
   among the missing. Delta is defined as 1-3 Hz, contiguous with the Theta
   band that follows it.

7. `rereference_method` cannot be set to "no re-referencing". The docstring
   offers `None`, the annotation is `Literal["average", "REST", "None"]` -- the
   *string* -- so `null` fails pydantic validation, and `"None"` passes the
   truthiness guard and reaches `mne.set_eeg_reference("None")`, which fails
   with `TypeError: can only concatenate str (not "list") to str`. The guard
   now treats the string as the absent value it was meant to be, which makes
   the documented option reachable without changing the schema.

Each anchor is asserted to exist first, so an upstream fix or rename fails the
build here rather than silently leaving the defect in place.
"""

from importlib.util import find_spec
from pathlib import Path

package_spec = find_spec("meganorm")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("meganorm is not installed")

package_dir = Path(next(iter(package_spec.submodule_search_locations)))

EDITS = {
    "utils/IO.py": [
        # --- the four defaults that cannot complete a run -------------------
        (
            '    ica_method: Literal["fastica", "infomax", "picard"] = "fastica"\n',
            # fit_params=dict(extended=True) is hardcoded in preprocess.py and
            # only infomax accepts it.
            '    ica_method: Literal["fastica", "infomax", "picard"] = "infomax"\n',
        ),
        (
            "    apply_gedai: bool = True\n",
            # No published gedai accepts the keywords preprocess.py passes.
            "    apply_gedai: bool = False\n",
        ),
        (
            "    eeg_flat_threshold: float = 40e-6\n",
            # Was equal to eeg_var_threshold, so every epoch matched one of the
            # two rejection conditions and none survived.
            "    eeg_flat_threshold: float = 1e-7\n",
        ),
        (
            '    parametrization_method: Literal["fooof", "irasa"] = "irasa"\n',
            # IRASA returned negative R-squared on every channel, so the
            # min_r_squared gate discarded all of them.
            '    parametrization_method: Literal["fooof", "irasa"] = "fooof"\n',
        ),
        # --- the band four of the default ratios refer to -------------------
        (
            '    freq_bands: Dict[str, Tuple[int, int]] = {\n        "Theta": (3, 8),\n',
            '    freq_bands: Dict[str, Tuple[int, int]] = {\n'
            '        # Delta is referenced by four of the default band ratios,\n'
            '        # including Delta/Theta; without it they were skipped in\n'
            '        # silence and the shipped defaults yielded six ratios of ten.\n'
            '        "Delta": (1, 3),\n        "Theta": (3, 8),\n',
        ),
        # --- and the docstrings that describe them --------------------------
        (
            '    ica_method : {"fastica", "infomax", "picard"}, default="fastica"\n',
            '    ica_method : {"fastica", "infomax", "picard"}, default="infomax"\n',
        ),
        (
            "    apply_gedai : bool, default=True\n",
            "    apply_gedai : bool, default=False\n",
        ),
        (
            '    parametrization_method : {"fooof", "irasa"}, default="irasa"\n',
            '    parametrization_method : {"fooof", "irasa"}, default="fooof"\n',
        ),
        (
            "        Canonical frequency band definitions (Theta, Alpha, Beta, Gamma).\n",
            "        Canonical frequency band definitions (Delta, Theta, Alpha, Beta,\n"
            "        Gamma).\n",
        ),
    ],
    "src/preprocess.py": [
        (
            '    if which_sensor["eeg"] and rereference_method:\n',
            # rereference_method is annotated as the *string* "None", which is
            # truthy, so the documented "no re-referencing" option was passed
            # to mne.set_eeg_reference() and failed there.
            '    if which_sensor["eeg"] and rereference_method not in (None, "None"):\n',
        ),
    ],
    "src/featureExtraction.py": [
        (
            '    logger.info(f"The shape of the extracted features: {final_df.shape}")\n',
            '    if final_df.shape[1] == 0:\n'
            '        # Every channel was discarded, usually by the min_r_squared\n'
            '        # gate. Upstream wrote an empty CSV, logged completion and\n'
            '        # exited 0, so a whole cohort could produce empty files and\n'
            '        # clean exit statuses. The all-epochs-rejected path raises;\n'
            '        # this does the same rather than reporting a success.\n'
            '        raise Exception(\n'
            '            "No features were extracted: every channel was discarded, "\n'
            '            "most likely by the min_r_squared threshold during PSD "\n'
            '            "parametrization. Lower min_r_squared or change "\n'
            '            "parametrization_method."\n'
            '        )\n\n'
            '    logger.info(f"The shape of the extracted features: {final_df.shape}")\n',
        ),
    ],
}

for relative_path, replacements in EDITS.items():
    path = package_dir / relative_path
    source = path.read_text()
    for old, new in replacements:
        if old not in source:
            raise RuntimeError(
                f"meganorm {relative_path} changed; review this patch:\n{old!r}"
            )
        source = source.replace(old, new, 1)
    path.write_text(source)
    print(f"patched {relative_path}")
