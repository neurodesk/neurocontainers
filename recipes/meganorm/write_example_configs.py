"""Bake example configuration files into the image.

`meganorm` takes four positional arguments and the fourth, `configs`, is a path
to a JSON file that is parsed into a 106-field pydantic model with
`extra: forbid`. It is mandatory, upstream ships no example, and the README
mentions it once in the synopsis and never again -- not its format, not that it
is JSON, not where to get one. A user following the documentation could not
produce a working run without reading `meganorm/utils/IO.py` and reconstructing
a config from the model's defaults. Upstream carries a TODO to make the
argument optional (`mainParallel.py:88`).

Two files are written, both straight from the model so they cannot drift from
it:

  default.json  every field at its default, i.e. the MEG-oriented pipeline.
  eeg.json      the same, with the MEG-only stages turned off: oversampled
                temporal projection, head-movement correction and
                environmental-noise correction have nothing to act on in an
                EEG recording.

The path is deliberately outside $HOME, which does not exist at container
runtime.
"""

import sys
from pathlib import Path

from meganorm.utils.IO import Config

out_dir = Path(sys.argv[1])
out_dir.mkdir(parents=True, exist_ok=True)

Config().save(str(out_dir / "default.json"), overwrite=True)

Config(
    which_sensor="eeg",
    apply_oversampled_temporal_projection=False,
    apply_Head_movement_correction=False,
    apply_environmental_noise_correction=False,
).save(str(out_dir / "eeg.json"), overwrite=True)

# Round-trip both, so a file that cannot be loaded back fails the build rather
# than a user's first run.
for path in sorted(out_dir.glob("*.json")):
    Config.load(str(path))
    print(f"wrote {path}")
