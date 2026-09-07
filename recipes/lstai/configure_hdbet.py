"""Keep HD-BET's pretrained weights available without a runtime home directory."""

from pathlib import Path

from HD_BET import paths


path = Path(paths.__file__)
source = path.read_text()
original = "os.path.join(os.path.expanduser('~'), 'hd-bet_params')"
if source.count(original) != 1:
    raise RuntimeError("HD-BET changed its model location; update the installation mapping")
path.write_text(source.replace(original, "'/opt/hd-bet_params'"))
