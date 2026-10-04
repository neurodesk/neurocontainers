from importlib.metadata import distribution
from pathlib import Path

bindir = Path("/opt/pyem-bin")
bindir.mkdir()
names = {entry.name for entry in distribution("pyem").entry_points
         if entry.group == "console_scripts"} | {"python", "python3"}
for name in names:
    assert (Path("/opt/pyem-venv/bin") / name).is_file(), name
    (bindir / name).symlink_to("/opt/pyem-launch")
