import json
import sys
from pathlib import Path

path = Path("/opt/chimera-source/chimera/config/pipe_config.json")
config = json.loads(path.read_text())
for package, version in zip(("ants", "fsl", "freesurfer"), sys.argv[1:], strict=True):
    config["packages"][package]["version"] = version
path.write_text(json.dumps(config))
