#!/opt/chimera-env/bin/python
import json
import os
import sys
import tempfile
from pathlib import Path

os.makedirs(os.environ["TEMPLATEFLOW_HOME"], exist_ok=True)
from chimera import cli

if any(arg in {"--config", "-c"} or arg.startswith("--config=") for arg in sys.argv[1:]):
    cli.main()
else:
    config = json.loads(Path("/opt/chimera-source/chimera/config/pipe_config.json").read_text())
    config["packages"]["freesurfer"]["license"] = os.environ.get("FS_LICENSE")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as stream:
        json.dump(config, stream)
        stream.flush()
        sys.argv.extend(["--config", stream.name])
        cli.main()
