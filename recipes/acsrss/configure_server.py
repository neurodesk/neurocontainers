"""Configure the pinned MRD server for parameterless OpenRecon adjustment streams."""

from __future__ import annotations

import argparse
from pathlib import Path


def configure(server_dir: Path, app: str) -> None:
    if app not in ("acsrss", "slicegrappa"):
        raise ValueError("Unsupported FIRE POC application")
    main = server_dir / "main.py"
    server = server_dir / "server.py"
    main_text = main.read_text()
    server_text = server.read_text()
    placeholder = "'defaultConfig':  'default_replace_with_valid_name'"
    parameter_check = "if ('parameters' in configAdditional):"
    if main_text.count(placeholder) != 1 or server_text.count(parameter_check) != 1:
        raise ValueError(
            "Pinned MRD server changed; review adjustment dispatch before building"
        )
    main.write_text(main_text.replace(placeholder, f"'defaultConfig':  '{app}'"))
    server.write_text(
        server_text.replace(
            parameter_check,
            "if isinstance(configAdditional, dict) and "
            "isinstance(configAdditional.get('parameters'), dict):",
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server_dir", type=Path)
    parser.add_argument("app")
    args = parser.parse_args()
    configure(args.server_dir, args.app)
