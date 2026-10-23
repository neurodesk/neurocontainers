"""Populate NCT's offline cache from the pinned data archive."""

import os
from pathlib import Path
import shutil
import sys

import pooch
from cbig_network_correspondence import datasets

data = Path(os.environ["CBIG_NETWORK_CORRESPONDENCE_DATA_DIR"])
spin_parts = Path(datasets.SPIN.abspath) / "spin_rotations"
spin_parts.parent.mkdir(parents=True, exist_ok=True)
shutil.move(data / "core" / "spin_rotations", spin_parts)

for fetcher in (datasets.CORE, datasets.SPIN):
    for name, expected in fetcher.registry.items():
        path = Path(fetcher.abspath) / name
        if not path.is_file() or pooch.file_hash(path) != expected:
            raise RuntimeError(
                f"Pinned NCT data does not match the package registry: {name}"
            )

datasets.prefetch_all(include_spin=True)
shutil.rmtree(spin_parts)
(data / "source-revision").write_text(sys.argv[1] + "\n")
