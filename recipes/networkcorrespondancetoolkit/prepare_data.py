"""Expand pinned NCT data during the build and remove import-time updates."""

import gzip
from importlib.util import find_spec
from pathlib import Path
import shutil
import sys

package = Path(find_spec("cbig_network_correspondence").origin).parent
source = package / "__init__.py"
text = source.read_text()
marker = "if not os.path.exists(data_dir):\n"
head, found, bootstrap = text.partition(marker)
if not found or "Repo.clone_from(" not in bootstrap or "repo_dir.remotes.origin.pull('master')" not in bootstrap:
    raise RuntimeError("NCT data bootstrap changed; review offline installation")

for space, count in (("fs_LR_32k", 3), ("fsaverage6", 5)):
    model = package / "data" / "spin_rotations" / space / "1000_spin_permutations_state0.pkl"
    compressed = model.with_suffix(".pkl.gz")
    with compressed.open("wb") as output:
        for index in range(1, count + 1):
            with Path(f"{compressed}.part{index}").open("rb") as part:
                shutil.copyfileobj(part, output)
    with gzip.open(compressed, "rb") as archive, model.open("wb") as output:
        shutil.copyfileobj(archive, output)
    compressed.unlink()
    for index in range(1, count + 1):
        Path(f"{compressed}.part{index}").unlink()

source.write_text(head)
(package / "data" / "source-revision").write_text(sys.argv[1] + "\n")
