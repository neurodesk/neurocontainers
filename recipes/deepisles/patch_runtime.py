"""Apply narrowly matched fixes to the installed upstream runtime."""

import sys
from pathlib import Path


def replace_once(path: Path, before: str, after: str) -> None:
    source = path.read_text()
    if source.count(before) != 1:
        raise RuntimeError(f"Upstream changed; review runtime patch for {path}: {before!r}")
    path.write_text(source.replace(before, after))


def main(root: Path) -> None:
    ensemble = root / "src/isles22_ensemble.py"
    replace_once(
        ensemble,
        "dwi_msk_nii = self.input_dwi_path.replace('dwi_ss.nii.gz', 'dwi_ss_mask.nii.gz')",
        "dwi_msk_nii = nib.load(self.input_dwi_path.replace('dwi_ss.nii.gz', 'dwi_ss_mask.nii.gz'))",
    )
    replace_once(
        ensemble,
        "subprocess.run(command, shell=True, cwd=cwd)",
        "subprocess.run(command, shell=True, cwd=cwd, check=True)",
    )
    replace_once(
        ensemble,
        "subprocess.call(command_voting, shell=True, cwd=path_voting)",
        "subprocess.run(command_voting, shell=True, cwd=path_voting, check=True)",
    )
    replace_once(
        root / "src/utils.py",
        "subprocess.call(command_hd_bet, shell=True)",
        "subprocess.run(command_hd_bet, shell=True, check=True)",
    )
    launcher = root / "src/SEALS/nnunet_launcher.sh"
    replace_once(launcher, "#!/bin/bash\n", "#!/bin/bash\nset -e\n")
    # Parameters are installed readable. A SIF cannot chmod them at runtime.
    replace_once(launcher, 'chmod -R u+rwX "$RESULTS_FOLDER_PATH"\n', "")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
