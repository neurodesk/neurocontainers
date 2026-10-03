"""Run the FLUX first-look loading and PSD cells without a GUI."""

import argparse
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import mne
import numpy as np
from mne_bids import BIDSPath, write_raw_bids


def run(dataset: Path, notebook: Path) -> None:
    cells = json.loads(notebook.read_text())["cells"]
    namespace = {"bids_root": str(dataset)}
    markers = (
        "import openneuro",
        "bids_path = BIDSPath",
        "raw1 = read_raw_bids",
        "data1PSD = raw1.compute_psd",
    )
    for marker in markers:
        matches = [
            "".join(cell["source"])
            for cell in cells
            if cell["cell_type"] == "code" and marker in "".join(cell["source"])
        ]
        if len(matches) != 1:
            raise ValueError(f"Expected one notebook cell containing {marker!r}")
        if marker.startswith("data1PSD"):
            namespace["raw1"].crop(tmin=0, tmax=10)
        exec(compile(matches[0], str(notebook), "exec"), namespace)
    spectrum = namespace["data1PSD"]
    data, frequencies = spectrum.get_data(return_freqs=True)
    assert data.shape[0] > 0 and frequencies.size > 0
    assert np.all(np.isfinite(data)) and np.max(data) > 0
    assert frequencies[0] >= 1 and frequencies[-1] <= 60
    print("FLUX notebook loading and PSD passed", data.shape)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path)
    parser.add_argument(
        "--notebook", type=Path,
        default=Path(__file__).parent / "MNEPython" / "Afirstlook.ipynb",
    )
    args = parser.parse_args()
    if args.dataset:
        recording = args.dataset / "sub-01/ses-01/meg/sub-01_ses-01_task-SpAtt_run-01_meg.fif"
        digest = hashlib.sha256()
        with recording.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        expected = "02415628d82f14033b2bc6f5638d6240f894485f87ab66d458fc4160ae91dce9"
        assert digest.hexdigest() == expected, "Recording differs from ds004346 1.0.2"
        run(args.dataset, args.notebook)
        return
    with TemporaryDirectory() as work:
        sampling_frequency = 1000
        times = np.arange(30000) / sampling_frequency
        data = np.array([1e-12 * np.sin(2 * np.pi * frequency * times) for frequency in (10, 20)])
        info = mne.create_info(["MEG0111", "MEG0121"], sampling_frequency, ch_types="mag")
        raw = mne.io.RawArray(data, info)
        bids_path = BIDSPath(
            subject="01", session="01", task="SpAtt", run="01", suffix="meg", root=work,
        )
        write_raw_bids(raw, bids_path, format="FIF", allow_preload=True, overwrite=True, verbose=False)
        run(Path(work), args.notebook)


if __name__ == "__main__":
    main()
