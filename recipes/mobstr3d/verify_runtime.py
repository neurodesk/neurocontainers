"""Runtime check: one slice of mobstr3D's example exam through the DICOM and the MRD server paths.

The DICOM path (mobstr3d_slice) must give a plausible segmentation and displacement field,
and the OpenRecon path (dicom2mrd -> client.py -> MRD server running mobstr3d) must return
the same labels, contours and displacements, encoded as documented.
"""
import base64
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import h5py
import ismrmrd
import numpy as np
import pydicom

import mobstr3d_slice as pipeline

EXAMPLE = Path(os.environ.get("MOBSTR3D_EXAMPLE", "/opt/mobstr3D/data/example/example_human_data.zip"))
SERVER = Path(os.environ.get("MOBSTR3D_SERVER", "/opt/code/python-ismrmrd-server"))
SLICE = 1  # second slice from the base


def extract_slice(work):
    with zipfile.ZipFile(EXAMPLE) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".dcm")]
        archive.extractall(work / "all", members)
    files = sorted((work / "all").rglob("*.dcm"))
    locations = {}
    for path in files:
        ds = pydicom.dcmread(path, stop_before_pixels=True)
        iop = [float(v) for v in ds.ImageOrientationPatient]
        locations[path] = round(pipeline.slice_location(ds.ImagePositionPatient, iop[:3], iop[3:]), 2)
    chosen = sorted(set(locations.values()), reverse=True)[SLICE]
    target = work / "slice"
    target.mkdir()
    for path, location in locations.items():
        if location == chosen:
            path.rename(target / path.name)
    return target, len(list(target.iterdir()))


def run_dicom(folder):
    slices = pipeline.load_dicom_slices(folder)
    assert len(slices) == 1, f"expected one slice, found {len(slices)}"
    dense = slices[0]
    result = pipeline.process_slice(dense, pipeline.Settings(), pipeline.Segmenter())
    assert result.frames == list(range(5, 29)), result.frames
    seed = result.seed_points["Frame_5"]["x"][0]
    assert result.labels[0][tuple(seed)] == 1, f"seed {seed} is not in the myocardium"
    es = result.displacements["Frame_21"]
    magnitude = np.linalg.norm(es, axis=1)
    assert len(es) > 100, f"only {len(es)} myocardial pixels unwrapped at end systole"
    assert 2 < np.median(magnitude) < 15, f"implausible end-systolic displacement {np.median(magnitude):.2f} mm"
    assert result.contours["endo"]["Frame_21"] is not None and result.contours["epi"]["Frame_21"] is not None
    print(f"DICOM path: {len(result.frames)} frames, seed {seed}, end-systolic median |d| "
          f"{np.median(magnitude):.2f} mm over {len(es)} pixels")
    return result


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run_mrd(folder, work):
    source = work / "input.h5"
    output = work / "output.h5"
    subprocess.run([sys.executable, str(SERVER / "dicom2mrd.py"), str(folder), "-o", str(source)],
                   check=True, cwd=SERVER, stdout=subprocess.DEVNULL)
    port = free_port()
    log = open(work / "server.log", "w")
    server = subprocess.Popen([sys.executable, str(SERVER / "main.py"), "-p", str(port), "-d", "mobstr3d"],
                              cwd=SERVER, stdout=log, stderr=subprocess.STDOUT)
    try:
        for _ in range(120):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                break
            except OSError:
                time.sleep(0.5)
        else:
            raise RuntimeError("MRD server did not start")
        subprocess.run([sys.executable, str(SERVER / "client.py"), "-a", "127.0.0.1", "-p", str(port), "-c", "mobstr3d",
                        "-o", str(output), str(source)], check=True, cwd=work, stdout=subprocess.DEVNULL)
    finally:
        server.terminate()
        server.wait(30)
        log.close()
    server_log = (work / "server.log").read_text()
    assert "failed" not in server_log, server_log[-4000:]
    series = {}
    with h5py.File(output, "r") as handle:
        groups = [name for name in handle]
    for group in groups:
        dataset = ismrmrd.Dataset(str(output), group, False)
        for name in dataset.list():
            if not name.startswith("image"):
                continue
            for index in range(dataset.number_of_images(name)):
                image = dataset.read_image(name, index)
                meta = ismrmrd.Meta.deserialize(image.attribute_string)
                series.setdefault(meta["SeriesDescription"], []).append((image, meta))
        dataset.close()
    return series


def by_suffix(series, suffix):
    matches = [images for name, images in series.items() if name.endswith(suffix)]
    assert len(matches) == 1, f"expected one series ending {suffix!r}, found {sorted(series)}"
    return sorted(matches[0], key=lambda item: int(item[1]["Mobstr3DFrame"]))


def main():
    with tempfile.TemporaryDirectory(prefix="mobstr3d-") as directory:
        work = Path(directory)
        folder, count = extract_slice(work)
        assert count == 4 * 29, f"expected 116 DICOMs for one slice, found {count}"
        result = run_dicom(folder)
        series = run_mrd(folder, work)
        originals = [name for name in series if name.endswith("original")]
        assert len(originals) == 4 and all(len(series[name]) == 29 for name in originals), \
            {name: len(images) for name, images in series.items()}
        labels = by_suffix(series, "mobstr3d-labels")
        assert [int(meta["Mobstr3DFrame"]) for _, meta in labels] == result.frames
        for index, (image, meta) in enumerate(labels):
            assert np.array_equal(np.asarray(image.data)[0, 0], result.labels[index]), f"labels differ at frame {index}"
            contours = json.loads(base64.b64decode(meta["Mobstr3DContours"]))
            frame = f"Frame_{result.frames[index]}"
            for side in ("endo", "epi"):
                assert np.allclose(contours[side], result.contours[side][frame]), f"{side} contour differs at {frame}"
        for component, axis in enumerate("xyz"):
            images = by_suffix(series, f"mobstr3d-d{axis}-mm")
            assert len(images) == len(result.frames)
            for index, (image, meta) in enumerate(images):
                stored = np.asarray(image.data)[0, 0].astype(float)
                expected = result.displacement[index, :, :, component]
                assert np.array_equal(stored > 0, ~np.isnan(expected)), f"d{axis} support differs at frame {index}"
                decoded = stored * float(meta["RescaleSlope"]) + float(meta["RescaleIntercept"])
                error = np.abs(decoded - expected)[stored > 0]
                assert error.max() <= 0.005 + 1e-9, f"d{axis} differs by {error.max()} mm at frame {index}"
            summary = json.loads(base64.b64decode(images[0][1]["Mobstr3DSlice"]))
            assert summary["seed_points"] == [list(p) for p in result.seed_points["Frame_5"][axis]]
    print("mobstr3D DICOM and MRD single-slice round trip passed")


if __name__ == "__main__":
    os.environ.setdefault("MPLBACKEND", "Agg")
    main()
