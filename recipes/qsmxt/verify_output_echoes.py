import argparse
import logging
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import ismrmrd
import nibabel as nib
import numpy as np

import qsmxt
from verify_v10_runtime import fixture


class Connection:
    def __init__(self, images):
        self.images = images
        self.returned = []
        self.errors = []
        self.closed = False

    def __iter__(self):
        yield from self.images
        yield None

    def send_image(self, images):
        self.returned.extend(images if isinstance(images, list) else [images])

    def send_logging(self, level, message):
        self.errors.append(message)

    def send_close(self):
        self.closed = True


def input_images(bids, n_echoes):
    images = []
    for series, part, kind in (
        (1, "mag", ismrmrd.IMTYPE_MAGNITUDE),
        (2, "phase", ismrmrd.IMTYPE_PHASE),
    ):
        for echo in range(1, n_echoes + 1):
            path = next(bids.rglob(f"*_echo-{echo}_part-{part}_*.nii.gz"))
            volume = nib.load(path).get_fdata().astype(np.float32)
            for z in range(volume.shape[2]):
                image = ismrmrd.Image.from_array(volume[:, :, z].T[None, :, :], transpose=False)
                header = image.getHead()
                header.image_series_index = series
                header.image_index = (echo - 1) * volume.shape[2] + z + 1
                header.image_type = kind
                header.slice = z
                header.contrast = echo - 1
                for field, values in (
                    ("field_of_view", [volume.shape[0], volume.shape[1], 1.0]),
                    ("position", [0.0, 0.0, float(z)]),
                    ("read_dir", [1.0, 0.0, 0.0]),
                    ("phase_dir", [0.0, 1.0, 0.0]),
                    ("slice_dir", [0.0, 0.0, 1.0]),
                ):
                    for index, value in enumerate(values):
                        getattr(header, field)[index] = value
                image.setHead(header)
                meta = ismrmrd.Meta()
                meta["SequenceDescription"] = "gre_qsm"
                meta["EchoNumber"] = str(echo)
                image.attribute_string = meta.serialize()
                images.append(image)
    return images


def verify(binary, root):
    bids, _, _ = fixture(root / "fixture")
    metadata = SimpleNamespace(
        acquisitionSystemInformation=SimpleNamespace(systemFieldStrength_T=3.0),
        measurementInformation=SimpleNamespace(protocolName="gre_qsm"),
        sequenceParameters=SimpleNamespace(TE=[6.0, 12.0, 18.0]),
    )
    compatible = {
        "QSMxT QSM DC", "QSMxT mask DC", "QSMxT SWI DC",
        "QSMxT SMWI paramagnetic DC", "QSMxT SMWI diamagnetic DC",
    }
    previous_root = qsmxt.OPENRECON_WORK_ROOT
    try:
        qsmxt.OPENRECON_WORK_ROOT = root / "adapter"
        for count in (1, 2, 3):
            connection = Connection(input_images(bids, count))
            started = time.monotonic()
            qsmxt.process(connection, {"parameters": {
                "qsmxtbinary": binary, "sendoutputs": "all",
            }}, metadata)
            assert connection.closed
            assert not connection.errors, "\n".join(connection.errors)
            derived = {}
            for image in connection.returned:
                name = str(ismrmrd.Meta.deserialize(image.attribute_string).get("SequenceDescription"))
                if name.startswith("QSMxT "):
                    derived.setdefault(name, []).append(image.data)
            expected = compatible | ({"QSMxT T2star DC", "QSMxT R2star DC"} if count == 3 else set())
            assert set(derived) == expected, (count, sorted(derived), sorted(expected))
            for name, slices in derived.items():
                data = np.asarray(slices)
                assert np.isfinite(data).all() and np.any(data != 0), (count, name)
            print(f"{count} echo(s): returned {len(derived)} finite nonzero map series in "
                  f"{time.monotonic() - started:.2f}s: {', '.join(sorted(derived))}", flush=True)
    finally:
        qsmxt.OPENRECON_WORK_ROOT = previous_root
    print("All returns compatible maps at one and two echoes and retains T2*/R2* at three echoes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="/opt/qsmxt/qsmxt")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with tempfile.TemporaryDirectory(prefix="qsmxt-output-echoes-") as temporary:
        verify(arguments.binary, Path(temporary))
