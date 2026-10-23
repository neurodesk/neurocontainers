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
        "QSMxT QSM DC", "QSMxT mask DC", "QSMxT magnitude DC", "QSMxT SWI DC",
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

        connection = Connection(input_images(bids, 3))
        qsmxt.process(connection, {"parameters": {
            "qsmxtbinary": binary, "sendoutputs": "magnitude",
            "inhomogeneitycorrection": "off",
        }}, metadata)
        assert connection.closed
        assert not connection.errors, "\n".join(connection.errors)
        magnitude_images = []
        for image in connection.returned:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            name = str(meta.get("SequenceDescription"))
            if name.startswith("QSMxT "):
                assert name == "QSMxT magnitude DC", name
                magnitude_images.append(image)
        echoes = np.stack([
            nib.load(next(bids.rglob(f"*_echo-{echo}_part-mag_*.nii.gz"))).get_fdata()
            for echo in (1, 2, 3)
        ])
        expected_magnitude = np.sqrt(np.sum(echoes ** 2, axis=0))
        assert len(magnitude_images) == expected_magnitude.shape[2]
        for z, image in enumerate(magnitude_images):
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            slope = float(meta["RescaleSlope"])
            decoded = image.data[0, 0].astype(np.float64) * slope
            decoded += float(meta["RescaleIntercept"])
            np.testing.assert_allclose(
                decoded, expected_magnitude[:, :, z].T,
                rtol=1e-5, atol=slope / 2 + 1e-3,
                err_msg=f"Combined magnitude differs from three-echo RSS at slice {z}",
            )
        print("Magnitude-only returns the three-echo RSS volume within MRD quantization", flush=True)
    finally:
        qsmxt.OPENRECON_WORK_ROOT = previous_root
    print("All returns compatible maps at one and two echoes and retains T2*/R2* at three echoes")


def verify_nd_fallback(binary, root):
    bids, _, _ = fixture(root / "fixture")
    metadata = SimpleNamespace(
        acquisitionSystemInformation=SimpleNamespace(systemFieldStrength_T=3.0),
        measurementInformation=SimpleNamespace(protocolName="gre_qsm"),
        sequenceParameters=SimpleNamespace(TE=[6.0]),
    )

    def sources(name, offset):
        images = input_images(bids, 1)
        for image in images:
            header = image.getHead()
            header.image_series_index += offset
            image.setHead(header)
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["SequenceDescription"] = name
            image.attribute_string = meta.serialize()
        return images

    nd = sources("gre_qsm_ND", 0)
    dc = sources("gre_qsm_DC", 2)
    unmarked = sources("gre_qsm", 4)
    incomplete = [image for image in dc if image.image_type == ismrmrd.IMTYPE_MAGNITUDE]
    cases = (
        ("ND-only default", nd, {}, "ND", None),
        ("ND-only explicit", nd, {"inputseries": "non-distortion-corrected"}, "ND", None),
        ("incomplete preferred", incomplete + nd, {}, "ND", None),
        ("complete preferred", nd + dc, {}, "DC", None),
        ("unmarked preferred", nd + unmarked, {}, "DC", None),
        ("ambiguous preferred", nd + dc + sources("other", 6), {}, None, "Ambiguous"),
        ("no usable pair", incomplete, {}, None, "No magnitude image series"),
    )
    previous_root = qsmxt.OPENRECON_WORK_ROOT
    try:
        qsmxt.OPENRECON_WORK_ROOT = root / "adapter"
        for name, images, parameters, label, error in cases:
            originals = [image.data.copy() for image in images]
            connection = Connection(images)
            qsmxt.process(connection, {"parameters": {
                "qsmxtbinary": binary, **parameters,
            }}, metadata)
            assert connection.closed, name
            derived = []
            returned_originals = []
            for image in connection.returned:
                description = str(ismrmrd.Meta.deserialize(image.attribute_string).get("SequenceDescription"))
                if description.startswith("QSMxT "):
                    derived.append((description, image.data))
                else:
                    returned_originals.append(image)
            if error:
                assert error in "\n".join(connection.errors), (name, connection.errors)
                assert not derived, name
            else:
                assert not connection.errors, (name, connection.errors)
                assert {description for description, _ in derived} == {f"QSMxT QSM {label}"}, name
                data = np.asarray([data for _, data in derived])
                assert np.isfinite(data).all() and np.any(data != 0), name
                assert len(returned_originals) == len(images), name
                for original, returned in zip(originals, returned_originals):
                    np.testing.assert_array_equal(returned.data, original, err_msg=name)
            print(f"{name}: verified", flush=True)
    finally:
        qsmxt.OPENRECON_WORK_ROOT = previous_root
    print("ND fallback returns finite nonzero maps with actual input labels and preserves originals")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="/opt/qsmxt/qsmxt")
    parser.add_argument("--nd-fallback", action="store_true")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    with tempfile.TemporaryDirectory(prefix="qsmxt-output-echoes-") as temporary:
        if arguments.nd_fallback:
            verify_nd_fallback(arguments.binary, Path(temporary))
        else:
            verify(arguments.binary, Path(temporary))
