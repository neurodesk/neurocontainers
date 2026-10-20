"""Exercise the packaged OpenRecon adapter with synthetic dual-polarity images."""

from __future__ import annotations

import copy
import tempfile
from pathlib import Path

import ismrmrd
import numpy as np

import ep3d
from ep3d_fixture import dual_polarity_phantom


class Connection:
    def __init__(self, images):
        self.images = images
        self.sent = []
        self.logs = []
        self.closed = False

    def __iter__(self):
        yield from self.images
        yield None

    def send_image(self, images):
        self.sent.extend(images if isinstance(images, list) else [images])

    def send_logging(self, level, message):
        self.logs.append(message)

    def send_close(self):
        self.closed = True


def images_for_volume(data, repetition, *, phase=False, sliced=False, nd=False):
    shape = data.shape
    spacing = np.array([1.0, 1.5, 2.0])
    angle = np.deg2rad(20)
    chunks = (
        [data[:, :, index : index + 1] for index in range(shape[2])]
        if sliced
        else [data]
    )
    images = []
    for index, chunk in enumerate(chunks):
        image = ismrmrd.Image.from_array(
            np.transpose(chunk, (2, 1, 0)).astype(np.float32),
            transpose=False,
        )
        header = image.getHead()
        header.image_series_index = (2 if phase else 1) + (10 if nd else 0)
        header.image_index = repetition * len(chunks) + index + 1
        header.image_type = ismrmrd.IMTYPE_PHASE if phase else ismrmrd.IMTYPE_MAGNITUDE
        header.repetition = repetition
        header.slice = index
        position = np.array([11.0, -9.0, 7.0])
        if sliced:
            position[2] += (index - (shape[2] - 1) / 2) * spacing[2]
        for field, values in (
            ("field_of_view", np.array(chunk.shape) * spacing),
            ("position", position),
            ("read_dir", [np.cos(angle), np.sin(angle), 0]),
            ("phase_dir", [-np.sin(angle), np.cos(angle), 0]),
            ("slice_dir", [0, 0, 1]),
        ):
            for axis, value in enumerate(values):
                getattr(header, field)[axis] = value
        image.setHead(header)
        meta = ismrmrd.Meta()
        meta["SequenceDescription"] = (
            "ep3d_test" + ("_Pha" if phase else "") + ("_ND" if nd else "")
        )
        meta["EchoNumber"] = "1"
        meta["SeriesInstanceUID"] = f"1.2.3.{header.image_series_index}"
        image.attribute_string = meta.serialize()
        images.append(image)
    return images


def run_case(signals, *, sliced=False, denoise=False, both=False):
    images = []
    for nd in [False, True] if both else [False]:
        for repetition, signal in enumerate(signals):
            phase = np.angle(signal)
            if not sliced:
                phase = np.mod(phase, 2 * np.pi) * (4096 / (2 * np.pi))
            images.extend(
                images_for_volume(np.abs(signal), repetition, sliced=sliced, nd=nd)
            )
            images.extend(
                images_for_volume(phase, repetition, phase=True, sliced=sliced, nd=nd)
            )
    connection = Connection(images)
    ep3d.process(
        connection,
        {
            "parameters": {
                "denoise": denoise,
                "inputseries": "both" if both else "distortion-corrected",
            }
        },
        None,
    )
    assert connection.closed and not connection.logs, connection.logs
    slice_count = signals[0].shape[2]
    assert len(connection.sent) == slice_count * (4 if both else 2)
    products = {}
    series_uids = {}
    instance_uids = set()
    for image in connection.sent:
        header = image.getHead()
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        output = meta["EP3DOutput"]
        key = (meta["QSMxTInputSeries"], output)
        products.setdefault(key, []).append(image)
        series_uids.setdefault(key, set()).add(meta["SeriesInstanceUID"])
        assert meta["SOPInstanceUID"] not in instance_uids
        instance_uids.add(meta["SOPInstanceUID"])
        assert meta["OpenReconSoftware"] == "ep3d"
        assert meta["ImageProcessingHistory"] == ["PYTHON", "EP3D"]
        assert image.data.dtype == np.uint16 and image.data.max() <= 4095
        expected_type = (
            ismrmrd.IMTYPE_PHASE if output == "phase" else ismrmrd.IMTYPE_MAGNITUDE
        )
        assert header.image_type == expected_type
        assert meta["ComplexImageComponent"] == (
            "PHASE" if output == "phase" else "MAGNITUDE"
        )
        assert meta["EP3DUnits"] == ("rad" if output == "phase" else "a.u.")
        np.testing.assert_allclose(header.read_dir, images[0].getHead().read_dir)
        np.testing.assert_allclose(header.phase_dir, images[0].getHead().phase_dir)
        assert header.repetition == header.set == header.average == 0
    assert all(len(uids) == 1 for uids in series_uids.values())
    assert len(set.union(*series_uids.values())) == (4 if both else 2)

    decoded = {}
    for key, slices in products.items():
        slices.sort(key=lambda image: image.getHead().slice)
        arrays = []
        for image in slices:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            arrays.append(
                image.data[0, 0].astype(float) * float(meta["RescaleSlope"])
                + float(meta["RescaleIntercept"])
            )
        decoded[key] = np.transpose(np.stack(arrays), (2, 1, 0))
        np.testing.assert_allclose(
            [image.getHead().position[2] for image in slices],
            7 + (np.arange(slice_count) - (slice_count - 1) / 2) * 2,
            atol=1e-5,
        )
    if both:
        for output in ("magnitude", "phase"):
            np.testing.assert_array_equal(
                decoded[("distortion-corrected", output)],
                decoded[("non-distortion-corrected", output)],
            )
    return decoded


def main() -> None:
    phantom = dual_polarity_phantom()
    magnitude, truth, core = phantom.magnitude, phantom.truth, phantom.core
    signals = phantom.signals

    with tempfile.TemporaryDirectory() as temporary:
        ep3d.WORK_ROOT = Path(temporary)
        for sliced, denoise, both in [
            (False, False, True),
            (True, False, False),
            (False, True, False),
        ]:
            decoded = run_case(signals, sliced=sliced, denoise=denoise, both=both)
            corrected_magnitude = decoded[("distortion-corrected", "magnitude")]
            corrected_phase = decoded[("distortion-corrected", "phase")]
            difference = (
                corrected_magnitude * np.exp(1j * corrected_phase) * np.conj(truth)
            )
            phase_error = np.angle(
                difference * np.exp(-1j * np.angle(difference[core].sum()))
            )[core]
            magnitude_error = np.median(
                np.abs(corrected_magnitude[core] - magnitude[core]) / magnitude[core]
            )
            assert magnitude_error < 0.08, magnitude_error
            assert phase_error.std() < 0.15, phase_error.std()
            assert not list(ep3d.WORK_ROOT.iterdir()), "Request scratch files remain"

        empty = Connection([])
        ep3d.process(empty, {}, None)
        assert empty.closed and not empty.sent and not empty.logs
        single = images_for_volume(np.abs(signals[0]), 0)
        single += images_for_volume(np.angle(signals[0]), 0, phase=True)
        invalid = copy.deepcopy(single)
        invalid[1].repetition = 1
        for images, config, message in (
            (single, {}, "at least two repetitions"),
            (invalid, {}, "keys differ"),
            (single, {"phasewrap": 0}, "phasewrap must be"),
            (single[:1], {}, "phase"),
        ):
            failed = Connection(images)
            ep3d.process(failed, config, None)
            assert failed.closed and not failed.sent
            assert any(message in log for log in failed.logs), failed.logs
    print("3D EPI corrected magnitude and phase verified")


if __name__ == "__main__":
    main()
