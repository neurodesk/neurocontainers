#!/usr/bin/env python3
"""Run each side in its own image with the same --share directory mounted."""

import argparse
import json
import os
from pathlib import Path

import ismrmrd
import numpy as np

BASELINE = np.array([0.4, -0.2, 0.3])
ABSOLUTE = np.array([-0.3, 0.1, 0.2])
ISOCENTRE = np.array([2.0, -3.0, 4.0])
GAMMA_HZ_PER_UT_PER_MM = 0.042577478517832555
PARAMETERS = {
    "shimnativebaseline": ".4,-.2,.3",
    "shimnativelower": "-10,-10,-10",
    "shimnativeupper": "10,10,10",
    "shimisocentrerasmm": "2,-3,4",
}


class Connection:
    def __init__(self, images):
        self.images, self.sent, self.logs, self.closed = images, [], [], False

    def __iter__(self):
        return iter(self.images)

    def send_image(self, images):
        self.sent.extend(images)

    def send_logging(self, level, text):
        self.logs.append((level, text))

    def send_close(self):
        self.closed = True


def world_field(points):
    lai = (points - ISOCENTRE[:, None]) * np.array([-1, 1, -1])[:, None]
    return 37.0 + (GAMMA_HZ_PER_UT_PER_MM * lai).T @ (BASELINE - ABSOLUTE)


def images_from_geometry(
    shape, axes_lps, spacing, centre_lps, *, values=None, packed=False
):
    centre_lps = np.asarray(centre_lps, float)
    spacing = np.asarray(spacing, float)
    axes_lps = np.asarray(axes_lps, float)
    shape = np.asarray(shape, int)
    values = np.ones(shape, dtype=np.float32) * 100 if values is None else values
    first_centre = centre_lps - axes_lps[:, 2] * spacing[2] * (shape[2] - 1) / 2
    corner_lps = centre_lps - axes_lps @ (spacing * (shape - 1) / 2)
    points_lps = corner_lps[:, None] + axes_lps @ (
        spacing[:, None] * np.indices(shape).reshape(3, -1)
    )
    points_ras = points_lps * np.array([-1, -1, 1])[:, None]
    images = []
    for z in ([None] if packed else range(shape[2])):
        data = (
            values.transpose(2, 1, 0)[None]
            if z is None
            else values[:, :, z].T[None, None]
        )
        image = ismrmrd.Image.from_array(data.astype(np.float32), transpose=False)
        head = image.getHead()
        head.image_type = ismrmrd.IMTYPE_MAGNITUDE
        head.image_series_index = 5
        head.field_of_view[:] = spacing * (
            shape if z is None else [shape[0], shape[1], 1]
        )
        head.position[:] = (
            centre_lps if z is None else first_centre + axes_lps[:, 2] * spacing[2] * z
        )
        head.read_dir[:], head.phase_dir[:], head.slice_dir[:] = axes_lps.T
        head.slice = z or 0
        image.setHead(head)
        meta = ismrmrd.Meta()
        meta["StudyInstanceUID"] = "1.2.826.0.1.3680043.10.543.1"
        meta["FrameOfReferenceUID"] = "1.2.826.0.1.3680043.10.543.2"
        meta["SeriesInstanceUID"] = "1.2.826.0.1.3680043.10.543.3"
        image.attribute_string = meta.serialize()
        images.append(image)
    expected_affine = np.eye(4)
    expected_affine[:3, :3] = np.diag([-1, -1, 1]) @ axes_lps @ np.diag(spacing)
    expected_affine[:3, 3] = corner_lps * [-1, -1, 1]
    return images, points_ras, expected_affine


def target_images():
    angle = 0.31
    tilt = 0.19
    rotation = np.array(
        [
            [np.cos(angle), -np.sin(angle), 0],
            [np.sin(angle), np.cos(angle), 0],
            [0, 0, 1],
        ]
    ) @ np.array(
        [[1, 0, 0], [0, np.cos(tilt), -np.sin(tilt)], [0, np.sin(tilt), np.cos(tilt)]]
    )
    return images_from_geometry((9, 8, 7), rotation, [1.1, 1.4, 1.7], [1.3, -2.1, 0.9])


def decode(images):
    result = []
    for image in images:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        result.append(
            image.data[0, 0].T.astype(float) * float(meta["RescaleSlope"])
            + float(meta["RescaleIntercept"])
        )
    return np.stack(result, axis=2)


def produce(share):
    import copy
    import b0mapromeo
    from b0_artifact import read_map

    images, points, affine = images_from_geometry(
        (24, 22, 20), np.eye(3), [1.5, 1.7, 2.0], [0, 0, 0]
    )
    expected = world_field(points).reshape(24, 22, 20)
    inputs = []
    times = [2.0, 4.0, 6.0]
    for echo, te in enumerate(times):
        for kind in (ismrmrd.IMTYPE_MAGNITUDE, ismrmrd.IMTYPE_PHASE):
            for z, original in enumerate(images):
                image = copy.deepcopy(original)
                head = image.getHead()
                head.contrast = echo
                head.image_type = kind
                head.image_series_index = kind
                image.setHead(head)
                if kind == ismrmrd.IMTYPE_PHASE:
                    phase = (
                        2 * np.pi * expected[:, :, z] * te / 1000 + 0.2 + np.pi
                    ) % (2 * np.pi) - np.pi
                    image.data[:] = phase.T[None, None]
                inputs.append(image)
    connection = Connection(inputs)
    b0mapromeo.WORK_ROOT = share / "scratch"
    b0mapromeo.process(
        connection,
        {
            "phaseunits": "radians",
            "echotimesms": "2,4,6",
            "b0mapid": "shared-fixture",
            **PARAMETERS,
        },
        None,
    )
    assert connection.closed and not connection.logs and connection.sent
    assert not list(b0mapromeo.WORK_ROOT.iterdir())
    saved = read_map("shared-fixture")
    np.testing.assert_allclose(saved.affine, affine, atol=2e-6)
    error = np.abs(saved.field_hz[saved.support] - expected[saved.support])
    assert error.mean() < 0.5 and error.max() < 5, (error.mean(), error.max())
    for image in connection.sent:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        assert meta["B0MapId"] == "shared-fixture"
        assert "B0MapId=shared-fixture" in meta["ImageComment"] == meta["ImageComments"]
    (share / "producer-proof.json").write_text(
        json.dumps(
            {
                "map": saved.id,
                "digest": saved.digest,
                "max_error_hz": float(error.max()),
            }
        )
    )
    print("Real ROMEO producer persisted shared-fixture after scratch cleanup")


def consume(share):
    import shim_toolbox
    from b0_artifact import read_map, evaluate_target
    from b0_geometry import assemble_target

    images, points, affine = target_images()
    target = assemble_target(list(reversed(images)))
    np.testing.assert_allclose(target.affine, affine, atol=2e-6)
    saved = read_map("shared-fixture")
    proof = json.loads((share / "producer-proof.json").read_text())
    assert saved.digest == proof["digest"]
    evaluation = evaluate_target(saved, target)
    expected = world_field(points).reshape(target.magnitude.shape)
    assert evaluation.support.all()
    np.testing.assert_allclose(evaluation.field_hz, expected, atol=0.5)
    np.testing.assert_allclose(
        [v.absolute for v in evaluation.shim.settings], ABSOLUTE, atol=0.05
    )
    np.testing.assert_allclose(evaluation.shim.predicted_field_hz, 37, atol=0.5)
    connection = Connection(list(reversed(images)))
    shim_toolbox.process(connection, {"b0mapid": "shared-fixture"}, None)
    assert (
        connection.closed
        and not connection.logs
        and len(connection.sent) == len(images) * 2
    )
    field, predicted = connection.sent[: len(images)], connection.sent[len(images) :]
    np.testing.assert_allclose(decode(field), expected, atol=1)
    np.testing.assert_allclose(decode(predicted), 37, atol=1)
    series = set()
    for batch in (field, predicted):
        for z, image in enumerate(batch):
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            assert meta["ImageComment"] == meta["ImageComments"]
            assert "ABSOLUTE ANALYTICAL ESTIMATE" in meta["ImageComments"]
            assert "ideal field estimate" in meta["ImageComments"]
            assert "supported ROI 504/504" in meta["ImageComments"]
            assert meta["PixelPaddingValue"] == "0" and meta["RescaleType"] == "Hz"
            np.testing.assert_allclose(image.position, images[z].position)
            series.add(meta["SeriesInstanceUID"])
    assert len(series) == 2
    for selected in ("missing-map", ""):
        failed = Connection(images)
        shim_toolbox.process(failed, {"b0mapid": selected}, None)
        assert failed.closed and failed.logs and not failed.sent
    (share / "consumer-proof.json").write_text(
        json.dumps(
            {
                "map": saved.id,
                "series_count": len(series),
                "absolute_native": [v.absolute for v in evaluation.shim.settings],
            }
        )
    )
    print(
        "Translated oblique target returned field and ideal prediction with independent Hz/settings/geometry checks"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["produce", "consume"])
    parser.add_argument("--share", type=Path, required=True)
    args = parser.parse_args()
    args.share.mkdir(parents=True, exist_ok=True)
    os.environ["B0_MAP_STORE"] = str(args.share / "b0maps")
    (produce if args.mode == "produce" else consume)(args.share)


if __name__ == "__main__":
    main()
