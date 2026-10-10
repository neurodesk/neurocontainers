"""Evaluate an immutable source B0 map on one MRD magnitude target volume."""

import argparse
from collections import defaultdict
import logging
import os

import ismrmrd
import openreconi2iexample as helpers

from b0_artifact import evaluate_target, read_map
from b0_geometry import assemble_target
from b0_images import output_images
from b0_settings import _settings

VERSION = os.environ.get("SHIM_TOOLBOX_VERSION", "development")


def process(connection, config, metadata):
    try:
        images = []
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                raise ValueError(
                    "Target shimming requires reconstructed image messages"
                )
            images.append(item)
        settings = _settings(config, metadata)
        saved = read_map(settings["b0mapid"])
        target = assemble_target(images)
        evaluation = evaluate_target(saved, target, settings)
        series = max(190, max(int(im.image_series_index) for im in images) + 1)
        batches = [
            output_images(
                evaluation.field_hz,
                target.anchors,
                series,
                evaluation.shim,
                support=evaluation.support,
                name="Target B0 Hz",
                comment=evaluation.comment,
                map_id=saved.id,
            )
        ]
        prediction = evaluation.shim.predicted_field_hz
        if prediction is not None:
            batches.append(
                output_images(
                    prediction,
                    target.anchors,
                    series + 1,
                    evaluation.shim,
                    support=evaluation.support,
                    name="Predicted B0 Hz",
                    comment=evaluation.comment,
                    map_id=saved.id,
                )
            )
        if settings["send_original"]:
            originals = defaultdict(list)
            for image in helpers._restamp_originals(images):
                originals[int(image.image_series_index)].append(image)
            for batch in originals.values():
                connection.send_image(batch)
        for batch in batches:
            connection.send_image(batch)
    except Exception:
        logging.exception("Target B0 evaluation failed")
        connection.send_logging(
            3, "Target B0 evaluation failed; check the local server log"
        )
    finally:
        connection.send_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version", action="version", version=f"shim_toolbox {VERSION}"
    )
    parser.parse_args()
    parser.error("Use the OpenRecon MRD server with config shim_toolbox")


if __name__ == "__main__":
    main()
