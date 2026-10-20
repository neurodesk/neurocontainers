"""OpenRecon 3D EPI motion correction, phase matching, and complex averaging."""

from __future__ import annotations

import logging
import tempfile
import traceback
from pathlib import Path

import ismrmrd
import numpy as np

import ep3d_mrd
from openrecon_metadata import processing_metadata
import qsmxt


WORK_ROOT = Path("/tmp/share/ep3d")


def _settings(config) -> dict:
    parameters = qsmxt._config_parameters(config)
    phase_wrap = float(parameters.get("phasewrap", 4096))
    if not np.isfinite(phase_wrap) or phase_wrap <= 0:
        raise ValueError(
            "phasewrap must be a finite, positive number of counts per 2*pi"
        )
    return {
        "input_series": qsmxt._config_text(
            parameters,
            "inputseries",
            qsmxt.DEFAULT_INPUT_SERIES,
        ).lower(),
        "phase_wrap": phase_wrap,
        "ep3d_denoise": qsmxt._config_bool(parameters, "denoise", False),
    }


def _corrected_images(
    pair: dict, settings: dict, work_dir: Path, used_series: set[int]
) -> list[ismrmrd.Image]:
    magnitude_group, phase_group = pair["groups"]
    source_images = magnitude_group["images"] + phase_group["images"]
    echo_numbers = {
        number
        for image in source_images
        if (number := qsmxt._source_echo_number(image)) is not None
    }
    contrasts = {int(image.getHead().contrast) for image in source_images}
    if len(echo_numbers) > 1 or len(contrasts) > 1:
        raise ValueError(
            "3D EPI post-processing requires a single echo across repetitions"
        )

    preprocessed = ep3d_mrd.preprocess_images(
        magnitude_group["images"],
        phase_group["images"],
        work_dir,
        read_volume=qsmxt._images_to_nifti_volume,
        phase_wrap=settings["phase_wrap"],
        denoise=settings["ep3d_denoise"],
        n_procs=qsmxt._cpu_budget(),
    )
    processing = processing_metadata("ep3d", __file__, settings)
    outputs = []
    for output_id, path, geometry, image_type, token, units in (
        (
            "magnitude",
            preprocessed.magnitude_path,
            preprocessed.magnitude_reference,
            ismrmrd.IMTYPE_MAGNITUDE,
            "OR_EP3D_MAG",
            "a.u.",
        ),
        (
            "phase",
            preprocessed.phase_path,
            preprocessed.phase_reference,
            ismrmrd.IMTYPE_PHASE,
            "OR_EP3D_PHA",
            "rad",
        ),
    ):
        series_index = qsmxt._reserve_series_index(
            used_series, qsmxt.OUTPUT_SERIES_START
        )
        images = qsmxt._nifti_to_mrd_images(
            path,
            geometry[0],
            series_index,
            f"ep3d corrected {output_id} {pair['short_label']}",
            token,
            output_id,
            units,
            source_geometry_images=geometry,
            input_variant=pair["variant"],
            processing=processing,
        )
        component = "PHASE" if output_id == "phase" else "MAGNITUDE"
        component_token = "P" if output_id == "phase" else "M"
        for image in images:
            header = image.getHead()
            header.image_type = image_type
            for field in ("repetition", "set", "average", "phase"):
                setattr(header, field, 0)
            image.setHead(header)
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["ImageProcessingHistory"] = ["PYTHON", "EP3D"]
            meta["ComplexImageComponent"] = component
            meta["ImageType"] = f"DERIVED\\PRIMARY\\{component_token}\\{token}"
            meta["DicomImageType"] = meta["ImageType"]
            meta["EP3DOutput"] = output_id
            meta["EP3DUnits"] = units
            meta["WindowCenter"] = meta["QSMxTPhysicalWindowCenter"]
            meta["WindowWidth"] = meta["QSMxTPhysicalWindowWidth"]
            image.attribute_string = meta.serialize()
        outputs.extend(images)
    return outputs


def process(connection, config, metadata) -> None:
    """Return corrected magnitude and wrapped phase for each selected input pair."""
    try:
        images = []
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                images.append(item)
            else:
                logging.info("Ignoring unsupported MRD message %s", type(item).__name__)
        if not images:
            return

        settings = _settings(config)
        pairs = qsmxt._select_magnitude_phase_pairs(
            qsmxt._group_images_by_series(images),
            settings["input_series"],
        )
        used_series = {int(image.getHead().image_series_index) for image in images}
        WORK_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="run_", dir=WORK_ROOT) as temporary:
            for pair in pairs:
                outputs = _corrected_images(
                    pair,
                    settings,
                    Path(temporary) / pair["short_label"],
                    used_series,
                )
                qsmxt._validate_output_images(outputs, images)
                qsmxt._send_images_by_series(connection, outputs)
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(qsmxt.constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()
