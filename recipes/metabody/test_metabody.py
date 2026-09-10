import importlib.util
import json
import subprocess
import sys
import types
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np
import pytest


RECIPE_DIR = Path(__file__).parent


def load_metabody_module():
    mrdhelper = types.ModuleType("mrdhelper")
    mrdhelper.get_json_config_param = (
        lambda config, key, default=None: config.get(key, default)
    )
    mrdhelper.get_meta_value = lambda metadata, key: metadata.get(key)
    mrdhelper.extract_minihead_bool_param = lambda *_args: False
    sys.modules["mrdhelper"] = mrdhelper
    sys.modules["constants"] = types.ModuleType("constants")

    spec = importlib.util.spec_from_file_location(
        "metabody_under_test", RECIPE_DIR / "metabody.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_nifti2mrd_module():
    spec = importlib.util.spec_from_file_location(
        "nifti2mrd_under_test", RECIPE_DIR / "nifti2mrd.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_image(
    slice_id: int,
    repetition_id: int,
    value: float,
    repetition_time_ms: float | None = None,
):
    image = ismrmrd.Image.from_array(
        np.full((2, 3), value, dtype=np.float32), transpose=False
    )
    image.slice = slice_id
    image.repetition = repetition_id
    image.image_series_index = 12
    image.field_of_view[:] = [3, 2, 1]
    image.position[:] = [slice_id, 0, 0]
    image.read_dir[:] = [1, 0, 0]
    image.phase_dir[:] = [0, 1, 0]
    image.slice_dir[:] = [0, 0, 1]

    metadata = ismrmrd.Meta()
    metadata["PixelSpacing"] = ["1", "1"]
    metadata["SliceThickness"] = "1"
    if repetition_time_ms is not None:
        metadata["RepetitionTime"] = str(repetition_time_ms)
    image.attribute_string = metadata.serialize()
    return image


def render_single_stat(monkeypatch, colormap: str):
    metabody = load_metabody_module()
    source = make_image(0, 0, 10)
    monkeypatch.setattr(metabody.nib, "save", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(metabody.subprocess, "run", lambda *_args, **_kwargs: None)

    stat_data = np.arange(6, dtype=np.float32).reshape(2, 3, 1, 1) + 1
    stat_image = nib.Nifti1Image(stat_data, np.eye(4))
    monkeypatch.setattr(
        metabody,
        "show_stats",
        lambda *_args, **_kwargs: (["stat-0"], stat_image),
    )

    output = metabody.process_image(
        [source],
        None,
        {"sendOriginal": False, "colormap": colormap},
        None,
    )
    assert len(output) == 1
    return output[0], ismrmrd.Meta.deserialize(output[0].attribute_string)


def test_openrecon_label_exposes_output_appearance_choices():
    label = json.loads((RECIPE_DIR / "OpenReconLabel.json").read_text())
    parameter = next(
        item for item in label["parameters"] if item["id"] == "colormap"
    )

    assert parameter["label"]["en"] == "Output appearance"
    assert parameter["default"] == "scanner_lut"
    assert [choice["id"] for choice in parameter["values"]] == [
        "scanner_lut",
        "seismic",
        "jet",
        "gray",
        "coolwarm",
        "bwr",
        "none",
    ]


def test_missing_output_appearance_preserves_legacy_seismic_default():
    metabody = load_metabody_module()

    output_spec = metabody.resolve_output_spec({})

    assert isinstance(output_spec, metabody.EmbeddedRgbOutput)
    assert output_spec.colormap == "seismic"


@pytest.mark.parametrize("colormap", ["seismic", "jet", "gray", "coolwarm", "bwr"])
def test_embedded_rgb_has_no_scanner_lut(monkeypatch, colormap):
    image, metadata = render_single_stat(monkeypatch, colormap)

    assert image.data.shape == (3, 1, 2, 3)
    assert image.channels == 3
    assert image.image_type == 6
    assert image.data.dtype == np.uint16
    assert 0 <= image.data.min() <= image.data.max() <= 255
    assert metadata["InternalSend"] == "1"
    assert "RGB" in metadata["ImageProcessingHistory"]
    assert "LUTFileName" not in metadata
    assert "WindowCenter" not in metadata
    assert "WindowWidth" not in metadata


def test_scanner_palette_uses_one_scalar_channel(monkeypatch):
    image, metadata = render_single_stat(monkeypatch, "scanner_lut")

    assert image.data.shape == (1, 1, 2, 3)
    assert image.channels == 1
    assert image.image_type == ismrmrd.IMTYPE_MAGNITUDE
    assert 0 <= image.data.min() <= image.data.max() <= 4095
    assert metadata["LUTFileName"] == "MicroDeltaHotMetal.pal"
    assert "InternalSend" not in metadata
    assert "RGB" not in metadata["ImageProcessingHistory"]
    assert metadata["WindowCenter"] == "2048.0"
    assert metadata["WindowWidth"] == "4096"


def test_grayscale_uses_one_scalar_channel_without_lut(monkeypatch):
    image, metadata = render_single_stat(monkeypatch, "none")

    assert image.data.shape == (1, 1, 2, 3)
    assert image.channels == 1
    assert image.image_type == ismrmrd.IMTYPE_MAGNITUDE
    assert 0 <= image.data.min() <= image.data.max() <= 4095
    assert "LUTFileName" not in metadata
    assert "InternalSend" not in metadata
    assert "RGB" not in metadata["ImageProcessingHistory"]
    assert metadata["WindowCenter"] == "2048.0"
    assert metadata["WindowWidth"] == "4096"


def test_sparse_counters_use_dense_stacking_and_slice_headers(monkeypatch):
    metabody = load_metabody_module()
    images = [
        make_image(4, 11, 41),
        make_image(2, 7, 20),
        make_image(2, 11, 21),
        make_image(4, 7, 40),
    ]
    original_attributes = [image.attribute_string for image in images]
    saved_images = []
    monkeypatch.setattr(metabody.nib, "save", lambda image, _path: saved_images.append(image))
    monkeypatch.setattr(metabody.subprocess, "run", lambda *_args, **_kwargs: None)

    stat_data = np.arange(36, dtype=np.float32).reshape(2, 3, 2, 3) + 1
    stat_image = nib.Nifti1Image(stat_data, np.eye(4))
    monkeypatch.setattr(
        metabody,
        "show_stats",
        lambda *_args, **_kwargs: (["stat-0", "stat-1", "stat-2"], stat_image),
    )

    output = metabody.process_image(images, None, {}, None)

    stacked = saved_images[0].get_fdata()
    assert stacked.shape == (2, 3, 2, 2)
    np.testing.assert_array_equal(saved_images[0].affine[:3, 3], [2, 0, 0])
    np.testing.assert_array_equal(stacked[:, :, 0, 0], 20)
    np.testing.assert_array_equal(stacked[:, :, 0, 1], 21)
    np.testing.assert_array_equal(stacked[:, :, 1, 0], 40)
    np.testing.assert_array_equal(stacked[:, :, 1, 1], 41)

    original_output, stats_output = output[:4], output[4:]
    assert len(stats_output) == 6
    assert [image.slice for image in stats_output] == [2, 4, 2, 4, 2, 4]
    assert [image.repetition for image in stats_output] == [0, 0, 1, 1, 2, 2]
    assert [image.position[0] for image in stats_output] == [2, 4, 2, 4, 2, 4]

    assert all(cloned is not source for cloned, source in zip(original_output, images))
    assert all(image.image_series_index == 99 for image in original_output)
    assert all(image.image_series_index == 100 for image in stats_output)
    assert all(image.image_series_index == 12 for image in images)
    assert [image.attribute_string for image in images] == original_attributes


def test_afni_failure_returns_originals_and_preserves_image_tr(monkeypatch):
    metabody = load_metabody_module()
    images = [
        make_image(0, 0, 10, repetition_time_ms=2000),
        make_image(0, 1, 11, repetition_time_ms=2000),
    ]
    saved_images = []
    commands = []
    monkeypatch.setattr(
        metabody.nib,
        "save",
        lambda image, _path: saved_images.append(image),
    )

    def fail_afni(command, **kwargs):
        commands.append((command, kwargs))
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(metabody.subprocess, "run", fail_afni)
    monkeypatch.setattr(
        metabody,
        "show_stats",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("show_stats must not run after AFNI fails")
        ),
    )

    output = metabody.process_image(
        images,
        None,
        {"sendOriginal": True, "colormap": "none"},
        None,
    )

    assert len(output) == len(images)
    assert all(cloned is not source for cloned, source in zip(output, images))
    assert all(image.image_series_index == 99 for image in output)
    assert [image.image_series_index for image in images] == [12, 12]
    assert commands == [
        (
            [
                "/opt/code/afni_processing.sh",
                "--input",
                "nifti_from_h5.nii",
                "--output",
                "output_afni",
                "--tr",
                "2",
            ],
            {"check": True},
        )
    ]
    assert saved_images[0].header.get_zooms()[-1] == 2
    assert saved_images[0].header.get_xyzt_units() == ("mm", "sec")


def test_nifti_converter_writes_3d_images_and_header(tmp_path):
    nifti2mrd = load_nifti2mrd_module()
    input_path = tmp_path / "input.nii.gz"
    output_path = tmp_path / "output.h5"
    nib.save(
        nib.Nifti1Image(np.ones((3, 4, 2), dtype=np.float32), np.eye(4)),
        input_path,
    )

    nifti2mrd.convert_nifti_to_ismrmrd(str(input_path), str(output_path))

    with ismrmrd.Dataset(str(output_path), "dataset") as dataset:
        header = ismrmrd.xsd.CreateFromDocument(dataset.read_xml_header())
        assert header.experimentalConditions.H1resonanceFrequency_Hz == 1
        assert header.encoding[0].encodedSpace.matrixSize.z == 1
        assert header.encoding[0].reconSpace.matrixSize.z == 1
        assert dataset.number_of_images("image_1") == 2
