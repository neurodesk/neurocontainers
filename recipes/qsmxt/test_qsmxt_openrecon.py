import base64
import json
import logging
import stat
import sys
import textwrap
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np
import pytest

import qsmxt


@pytest.mark.parametrize(
    "cpu_count,affinity_count,quota,expected",
    [
        (64, 16, "400000 100000", 4),
        (64, 16, "max 100000", 16),
        (64, 64, "max 100000", 24),
        (8, 8, "50000 100000", 1),
        (64, 16, "unavailable", 16),
        (None, 8, "max 100000", 1),
    ],
)
def test_cpu_budget_respects_available_resources(
    monkeypatch, cpu_count, affinity_count, quota, expected
):
    monkeypatch.setattr(qsmxt.os, "cpu_count", lambda: cpu_count)
    monkeypatch.setattr(qsmxt.os, "sched_getaffinity", lambda pid: set(range(affinity_count)))
    monkeypatch.setattr(Path, "read_text", lambda self: quota)
    assert qsmxt._cpu_budget() == expected


class FakeAcquisitionSystemInformation:
    systemFieldStrength_T = 7.0


class FakeMeasurementInformation:
    protocolName = "gre_qsm"


class FakeSequenceParameters:
    TE = [20.0]


class FakeMetadata:
    acquisitionSystemInformation = FakeAcquisitionSystemInformation()
    measurementInformation = FakeMeasurementInformation()
    sequenceParameters = FakeSequenceParameters()


class FakeConnection:
    def __init__(self, images):
        self.images = images
        self.sent_batches = []
        self.logs = []
        self.closed = False

    def __iter__(self):
        yield from self.images
        yield None

    def send_image(self, images):
        self.sent_batches.append(images if isinstance(images, list) else [images])

    def send_logging(self, level, contents):
        self.logs.append((level, contents))

    def send_close(self):
        self.closed = True


def _sent_series_descriptions(connection):
    return {
        ismrmrd.Meta.deserialize(image.attribute_string)["SeriesDescription"]
        for batch in connection.sent_batches
        for image in batch
    }


def _set_header_vector(header, field_name, values):
    target = getattr(header, field_name)
    for index, value in enumerate(values):
        target[index] = value


def _image(
    series_index,
    image_index,
    sequence,
    data,
    image_type=ismrmrd.IMTYPE_MAGNITUDE,
    meta_values=None,
    minihead_text=None,
    header_values=None,
):
    image = ismrmrd.Image.from_array(data.astype(np.float32), transpose=False)
    header = image.getHead()
    header.image_series_index = series_index
    header.image_index = image_index
    header.image_type = image_type
    for key, value in (header_values or {}).items():
        setattr(header, key, value)
    _set_header_vector(header, "matrix_size", [data.shape[2], data.shape[1], data.shape[0]])
    _set_header_vector(header, "field_of_view", [4.0, 3.0, 2.0])
    _set_header_vector(header, "position", [0.0, 0.0, 0.0])
    _set_header_vector(header, "read_dir", [1.0, 0.0, 0.0])
    _set_header_vector(header, "phase_dir", [0.0, 1.0, 0.0])
    _set_header_vector(header, "slice_dir", [0.0, 0.0, 1.0])
    image.setHead(header)

    meta = ismrmrd.Meta()
    meta["SequenceDescription"] = sequence
    for key, value in (meta_values or {}).items():
        meta[key] = value
    if minihead_text:
        meta["IceMiniHead"] = base64.b64encode(
            minihead_text.encode("utf-8")
        ).decode("ascii")
    image.attribute_string = meta.serialize()
    return image


def _set_image_orientation(image, read_dir, phase_dir, slice_dir):
    header = image.getHead()
    _set_header_vector(header, "read_dir", read_dir)
    _set_header_vector(header, "phase_dir", phase_dir)
    _set_header_vector(header, "slice_dir", slice_dir)
    image.setHead(header)


def _scanner_minihead(echo, slice_index, image_type_value, series_end_slices=None):
    if series_end_slices is None:
        series_end_slices = {19}
    is_series_end = echo == 5 and slice_index in series_end_slices
    slice_value = "" if slice_index == 0 else str(slice_index)
    return textwrap.dedent(
        f"""\
        <XProtocol>
        {{
          <ParamMap."DICOM">
          {{
            <ParamLong."NumberInSeries">{{ {slice_index * 5 + echo} }}
            <ParamLong."EchoNumber">{{ {echo} }}
            <ParamLong."SliceNo">{{ {slice_value} }}
            <ParamLong."ProtocolSliceNumber">{{ {slice_index} }}
            <ParamString."ImageTypeValue3">{{ "{image_type_value}" }}
            <ParamString."ComplexImageComponent">{{ "{'PHASE' if image_type_value == 'P' else 'MAGNITUDE'}" }}
            <ParamString."SOPInstanceUID">{{ "1.2.826.0.1.{echo}.{slice_index}.{image_type_value}" }}
          }}
          <ParamMap."CONTROL">
          {{
            <ParamLong."AnatomicalSliceNo">{{ {slice_index} }}
            <ParamLong."ChronSliceNo">{{ {slice_index} }}
            <ParamBool."BIsSeriesEnd">{{ "{str(is_series_end).lower()}" }}
            <ParamBool."ConcatenationEnd">{{ "{str(is_series_end).lower()}" }}
            <ParamString."SeriesInstanceUID">{{ "1.2.826.0.1.series.{image_type_value}" }}
          }}
        }}
        """
    )


def _scanner_echo_slice_images():
    images = []
    for slice_index in range(20):
        for echo in range(1, 6):
            images.append(
                _image(
                    1,
                    slice_index * 5 + echo,
                    "gre",
                    np.full((1, 3, 4), 1000 + echo * 10 + slice_index, dtype=np.float32),
                    meta_values={"SeriesInstanceUID": "1.2.826.0.1.series.M"},
                    minihead_text=_scanner_minihead(echo, slice_index, "M"),
                    header_values={"slice": slice_index, "contrast": echo - 1},
                )
            )
    for slice_index in range(20):
        for echo in range(1, 6):
            images.append(
                _image(
                    2,
                    slice_index * 5 + echo,
                    "gre",
                    np.full((1, 3, 4), 2000 + echo * 10 + slice_index, dtype=np.float32),
                    image_type=getattr(ismrmrd, "IMTYPE_PHASE", 2),
                    meta_values={"SeriesInstanceUID": "1.2.826.0.1.series.P"},
                    minihead_text=_scanner_minihead(echo, slice_index, "P"),
                    header_values={"slice": slice_index, "contrast": echo - 1},
                )
            )
    return images


def _scanner_echo_slice_images_with_split_end_markers():
    images = []
    for slice_index in range(20):
        for echo in range(1, 6):
            images.append(
                _image(
                    1,
                    slice_index * 5 + echo,
                    "gre",
                    np.full((1, 3, 4), 1000 + echo * 10 + slice_index, dtype=np.float32),
                    meta_values={"SeriesInstanceUID": "1.2.826.0.1.series.M"},
                    minihead_text=_scanner_minihead(echo, slice_index, "M", {9, 19}),
                    header_values={"slice": slice_index, "contrast": echo - 1},
                )
            )
    for slice_index in range(20):
        for echo in range(1, 6):
            images.append(
                _image(
                    2,
                    slice_index * 5 + echo,
                    "gre",
                    np.full((1, 3, 4), 2000 + echo * 10 + slice_index, dtype=np.float32),
                    image_type=getattr(ismrmrd, "IMTYPE_PHASE", 2),
                    meta_values={"SeriesInstanceUID": "1.2.826.0.1.series.P"},
                    minihead_text=_scanner_minihead(echo, slice_index, "P", {9, 19}),
                    header_values={"slice": slice_index, "contrast": echo - 1},
                )
            )
    return images


def _input_images():
    shape_zyx = (2, 3, 4)
    images = []
    for echo in (1, 2):
        images.append(
            _image(
                1,
                echo,
                "gre_qsm",
                np.full(shape_zyx, 1000 + echo, dtype=np.float32),
            )
        )
    for echo in (1, 2):
        images.append(
            _image(
                2,
                echo,
                "gre_qsm_Pha",
                np.full(shape_zyx, 2000 + echo, dtype=np.float32),
                image_type=getattr(ismrmrd, "IMTYPE_PHASE", 2),
            )
        )
    return images


def _dual_input_images():
    shape_zyx = (2, 3, 4)
    images = []
    for series_index, sequence, image_type, base_value in (
        (1, "gre_qsm_ND", ismrmrd.IMTYPE_MAGNITUDE, 1000),
        (2, "gre_qsm_ND", getattr(ismrmrd, "IMTYPE_PHASE", 2), 2000),
        (3, "gre_qsm", ismrmrd.IMTYPE_MAGNITUDE, 3000),
        (4, "gre_qsm", getattr(ismrmrd, "IMTYPE_PHASE", 2), 4000),
    ):
        for echo in (1, 2):
            images.append(
                _image(
                    series_index,
                    echo,
                    sequence,
                    np.full(shape_zyx, base_value + echo, dtype=np.float32),
                    image_type=image_type,
                )
            )
    return images


def _packed_scanner_images():
    shape_zyx = (144, 3, 4)
    images = []
    for series_index, sequence, image_type, image_type_value in (
        (1, "gre_qsm", ismrmrd.IMTYPE_MAGNITUDE, "M"),
        (2, "gre_qsm_Pha", getattr(ismrmrd, "IMTYPE_PHASE", 2), "P"),
    ):
        image = _image(
            series_index,
            1,
            sequence,
            np.full(shape_zyx, 1000 + series_index, dtype=np.float32),
            image_type=image_type,
            meta_values={
                "NumberOfSlices": str(shape_zyx[0]),
                "slice_count": str(shape_zyx[0]),
                "SlicePosLightMarker": ["10", "20", "30"],
            },
            minihead_text=_scanner_minihead(1, 0, image_type_value),
            header_values={"contrast": 0, "slice": 0},
        )
        header = image.getHead()
        _set_header_vector(header, "field_of_view", [4.0, 3.0, 1.0])
        _set_header_vector(header, "position", [10.0, 20.0, 30.0])
        image.setHead(header)
        images.append(image)
    return images


def _fake_qsmxt_binary(tmp_path, affine_offset=None):
    fake = tmp_path / "qsmxt"
    affine_offset = list(affine_offset or [0.0, 0.0, 0.0])
    fake.write_text(
        textwrap.dedent(
            f"""\
            #!{sys.executable}
            import sys
            from pathlib import Path

            import nibabel as nib
            import numpy as np

            if len(sys.argv) < 4 or sys.argv[1] != "run":
                raise SystemExit(2)
            try:
                nprocs_index = sys.argv.index("--n-procs")
            except ValueError:
                raise SystemExit("qsmxt OpenRecon should set its worker budget")
            if not 1 <= int(sys.argv[nprocs_index + 1]) <= 24:
                raise SystemExit("qsmxt OpenRecon worker budget must be within 1..24")
            if "--no-mem-limit" not in sys.argv:
                raise SystemExit("single-acquisition OpenRecon should enable intra-run workers")
            for flag, expected in (
                ("--qsm-algorithm", "rts"),
                ("--unwrapping-algorithm", "romeo"),
                ("--bf-algorithm", "vsharp"),
            ):
                try:
                    flag_index = sys.argv.index(flag)
                except ValueError:
                    raise SystemExit(f"qsmxt OpenRecon should pass {{flag}} {{expected}}")
                if flag_index + 1 >= len(sys.argv) or sys.argv[flag_index + 1] != expected:
                    raise SystemExit(f"qsmxt OpenRecon should pass {{flag}} {{expected}}")
            try:
                mask_index = sys.argv.index("--mask")
            except ValueError:
                raise SystemExit("qsmxt OpenRecon must pass --mask")
            if sys.argv[mask_index + 1] != "magnitude,hd-bet,close:1,fill-holes:0":
                raise SystemExit("qsmxt OpenRecon must pass the default HD-BET section")
            bids = Path(sys.argv[2])
            output = Path(sys.argv[3])
            phase = sorted(bids.glob("sub-*/anat/*_echo-1_part-phase_MEGRE.nii.gz"))[0]
            img = nib.load(str(phase))
            data = np.zeros(img.shape, dtype=np.float32) + 1.5
            affine = img.affine.copy()
            affine[:3, 3] += np.asarray({affine_offset!r}, dtype=float)
            name = phase.name.replace("_echo-1_part-phase_MEGRE.nii.gz", "_Chimap.nii")
            dest = output / "derivatives" / "qsmxt" / "sub-01" / "anat" / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            nib.save(nib.Nifti1Image(data, affine), str(dest))
            """
        )
    )
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    return fake


def test_openrecon_defaults_select_rts_romeo_and_vsharp():
    assert qsmxt.QSMXT_DERIVATIVE_ROOT == Path("derivatives/qsmxt")

    settings = qsmxt._settings_from_config({}, FakeMetadata())

    assert settings["pipeline_preset"] == "custom"
    assert settings["qsm_algorithm"] == "rts"
    assert settings["unwrapping_algorithm"] == "romeo"
    assert settings["bf_algorithm"] == "vsharp"
    assert settings["mask_preset"] == "hd-bet"
    assert settings["masking_input"] == "magnitude"
    assert settings["bet_fractional_intensity"] == 0.5
    assert settings["mask_cleanup"] == "close-fill"
    assert settings["mask_dilate"] == 0
    assert settings["mask_close"] == 1
    assert settings["mask_fill_holes"] is True
    assert settings["mask_erode"] == 0
    assert settings["input_series"] == "distortion-corrected"


def test_openrecon_input_series_rejects_unknown_value():
    with pytest.raises(ValueError, match="unknown inputseries"):
        qsmxt._settings_from_config(
            {"parameters": {"inputseries": "arrival-order"}},
            FakeMetadata(),
        )


def test_openrecon_label_exposes_all_input_series_modes():
    label = qsmxt.json.loads(
        (Path(qsmxt.__file__).with_name("OpenReconLabel.json")).read_text()
    )
    parameters = {
        parameter["id"]: parameter for parameter in label["parameters"]
    }

    selector = parameters["inputseries"]
    assert selector["default"] == "distortion-corrected"
    assert [value["id"] for value in selector["values"]] == [
        "distortion-corrected",
        "non-distortion-corrected",
        "both",
    ]
    assert "voxelsizemm" not in parameters


def test_select_magnitude_phase_pairs_matches_requested_distortion_variant():
    groups = qsmxt._group_images_by_series(_dual_input_images())

    corrected = qsmxt._select_magnitude_phase_pairs(
        groups,
        "distortion-corrected",
    )
    non_corrected = qsmxt._select_magnitude_phase_pairs(
        groups,
        "non-distortion-corrected",
    )
    both = qsmxt._select_magnitude_phase_pairs(groups, "both")

    assert [pair["variant"] for pair in both] == [
        "distortion-corrected",
        "non-distortion-corrected",
    ]
    assert [int(group["key"][0]) for group in corrected[0]["groups"]] == [3, 4]
    assert [int(group["key"][0]) for group in non_corrected[0]["groups"]] == [1, 2]


def test_default_selects_nd_when_preferred_input_is_missing(caplog):
    images = [image for image in _dual_input_images() if image.image_series_index in (1, 2)]
    settings = qsmxt._settings_from_config({}, FakeMetadata())
    with caplog.at_level(logging.WARNING):
        pairs = qsmxt._select_magnitude_phase_pairs(
            qsmxt._group_images_by_series(images), settings["input_series"],
        )
    assert len(pairs) == 1
    assert pairs[0]["variant"] == "non-distortion-corrected"
    assert pairs[0]["short_label"] == "ND"
    assert [group["name"] for group in pairs[0]["groups"]] == ["gre_qsm_ND", "gre_qsm_ND"]
    assert "No magnitude image series found for distortion-corrected" in caplog.text
    assert "gre_qsm_ND" in caplog.text
    assert settings["input_series"] == "distortion-corrected"


@pytest.mark.parametrize("preferred_series", [3, 4])
def test_incomplete_preferred_pair_falls_back_without_mixing(preferred_series):
    images = [image for image in _dual_input_images()
              if image.image_series_index in (1, 2, preferred_series)]
    pair = qsmxt._select_magnitude_phase_pairs(
        qsmxt._group_images_by_series(images), "distortion-corrected",
    )[0]
    assert pair["short_label"] == "ND"
    assert [int(group["key"][0]) for group in pair["groups"]] == [1, 2]


def test_default_nd_fallback_writes_selected_magnitude(tmp_path):
    images = [image for image in _dual_input_images() if image.image_series_index in (1, 2)]
    settings = qsmxt._settings_from_config({}, FakeMetadata())
    result = qsmxt.write_bids_dataset(images, FakeMetadata(), tmp_path / "bids", settings)
    assert len(result["magnitude_paths"]) == 2
    np.testing.assert_allclose(nib.load(result["magnitude_paths"][0]).get_fdata(), 1001)
    assert settings["input_series"] == "distortion-corrected"


@pytest.mark.parametrize("selection,indices", [
    ("both", (1, 2)), ("both", (3, 4)),
    ("non-distortion-corrected", (3, 4)), ("distortion-corrected", (3,)),
])
def test_missing_pairs_remain_strict(selection, indices, caplog):
    images = [image for image in _dual_input_images() if image.image_series_index in indices]
    with pytest.raises(ValueError, match="No (magnitude|phase) image series"):
        qsmxt._select_magnitude_phase_pairs(qsmxt._group_images_by_series(images), selection)
    assert "falling back" not in caplog.text


def test_ambiguous_preferred_pair_does_not_fall_back(caplog):
    images = _dual_input_images()
    images.extend([
        _image(5, 1, "other_M", np.ones((2, 3, 4))),
        _image(6, 1, "other_P", np.ones((2, 3, 4)), image_type=ismrmrd.IMTYPE_PHASE),
    ])
    with pytest.raises(ValueError, match="Ambiguous distortion-corrected"):
        qsmxt._select_magnitude_phase_pairs(
            qsmxt._group_images_by_series(images), "distortion-corrected",
        )
    assert "falling back" not in caplog.text


def test_preferred_phase_inference_runs_before_nd_fallback():
    images = _dual_input_images()
    for image in images:
        if image.image_series_index == 4:
            header = image.getHead()
            header.image_type = ismrmrd.IMTYPE_MAGNITUDE
            image.setHead(header)
    pair = qsmxt._select_magnitude_phase_pairs(
        qsmxt._group_images_by_series(images), "distortion-corrected",
    )[0]
    assert pair["short_label"] == "DC"
    assert {int(group["key"][0]) for group in pair["groups"]} == {3, 4}


@pytest.mark.parametrize("processed_nd", [False, True])
def test_processed_products_cannot_supply_fallback_components(processed_nd):
    images = _dual_input_images()
    for image in images:
        if image.image_series_index == 4 or (processed_nd and image.image_series_index == 2):
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["SequenceDescription"] = "gre_Romeo_P" + (
                "_ND" if image.image_series_index == 2 else ""
            )
            image.attribute_string = meta.serialize()
    groups = qsmxt._group_images_by_series(images)
    if processed_nd:
        with pytest.raises(ValueError, match="No phase.*non-distortion-corrected"):
            qsmxt._select_magnitude_phase_pairs(groups, "distortion-corrected")
    else:
        assert qsmxt._select_magnitude_phase_pairs(groups, "distortion-corrected")[0]["short_label"] == "ND"


@pytest.mark.parametrize("failure", ["echo", "geometry"])
def test_invalid_preferred_pair_does_not_use_valid_nd(tmp_path, failure, caplog):
    images = _dual_input_images()
    if failure == "echo":
        images = [image for image in images
                  if not (image.image_series_index == 4 and image.image_index == 2)]
    else:
        for image in images:
            if image.image_series_index == 4:
                header = image.getHead()
                _set_header_vector(header, "position", [5.0, 0.0, 0.0])
                image.setHead(header)
    settings = qsmxt._settings_from_config({}, FakeMetadata())
    with pytest.raises(ValueError, match="[Ee]cho|[Gg]eometry|[Aa]ffine"):
        qsmxt.write_bids_dataset(images, FakeMetadata(), tmp_path / "bids", settings)
    assert "falling back" not in caplog.text


def test_native_failure_does_not_retry_nd(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    attempted = []

    def fail_native(bids_dir, output_dir, settings):
        attempted.append(bids_dir.parent.name)
        raise RuntimeError("native reconstruction failed")

    monkeypatch.setattr(qsmxt, "_run_qsmxt", fail_native)
    connection = FakeConnection(_dual_input_images())
    qsmxt.process(connection, {}, FakeMetadata())
    assert connection.closed
    assert attempted == ["dc"]
    assert len(connection.sent_batches) == 4
    assert _sent_series_descriptions(connection) == {
        "gre_qsm_original", "gre_qsm_ND_original",
    }
    assert "native reconstruction failed" in connection.logs[0][1]


def test_select_magnitude_phase_pairs_rejects_incomplete_both_selection():
    groups = qsmxt._group_images_by_series(_input_images())

    with pytest.raises(ValueError, match="non-distortion-corrected"):
        qsmxt._select_magnitude_phase_pairs(groups, "both")


def _clearqsm_input_images():
    images = []
    for series_index, suffix, image_type, echoes, base_value in (
        (1, "Romeo_Mask", ismrmrd.IMTYPE_MAGNITUDE, 1, 1),
        (2, "Aspire_P", ismrmrd.IMTYPE_PHASE, 5, 2000),
        (3, "Romeo_P", ismrmrd.IMTYPE_PHASE, 5, 3000),
        (4, "Romeo_B0", ismrmrd.IMTYPE_PHASE, 1, 2048),
        (5, "sensitivity_corrected_mag", ismrmrd.IMTYPE_MAGNITUDE, 1, 800),
        (6, "CLEAR-SWI_mIP", ismrmrd.IMTYPE_MAGNITUDE, 1, 500),
        (7, "CLEAR-SWI", ismrmrd.IMTYPE_MAGNITUDE, 1, 600),
        (8, "Aspire_M", ismrmrd.IMTYPE_MAGNITUDE, 5, 1000),
    ):
        for echo in range(1, echoes + 1):
            images.append(
                _image(
                    series_index,
                    echo,
                    f"anat-MEGRE_acq-clearQsm_07mm_{suffix}",
                    np.full((2, 3, 4), base_value + echo, dtype=np.float32),
                    image_type=image_type,
                    meta_values={
                        "EchoNumber": str(echo),
                        "ImageType": "DERIVED\\PRIMARY\\" + (
                            "P" if image_type == ismrmrd.IMTYPE_PHASE else "M"
                        ),
                    },
                )
            )
    return images


@pytest.mark.parametrize("reverse_arrival", [False, True])
def test_write_bids_dataset_selects_source_pair_among_clearqsm_products(
    tmp_path, reverse_arrival,
):
    images = _clearqsm_input_images()
    if reverse_arrival:
        images.reverse()
    settings = qsmxt._settings_from_config(
        {"parameters": {"echotimesms": "5,10,15,20,25"}},
        FakeMetadata(),
    )

    conversion = qsmxt.write_bids_dataset(
        images, FakeMetadata(), tmp_path / "bids", settings,
    )

    assert conversion["n_echoes"] == 5
    assert {qsmxt._source_series_name(image) for image in conversion["magnitude_images"]} == {
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_M",
    }
    assert {qsmxt._source_series_name(image) for image in conversion["phase_images"]} == {
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_P",
    }
    for echo, (magnitude_path, phase_path) in enumerate(
        zip(conversion["magnitude_paths"], conversion["phase_paths"]), start=1,
    ):
        magnitude = nib.load(str(magnitude_path))
        phase = nib.load(str(phase_path))
        assert magnitude.shape == phase.shape == (4, 3, 2)
        np.testing.assert_array_equal(magnitude.get_fdata(), 1000 + echo)
        np.testing.assert_array_equal(phase.get_fdata(), 2000 + echo)


def test_select_source_pair_rejects_multiple_complete_families():
    images = []
    for series_index, name, image_type in (
        (1, "gre_first_M", ismrmrd.IMTYPE_MAGNITUDE),
        (2, "gre_first_P", ismrmrd.IMTYPE_PHASE),
        (3, "gre_second_M", ismrmrd.IMTYPE_MAGNITUDE),
        (4, "gre_second_P", ismrmrd.IMTYPE_PHASE),
    ):
        images.append(
            _image(series_index, 1, name, np.ones((2, 3, 4)), image_type=image_type)
        )

    with pytest.raises(ValueError, match="Ambiguous distortion-corrected input"):
        qsmxt._select_magnitude_phase_pairs(
            qsmxt._group_images_by_series(images), "distortion-corrected",
        )


def test_select_source_pair_uses_unique_complete_family():
    images = [
        _image(1, 1, "gre_other_M", np.ones((2, 3, 4))),
        _image(2, 1, "gre_source_M", np.ones((2, 3, 4))),
        _image(3, 1, "gre_source_P", np.ones((2, 3, 4)), image_type=ismrmrd.IMTYPE_PHASE),
    ]

    pair = qsmxt._select_magnitude_phase_pairs(
        qsmxt._group_images_by_series(images), "distortion-corrected",
    )[0]

    assert [group["name"] for group in pair["groups"]] == [
        "gre_source_M", "gre_source_P",
    ]


def test_select_source_pair_rejects_duplicate_family_members():
    images = [
        _image(1, 1, "gre_M", np.ones((2, 3, 4))),
        _image(2, 1, "gre_M", np.ones((2, 3, 4))),
        _image(3, 1, "gre_P", np.ones((2, 3, 4)), image_type=ismrmrd.IMTYPE_PHASE),
    ]

    with pytest.raises(ValueError, match="Ambiguous distortion-corrected input"):
        qsmxt._select_magnitude_phase_pairs(
            qsmxt._group_images_by_series(images), "distortion-corrected",
        )


def test_select_source_pair_preserves_names_containing_product_like_tokens():
    images = [
        _image(1, 1, "gre_Romeo_Pilot_M", np.ones((2, 3, 4))),
        _image(2, 1, "gre_Romeo_Pilot_P", np.ones((2, 3, 4)), image_type=ismrmrd.IMTYPE_PHASE),
    ]

    pair = qsmxt._select_magnitude_phase_pairs(
        qsmxt._group_images_by_series(images), "distortion-corrected",
    )[0]

    assert [group["name"] for group in pair["groups"]] == [
        "gre_Romeo_Pilot_M", "gre_Romeo_Pilot_P",
    ]


def test_select_source_pair_handles_clearqsm_products_in_both_correction_variants():
    non_corrected = _clearqsm_input_images()
    for image in non_corrected:
        header = image.getHead()
        header.image_series_index += 10
        image.setHead(header)
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        meta["SequenceDescription"] += "_ND"
        image.attribute_string = meta.serialize()

    pairs = qsmxt._select_magnitude_phase_pairs(
        qsmxt._group_images_by_series(_clearqsm_input_images() + non_corrected), "both",
    )

    assert [pair["short_label"] for pair in pairs] == ["DC", "ND"]
    assert [group["name"] for group in pairs[0]["groups"]] == [
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_M",
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_P",
    ]
    assert [group["name"] for group in pairs[1]["groups"]] == [
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_M_ND",
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_P_ND",
    ]


def test_select_source_pair_rejects_processed_only_input():
    images = [
        _image(1, 1, "gre_Romeo_Mask", np.ones((2, 3, 4))),
        _image(2, 1, "gre_Romeo_P", np.ones((2, 3, 4)), image_type=ismrmrd.IMTYPE_PHASE),
    ]

    with pytest.raises(ValueError, match="No (magnitude|phase).*series"):
        qsmxt._select_magnitude_phase_pairs(
            qsmxt._group_images_by_series(images), "distortion-corrected",
        )


@pytest.mark.parametrize(
    "magnitude_echoes,phase_echoes", [([1, 2, 3], [1, 2]), ([1, 3], [1, 2])],
)
@pytest.mark.parametrize("max_echoes", [0, 1])
def test_write_bids_dataset_rejects_incompatible_source_echoes(
    tmp_path, magnitude_echoes, phase_echoes, max_echoes,
):
    images = []
    for series_index, name, image_type, echoes in (
        (1, "gre_M", ismrmrd.IMTYPE_MAGNITUDE, magnitude_echoes),
        (2, "gre_P", ismrmrd.IMTYPE_PHASE, phase_echoes),
    ):
        for index, echo in enumerate(echoes, start=1):
            images.append(
                _image(
                    series_index, index, name, np.full((2, 3, 4), 1000 + echo),
                    image_type=image_type, meta_values={"EchoNumber": str(echo)},
                )
            )
    settings = qsmxt._settings_from_config(
        {"parameters": {"maxechoes": str(max_echoes)}}, FakeMetadata(),
    )

    with pytest.raises(ValueError, match="[Ee]cho"):
        qsmxt.write_bids_dataset(images, FakeMetadata(), tmp_path / "bids", settings)


@pytest.mark.parametrize("explicit_pair", [False, True])
def test_write_bids_dataset_rejects_source_affine_mismatch(tmp_path, explicit_pair):
    images = _input_images()
    for image in images:
        if image.image_type == ismrmrd.IMTYPE_PHASE:
            header = image.getHead()
            _set_header_vector(header, "position", [5.0, 0.0, 0.0])
            image.setHead(header)
    settings = qsmxt._settings_from_config({}, FakeMetadata())
    pair = qsmxt._select_magnitude_phase_pairs(
        qsmxt._group_images_by_series(images), "distortion-corrected",
    )[0] if explicit_pair else None

    with pytest.raises(ValueError, match="[Gg]eometry|[Aa]ffine"):
        qsmxt.write_bids_dataset(
            images, FakeMetadata(), tmp_path / "bids", settings, input_pair=pair,
        )


@pytest.mark.parametrize("identity_namespace", ["EchoNumber", "contrast"])
def test_write_bids_dataset_aligns_source_echo_identities(tmp_path, identity_namespace):
    images = []
    for series_index, name, image_type, echoes, base_value in (
        (1, "gre_M", ismrmrd.IMTYPE_MAGNITUDE, [3, 1, 2], 1000),
        (2, "gre_P", ismrmrd.IMTYPE_PHASE, [2, 3, 1], 2000),
    ):
        for image_index, echo in enumerate(echoes, start=1):
            images.append(
                _image(
                    series_index, image_index, name,
                    np.full((2, 3, 4), base_value + echo), image_type=image_type,
                    meta_values={"EchoNumber": str(echo)} if identity_namespace == "EchoNumber" else {},
                    header_values={"contrast": echo - 1} if identity_namespace == "contrast" else {},
                )
            )
    settings = qsmxt._settings_from_config({}, FakeMetadata())

    conversion = qsmxt.write_bids_dataset(
        images, FakeMetadata(), tmp_path / "bids", settings,
    )

    assert conversion["n_echoes"] == 3
    for echo, (magnitude_path, phase_path) in enumerate(
        zip(conversion["magnitude_paths"], conversion["phase_paths"]), start=1,
    ):
        np.testing.assert_array_equal(nib.load(str(magnitude_path)).get_fdata(), 1000 + echo)
        np.testing.assert_array_equal(nib.load(str(phase_path)).get_fdata(), 2000 + echo)


@pytest.mark.parametrize("corrected_suffix", ["", "_DC"])
def test_process_clearqsm_products_returns_only_genuine_original_series(
    tmp_path, monkeypatch, corrected_suffix,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    images = []
    for suffix, series_offset in ((corrected_suffix, 0), ("_ND", 10)):
        for image in _clearqsm_input_images():
            header = image.getHead()
            header.image_series_index += series_offset
            image.setHead(header)
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["SequenceDescription"] += suffix
            image.attribute_string = meta.serialize()
            images.append(image)
    connection = FakeConnection(images)

    qsmxt.process(
        connection,
        {"parameters": {"qsmxtbinary": str(_fake_qsmxt_binary(tmp_path))}},
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert len(connection.sent_batches) == 5
    expected_names = {
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_M" + corrected_suffix,
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_P" + corrected_suffix,
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_M_ND",
        "anat-MEGRE_acq-clearQsm_07mm_Aspire_P_ND",
    }
    sources = {
        (meta["SequenceDescription"], meta["EchoNumber"]): image
        for image in images
        for meta in [ismrmrd.Meta.deserialize(image.attribute_string)]
        if meta["SequenceDescription"] in expected_names
    }
    originals = [image for batch in connection.sent_batches[:-1] for image in batch]
    assert len(originals) == 20
    returned_keys = set()
    for original in originals:
        meta = ismrmrd.Meta.deserialize(original.attribute_string)
        assert meta["SeriesDescription"].endswith("_original")
        key = (meta["SeriesDescription"].removesuffix("_original"), meta["EchoNumber"])
        returned_keys.add(key)
        source = sources[key]
        np.testing.assert_array_equal(original.data, source.data)
        for field in ("matrix_size", "field_of_view", "position", "read_dir", "phase_dir", "slice_dir"):
            np.testing.assert_array_equal(
                getattr(original.getHead(), field), getattr(source.getHead(), field),
            )
        assert original.getHead().contrast == source.getHead().contrast
    assert returned_keys == set(sources)
    assert len(connection.sent_batches[-1]) == 2
    for image in connection.sent_batches[-1]:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        assert meta["QSMxTOutput"] == "qsm"
        assert np.count_nonzero(image.data) == image.data.size


@pytest.mark.parametrize("companion_name", ["gre_Romeo_Mask", "gre_other_M"])
def test_process_rejects_invalid_original_metadata_only_for_genuine_sources(
    tmp_path, monkeypatch, companion_name,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    companion = _image(
        100, 1, companion_name, np.ones((2, 3, 4)),
        meta_values={"SliceNo": "2", "NumberOfSlices": "2"},
    )
    connection = FakeConnection(_input_images() + [companion])

    qsmxt.process(
        connection,
        {"parameters": {"qsmxtbinary": str(_fake_qsmxt_binary(tmp_path))}},
        FakeMetadata(),
    )

    assert connection.closed is True
    if companion_name == "gre_other_M":
        assert connection.sent_batches == []
        assert len(connection.logs) == 1
        assert "Invalid qsmxt original passthrough contract" in connection.logs[0][1]
        assert "SliceNo 2 outside [0..2)" in connection.logs[0][1]
        return

    assert connection.logs == []
    assert len(connection.sent_batches) == 3
    originals = [image for batch in connection.sent_batches[:-1] for image in batch]
    assert len(originals) == 4
    assert {
        ismrmrd.Meta.deserialize(image.attribute_string)["SeriesDescription"]
        for image in originals
    } == {"gre_qsm_original", "gre_qsm_Pha_original"}
    for original, source in zip(originals, connection.images[:4]):
        np.testing.assert_array_equal(original.data, source.data)
    assert {int(image.getHead().image_series_index) for image in originals}.isdisjoint(
        {1, 2, 100},
    )
    assert len(connection.sent_batches[-1]) == 2
    for image in connection.sent_batches[-1]:
        assert ismrmrd.Meta.deserialize(image.attribute_string)["QSMxTOutput"] == "qsm"
        assert np.count_nonzero(image.data) == image.data.size


def test_openrecon_pipeline_presets_select_all_three_algorithms():
    expected = {
        "romeo-resharp-rts": ("romeo", "resharp", "rts"),
        "romeo-ismv-hdqsm": ("romeo", "ismv", "hdqsm"),
        "romeo-resharp-tikhonov": ("romeo", "resharp", "tikhonov"),
        "romeo-resharp-tv": ("romeo", "resharp", "tv"),
        "romeo-resharp-hdqsm": ("romeo", "resharp", "hdqsm"),
        "romeo-ismv-rts": ("romeo", "ismv", "rts"),
        "romeo-ismv-whqsm": ("romeo", "ismv", "whqsm"),
        "romeo-sharp-whqsm": ("romeo", "sharp", "whqsm"),
        "romeo-resharp-whqsm": ("romeo", "resharp", "whqsm"),
        "romeo-sharp-tikhonov": ("romeo", "sharp", "tikhonov"),
        "qsmart": ("romeo", None, "qsmart"),
        "tgv": ("romeo", None, "tgv"),
        "autoqsm": ("romeo", None, "autoqsm"),
        "nextqsm": ("romeo", None, "nextqsm"),
        "iqsm": (None, None, "iqsm"),
        "iqsm-plus": (None, None, "iqsm-plus"),
    }

    assert set(qsmxt.ALGORITHM_PIPELINE_PRESETS) == set(expected)
    for preset_id, algorithms in expected.items():
        settings = qsmxt._settings_from_config(
            {
                "parameters": {
                    "pipelinepreset": preset_id,
                    "qsmalgorithm": "tkd",
                    "unwrappingalgorithm": "laplacian",
                    "bfalgorithm": "pdf",
                }
            },
            FakeMetadata(),
        )

        assert settings["pipeline_preset"] == preset_id
        assert (
            settings["unwrapping_algorithm"],
            settings["bf_algorithm"],
            settings["qsm_algorithm"],
        ) == algorithms


@pytest.mark.parametrize("preset", qsmxt.ALGORITHM_PIPELINE_PRESETS)
def test_openrecon_pipeline_preset_reaches_qsmxt_command(preset, tmp_path, monkeypatch):
    settings = qsmxt._settings_from_config(
        {"parameters": {"pipelinepreset": preset}},
        FakeMetadata(),
    )
    settings["qsmxt_binary"] = "/opt/qsmxt/qsmxt"
    bids_dir = tmp_path / "bids"
    bids_dir.mkdir()
    commands = []

    def fake_run(command, **kwargs):
        commands.append((command, kwargs))
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", fake_run)

    qsmxt._run_qsmxt(bids_dir, tmp_path / "output", settings)

    command, kwargs = commands[0]
    for setting, flag in (
        ("unwrapping_algorithm", "--unwrapping-algorithm"),
        ("bf_algorithm", "--bf-algorithm"),
        ("qsm_algorithm", "--qsm-algorithm"),
    ):
        expected = qsmxt.ALGORITHM_PIPELINE_PRESETS[preset][setting]
        if expected is None:
            assert flag not in command
        else:
            assert command[command.index(flag) + 1] == expected
    assert command[command.index("--mask") + 1] == (
        "magnitude,hd-bet,close:1,fill-holes:0"
    )
    assert kwargs["cwd"] == str(bids_dir)


def test_openrecon_custom_threshold_mask_reaches_qsmxt_command(
    tmp_path,
    monkeypatch,
):
    settings = qsmxt._settings_from_config(
        {
            "parameters": {
                "maskpreset": "robust-threshold",
                "maskinginput": "phase-quality",
                "maskthresholdmethod": "percentile",
                "maskthresholdpercentile": "65",
                "maskdilate": "2",
                "maskclose": "3",
                "maskfillholes": "true",
                "maskmaxholesize": "12000",
                "maskerode": "1",
            }
        },
        FakeMetadata(),
    )
    settings["qsmxt_binary"] = "/opt/qsmxt/qsmxt"
    bids_dir = tmp_path / "bids"
    bids_dir.mkdir()
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", fake_run)

    qsmxt._run_qsmxt(bids_dir, tmp_path / "output", settings)

    command = commands[0]
    assert command[command.index("--mask") + 1] == (
        "phase-quality,threshold:percentile:65,dilate:2,close:3,"
        "fill-holes:12000,erode:1"
    )
    assert "--mask-preset" not in command


def test_openrecon_combined_mask_passes_two_sections(tmp_path, monkeypatch):
    settings = qsmxt._settings_from_config(
        {"parameters": {"maskpreset": "combined"}},
        FakeMetadata(),
    )
    settings["qsmxt_binary"] = "/opt/qsmxt/qsmxt"
    bids_dir = tmp_path / "bids"
    bids_dir.mkdir()
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", fake_run)

    qsmxt._run_qsmxt(bids_dir, tmp_path / "output", settings)

    command = commands[0]
    sections = [
        command[index + 1]
        for index, value in enumerate(command)
        if value == "--mask"
    ]
    assert sections == [
        "magnitude,bet:0.5,close:1,fill-holes:0",
        "magnitude,threshold:otsu,close:1,fill-holes:0",
    ]


def test_openrecon_processing_defaults_can_be_overridden():
    settings = qsmxt._settings_from_config(
        {
            "parameters": {
                "qsmalgorithm": "rts",
                "unwrappingalgorithm": "laplacian",
                "bfalgorithm": "pdf",
            }
        },
        FakeMetadata(),
    )

    assert settings["qsm_algorithm"] == "rts"
    assert settings["unwrapping_algorithm"] == "laplacian"
    assert settings["bf_algorithm"] == "pdf"


def test_openrecon_label_exposes_processing_defaults():
    label_path = Path(qsmxt.__file__).with_name("OpenReconLabel.json")
    label = qsmxt.json.loads(label_path.read_text())
    parameters = {
        parameter["id"]: parameter for parameter in label["parameters"]
    }

    assert parameters["pipelinepreset"]["default"] == "custom"
    assert [value["id"] for value in parameters["pipelinepreset"]["values"]] == [
        "custom", *qsmxt.ALGORITHM_PIPELINE_PRESETS,
    ]
    assert parameters["twopass"]["default"] is False
    assert "motioncorrection" not in parameters
    assert parameters["qsmreference"]["default"] == "mean"
    assert parameters["vsharpgrid"]["default"] == "native"
    assert parameters["regionalqsmtable"]["default"] is False
    assert parameters["regionalqsmtable"]["type"] == "boolean"
    assert "swiunwrapping" not in parameters
    assert parameters["qsmalgorithm"]["default"] == "rts"
    qsm_algorithms = {
        value["id"] for value in parameters["qsmalgorithm"]["values"]
    }
    assert {"hdqsm", "whqsm"} <= qsm_algorithms
    assert parameters["unwrappingalgorithm"]["default"] == "romeo"
    assert parameters["bfalgorithm"]["default"] == "vsharp"
    bf_algorithms = {
        value["id"] for value in parameters["bfalgorithm"]["values"]
    }
    assert "resharp" in bf_algorithms
    assert parameters["maskpreset"]["default"] == qsmxt.DEFAULT_MASK_PRESET == "hd-bet"
    assert tuple(value["id"] for value in parameters["maskpreset"]["values"]) == qsmxt.MASK_PRESETS
    assert "maskinginput" not in parameters
    assert "betfractionalintensity" not in parameters
    assert list(parameters) == [
        "config",
        "inputseries",
        "sendoutputs",
        "inhomogeneitycorrection",
        "maskpreset",
        "pipelinepreset",
        "unwrappingalgorithm",
        "bfalgorithm",
        "qsmalgorithm",
        "vsharpgrid",
        "twopass",
        "qsmreference",
        "sourceseparation",
        "regionalqsmtable",
    ]
    assert parameters["qsmalgorithm"]["label"]["en"] == "Dipole inversion"
    assert len(label["parameters"]) == len(parameters) == 14
    assert "maskcleanup" not in parameters
    correction = parameters["inhomogeneitycorrection"]
    assert correction["type"] == "choice"
    assert correction["label"]["en"] == "Homogeneity correction"
    assert correction["default"] == qsmxt.DEFAULT_HOMOGENEITY_PROFILE == "standard"
    assert {value["id"] for value in correction["values"]} == set(qsmxt.HOMOGENEITY_PROFILES)
    assert [value["name"]["en"] for value in correction["values"]] == ["Off", "Standard", "Fine"]
    assert "sendoriginal" not in parameters
    assert "maskthresholdpercentile" not in parameters
    assert "voxelsizemm" not in parameters


def test_scanner_display_volume_scales_qsm_ppm_range_to_uint12_interval():
    data = np.asarray([[[-0.01, 0.0, 0.01]]], dtype=np.float32)

    display, meta = qsmxt._scanner_display_volume(data, "qsm", "ppm")

    assert display.dtype == np.uint16
    assert display.tolist() == [[[1048, 2048, 3048]]]
    assert meta["scale"] == 100000.0
    assert meta["offset"] == 2048.0
    assert meta["formula"] == "ppm = (display - 2048) / 100000"
    assert meta["rescale_slope"] == 0.00001
    assert meta["rescale_intercept"] == -0.02048
    assert meta["padding_value"] == 0
    assert meta["clipped_voxels"] == 0


def test_scanner_display_volume_reserves_stored_zero_for_qsm_padding():
    data = np.asarray([[[-0.02048, 0.0, 0.01]]], dtype=np.float32)

    display, meta = qsmxt._scanner_display_volume(data, "qsm", "ppm")

    assert 0 not in display
    decoded = display.astype(np.float64) * meta["rescale_slope"]
    decoded += meta["rescale_intercept"]
    assert np.allclose(decoded, data, atol=meta["rescale_slope"] / 2)
    assert meta["padding_value"] == 0


def test_scanner_display_volume_uses_robust_qsm_window_without_clipping_extrema():
    data = np.concatenate(
        (
            np.full(500, -0.05, dtype=np.float32),
            np.full(500, 0.05, dtype=np.float32),
            np.asarray([-0.4, 0.5], dtype=np.float32),
        )
    ).reshape(1, 1, -1)

    display, meta = qsmxt._scanner_display_volume(data, "qsm", "ppm")

    assert display.min() == 1648
    assert display.max() == 2548
    assert meta["scale_input_min"] == pytest.approx(-0.4)
    assert meta["scale_input_max"] == pytest.approx(0.5)
    assert meta["window_input_min"] == pytest.approx(-0.05)
    assert meta["window_input_max"] == pytest.approx(0.05)


def test_output_meta_publishes_qsm_window_in_integer_ppb_domain():
    source = _image(
        1,
        1,
        "gre_qsm",
        np.ones((1, 1, 3), dtype=np.float32),
    )
    physical = np.concatenate(
        (
            np.full(500, -0.05, dtype=np.float32),
            np.full(500, 0.05, dtype=np.float32),
            np.asarray([-0.4, 0.5], dtype=np.float32),
        )
    ).reshape(1, 1, -1)
    display, display_meta = qsmxt._scanner_display_volume(
        physical,
        "qsm",
        "ppm",
    )

    meta = qsmxt._output_meta(
        source,
        source.getHead(),
        qsmxt.OUTPUT_SERIES_START,
        "QSMxT QSM",
        "OR_QSM_CHIMAP",
        "qsm",
        "ppm",
        physical,
        Path("/tmp/qsm.nii.gz"),
        0,
        1,
        display_meta,
        series_uid=qsmxt.new_uid(),
    )

    assert meta["RescaleType"] == "US"
    assert float(meta["RescaleSlope"]) == 1.0
    assert float(meta["RescaleIntercept"]) == -2048.0
    assert float(meta["WindowCenter"]) == 0.0
    assert float(meta["WindowWidth"]) == 100.0
    assert meta["QSMxTWindowDomain"] == "ppb"
    decoded_ppb = (
        display.astype(np.float64) * float(meta["RescaleSlope"])
        + float(meta["RescaleIntercept"])
    )
    np.testing.assert_allclose(decoded_ppb, physical * 1000.0, atol=0.5)


def test_output_meta_replaces_source_scaling_with_qsm_dicom_contract():
    source = _image(
        1,
        1,
        "gre_qsm",
        np.ones((1, 1, 3), dtype=np.float32),
        meta_values={
            "RescaleSlope": "2",
            "RescaleIntercept": "-4096",
            "RescaleType": "PHASE",
            "PixelPaddingValue": "4095",
            "PixelPaddingRangeLimit": "4095",
            "WindowCenter": "2048",
            "WindowWidth": "4096",
            "VOILUTFunction": "LINEAR",
        },
    )
    physical = np.asarray([[[-0.01, 0.0, 0.01]]], dtype=np.float32)
    display, display_meta = qsmxt._scanner_display_volume(physical, "qsm", "ppm")

    meta = qsmxt._output_meta(
        source,
        source.getHead(),
        qsmxt.OUTPUT_SERIES_START,
        "QSMxT QSM",
        "OR_QSM_CHIMAP",
        "qsm",
        "ppm",
        physical,
        Path("/tmp/qsm.nii.gz"),
        0,
        1,
        display_meta,
        series_uid=qsmxt.new_uid(),
    )

    assert meta["DataRole"] == ["Image", "Quantitative"]
    assert float(meta["RescaleSlope"]) == 0.01
    assert float(meta["RescaleIntercept"]) == -20.48
    assert meta["RescaleType"] == "US"
    assert meta["PixelPaddingValue"] == "0"
    assert meta["PixelPaddingRangeLimit"] == "0"
    assert float(meta["WindowCenter"]) == 0.0
    assert float(meta["WindowWidth"]) == 20.0
    assert "VOILUTFunction" not in meta
    assert float(meta["QSMxTPhysicalWindowCenter"]) == 0.0
    assert float(meta["QSMxTPhysicalWindowWidth"]) == pytest.approx(0.02)
    assert meta["QSMxTWindowDomain"] == "ppb"
    assert 0 not in display

    decoded_ppb = (
        display.astype(np.float64) * float(meta["RescaleSlope"])
        + float(meta["RescaleIntercept"])
    )
    center = float(meta["WindowCenter"])
    width = float(meta["WindowWidth"])
    rendered = np.clip(
        (decoded_ppb - (center - width / 2.0)) / width,
        0.0,
        1.0,
    )
    assert np.unique(rendered).size == 3


def test_scanner_display_volume_maps_mask_foreground_to_valid_uint12_maximum():
    data = np.asarray([[[0.0, 1.0]]], dtype=np.float32)

    display, meta = qsmxt._scanner_display_volume(data, "mask", "binary")

    assert display.dtype == np.uint16
    assert display.tolist() == [[[0, 4095]]]
    assert meta["scale"] == 4095.0
    assert meta["formula"] == "binary = display / 4095"
    assert meta["display_max"] == 4095


def test_scanner_display_volume_ignores_sparse_t2star_fit_outlier_for_scaling():
    data = np.full((10, 10, 10), 0.05, dtype=np.float32)
    data[0, 0, 0] = 10000.0

    display, meta = qsmxt._scanner_display_volume(data, "t2star", "s")

    assert display.dtype == np.uint16
    assert np.all(display.reshape(-1)[1:] == 500)
    assert display[0, 0, 0] == 4095
    assert meta["scale"] == 10000.0
    assert meta["clipped_voxels"] == 1
    assert meta["input_max"] == 10000.0


def test_output_meta_uses_robust_t2star_range_for_window():
    source = _image(
        1,
        1,
        "gre_qsm",
        np.ones((1, 10, 10), dtype=np.float32),
    )
    physical = np.full((10, 10, 10), 0.05, dtype=np.float32)
    physical[0, 0, 0] = 10000.0
    _, display_meta = qsmxt._scanner_display_volume(physical, "t2star", "s")

    meta = qsmxt._output_meta(
        source,
        source.getHead(),
        qsmxt.OUTPUT_SERIES_START,
        "QSMxT T2star",
        "OR_QSM_T2STAR",
        "t2star",
        "s",
        physical,
        Path("/tmp/t2star.nii.gz"),
        0,
        10,
        display_meta,
        series_uid=qsmxt.new_uid(),
    )

    assert float(meta["WindowCenter"]) == 25.0
    assert float(meta["WindowWidth"]) == 50.0
    assert float(meta["RescaleSlope"]) == pytest.approx(0.1)
    assert float(meta["RescaleIntercept"]) == 0.0
    assert meta["QSMxTWindowDomain"] == "ms"
    assert meta["QSMxTUnits"] == "s"
    assert "DICOM values in ms" in meta["ImageComments"]


def test_t2star_long_fit_tail_preserves_millisecond_precision_and_tissue_window():
    tissue = np.linspace(0.020, 0.080, 9800, dtype=np.float32)
    data = np.concatenate([tissue, np.full(200, 12.0)]).reshape(10, 10, 100)
    original = data.copy()
    display, display_meta = qsmxt._scanner_display_volume(data, "t2star", "s")
    source = _image(1, 1, "gre_qsm", np.ones((1, 10, 100), dtype=np.float32))
    meta = qsmxt._output_meta(
        source, source.getHead(), qsmxt.OUTPUT_SERIES_START,
        "QSMxT T2star", "OR_QSM_T2STAR", "t2star", "s", data,
        Path("/tmp/t2star.nii.gz"), 0, 10, display_meta,
        series_uid=qsmxt.new_uid(),
    )

    decoded_ms = display.reshape(-1) * float(meta["RescaleSlope"])
    decoded_ms += float(meta["RescaleIntercept"])
    assert display_meta["rescale_slope"] <= 0.001
    assert np.max(np.abs(decoded_ms[:9800] - tissue * 1000)) <= 0.501
    assert 50 <= float(meta["WindowWidth"]) <= 80
    assert float(meta["WindowCenter"]) == round(float(meta["WindowWidth"]) / 2)
    assert display_meta["clipped_voxels"] == 200
    assert np.all(display.reshape(-1)[9800:] == 4095)
    assert np.array_equal(data, original)


@pytest.mark.parametrize("values", [[0.0], [np.nan, np.inf, -np.inf], [-1.0, 12.0]])
def test_t2star_empty_or_small_maps_have_valid_window_and_scaling(values):
    data = np.asarray(values, dtype=np.float32).reshape(1, 1, -1)
    display, display_meta = qsmxt._scanner_display_volume(data, "t2star", "s")
    source = _image(1, 1, "gre_qsm", np.ones_like(data))
    meta = qsmxt._output_meta(
        source, source.getHead(), qsmxt.OUTPUT_SERIES_START,
        "QSMxT T2star", "OR_QSM_T2STAR", "t2star", "s", data,
        Path("/tmp/t2star.nii.gz"), 0, 1, display_meta,
        series_uid=qsmxt.new_uid(),
    )
    assert 0 < float(meta["RescaleSlope"]) <= 1
    assert float(meta["WindowWidth"]) >= 1
    assert meta["QSMxTWindowDomain"] == "ms"
    assert np.all(display[(~np.isfinite(data)) | (data <= 0)] == 0)


def test_find_qsmxt_outputs_accepts_v9_combined_magnitude_name(tmp_path):
    anat_dir = tmp_path / "output" / "derivatives" / "qsmxt" / "sub-01" / "anat"
    anat_dir.mkdir(parents=True)
    combined_magnitude = anat_dir / "sub-01_acq-greqsm_CombinedMagnitude.nii"
    nib.save(
        nib.Nifti1Image(np.ones((2, 2, 2), dtype=np.float32), np.eye(4)),
        combined_magnitude,
    )
    settings = qsmxt._settings_from_config(
        {"parameters": {"sendoutputs": "magnitude"}},
        FakeMetadata(),
    )

    specs = qsmxt._find_qsmxt_outputs(
        tmp_path / "output",
        {"subject": "01"},
        settings,
    )

    assert specs[0][2] == combined_magnitude


@pytest.mark.parametrize("diagnostics", ["", "full"])
def test_write_bids_dataset_logs_optional_source_and_written_volume_statistics(
    tmp_path,
    caplog,
    monkeypatch,
    diagnostics,
):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("QSMXT_OPENRECON_DIAGNOSTICS", diagnostics)
    settings = qsmxt._settings_from_config(
        {"parameters": {"echotimesms": "10,20"}},
        FakeMetadata(),
    )

    qsmxt.write_bids_dataset(
        _input_images(),
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    full = diagnostics == "full"
    assert ("QSMxT diagnostic source series" in caplog.text) == full
    assert "QSMxT diagnostic selected input pair" in caplog.text
    assert ("QSMxT diagnostic BIDS magnitude echo=1" in caplog.text) == full
    assert ("QSMxT diagnostic BIDS phase echo=1" in caplog.text) == full
    assert ("sha256=" in caplog.text) == full


def test_write_bids_dataset_pairs_magnitude_and_phase(tmp_path):
    settings = qsmxt._settings_from_config(
        {"parameters": {"echotimesms": "10,20"}},
        FakeMetadata(),
    )

    result = qsmxt.write_bids_dataset(
        _input_images(),
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    assert result["n_echoes"] == 2
    assert result["magnitude_paths"][0].name == (
        "sub-01_acq-greqsm_echo-1_part-mag_MEGRE.nii.gz"
    )
    assert result["phase_paths"][1].name == (
        "sub-01_acq-greqsm_echo-2_part-phase_MEGRE.nii.gz"
    )

    magnitude = nib.load(str(result["magnitude_paths"][0]))
    np.testing.assert_allclose(magnitude.header.get_zooms()[:3], [1.0, 1.0, 1.0])
    np.testing.assert_allclose(magnitude.affine[:3, 3], [-1.5, -1.0, -0.5])

    phase_sidecar = qsmxt.json.loads(qsmxt._nifti_sidecar_path(result["phase_paths"][0]).read_text())
    assert phase_sidecar["EchoTime"] == 0.01
    assert phase_sidecar["MagneticFieldStrength"] == 7.0
    assert phase_sidecar["ImageType"] == ["ORIGINAL", "PRIMARY", "P"]


def test_write_bids_dataset_derives_b0_dir_from_nifti_affine(tmp_path):
    images = _input_images()
    for image in images:
        _set_image_orientation(
            image,
            read_dir=[0.0, 1.0, 0.0],
            phase_dir=[0.0, 0.0, 1.0],
            slice_dir=[1.0, 0.0, 0.0],
        )
    settings = qsmxt._settings_from_config({}, FakeMetadata())

    result = qsmxt.write_bids_dataset(
        images,
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    sidecar = qsmxt.json.loads(
        qsmxt._nifti_sidecar_path(result["phase_paths"][0]).read_text()
    )
    np.testing.assert_allclose(sidecar["B0_dir"], [0.0, 1.0, 0.0])
    np.testing.assert_allclose(result["b0_dir"], [0.0, 1.0, 0.0])
    assert result["b0_dir_source"] == "nifti_affine"


def test_write_bids_dataset_allows_hidden_b0_dir_override(tmp_path):
    images = _input_images()
    for image in images:
        _set_image_orientation(
            image,
            read_dir=[0.0, 1.0, 0.0],
            phase_dir=[0.0, 0.0, 1.0],
            slice_dir=[1.0, 0.0, 0.0],
        )
    settings = qsmxt._settings_from_config(
        {"parameters": {"b0dir": "1,0,0"}},
        FakeMetadata(),
    )

    result = qsmxt.write_bids_dataset(
        images,
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    sidecar = qsmxt.json.loads(
        qsmxt._nifti_sidecar_path(result["phase_paths"][0]).read_text()
    )
    assert sidecar["B0_dir"] == [1.0, 0.0, 0.0]
    assert result["b0_dir"] == (1.0, 0.0, 0.0)
    assert result["b0_dir_source"] == "config"


def test_write_bids_dataset_preserves_scanner_packed_volume_slice_spacing(tmp_path):
    settings = qsmxt._settings_from_config({}, FakeMetadata())

    result = qsmxt.write_bids_dataset(
        _packed_scanner_images(),
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    magnitude = nib.load(str(result["magnitude_paths"][0]))
    assert magnitude.shape == (4, 3, 144)
    np.testing.assert_allclose(magnitude.header.get_zooms()[:3], [1.0, 1.0, 1.0])
    np.testing.assert_allclose(magnitude.affine[:3, 2], [0.0, 0.0, 1.0])
    np.testing.assert_allclose(magnitude.affine[:3, 3], [8.5, 19.0, 30.0])


def test_write_bids_dataset_stacks_single_slice_echo_groups_from_metadata(tmp_path):
    images = []
    for slice_index in range(3):
        images.append(
            _image(
                1,
                slice_index + 1,
                "gre_qsm_Mag",
                np.full((1, 3, 4), 1000 + slice_index, dtype=np.float32),
                meta_values={
                    "EchoNumber": "1",
                    "Actual3DImagePartNumber": str(slice_index),
                    "partition_count": "3",
                },
            )
        )
        images.append(
            _image(
                2,
                slice_index + 1,
                "gre_qsm_Pha",
                np.full((1, 3, 4), 2000 + slice_index, dtype=np.float32),
                image_type=getattr(ismrmrd, "IMTYPE_PHASE", 2),
                meta_values={
                    "EchoNumber": "1",
                    "Actual3DImagePartNumber": str(slice_index),
                    "partition_count": "3",
                },
            )
        )

    settings = qsmxt._settings_from_config({}, FakeMetadata())
    result = qsmxt.write_bids_dataset(
        images,
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    assert result["n_echoes"] == 1
    mag = nib.load(str(result["magnitude_paths"][0]))
    phase = nib.load(str(result["phase_paths"][0]))
    assert mag.shape == (4, 3, 3)
    assert phase.shape == (4, 3, 3)

    sidecar = qsmxt.json.loads(
        qsmxt._nifti_sidecar_path(result["phase_paths"][0]).read_text()
    )
    assert sidecar["EchoTime"] == 0.02
    assert sidecar["MagneticFieldStrength"] == 7.0
    assert sidecar["B0_dir"] == [0.0, 0.0, 1.0]


def test_images_to_nifti_volume_rejects_implausibly_small_position_spacing():
    images = []
    for slice_index in range(4):
        image = _image(
            1,
            slice_index + 1,
            "gre_qsm_Mag",
            np.full((1, 3, 4), 1000 + slice_index, dtype=np.float32),
            meta_values={"Actual3DImagePartNumber": str(slice_index)},
            header_values={"slice": slice_index},
        )
        header = image.getHead()
        _set_header_vector(header, "field_of_view", [4.0, 3.0, 1.0])
        _set_header_vector(header, "slice_dir", [0.0, 0.0, -1.0])
        _set_header_vector(
            header,
            "position",
            [0.0, 0.0, -float(slice_index) / 3.0],
        )
        image.setHead(header)
        images.append(image)

    volume, affine = qsmxt._images_to_nifti_volume(images, kind="magnitude")

    assert volume.shape == (4, 3, 4)
    np.testing.assert_allclose(affine[:3, 2], [0.0, 0.0, -1.0])


def test_write_bids_dataset_groups_scanner_slices_by_echo_from_minihead(tmp_path):
    settings = qsmxt._settings_from_config({}, FakeMetadata())

    result = qsmxt.write_bids_dataset(
        _scanner_echo_slice_images(),
        FakeMetadata(),
        tmp_path / "bids",
        settings,
    )

    assert result["n_echoes"] == 5
    assert len(result["magnitude_images"]) == 100
    assert len(result["phase_images"]) == 100
    assert len(result["magnitude_paths"]) == 5
    assert len(result["phase_paths"]) == 5

    first_mag = nib.load(str(result["magnitude_paths"][0]))
    last_phase = nib.load(str(result["phase_paths"][-1]))
    assert first_mag.shape == (4, 3, 20)
    assert last_phase.shape == (4, 3, 20)

    first_sidecar = qsmxt.json.loads(
        qsmxt._nifti_sidecar_path(result["phase_paths"][0]).read_text()
    )
    last_sidecar = qsmxt.json.loads(
        qsmxt._nifti_sidecar_path(result["phase_paths"][-1]).read_text()
    )
    assert first_sidecar["EchoTime"] == 0.02
    assert last_sidecar["EchoTime"] == 0.04


def test_process_runs_qsmxt_and_sends_derived_mrd_image(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    fake_qsmxt = _fake_qsmxt_binary(tmp_path)
    connection = FakeConnection(_input_images())

    qsmxt.process(
        connection,
        {
            "parameters": {
                "qsmxtbinary": str(fake_qsmxt),
                "echotimesms": "10,20",
                "sendoriginal": "false",
            }
        },
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert len(connection.sent_batches) == 3
    assert len(connection.sent_batches[-1]) == 2

    for original, source in zip(
        [image for batch in connection.sent_batches[:2] for image in batch],
        connection.images,
    ):
        np.testing.assert_array_equal(original.data, source.data)
    output = connection.sent_batches[-1][0]
    header = output.getHead()
    meta = ismrmrd.Meta.deserialize(output.attribute_string)

    assert int(header.image_series_index) == qsmxt.OUTPUT_SERIES_START
    assert int(header.image_index) == 1
    assert int(header.slice) == 0
    assert output.data.shape == (1, 1, 3, 4)
    assert output.data.dtype == np.uint16
    assert np.all(output.data == 3548)
    assert meta["QSMxTOutput"] == "qsm"
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\OR_QSM_CHIMAP"
    assert meta["QSMxTDisplayScale"] == "1000"
    assert meta["QSMxTDisplayOffset"] == "2048"
    assert meta["QSMxTDisplayFormula"] == "ppm = (display - 2048) / 1000"
    assert meta["DataRole"] == ["Image", "Quantitative"]
    assert float(meta["RescaleSlope"]) == 1.0
    assert float(meta["RescaleIntercept"]) == -2048.0
    assert meta["RescaleType"] == "US"
    assert meta["PixelPaddingValue"] == "0"
    assert meta["PixelPaddingRangeLimit"] == "0"
    assert float(meta["WindowCenter"]) == 1500.0
    assert float(meta["WindowWidth"]) == 1000.0
    assert "VOILUTFunction" not in meta
    assert float(meta["QSMxTPhysicalWindowCenter"]) == 1.5
    assert float(meta["QSMxTPhysicalWindowWidth"]) == 1.0
    assert meta["QSMxTWindowDomain"] == "ppb"
    assert meta["QSMxTDisplayScaleInputMin"] == "1.5"
    assert meta["QSMxTDisplayScaleInputMax"] == "1.5"
    assert meta["QSMxTDisplayMin"] == "3548"
    assert meta["QSMxTDisplayMax"] == "3548"
    assert meta["QSMxTDisplayClippedVoxels"] == "0"
    assert meta["ImageComment"].startswith(
        "QSMxT QSM DC; scanner display uint16 0-4095; "
        "ppm = (display - 2048) / 1000; "
        "stored 0 is DICOM pixel padding"
    )
    assert meta["ImageComments"] == meta["ImageComment"]
    assert meta["OpenReconSoftware"] == "QSMxT"
    assert json.loads(meta["OpenReconParameters"])["n_echoes"] == 2
    assert "product=OR_QSM_CHIMAP" in meta["DerivationDescription"]
    assert meta["slice_count"] == "2"
    assert meta["NumberOfSlices"] == "2"
    assert meta["ImagesInAcquisition"] == "2"
    assert meta["NumberInSeries"] == "1"
    assert meta["SliceNo"] == "0"
    assert meta["IsmrmrdSliceNo"] == "0"
    assert "ImageTypeValue3" not in meta

    second_output = connection.sent_batches[-1][1]
    second_header = second_output.getHead()
    second_meta = ismrmrd.Meta.deserialize(second_output.attribute_string)
    assert int(second_header.image_series_index) == qsmxt.OUTPUT_SERIES_START
    assert int(second_header.image_index) == 2
    assert int(second_header.slice) == 1
    assert second_output.data.shape == (1, 1, 3, 4)
    assert second_meta["NumberInSeries"] == "2"
    assert second_meta["SliceNo"] == "1"
    assert second_meta["IsmrmrdSliceNo"] == "1"
    assert second_meta["SeriesInstanceUID"] == meta["SeriesInstanceUID"]
    assert second_meta["SOPInstanceUID"] != meta["SOPInstanceUID"]


def test_processing_runs_have_distinct_uids_and_report_their_versions(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    binary = _fake_qsmxt_binary(tmp_path)
    outputs = []
    for version in ("10.0.0", "10.0.1"):
        monkeypatch.setenv("OPENRECON_SOFTWARE_VERSION", version)
        connection = FakeConnection(_input_images())
        qsmxt.process(connection, {"parameters": {
            "qsmxtbinary": str(binary), "echotimesms": "10,20",
        }}, FakeMetadata())
        assert connection.closed and not connection.logs
        metas = [ismrmrd.Meta.deserialize(image.attribute_string)
                 for image in connection.sent_batches[-1]]
        assert {meta["OpenReconSoftwareVersion"] for meta in metas} == {version}
        assert len({meta["SeriesInstanceUID"] for meta in metas}) == 1
        assert len({meta["SOPInstanceUID"] for meta in metas}) == len(metas)
        outputs.append(metas)
    for key in ("SeriesInstanceUID", "SOPInstanceUID"):
        assert {meta[key] for meta in outputs[0]}.isdisjoint(
            meta[key] for meta in outputs[1]
        )


def test_all_qsm_products_fit_the_scanner_image_type_component():
    tokens = [spec["token"] for spec in qsmxt.QSMXT_OUTPUTS.values()]
    assert len(tokens) == len(set(tokens))
    assert all(qsmxt.re.fullmatch(r"OR_[A-Z0-9_]{1,13}", token) for token in tokens)


def test_process_both_input_series_runs_separately_and_returns_unique_outputs(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    fake_qsmxt = _fake_qsmxt_binary(tmp_path)
    connection = FakeConnection(_dual_input_images())

    qsmxt.process(
        connection,
        {
            "parameters": {
                "qsmxtbinary": str(fake_qsmxt),
                "echotimesms": "10,20",
                "inputseries": "both",
            }
        },
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert len(connection.sent_batches) == 6
    assert [len(batch) for batch in connection.sent_batches] == [2, 2, 2, 2, 2, 2]

    first_meta = [
        ismrmrd.Meta.deserialize(batch[0].attribute_string)
        for batch in connection.sent_batches[-2:]
    ]
    assert [meta["SeriesDescription"] for meta in first_meta] == [
        "QSMxT QSM DC",
        "QSMxT QSM ND",
    ]
    assert [meta["QSMxTInputSeries"] for meta in first_meta] == [
        "distortion-corrected",
        "non-distortion-corrected",
    ]
    assert len(
        {
            int(batch[0].getHead().image_series_index)
            for batch in connection.sent_batches[-2:]
        }
    ) == 2
    assert len({meta["SeriesInstanceUID"] for meta in first_meta}) == 2


def test_process_returns_scanner_packed_volume_as_one_mm_slices(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    fake_qsmxt = _fake_qsmxt_binary(tmp_path)
    connection = FakeConnection(_packed_scanner_images())

    qsmxt.process(
        connection,
        {"parameters": {"qsmxtbinary": str(fake_qsmxt)}},
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert len(connection.sent_batches) == 3
    assert len(connection.sent_batches[-1]) == 144

    first = connection.sent_batches[-1][0]
    last = connection.sent_batches[-1][-1]
    first_meta = ismrmrd.Meta.deserialize(first.attribute_string)
    np.testing.assert_allclose(first.getHead().field_of_view, [4.0, 3.0, 1.0])
    np.testing.assert_allclose(first.getHead().position, [10.0, 20.0, 30.0])
    np.testing.assert_allclose(last.getHead().position, [10.0, 20.0, 173.0])
    assert first_meta["Keep_image_geometry"] == "0"
    assert first_meta["NumberOfSlices"] == "144"


def test_original_passthrough_restamps_multi_partition_source_geometry():
    minihead = textwrap.dedent(
        """\
        <XProtocol>
        {
          <ParamMap."DICOM">
          {
            <ParamLong."Actual3DImagePartNumber">{ }
            <ParamLong."Actual3DImaPartNumber">{ }
            <ParamLong."NumberInSeries">{ 99 }
            <ParamLong."ProtocolSliceNumber">{ }
            <ParamLong."SliceNo">{ }
            <ParamString."SOPInstanceUID">{ "1.2.3.old-sop" }
          }
          <ParamMap."CONTROL">
          {
            <ParamLong."AnatomicalPartitionNo">{ }
            <ParamLong."AnatomicalSliceNo">{ }
            <ParamLong."ChronSliceNo">{ }
            <ParamLong."IsmrmrdSliceNo">{ }
            <ParamString."SeriesInstanceUID">{ "1.2.3.old-series" }
          }
        }
        """
    )
    images = [
        _image(
            7,
            index + 1,
            "qsm_source",
            np.full((1, 3, 4), index, dtype=np.float32),
            meta_values={
                "partition_count": "3",
                "SeriesInstanceUID": "1.2.3.old-series",
                "SOPInstanceUID": f"1.2.3.old-sop.{index}",
            },
            minihead_text=minihead,
        )
        for index in range(3)
    ]

    outputs = qsmxt._build_original_passthrough_images(images)

    assert len(outputs) == 3
    for index, output in enumerate(outputs):
        assert output is not images[index]
        np.testing.assert_array_equal(output.data, images[index].data)

        header = output.getHead()
        meta = ismrmrd.Meta.deserialize(output.attribute_string)
        patched_minihead = qsmxt._decode_ice_minihead(meta)

        assert int(header.image_series_index) == qsmxt.ORIGINAL_SERIES_START
        assert int(header.image_index) == index + 1
        assert int(header.slice) == 0
        assert meta["Keep_image_geometry"] == "1"
        assert meta["partition_count"] == "3"
        assert meta["slice_count"] == "1"
        assert meta["NumberOfSlices"] == "1"
        assert meta["ImagesInAcquisition"] == "3"
        assert meta["Actual3DImagePartNumber"] == str(index)
        assert meta["Actual3DImaPartNumber"] == str(index)
        assert meta["AnatomicalPartitionNo"] == str(index)
        assert meta["SliceNo"] == "0"
        assert meta["IsmrmrdSliceNo"] == "0"
        assert meta["ChronSliceNo"] == "0"
        assert meta["NumberInSeries"] == str(index + 1)
        assert meta["SeriesInstanceUID"] != "1.2.3.old-series"
        assert meta["SOPInstanceUID"] != f"1.2.3.old-sop.{index}"
        assert qsmxt._extract_minihead_long_value(
            patched_minihead,
            "Actual3DImagePartNumber",
        ) == index
        assert qsmxt._extract_minihead_long_value(
            patched_minihead,
            "Actual3DImaPartNumber",
        ) == index
        assert qsmxt._extract_minihead_long_value(
            patched_minihead,
            "AnatomicalPartitionNo",
        ) == index
        assert qsmxt._extract_minihead_long_value(patched_minihead, "SliceNo") == 0


def test_original_passthrough_merges_concatenations_into_one_series_per_contrast():
    # Siemens 3D sequences can split one logical contrast into several
    # concatenations that arrive as distinct image_series_index values while
    # sharing a SeriesDescription. The passthrough must emit a single series
    # per contrast (all slices), not one series per concatenation.
    def contrast_images(sequence, image_type, series_indices):
        images = []
        image_index = 1
        for series_index in series_indices:
            for _ in range(3):
                images.append(
                    _image(
                        series_index,
                        image_index,
                        sequence,
                        np.full((1, 4, 4), image_index, dtype=np.float32),
                        image_type=image_type,
                    )
                )
                image_index += 1
        return images

    magnitude = contrast_images("qsm_source", ismrmrd.IMTYPE_MAGNITUDE, [3, 5])
    phase = contrast_images("qsm_source_Pha", ismrmrd.IMTYPE_PHASE, [4, 6])

    outputs = qsmxt._build_original_passthrough_images(magnitude + phase)

    series_counts = {}
    for output in outputs:
        series_index = int(output.getHead().image_series_index)
        series_counts[series_index] = series_counts.get(series_index, 0) + 1

    assert len(series_counts) == 2
    assert sorted(series_counts.values()) == [6, 6]

    for series_index, count in series_counts.items():
        numbers = sorted(
            int(ismrmrd.Meta.deserialize(output.attribute_string)["NumberInSeries"])
            for output in outputs
            if int(output.getHead().image_series_index) == series_index
        )
        assert numbers == list(range(1, count + 1))


def test_original_passthrough_infers_scanner_slice_count_from_minihead():
    outputs = qsmxt._build_original_passthrough_images(_scanner_echo_slice_images())

    assert len(outputs) == 200
    assert [len(batch) for batch in _group_test_images_by_series(outputs)] == [100, 100]

    first_series = _group_test_images_by_series(outputs)[0]
    first_meta = ismrmrd.Meta.deserialize(first_series[0].attribute_string)
    last_meta = ismrmrd.Meta.deserialize(first_series[-1].attribute_string)

    assert first_meta["partition_count"] == "1"
    assert first_meta["slice_count"] == "20"
    assert first_meta["NumberOfSlices"] == "20"
    assert first_meta["ImagesInAcquisition"] == "100"
    assert first_meta["SliceNo"] == "0"
    assert first_meta["IsmrmrdSliceNo"] == "0"
    assert first_meta["NumberInSeries"] == "1"
    assert qsmxt._extract_minihead_long_value(
        qsmxt._decode_ice_minihead(first_meta),
        "SliceNo",
    ) == 0

    assert last_meta["slice_count"] == "20"
    assert last_meta["ImagesInAcquisition"] == "100"
    assert last_meta["SliceNo"] == "19"
    assert last_meta["IsmrmrdSliceNo"] == "19"
    assert last_meta["NumberInSeries"] == "100"
    assert qsmxt._extract_minihead_long_value(
        qsmxt._decode_ice_minihead(last_meta),
        "SliceNo",
    ) == 19


def test_original_passthrough_rewrites_source_series_end_markers():
    outputs = qsmxt._build_original_passthrough_images(
        _scanner_echo_slice_images_with_split_end_markers()
    )
    first_series = _group_test_images_by_series(outputs)[0]

    early_end_meta = ismrmrd.Meta.deserialize(first_series[49].attribute_string)
    early_end_minihead = qsmxt._decode_ice_minihead(early_end_meta)
    final_meta = ismrmrd.Meta.deserialize(first_series[-1].attribute_string)
    final_minihead = qsmxt._decode_ice_minihead(final_meta)

    assert _minihead_bool(early_end_minihead, "BIsSeriesEnd") is False
    assert _minihead_bool(early_end_minihead, "ConcatenationEnd") is False
    assert _minihead_bool(final_minihead, "BIsSeriesEnd") is True
    assert _minihead_bool(final_minihead, "ConcatenationEnd") is True


def _group_test_images_by_series(images):
    groups = []
    by_series = {}
    for image in images:
        series_index = int(image.getHead().image_series_index)
        if series_index not in by_series:
            by_series[series_index] = []
            groups.append(by_series[series_index])
        by_series[series_index].append(image)
    return groups


def _minihead_bool(minihead_text, name):
    marker = f'<ParamBool."{name}">'
    section = minihead_text.split(marker, 1)[1]
    value = section.split("{", 1)[1].split("}", 1)[0]
    return value.strip().strip('"').lower() == "true"


def test_process_sends_restamped_originals_before_derived_output(tmp_path, monkeypatch):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    fake_qsmxt = _fake_qsmxt_binary(tmp_path)
    connection = FakeConnection(_input_images())

    qsmxt.process(
        connection,
        {
            "parameters": {
                "qsmxtbinary": str(fake_qsmxt),
                "echotimesms": "10,20",
            }
        },
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert len(connection.sent_batches) == 3
    assert [len(batch) for batch in connection.sent_batches] == [2, 2, 2]

    first_original = connection.sent_batches[0][0]
    first_original_meta = ismrmrd.Meta.deserialize(first_original.attribute_string)
    derived = connection.sent_batches[-1][0]
    derived_meta = ismrmrd.Meta.deserialize(derived.attribute_string)

    assert int(first_original.getHead().image_series_index) == qsmxt.ORIGINAL_SERIES_START
    assert first_original_meta["Keep_image_geometry"] == "1"
    assert int(first_original.getHead().image_series_index) not in {1, 2}
    assert derived_meta["QSMxTOutput"] == "qsm"


def test_originals_are_sent_before_qsmxt_runs_and_survive_its_failure(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    connection = FakeConnection(_input_images())
    sent_before_run = []

    def failing_run(*args, **kwargs):
        sent_before_run.extend(
            ismrmrd.Meta.deserialize(batch[0].attribute_string)["SeriesDescription"]
            for batch in connection.sent_batches
        )
        raise RuntimeError("QSMxT exited with status 1")

    monkeypatch.setattr(qsmxt, "_run_qsmxt", failing_run)
    qsmxt.process(connection, {"parameters": {"echotimesms": "10,20"}}, FakeMetadata())

    assert connection.closed is True
    assert sent_before_run == ["gre_qsm_original", "gre_qsm_Pha_original"]
    assert [len(batch) for batch in connection.sent_batches] == [2, 2]
    assert len(connection.logs) == 1
    assert "QSMxT exited with status 1" in connection.logs[0][1]


def test_process_handles_scanner_echo_slice_stream_with_original_passthrough(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    fake_qsmxt = _fake_qsmxt_binary(tmp_path)
    connection = FakeConnection(_scanner_echo_slice_images())

    qsmxt.process(
        connection,
        {
            "parameters": {
                "qsmxtbinary": str(fake_qsmxt),
            }
        },
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert [len(batch) for batch in connection.sent_batches] == [100, 100, 20]

    first_original_meta = ismrmrd.Meta.deserialize(
        connection.sent_batches[0][0].attribute_string
    )
    last_original_meta = ismrmrd.Meta.deserialize(
        connection.sent_batches[0][-1].attribute_string
    )
    first_derived_meta = ismrmrd.Meta.deserialize(
        connection.sent_batches[-1][0].attribute_string
    )

    assert first_original_meta["slice_count"] == "20"
    assert first_original_meta["ImagesInAcquisition"] == "100"
    assert last_original_meta["SliceNo"] == "19"
    assert last_original_meta["NumberInSeries"] == "100"
    assert first_derived_meta["QSMxTOutput"] == "qsm"
    assert first_derived_meta["slice_count"] == "20"


def test_derived_outputs_use_original_source_geometry_when_originals_are_sent(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    fake_qsmxt = _fake_qsmxt_binary(tmp_path, affine_offset=[100.0, -50.0, 25.0])
    images = _scanner_echo_slice_images()
    for image in images:
        header = image.getHead()
        slice_index = int(getattr(header, "slice", 0))
        _set_header_vector(
            header,
            "position",
            [10.0, 20.0, 30.0 + 1.5 * slice_index],
        )
        image.setHead(header)
    connection = FakeConnection(images)

    qsmxt.process(
        connection,
        {
            "parameters": {
                "qsmxtbinary": str(fake_qsmxt),
            }
        },
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert [len(batch) for batch in connection.sent_batches] == [100, 100, 20]

    magnitude_originals = connection.sent_batches[0]
    qsm_outputs = connection.sent_batches[-1]
    source_by_slice = {}
    for image in magnitude_originals:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        source_by_slice.setdefault(int(meta["SliceNo"]), image)

    for output in qsm_outputs:
        output_meta = ismrmrd.Meta.deserialize(output.attribute_string)
        source = source_by_slice[int(output_meta["SliceNo"])]
        source_header = source.getHead()
        output_header = output.getHead()

        assert output_meta["Keep_image_geometry"] == "1"
        np.testing.assert_allclose(output_header.position, source_header.position)
        np.testing.assert_allclose(output_header.read_dir, source_header.read_dir)
        np.testing.assert_allclose(output_header.phase_dir, source_header.phase_dir)
        np.testing.assert_allclose(output_header.slice_dir, source_header.slice_dir)


@pytest.mark.parametrize(
    "parameter,flag,value",
    [
        (parameter, flag, choice["id"])
        for parameter, flag in (
            ("qsmalgorithm", "--qsm-algorithm"),
            ("unwrappingalgorithm", "--unwrapping-algorithm"),
            ("bfalgorithm", "--bf-algorithm"),
        )
        for item in json.loads(Path(__file__).with_name("OpenReconLabel.json").read_text())["parameters"]
        if item["id"] == parameter
        for choice in item["values"]
        if choice["id"] != "default"
    ],
)
def test_gui_algorithm_reaches_command(parameter, flag, value, tmp_path, monkeypatch):
    settings = qsmxt._settings_from_config(
        {"parameters": {"pipelinepreset": "custom", parameter: value}}, FakeMetadata()
    )
    commands = []

    def capture(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", capture)
    qsmxt._run_qsmxt(tmp_path, tmp_path / "output", settings)
    command = commands[0]
    assert command[command.index(flag) + 1] == value


@pytest.mark.parametrize("output_id", sorted(qsmxt.SWI_LIKE_OUTPUTS))
@pytest.mark.parametrize("amplitude", [0.0001, 1.0, 10000.0])
def test_swi_window_survives_integer_bridge_and_is_shared_across_slices(
    amplitude, output_id, tmp_path
):
    physical = np.zeros((2, 32, 32), dtype=np.float32)
    physical[1] = np.linspace(0.1, 1.0, 1024).reshape(32, 32) * amplitude
    physical[1, -1, -1] = 3.269 * amplitude
    source = _image(1, 1, "gre_qsm", physical[:1])
    display, _ = qsmxt._scanner_display_volume(physical, output_id, "a.u.")
    path = tmp_path / f"{output_id}.nii.gz"
    nib.save(nib.Nifti1Image(np.moveaxis(physical, 0, -1), np.eye(4)), path)
    spec = qsmxt.QSMXT_OUTPUTS[output_id]
    images = qsmxt._nifti_to_mrd_images(
        path, source, qsmxt.OUTPUT_SERIES_START,
        spec["series"], spec["token"], output_id, spec["units"],
    )
    qsmxt._validate_output_images(images, [source])
    assert len(images) == 2
    windows = []
    for image in images:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        center, width = int(meta["WindowCenter"]), int(meta["WindowWidth"])
        assert width > 2
        windows.append((center, width))
        assert float(meta["RescaleSlope"]) == 1.0
        assert float(meta["RescaleIntercept"]) == 0.0
        assert meta["QSMxTWindowDomain"] == "scaled-a.u."
        scale = float(meta["QSMxTDisplayScale"])
        high = np.percentile(physical[physical > 0], 99)
        assert width == pytest.approx(high * scale, abs=1)
        assert center == pytest.approx(width / 2, abs=1)
        np.testing.assert_allclose(display / scale, physical, atol=0.51 / scale)
    assert windows[0] == windows[1]


@pytest.mark.parametrize("selection, expected", [
    ("smwi", True), ("all", True), ("qsm", False), ("swi", False),
])
def test_smwi_selection_requests_smwi(selection, expected, tmp_path, monkeypatch):
    settings = qsmxt._settings_from_config({"parameters": {"sendoutputs": selection}})
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", run)
    qsmxt._run_qsmxt(tmp_path, tmp_path / "output", settings)
    assert ("--do-smwi" in commands[0]) == expected


@pytest.mark.parametrize("selection, expected", [
    ("smwi", {"smwi-paramagnetic", "smwi-diamagnetic"}),
    ("swi", {"swi"}),
])
def test_smwi_and_swi_outputs_do_not_cross_match(selection, expected, tmp_path):
    anat = tmp_path / qsmxt.QSMXT_DERIVATIVE_ROOT / "sub-01" / "anat"
    anat.mkdir(parents=True)
    # File names written by QSMxT 9.22 for --do-swi and --do-smwi.
    for suffix in (
        "swi", "minIP", "desc-paramagnetic_smwi", "desc-diamagnetic_smwi",
        "desc-paramagnetic_minIP", "desc-diamagnetic_minIP",
    ):
        data = np.ones((4, 3, 2), dtype=np.float32)
        nib.save(nib.Nifti1Image(data, np.eye(4)), anat / f"sub-01_{suffix}.nii")
    settings = qsmxt._settings_from_config({"parameters": {"sendoutputs": selection}})
    specs = qsmxt._find_qsmxt_outputs(tmp_path, {"subject": "01"}, settings)
    assert {output_id for output_id, _, _ in specs} == expected
    for output_id, spec, path in specs:
        assert path.name == f"sub-01_{spec['suffix']}.nii"
        assert len(spec["token"]) <= 16


@pytest.mark.parametrize("method", qsmxt.SOURCE_SEPARATION_METHODS)
def test_source_separation_command(method, tmp_path, monkeypatch):
    settings = qsmxt._settings_from_config({"parameters": {"sourceseparation": method}})
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", run)
    qsmxt._run_qsmxt(tmp_path, tmp_path / "output", settings)
    command = commands[0]
    assert ("--do-chisep" in command) == (method != "off")
    if method != "off":
        assert command[command.index("--chisep") + 1] == method
        assert "--no-qsm" not in command
    else:
        assert "--chisep" not in command


@pytest.mark.parametrize("params, message", [
    ({"sourceseparation": "chi-sepnet"}, "unknown sourceseparation"),
    ({"sourceseparation": "decompose", "noqsm": True}, "requires QSM"),
])
def test_source_separation_rejects_invalid_settings(params, message):
    with pytest.raises(ValueError, match=message):
        qsmxt._settings_from_config({"parameters": params})


@pytest.mark.parametrize("method", ["r2star-qsm", "decompose"])
@pytest.mark.parametrize("max_echoes", [0, 2])
def test_source_separation_requires_three_selected_echoes(method, max_echoes, tmp_path):
    settings = qsmxt._settings_from_config({"parameters": {
        "sourceseparation": method, "maxechoes": max_echoes,
    }})
    images = _input_images() if max_echoes == 0 else _scanner_echo_slice_images()
    with pytest.raises(ValueError, match="at least 3 GRE"):
        qsmxt.write_bids_dataset(images, FakeMetadata(), tmp_path, settings)


@pytest.mark.parametrize("echo_times", ["10,10,20,30,40", "10,20,15,30,40", "0,10,20,30,40"])
def test_source_separation_requires_increasing_echo_times(echo_times, tmp_path):
    settings = qsmxt._settings_from_config({"parameters": {
        "sourceseparation": "decompose", "echotimesms": echo_times,
    }})
    with pytest.raises(ValueError, match="positive, increasing echo times"):
        qsmxt.write_bids_dataset(_scanner_echo_slice_images(), FakeMetadata(), tmp_path, settings)


@pytest.mark.parametrize("method", qsmxt.SOURCE_SEPARATION_METHODS)
@pytest.mark.parametrize("selection", ["qsm", "all"])
def test_source_maps_do_not_replace_qsm_and_preserve_scanner_units(method, selection, tmp_path):
    anat = tmp_path / "derivatives" / "qsmxt" / "sub-01" / "anat"
    anat.mkdir(parents=True)
    values = {"qsm": -0.04, "paramagnetic": 0.12, "diamagnetic": 0.16, "separated-total": -0.04}
    for output_id, value in values.items():
        suffix = qsmxt.QSMXT_OUTPUTS[output_id]["suffix"]
        data = np.full((4, 3, 2), value, dtype=np.float32)
        data[0] = 0
        nib.save(nib.Nifti1Image(data, np.eye(4)), anat / f"sub-01_{suffix}.nii.gz")
    settings = qsmxt._settings_from_config({"parameters": {
        "sourceseparation": method, "sendoutputs": selection,
    }})
    specs = qsmxt._find_qsmxt_outputs(tmp_path, {"subject": "01"}, settings)
    expected = {"qsm"} if method == "off" else set(values)
    assert {output_id for output_id, _, _ in specs} == expected
    anchor = _input_images()[0]
    for index, (output_id, spec, path) in enumerate(specs):
        assert path.name == f"sub-01_{spec['suffix']}.nii.gz"
        images = qsmxt._nifti_to_mrd_images(
            path, anchor, qsmxt.OUTPUT_SERIES_START + index,
            spec["series"], spec["token"], output_id, spec["units"],
        )
        qsmxt._validate_output_images(images, [anchor])
        recovered = []
        for image in images:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            assert meta["QSMxTWindowDomain"] == "ppb"
            assert meta["QSMxTUnits"] == "ppm"
            recovered.append((image.data * float(meta["RescaleSlope"]) + float(meta["RescaleIntercept"])) / 1000)
        np.testing.assert_allclose(np.max(recovered), max(values[output_id], 0), atol=0.001)
        np.testing.assert_allclose(np.min(recovered), min(values[output_id], 0), atol=0.001)
    (anat / "sub-01_Chimap.nii.gz").unlink()
    assert all(output_id != "qsm" for output_id, _, _ in qsmxt._find_qsmxt_outputs(tmp_path, {"subject": "01"}, settings))


def test_missing_source_map_fails_explicitly(tmp_path):
    settings = qsmxt._settings_from_config({"parameters": {"sourceseparation": "decompose"}})
    with pytest.raises(RuntimeError, match="required paramagnetic map"):
        qsmxt._find_qsmxt_outputs(tmp_path, {"subject": "01"}, settings)


@pytest.mark.parametrize("method", ["r2star-qsm", "decompose"])
def test_nonuniform_echo_spacing_is_only_rejected_for_r2star_qsm(method, tmp_path):
    settings = qsmxt._settings_from_config({"parameters": {
        "sourceseparation": method, "echotimesms": "5,10,20,30,40",
    }})
    if method == "r2star-qsm":
        with pytest.raises(ValueError, match="equally spaced"):
            qsmxt.write_bids_dataset(_scanner_echo_slice_images(), FakeMetadata(), tmp_path, settings)
    else:
        qsmxt.write_bids_dataset(_scanner_echo_slice_images(), FakeMetadata(), tmp_path, settings)


def _dual_polarity_images(repetitions=4, slices=2):
    images = []
    for series_index, sequence, image_type, base in (
        (1, "ep3d_qsm", ismrmrd.IMTYPE_MAGNITUDE, 1000.0),
        (2, "ep3d_qsm_Pha", getattr(ismrmrd, "IMTYPE_PHASE", 2), 0.0),
    ):
        for repetition in range(repetitions):
            for slice_index in range(slices):
                images.append(
                    _image(
                        series_index,
                        repetition * slices + slice_index + 1,
                        sequence,
                        np.full((1, 3, 4), base + 100 * repetition + slice_index),
                        image_type=image_type,
                        meta_values={"EchoNumber": "1"},
                        header_values={"repetition": repetition, "slice": slice_index},
                    )
                )
    return images


def test_ep3d_dual_polarity_averages_repetitions_into_one_echo(tmp_path, monkeypatch):
    seen = {}

    def fake_preprocessing(magnitude_path, phase_path, work_dir, *, denoise, n_procs):
        reference = nib.load(str(magnitude_path))
        magnitude = reference.get_fdata()
        phase = nib.load(str(phase_path)).get_fdata()
        seen.update(magnitude=magnitude, phase=phase, denoise=denoise)
        mean_magnitude = work_dir / "mean_magnitude.nii.gz"
        mean_phase = work_dir / "mean_phase.nii.gz"
        nib.save(nib.Nifti1Image(magnitude.mean(axis=-1), reference.affine), mean_magnitude)
        nib.save(nib.Nifti1Image(phase.mean(axis=-1), reference.affine), mean_phase)
        return {"magnitude": str(mean_magnitude), "phase": str(mean_phase)}

    monkeypatch.setattr(qsmxt.ep3d_mrd, "run_preprocessing", fake_preprocessing)
    settings = qsmxt._settings_from_config(
        {"parameters": {"ep3ddualpolarity": "true", "ep3ddenoise": "true", "sendoutputs": "all"}},
        FakeMetadata(),
    )

    result = qsmxt.write_bids_dataset(
        _dual_polarity_images(), FakeMetadata(), tmp_path / "bids", settings
    )

    assert seen["magnitude"].shape == (4, 3, 2, 4)
    np.testing.assert_allclose(seen["magnitude"][0, 0, 1, :], [1001, 1101, 1201, 1301])
    np.testing.assert_allclose(
        seen["phase"][0, 0, 1, :],
        [(1 + 100 * k) * np.pi / 2048 for k in range(4)],
        rtol=0,
        atol=1e-5,
    )
    assert seen["denoise"] is True
    assert result["n_echoes"] == 1
    assert "t2star" not in qsmxt._selected_output_ids(result["settings"]["send_outputs"])
    assert "r2star" not in qsmxt._selected_output_ids(result["settings"]["send_outputs"])
    assert settings["send_outputs"] == "all"
    magnitude = nib.load(str(result["magnitude_paths"][0]))
    assert magnitude.shape == (4, 3, 2)
    np.testing.assert_allclose(magnitude.get_fdata()[0, 0, :], [1150, 1151])


def test_ep3d_dual_polarity_rejects_a_single_repetition(tmp_path):
    settings = qsmxt._settings_from_config(
        {"parameters": {"ep3ddualpolarity": "true"}},
        FakeMetadata(),
    )

    with pytest.raises(ValueError, match="at least two repetitions"):
        qsmxt.write_bids_dataset(
            _dual_polarity_images(repetitions=1), FakeMetadata(), tmp_path / "bids", settings
        )


def test_ep3d_dual_polarity_rejects_mismatched_repetitions(tmp_path):
    images = _dual_polarity_images()
    for image in images:
        header = image.getHead()
        if image.image_type == ismrmrd.IMTYPE_MAGNITUDE and header.repetition == 3:
            header.repetition = 4
            image.setHead(header)
    settings = qsmxt._settings_from_config(
        {"parameters": {"ep3ddualpolarity": "true"}},
        FakeMetadata(),
    )

    with pytest.raises(ValueError, match="keys differ"):
        qsmxt.write_bids_dataset(images, FakeMetadata(), tmp_path / "bids", settings)


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ([0, 1024, 3072], [0.0, np.pi / 2, -np.pi / 2]),
        ([1000, 1512, 3000], [1000 * np.pi / 2048, 1512 * np.pi / 2048, 3000 * np.pi / 2048 - 2 * np.pi]),
        ([-3.0, 0.5, 3.1], [-3.0, 0.5, 3.1]),
    ],
)
def test_phase_to_radians_uses_fixed_count_cycle(counts, expected):
    np.testing.assert_allclose(
        qsmxt.ep3d_mrd.phase_to_radians(np.array(counts, dtype=np.float32), 4096.0),
        expected,
        atol=1e-5,
    )


def test_phase_to_radians_rejects_phase_wider_than_one_cycle():
    with pytest.raises(ValueError, match="set phasewrap"):
        qsmxt.ep3d_mrd.phase_to_radians(np.array([-4096.0, 4094.0]), 4096.0)


@pytest.mark.parametrize("dtype", [np.uint16, np.float32])
def test_phase_to_radians_bulk_scanner_counts_preserve_wrap_boundary(dtype):
    unique_counts = np.array([0, 1024, 2048, 3072, 4095], dtype=dtype)
    expected = qsmxt.ep3d_mrd.phase_to_radians(unique_counts, 4096.0)
    counts = np.tile(unique_counts, 2048)
    original = counts.copy()

    radians = qsmxt.ep3d_mrd.phase_to_radians(counts, 4096.0)

    assert radians.dtype == np.float32
    np.testing.assert_array_equal(
        radians.view(np.uint32), np.tile(expected, 2048).view(np.uint32)
    )
    np.testing.assert_allclose(
        radians,
        np.tile([0.0, np.pi / 2, -np.pi, -np.pi / 2, -2 * np.pi / 4096], 2048),
        rtol=0,
        atol=3e-7,
    )
    np.testing.assert_array_equal(counts, original)


@pytest.mark.parametrize("counts_per_cycle", [4096, 65536])
def test_phase_to_radians_preserves_every_scanner_count_when_repeated(counts_per_cycle):
    counts = np.arange(counts_per_cycle, dtype=np.uint16)
    expected = qsmxt.ep3d_mrd.phase_to_radians(counts, float(counts_per_cycle))
    repeated = qsmxt.ep3d_mrd.phase_to_radians(np.tile(counts, 3), float(counts_per_cycle))

    np.testing.assert_array_equal(
        repeated.view(np.uint32), np.tile(expected, 3).view(np.uint32)
    )


def test_phase_to_radians_bulk_fractional_counts_keep_sub_count_precision():
    unique_counts = np.array([1024.5, 2048.5, 3072.5], dtype=np.float32)
    expected = qsmxt.ep3d_mrd.phase_to_radians(unique_counts, 4096.0)
    counts = np.tile(unique_counts, 2048)
    actual = qsmxt.ep3d_mrd.phase_to_radians(counts, 4096.0)

    np.testing.assert_array_equal(
        actual.view(np.uint32), np.tile(expected, 2048).view(np.uint32)
    )
    np.testing.assert_allclose(
        actual,
        np.tile([
            np.pi / 2 + np.pi / 4096,
            -np.pi + np.pi / 4096,
            -np.pi / 2 + np.pi / 4096,
        ], 2048),
        rtol=0,
        atol=3e-7,
    )


@pytest.mark.parametrize("module_name", ["qsmxt", "qsmxt_gre_motion"])
@pytest.mark.parametrize("shape", ["bare", "nested", "json"])
@pytest.mark.parametrize("motion,reference", [(None, None), (False, "last"), (True, "2")])
def test_gre_configs_apply_motion_policy_without_mutation(
    module_name, shape, motion, reference, tmp_path, monkeypatch,
):
    import copy
    import importlib

    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    params = {
        "qsmxtbinary": str(_fake_qsmxt_binary(tmp_path)),
        "echotimesms": "10,20", "qsmreference": "none",
        "twopass": True, "inhomogeneitycorrection": "fine",
    }
    if motion is not None:
        params["motioncorrection"] = motion
    if reference is not None:
        params["motionreference"] = reference
    config = params if shape == "bare" else {"parameters": params}
    if shape == "json":
        config = json.dumps(config)
    before = copy.deepcopy(config)
    seen = []
    run = qsmxt._run_qsmxt

    def capture(bids, output, settings):
        seen.append((settings.copy(), qsmxt._qsmxt_command(bids, output, settings)))
        run(bids, output, settings)

    monkeypatch.setattr(qsmxt, "_run_qsmxt", capture)
    connection = FakeConnection(_input_images())
    importlib.import_module(module_name).process(connection, config, FakeMetadata())
    assert connection.closed and not connection.logs
    assert len(connection.sent_batches) == 3
    assert config == before
    assert len(seen) == 1
    settings, command = seen[0]
    expected_motion = module_name == "qsmxt_gre_motion" or motion is True
    assert settings["motion_correction"] is expected_motion
    assert settings["motion_reference"] == (reference or "first")
    assert settings["qsm_reference"] == "none"
    assert settings["two_pass"] is True
    assert settings["inhomogeneity_correction"] == "fine"
    assert settings["ep3d_dual_polarity"] is False
    assert settings["ep3d_denoise"] is False
    assert ("--motion-correction" in command) is expected_motion
    if expected_motion:
        assert command[command.index("--motion-reference") + 1] == (reference or "first")
    else:
        assert "--motion-reference" not in command
    assert "--two-pass" in command
    assert command[command.index("--homogeneity-nbox") + 1] == "15"


def test_gre_motion_config_keeps_unrelated_advanced_epi_flags(monkeypatch):
    import qsmxt_gre_motion

    config = {"parameters": {"motioncorrection": False, "ep3ddualpolarity": True,
                             "ep3ddenoise": True, "motionreference": "last"}}
    received = {}
    monkeypatch.setattr(qsmxt, "process", lambda connection, config, metadata: received.update(config))
    qsmxt_gre_motion.process(FakeConnection([]), config, FakeMetadata())
    settings = qsmxt._settings_from_config(received)
    assert settings["motion_correction"] is True
    assert settings["motion_reference"] == "last"
    assert settings["ep3d_dual_polarity"] is True
    assert settings["ep3d_denoise"] is True
    assert config["parameters"]["motioncorrection"] is False


@pytest.mark.parametrize("reference", ["0", "bad"])
def test_gre_motion_config_preserves_reference_validation(reference, monkeypatch):
    import qsmxt_gre_motion

    def normalize(connection, config, metadata):
        qsmxt._settings_from_config(config, metadata)

    monkeypatch.setattr(qsmxt, "process", normalize)
    with pytest.raises(ValueError, match="motionreference"):
        qsmxt_gre_motion.process(FakeConnection([]), {"motionreference": reference}, FakeMetadata())


def test_openrecon_config_choices_load_all_modules():
    label_path = Path(qsmxt.__file__).with_name("OpenReconLabel.json")
    config = next(
        parameter
        for parameter in qsmxt.json.loads(label_path.read_text())["parameters"]
        if parameter["id"] == "config"
    )
    assert [value["id"] for value in config["values"]] == [
        "qsmxt",
        "qsmxt_gre_motion",
        "qsmxt_ep3d",
        "qsmxt_ep3d_denoise",
    ]
    for value in config["values"]:
        assert Path(qsmxt.__file__).with_name(f"{value['id']}.py").is_file()


@pytest.mark.parametrize(
    ("module_name", "denoise"),
    [("qsmxt_ep3d", False), ("qsmxt_ep3d_denoise", True)],
)
@pytest.mark.parametrize("shape", ["bare", "nested", "json"])
def test_ep3d_configs_enable_preprocessing_and_keep_user_controls(
    module_name, denoise, shape, monkeypatch
):
    import importlib

    module = importlib.import_module(module_name)
    received = {}
    monkeypatch.setattr(
        qsmxt,
        "process",
        lambda connection, config, metadata: received.update(config=config),
    )

    params = {"config": module_name, "qsmalgorithm": "rts",
              "motioncorrection": True, "motionreference": "last"}
    config = params if shape == "bare" else {"parameters": params}
    if shape == "json":
        config = json.dumps(config)
    before = json.dumps(config, sort_keys=True)
    module.process(FakeConnection([]), config, FakeMetadata())
    assert json.dumps(config, sort_keys=True) == before

    settings = qsmxt._settings_from_config(received["config"], FakeMetadata())
    assert settings["motion_correction"] is True
    assert settings["motion_reference"] == "last"
    assert settings["ep3d_dual_polarity"] is True
    assert settings["ep3d_denoise"] is denoise
    assert settings["qsm_algorithm"] == "rts"
    assert qsmxt._settings_from_config({}, FakeMetadata())["ep3d_dual_polarity"] is False


@pytest.mark.parametrize("params,nbox", [
    ({}, 3), ({"inhomogeneitycorrection": None}, 3),
    ({"inhomogeneitycorrection": " "}, 3),
    ({"inhomogeneitycorrection": "standard"}, 3),
    ({"inhomogeneitycorrection": "fine"}, 15),
    ({"inhomogeneitycorrection": " FiNe "}, 15),
    ({"inhomogeneitycorrection": "off"}, None),
    ({"inhomogeneitycorrection": True}, 3),
    ({"inhomogeneitycorrection": False}, None),
    ({"inhomogeneitycorrection": 1}, 3),
    ({"inhomogeneitycorrection": 0}, None),
    ({"inhomogeneitycorrection": 1.0}, 3),
    ({"inhomogeneitycorrection": 0.0}, None),
    *[({"inhomogeneitycorrection": value}, 3)
      for value in ("true", "1", "yes", "y", "on", " TRUE ")],
    *[({"inhomogeneitycorrection": value}, None)
      for value in ("false", "0", "no", "n", " OFF ")],
])
def test_homogeneity_config_reaches_command(params, nbox, tmp_path, monkeypatch):
    settings = qsmxt._settings_from_config({"parameters": params})
    commands = []

    def capture(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", capture)
    qsmxt._run_qsmxt(tmp_path, tmp_path / "output", settings)
    command = commands[0]
    assert command.count("--inhomogeneity-correction") == (nbox is not None)
    assert command.count("--no-inhomogeneity-correction") == (nbox is None)
    if nbox is None:
        assert "--homogeneity-sigma-mm" not in command
        assert "--homogeneity-nbox" not in command
    else:
        assert command[command.index("--homogeneity-sigma-mm") + 1] == "7"
        assert command[command.index("--homogeneity-nbox") + 1] == str(nbox)


@pytest.mark.parametrize("value", ["fnie", "2", [], {}])
def test_homogeneity_config_rejects_invalid_choice(value, monkeypatch):
    def unexpected_run(*args, **kwargs):
        pytest.fail("invalid correction choice launched QSMxT")

    monkeypatch.setattr(qsmxt.subprocess, "run", unexpected_run)
    with pytest.raises(ValueError, match="inhomogeneitycorrection.*off.*standard.*fine"):
        qsmxt._settings_from_config({"parameters": {"inhomogeneitycorrection": value}})


@pytest.mark.parametrize("preset,sections", [
    ("hd-bet", ["magnitude,hd-bet,close:1,fill-holes:0"]),
    ("bet", ["magnitude,bet:0.5,close:1,fill-holes:0"]),
    ("robust-threshold", ["magnitude,threshold:otsu,close:1,fill-holes:0"]),
    ("combined", ["magnitude,bet:0.5,close:1,fill-holes:0",
                  "magnitude,threshold:otsu,close:1,fill-holes:0"]),
])
def test_mask_defaults_close_and_fill_each_section(preset, sections):
    settings = qsmxt._settings_from_config({"parameters": {"maskpreset": preset}})
    assert qsmxt._mask_sections(settings) == sections


def test_legacy_mask_cleanup_none_retains_close_fill_default():
    settings = qsmxt._settings_from_config({"parameters": {"maskcleanup": "none"}})
    assert qsmxt._mask_sections(settings) == ["magnitude,hd-bet,close:1,fill-holes:0"]


@pytest.mark.parametrize("module_name", ["qsmxt", "qsmxt_gre_motion", "qsmxt_ep3d", "qsmxt_ep3d_denoise"])
@pytest.mark.parametrize("params", [{}, {"maskpreset": ""}, {"maskpreset": "hd-bet"}])
def test_hdbet_default_reaches_all_config_commands(
    module_name, params, tmp_path, monkeypatch,
):
    import importlib

    received = {}
    if module_name == "qsmxt":
        config = {"parameters": params}
    else:
        module = importlib.import_module(module_name)
        monkeypatch.setattr(
            qsmxt, "process",
            lambda connection, config, metadata: received.update(config=config),
        )
        module.process(FakeConnection([]), {"parameters": params}, FakeMetadata())
        config = received["config"]
    settings = qsmxt._settings_from_config(config, FakeMetadata())
    settings["qsmxt_binary"] = "/opt/qsmxt/qsmxt"
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return qsmxt.subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(qsmxt.subprocess, "run", fake_run)
    qsmxt._run_qsmxt(tmp_path, tmp_path / "output", settings)
    command = commands[0]
    for flag, expected in (
        ("--mask", "magnitude,hd-bet,close:1,fill-holes:0"),
        ("--qsm-algorithm", "rts"),
        ("--unwrapping-algorithm", "romeo"),
        ("--bf-algorithm", "vsharp"),
        ("--homogeneity-sigma-mm", "7"),
        ("--homogeneity-nbox", "3"),
    ):
        assert command[command.index(flag) + 1] == expected
    assert command.count("--mask") == 1
    assert "--mask-preset" not in command
    assert "--inhomogeneity-correction" in command


def test_hdbet_does_not_use_bet_or_threshold_controls():
    settings = qsmxt._settings_from_config({"parameters": {
        "maskpreset": "hd-bet",
        "betfractionalintensity": 0.2,
        "maskinginput": "phase-quality",
        "maskthresholdmethod": "percentile",
        "maskthresholdpercentile": 90,
    }})
    assert qsmxt._mask_sections(settings) == [
        "magnitude,hd-bet,close:1,fill-holes:0",
    ]


@pytest.mark.parametrize("preset", ["HD-BET", "unknown", [], {}])
def test_invalid_mask_preset_fails_before_launch(preset, tmp_path, monkeypatch):
    def unexpected_run(*args, **kwargs):
        pytest.fail("invalid mask choice launched QSMxT")

    monkeypatch.setattr(qsmxt.subprocess, "run", unexpected_run)
    with pytest.raises(ValueError, match="maskpreset"):
        settings = qsmxt._settings_from_config({"parameters": {"maskpreset": preset}})
        qsmxt._run_qsmxt(tmp_path, tmp_path / "output", settings)


@pytest.mark.parametrize("reference", ["none", "ventricles", "left-thalamus", "4,43", "right-caudate,4"])
def test_v10_reference_and_independent_controls_reach_native_command(reference, tmp_path):
    settings = qsmxt._settings_from_config({"parameters": {
        "qsmreference": reference, "twopass": True, "motioncorrection": True,
        "motionreference": "last", "vsharpgrid": "isotropic", "vsharpisotropicspacing": 1.2,
        "swiunwrapping": "laplacian-slice", "ignoreslicegap": True,
    }})
    command = qsmxt._qsmxt_command(tmp_path, tmp_path / "output", settings)
    for flag, value in [
        ("--qsm-reference", reference), ("--motion-reference", "last"),
        ("--vsharp-grid", "isotropic"), ("--vsharp-grid-spacing", "1.2"),
        ("--swi-unwrapping", "laplacian-slice"),
        ("--mask", "magnitude,hd-bet,close:1,fill-holes:0"),
    ]:
        assert command[command.index(flag) + 1] == value
    assert {"--two-pass", "--motion-correction", "--ignore-slice-gap"} <= set(command)
    assert "--two-pass-mask" not in command


@pytest.mark.parametrize("params", [
    {"qsmreference": ""}, {"qsmreference": "4,,43"}, {"qsmreference": "mean,4"},
    {"vsharpgrid": "coarsest"}, {"swiunwrapping": "slice"}, {"motionreference": "0"},
    {"slicethicknessmm": "invalid"}, {"vsharpisotropicspacing": -1},
])
def test_v10_boundary_rejects_malformed_configuration(params):
    with pytest.raises(ValueError):
        qsmxt._settings_from_config({"parameters": params})


def test_v10_default_command_keeps_optional_processing_off(tmp_path):
    command = qsmxt._qsmxt_command(tmp_path, tmp_path / "out", qsmxt._settings_from_config({}))
    assert command[command.index("--qsm-reference") + 1] == "mean"
    assert command[command.index("--vsharp-grid") + 1] == "native"
    assert command[command.index("--swi-unwrapping") + 1] == "laplacian"
    assert not {"--two-pass", "--motion-correction", "--ignore-slice-gap"} & set(command)


def test_acquired_geometry_uses_explicit_thickness_and_rejects_conflicts():
    image = _image(1, 1, "GRE", np.ones((1, 3, 4)), meta_values={
        "SliceThickness": "2", "MRAcquisitionType": "2D",
    })
    settings = qsmxt._settings_from_config({})
    assert qsmxt._acquisition_geometry_sidecar([image], FakeMetadata(), settings) == {
        "SliceThickness": 2.0, "MRAcquisitionType": "2D", "SliceEncodingDirection": "k",
    }
    conflicting = _image(1, 2, "GRE", np.ones((1, 3, 4)), meta_values={"SliceThickness": "3"})
    with pytest.raises(ValueError, match="Inconsistent SliceThickness"):
        qsmxt._acquisition_geometry_sidecar([image, conflicting], FakeMetadata(), settings)
    override = qsmxt._settings_from_config({"parameters": {"slicethicknessmm": 1.5}})
    assert qsmxt._acquisition_geometry_sidecar([image, conflicting], FakeMetadata(), override)["SliceThickness"] == 1.5


def test_unknown_acquired_thickness_is_not_inferred_from_image_fov(caplog):
    image = _image(1, 1, "GRE", np.ones((1, 3, 4)))
    sidecar = qsmxt._acquisition_geometry_sidecar([image], FakeMetadata(), qsmxt._settings_from_config({}))
    assert sidecar == {"SliceEncodingDirection": "k"}
    assert "Supply independent acquired thickness" in caplog.text


def test_2d_nonuniform_positions_fail_instead_of_averaging_spacing():
    images = [_image(1, i, "GRE", np.ones((1, 3, 4))) for i in range(3)]
    for image, z in zip(images, [0, 2, 4]):
        header = image.getHead()
        _set_header_vector(header, "position", [0, 0, z])
        image.setHead(header)
    qsmxt._validate_2d_stack_positions(images)
    header = images[-1].getHead()
    _set_header_vector(header, "position", [0, 0, 5])
    images[-1].setHead(header)
    with pytest.raises(ValueError, match="Nonuniform 2D"):
        qsmxt._validate_2d_stack_positions(images)


def test_geometry_sidecars_preserve_independent_thickness_without_changing_pitch(tmp_path):
    images = _input_images()
    for image in images:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        meta["SliceThickness"] = "0.8"
        meta["MRAcquisitionType"] = "2D"
        image.attribute_string = meta.serialize()
    result = qsmxt.write_bids_dataset(images, FakeMetadata(), tmp_path / "bids", qsmxt._settings_from_config({}))
    for path in result["phase_paths"] + result["magnitude_paths"]:
        sidecar = json.loads(qsmxt._nifti_sidecar_path(path).read_text())
        assert sidecar["SliceThickness"] == 0.8
        assert sidecar["MRAcquisitionType"] == "2D"
        assert sidecar["SliceEncodingDirection"] == "k"
        assert nib.load(path).header.get_zooms()[2] == 1.0


def test_main_qsm_selection_excludes_two_pass_intermediate(tmp_path):
    output = tmp_path / "derivatives" / "qsmxt" / "sub-01" / "anat"
    output.mkdir(parents=True)
    primary = output / "sub-01_Chimap.nii.gz"
    single = output / "sub-01_desc-singlepass_Chimap.nii.gz"
    for path in (primary, single):
        nib.save(nib.Nifti1Image(np.ones((2, 3, 4)), np.eye(4)), path)
    specs = qsmxt._find_qsmxt_outputs(tmp_path, {"subject": "01"}, qsmxt._settings_from_config({}))
    assert [(output_id, path) for output_id, _, path in specs] == [("qsm", primary)]


@pytest.mark.parametrize("max_echoes", [1, 2, 3])
def test_all_outputs_follow_selected_echo_count(tmp_path, max_echoes, caplog):
    caplog.set_level(logging.INFO)
    settings = qsmxt._settings_from_config(
        {"parameters": {"sendoutputs": " All ", "maxechoes": max_echoes,
                        "echotimesms": "6,12,18,24,30"}}, FakeMetadata(),
    )
    before = settings.copy()
    conversion = qsmxt.write_bids_dataset(
        _scanner_echo_slice_images(), FakeMetadata(), tmp_path / "bids", settings,
    )
    pair_settings = conversion["settings"]
    command = qsmxt._qsmxt_command(tmp_path / "bids", tmp_path / "out", pair_settings)
    assert ("--do-t2starmap" in command) is (max_echoes >= 3), command
    assert ("--do-r2starmap" in command) is (max_echoes >= 3), command
    assert conversion["n_echoes"] == max_echoes
    if max_echoes < 3:
        assert set(qsmxt._selected_output_ids(pair_settings["send_outputs"])) == {
            "qsm", "mask", "magnitude", "swi", "paramagnetic", "diamagnetic",
            "separated-total", "smwi-paramagnetic", "smwi-diamagnetic",
        }
        assert f"All omits T2*/R2* for {max_echoes} selected echo(s)" in caplog.text
    else:
        assert pair_settings["send_outputs"] == " All "
        assert "All omits" not in caplog.text
    assert pair_settings is not settings
    assert settings == before


@pytest.mark.parametrize("params", [
    {"sendoutputs": "t2star", "dot2starmap": False},
    {"sendoutputs": "r2star", "dor2starmap": False},
    {"dot2starmap": True}, {"dor2starmap": True},
    {"sendoutputs": "all", "dot2starmap": True},
    {"sendoutputs": "all", "dor2starmap": True},
])
@pytest.mark.parametrize("epi", [False, True])
def test_explicit_relaxometry_rejected_before_processing(tmp_path, monkeypatch, params, epi):
    def unexpected(*args, **kwargs):
        pytest.fail("Unsupported map request reached preprocessing or native execution")

    monkeypatch.setattr(qsmxt.ep3d_mrd, "preprocess_images", unexpected)
    monkeypatch.setattr(qsmxt, "_run_qsmxt", unexpected)
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    parameters = dict(params, ep3ddualpolarity=epi, echotimesms="10,20")
    images = _dual_polarity_images() if epi else _input_images()
    connection = FakeConnection(images)
    qsmxt.process(connection, {"parameters": parameters}, FakeMetadata())
    assert connection.closed
    assert len(connection.logs) == 1
    error = connection.logs[0][1]
    assert "requires at least 3 echoes" in error
    assert f"this pair has {1 if epi else 2} selected echo(s)" in error
    assert "Choose qsm, swi, or smwi" in error
    assert connection.sent_batches
    assert all(
        name.endswith("_original") for name in _sent_series_descriptions(connection)
    )


def test_both_pairs_resolve_outputs_independently(tmp_path, monkeypatch):
    images = []
    for series, name, kind, count in (
        (1, "gre_qsm", ismrmrd.IMTYPE_MAGNITUDE, 1),
        (2, "gre_qsm", ismrmrd.IMTYPE_PHASE, 1),
        (3, "gre_qsm_ND", ismrmrd.IMTYPE_MAGNITUDE, 3),
        (4, "gre_qsm_ND", ismrmrd.IMTYPE_PHASE, 3),
    ):
        for echo in range(1, count + 1):
            images.append(_image(series, echo, name, np.full((2, 3, 4), 1000), kind,
                                 meta_values={"EchoNumber": str(echo)}))
    settings = qsmxt._settings_from_config({"parameters": {
        "sendoutputs": "all", "inputseries": "both", "echotimesms": "6,12,18",
        "qsmxtbinary": str(_fake_qsmxt_binary(tmp_path)),
    }}, FakeMetadata())
    before = settings.copy()
    launched, discovered = [], []
    run, find = qsmxt._run_qsmxt, qsmxt._find_qsmxt_outputs

    def capture_run(bids, output, resolved):
        launched.append(resolved)
        run(bids, output, resolved)

    def capture_find(output, conversion, resolved):
        discovered.append(resolved)
        return find(output, conversion, resolved)

    monkeypatch.setattr(qsmxt, "_settings_from_config", lambda *args: settings)
    monkeypatch.setattr(qsmxt, "_run_qsmxt", capture_run)
    monkeypatch.setattr(qsmxt, "_find_qsmxt_outputs", capture_find)
    monkeypatch.setattr(qsmxt, "OPENRECON_WORK_ROOT", tmp_path / "work")
    connection = FakeConnection(images)
    qsmxt.process(connection, {}, FakeMetadata())
    assert connection.closed and not connection.logs
    assert len(launched) == len(discovered) == 2
    assert all(a is b for a, b in zip(launched, discovered))
    assert launched[0] is not launched[1]
    assert all(resolved is not settings for resolved in launched)
    assert "t2star" not in qsmxt._selected_output_ids(launched[0]["send_outputs"])
    assert launched[1]["send_outputs"] == "all"
    assert settings == before
