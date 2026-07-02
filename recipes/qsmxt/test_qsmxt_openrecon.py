import base64
import stat
import sys
import textwrap

import ismrmrd
import nibabel as nib
import numpy as np

import qsmxt


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


def _fake_qsmxt_binary(tmp_path):
    fake = tmp_path / "qsmxt"
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
            bids = Path(sys.argv[2])
            output = Path(sys.argv[3])
            phase = sorted(bids.glob("sub-*/anat/*_echo-1_part-phase_MEGRE.nii.gz"))[0]
            img = nib.load(str(phase))
            data = np.zeros(img.shape, dtype=np.float32) + 1.5
            name = phase.name.replace("_echo-1_part-phase_MEGRE.nii.gz", "_Chimap.nii")
            dest = output / "derivatives" / "qsmxt.rs" / "sub-01" / "anat" / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            nib.save(nib.Nifti1Image(data, img.affine), str(dest))
            """
        )
    )
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    return fake


def test_scanner_display_volume_scales_qsm_ppm_range_to_uint12_interval():
    data = np.asarray([[[-0.01, 0.0, 0.01]]], dtype=np.float32)

    display, meta = qsmxt._scanner_display_volume(data, "qsm", "ppm")

    assert display.dtype == np.uint16
    assert display.tolist() == [[[1048, 2048, 3048]]]
    assert meta["scale"] == 100000.0
    assert meta["offset"] == 2048.0
    assert meta["formula"] == "ppm = (display - 2048) / 100000"
    assert meta["clipped_voxels"] == 0


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
        {"parameters": {"qsmxtbinary": str(fake_qsmxt), "echotimesms": "10,20"}},
        FakeMetadata(),
    )

    assert connection.closed is True
    assert connection.logs == []
    assert len(connection.sent_batches) == 1
    assert len(connection.sent_batches[0]) == 2

    output = connection.sent_batches[0][0]
    header = output.getHead()
    meta = ismrmrd.Meta.deserialize(output.attribute_string)

    assert int(header.image_series_index) == qsmxt.OUTPUT_SERIES_START
    assert int(header.image_index) == 1
    assert int(header.slice) == 0
    assert output.data.shape == (1, 1, 3, 4)
    assert output.data.dtype == np.uint16
    assert np.all(output.data == 3548)
    assert meta["QSMxTOutput"] == "qsm"
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\QSMXT_CHIMAP"
    assert meta["QSMxTDisplayScale"] == "1000"
    assert meta["QSMxTDisplayOffset"] == "2048"
    assert meta["QSMxTDisplayFormula"] == "ppm = (display - 2048) / 1000"
    assert meta["QSMxTDisplayMin"] == "3548"
    assert meta["QSMxTDisplayMax"] == "3548"
    assert meta["QSMxTDisplayClippedVoxels"] == "0"
    assert meta["ImageComment"] == (
        "QSMxT QSM; scanner display uint16 0-4096; "
        "ppm = (display - 2048) / 1000"
    )
    assert meta["ImageComments"] == meta["ImageComment"]
    assert meta["slice_count"] == "2"
    assert meta["NumberOfSlices"] == "2"
    assert meta["ImagesInAcquisition"] == "2"
    assert meta["NumberInSeries"] == "1"
    assert meta["SliceNo"] == "0"
    assert meta["IsmrmrdSliceNo"] == "0"
    assert "ImageTypeValue3" not in meta

    second_output = connection.sent_batches[0][1]
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
                "sendoriginal": "true",
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
                "sendoriginal": "true",
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
