import stat
import sys
import textwrap

import ismrmrd
import numpy as np

import qsmxt


class FakeAcquisitionSystemInformation:
    systemFieldStrength_T = 7.0


class FakeMeasurementInformation:
    protocolName = "gre_qsm"


class FakeMetadata:
    acquisitionSystemInformation = FakeAcquisitionSystemInformation()
    measurementInformation = FakeMeasurementInformation()


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


def _image(series_index, image_index, sequence, data, image_type=ismrmrd.IMTYPE_MAGNITUDE):
    image = ismrmrd.Image.from_array(data.astype(np.float32), transpose=False)
    header = image.getHead()
    header.image_series_index = series_index
    header.image_index = image_index
    header.image_type = image_type
    _set_header_vector(header, "matrix_size", [data.shape[2], data.shape[1], data.shape[0]])
    _set_header_vector(header, "field_of_view", [4.0, 3.0, 2.0])
    _set_header_vector(header, "position", [0.0, 0.0, 0.0])
    _set_header_vector(header, "read_dir", [1.0, 0.0, 0.0])
    _set_header_vector(header, "phase_dir", [0.0, 1.0, 0.0])
    _set_header_vector(header, "slice_dir", [0.0, 0.0, 1.0])
    image.setHead(header)

    meta = ismrmrd.Meta()
    meta["SequenceDescription"] = sequence
    image.attribute_string = meta.serialize()
    return image


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

    output = connection.sent_batches[0][0]
    header = output.getHead()
    meta = ismrmrd.Meta.deserialize(output.attribute_string)

    assert int(header.image_series_index) == qsmxt.OUTPUT_SERIES_START
    assert output.data.shape == (1, 2, 3, 4)
    assert meta["QSMxTOutput"] == "qsm"
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\QSMXT_CHIMAP"
    output_slice_count = str(int(output.data.shape[1]))
    assert meta["slice_count"] == output_slice_count
    assert meta["NumberOfSlices"] == output_slice_count
    assert meta["ImagesInAcquisition"] == output_slice_count
    assert meta["SliceNo"] == "0"
    assert meta["IsmrmrdSliceNo"] == "0"
    assert "ImageTypeValue3" not in meta
