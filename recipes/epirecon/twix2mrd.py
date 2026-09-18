#!/usr/bin/env python3

import argparse
from pathlib import Path

import ismrmrd
import ismrmrd.xsd
import numpy as np
import twixtools


DEFAULT_MATRIX_SIZE = 128
DEFAULT_FOV_MM = 220.0


def _clean_scalar(value, default=""):
    if value is None:
        return default
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, np.generic):
        value = value.item()
    text = str(value).strip()
    return text if text else default


def _float_or_default(value, default):
    try:
        return float(value)
    except Exception:
        return float(default)


def _int_or_default(value, default):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _build_header(twix_header, matrix_size, fov_mm, protocol_name, measurement_id, num_readouts):
    mrd_header = ismrmrd.xsd.ismrmrdHeader()

    measurement_information = ismrmrd.xsd.measurementInformationType()
    measurement_information.measurementID = measurement_id
    measurement_information.protocolName = protocol_name
    measurement_information.patientPosition = "HFS"
    mrd_header.measurementInformation = measurement_information

    acquisition_system_information = ismrmrd.xsd.acquisitionSystemInformationType()
    dicom_header = twix_header.get("Dicom", {})
    acquisition_system_information.systemVendor = _clean_scalar(
        dicom_header.get("Manufacturer"),
        "Siemens",
    )
    acquisition_system_information.systemModel = _clean_scalar(
        dicom_header.get("ManufacturersModelName"),
        "Unknown Siemens",
    )
    acquisition_system_information.systemFieldStrength_T = _float_or_default(
        dicom_header.get("MagneticFieldStrength"),
        7.0,
    )
    mrd_header.acquisitionSystemInformation = acquisition_system_information

    experimental_conditions = ismrmrd.xsd.experimentalConditionsType()
    experimental_conditions.H1resonanceFrequency_Hz = int(
        round(42.577478518e6 * acquisition_system_information.systemFieldStrength_T)
    )
    mrd_header.experimentalConditions = experimental_conditions

    encoding = ismrmrd.xsd.encodingType()
    encoding.trajectory = ismrmrd.xsd.trajectoryType.OTHER

    encoded_space = ismrmrd.xsd.encodingSpaceType()
    encoded_space.matrixSize = ismrmrd.xsd.matrixSizeType()
    encoded_space.matrixSize.x = matrix_size
    encoded_space.matrixSize.y = matrix_size
    encoded_space.matrixSize.z = matrix_size
    encoded_space.fieldOfView_mm = ismrmrd.xsd.fieldOfViewMm()
    encoded_space.fieldOfView_mm.x = fov_mm
    encoded_space.fieldOfView_mm.y = fov_mm
    encoded_space.fieldOfView_mm.z = fov_mm
    encoding.encodedSpace = encoded_space

    recon_space = ismrmrd.xsd.encodingSpaceType()
    recon_space.matrixSize = ismrmrd.xsd.matrixSizeType()
    recon_space.matrixSize.x = matrix_size
    recon_space.matrixSize.y = matrix_size
    recon_space.matrixSize.z = matrix_size
    recon_space.fieldOfView_mm = ismrmrd.xsd.fieldOfViewMm()
    recon_space.fieldOfView_mm.x = fov_mm
    recon_space.fieldOfView_mm.y = fov_mm
    recon_space.fieldOfView_mm.z = fov_mm
    encoding.reconSpace = recon_space

    encoding_limits = ismrmrd.xsd.encodingLimitsType()
    encoding_limits.kspace_encoding_step_1 = ismrmrd.xsd.limitType()
    encoding_limits.kspace_encoding_step_1.minimum = 0
    encoding_limits.kspace_encoding_step_1.maximum = max(num_readouts - 1, 0)
    encoding_limits.kspace_encoding_step_1.center = max(num_readouts // 2, 0)
    encoding_limits.slice = ismrmrd.xsd.limitType()
    encoding_limits.slice.minimum = 0
    encoding_limits.slice.maximum = 0
    encoding_limits.slice.center = 0
    encoding.encodingLimits = encoding_limits

    mrd_header.encoding.append(encoding)
    return mrd_header


def _populate_acquisition_header(acquisition, readout_index, measurement_uid):
    header = acquisition.getHead()
    header.measurement_uid = measurement_uid
    header.scan_counter = readout_index
    header.center_sample = acquisition.number_of_samples // 2
    header.encoding_space_ref = 0
    header.sample_time_us = 1.0
    header.position = (0.0, 0.0, 0.0)
    header.patient_table_position = (0.0, 0.0, 0.0)
    header.read_dir = (1.0, 0.0, 0.0)
    header.phase_dir = (0.0, 1.0, 0.0)
    header.slice_dir = (0.0, 0.0, 1.0)
    header.idx.kspace_encode_step_1 = readout_index
    header.idx.kspace_encode_step_2 = 0
    header.idx.slice = 0
    acquisition.setHead(header)


def convert_twix_to_mrd(input_file, output_file, group_name, matrix_size, fov_mm):
    twix = twixtools.read_twix(input_file, parse_pmu=False)
    measurement = twix[-1]
    twix_header = measurement["hdr"]
    protocol_name = _clean_scalar(
        twix_header.get("MeasYaps", {}).get("tProtocolName"),
        Path(input_file).stem,
    )
    measurement_uid = _int_or_default(twix_header.get("Meas", {}).get("MeasUID"), 0)

    image_mdbs = [mdb for mdb in measurement["mdb"] if mdb.is_image_scan()]
    if not image_mdbs:
        raise RuntimeError(f"No imaging scans found in {input_file}")

    mrd_dataset = ismrmrd.Dataset(output_file, group_name, create_if_needed=True)
    try:
        mrd_header = _build_header(
            twix_header,
            matrix_size=matrix_size,
            fov_mm=fov_mm,
            protocol_name=protocol_name,
            measurement_id=Path(input_file).stem,
            num_readouts=len(image_mdbs),
        )
        mrd_dataset.write_xml_header(bytes(mrd_header.toXML(), "utf-8"))

        for readout_index, mdb in enumerate(image_mdbs):
            acquisition_data = np.asarray(mdb.data, dtype=np.complex64)
            acquisition = ismrmrd.Acquisition(data=acquisition_data)
            _populate_acquisition_header(acquisition, readout_index, measurement_uid)
            if readout_index == len(image_mdbs) - 1:
                acquisition.setFlag(ismrmrd.ACQ_LAST_IN_MEASUREMENT)
            mrd_dataset.append_acquisition(acquisition)
    finally:
        mrd_dataset.close()

    print(
        "Converted Twix imaging data to MRD:",
        output_file,
        f"(readouts={len(image_mdbs)}, coils={image_mdbs[0].data.shape[0]}, samples={image_mdbs[0].data.shape[1]})",
    )


def main():
    parser = argparse.ArgumentParser(description="Convert Siemens Twix imaging scans to ISMRMRD HDF5.")
    parser.add_argument("-f", "--file", required=True, help="Input Siemens .dat file")
    parser.add_argument("-o", "--output", required=True, help="Output ISMRMRD HDF5 file")
    parser.add_argument("-g", "--group", default="dataset", help="ISMRMRD group name")
    parser.add_argument("--matrix-size", type=int, default=DEFAULT_MATRIX_SIZE, help="Reconstruction matrix size")
    parser.add_argument("--fov-mm", type=float, default=DEFAULT_FOV_MM, help="Field of view in mm")
    args = parser.parse_args()

    convert_twix_to_mrd(
        input_file=args.file,
        output_file=args.output,
        group_name=args.group,
        matrix_size=max(1, int(args.matrix_size)),
        fov_mm=float(args.fov_mm),
    )


if __name__ == "__main__":
    main()
