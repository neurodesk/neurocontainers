import pydicom
import argparse
import ismrmrd
import numpy as np
import os
import ctypes
import re
import base64

# Defaults for input arguments
defaults = {
    'outGroup':       'dataset',
}

# Lookup table between DICOM and MRD image types
imtype_map = {'M': ismrmrd.IMTYPE_MAGNITUDE,
              'P': ismrmrd.IMTYPE_PHASE,
              'R': ismrmrd.IMTYPE_REAL,
              'I': ismrmrd.IMTYPE_IMAG}

# Lookup table between DICOM and Siemens flow directions
venc_dir_map = {'rl'  : 'FLOW_DIR_R_TO_L',
                'lr'  : 'FLOW_DIR_L_TO_R',
                'ap'  : 'FLOW_DIR_A_TO_P',
                'pa'  : 'FLOW_DIR_P_TO_A',
                'fh'  : 'FLOW_DIR_F_TO_H',
                'hf'  : 'FLOW_DIR_H_TO_F',
                'in'  : 'FLOW_DIR_TP_IN',
                'out' : 'FLOW_DIR_TP_OUT'}


def _is_enhanced_mr(dset):
    return dset.SOPClassUID.name == 'Enhanced MR Image Storage'


def _normalize(vec):
    arr = np.asarray(vec, dtype=float)
    norm = np.linalg.norm(arr)
    if norm == 0:
        return arr
    return arr / norm


def _get_enhanced_group_item(dset, group_name):
    if not _is_enhanced_mr(dset):
        return None

    try:
        if group_name in dset.PerFrameFunctionalGroupsSequence[0]:
            return dset.PerFrameFunctionalGroupsSequence[0][group_name][0]
    except Exception:
        pass

    try:
        if group_name in dset.SharedFunctionalGroupsSequence[0]:
            return dset.SharedFunctionalGroupsSequence[0][group_name][0]
    except Exception:
        pass

    return None


def _get_pixel_spacing_and_thickness(dset):
    pixel_spacing = None
    slice_thickness = None

    if _is_enhanced_mr(dset):
        measures = _get_enhanced_group_item(dset, 'PixelMeasuresSequence')
        if measures is not None:
            pixel_spacing = getattr(measures, 'PixelSpacing', None)
            slice_thickness = getattr(measures, 'SliceThickness', None)

    if pixel_spacing is None:
        pixel_spacing = dset.get('PixelSpacing', [1.0, 1.0])
    if slice_thickness is None:
        slice_thickness = dset.get('SliceThickness', 1.0)

    row_spacing = float(pixel_spacing[0])
    col_spacing = float(pixel_spacing[1])
    return row_spacing, col_spacing, float(slice_thickness)


def _get_image_orientation(dset):
    iop = None

    if _is_enhanced_mr(dset):
        orient = _get_enhanced_group_item(dset, 'PlaneOrientationSequence')
        if orient is not None:
            iop = getattr(orient, 'ImageOrientationPatient', None)

    if iop is None:
        iop = dset.get('ImageOrientationPatient', [1.0, 0.0, 0.0, 0.0, 1.0, 0.0])

    iop = np.asarray(iop, dtype=float)
    if iop.size != 6:
        iop = np.asarray([1.0, 0.0, 0.0, 0.0, 1.0, 0.0], dtype=float)

    row_dir = _normalize(iop[0:3])
    col_dir = _normalize(iop[3:6])
    return row_dir, col_dir


def _get_image_position(dset):
    ipp = None

    if _is_enhanced_mr(dset):
        position = _get_enhanced_group_item(dset, 'PlanePositionSequence')
        if position is not None:
            ipp = getattr(position, 'ImagePositionPatient', None)

    if ipp is None:
        ipp = dset.get('ImagePositionPatient', [0.0, 0.0, 0.0])

    ipp = np.asarray(ipp, dtype=float)
    if ipp.size != 3:
        ipp = np.asarray([0.0, 0.0, 0.0], dtype=float)

    return ipp


def _get_slice_location(dset):
    row_dir, col_dir = _get_image_orientation(dset)
    slice_dir = _normalize(np.cross(row_dir, col_dir))
    if np.linalg.norm(slice_dir) == 0:
        return float(dset.get('SliceLocation', 0.0))

    position = _get_image_position(dset)
    return float(np.dot(position, slice_dir))


def _closest_index(values, value):
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return 0
    return int(np.argmin(np.abs(arr - float(value))))


def _parse_acquisition_time_ms(acq_time):
    acq_time = str(acq_time).strip()
    if len(acq_time) < 6:
        return 0

    try:
        h = int(acq_time[0:2])
        m = int(acq_time[2:4])
        s = int(acq_time[4:6])
        frac = float(acq_time[6:]) if len(acq_time) > 6 else 0.0
        return round((h*3600 + m*60 + s + frac) * 1000 / 2.5)
    except Exception:
        return 0


def CreateMrdHeader(dset):
    """Create MRD XML header from a DICOM file"""

    mrdHead = ismrmrd.xsd.ismrmrdHeader()

    mrdHead.measurementInformation                             = ismrmrd.xsd.measurementInformationType()
    mrdHead.measurementInformation.measurementID               = dset.SeriesInstanceUID
    mrdHead.measurementInformation.patientPosition             = dset.PatientPosition
    mrdHead.measurementInformation.protocolName                = dset.SeriesDescription
    mrdHead.measurementInformation.frameOfReferenceUID         = dset.FrameOfReferenceUID

    mrdHead.acquisitionSystemInformation                       = ismrmrd.xsd.acquisitionSystemInformationType()
    mrdHead.acquisitionSystemInformation.systemVendor          = dset.Manufacturer
    mrdHead.acquisitionSystemInformation.systemModel           = dset.ManufacturerModelName
    mrdHead.acquisitionSystemInformation.systemFieldStrength_T = float(dset.MagneticFieldStrength)
    try:
        mrdHead.acquisitionSystemInformation.institutionName       = dset.InstitutionName
    except:
        mrdHead.acquisitionSystemInformation.institutionName       = 'Virtual'
    try:
        mrdHead.acquisitionSystemInformation.stationName       = dset.StationName
    except:
        pass

    mrdHead.experimentalConditions                             = ismrmrd.xsd.experimentalConditionsType()
    mrdHead.experimentalConditions.H1resonanceFrequency_Hz     = int(dset.MagneticFieldStrength*4258e4)

    enc = ismrmrd.xsd.encodingType()
    enc.trajectory                                              = ismrmrd.xsd.trajectoryType('cartesian')
    encSpace                                                    = ismrmrd.xsd.encodingSpaceType()
    encSpace.matrixSize                                         = ismrmrd.xsd.matrixSizeType()
    encSpace.matrixSize.x                                       = dset.Columns
    encSpace.matrixSize.y                                       = dset.Rows
    encSpace.matrixSize.z                                       = 1
    encSpace.fieldOfView_mm                                     = ismrmrd.xsd.fieldOfViewMm()
    row_spacing, col_spacing, slice_thickness                   = _get_pixel_spacing_and_thickness(dset)
    # DICOM PixelSpacing is [row_spacing, col_spacing] => [y, x]
    encSpace.fieldOfView_mm.x                                   = col_spacing * dset.Columns
    encSpace.fieldOfView_mm.y                                   = row_spacing * dset.Rows
    encSpace.fieldOfView_mm.z                                   = slice_thickness
    enc.encodedSpace                                            = encSpace
    enc.reconSpace                                              = encSpace
    enc.encodingLimits                                          = ismrmrd.xsd.encodingLimitsType()
    enc.parallelImaging                                         = ismrmrd.xsd.parallelImagingType()

    enc.parallelImaging.accelerationFactor                      = ismrmrd.xsd.accelerationFactorType()
    if _is_enhanced_mr(dset):
        try:
            mod = dset.SharedFunctionalGroupsSequence[0].MRModifierSequence[0]
            enc.parallelImaging.accelerationFactor.kspace_encoding_step_1 = mod.ParallelReductionFactorInPlane
            enc.parallelImaging.accelerationFactor.kspace_encoding_step_2 = mod.ParallelReductionFactorOutOfPlane
        except Exception:
            enc.parallelImaging.accelerationFactor.kspace_encoding_step_1 = 1
            enc.parallelImaging.accelerationFactor.kspace_encoding_step_2 = 1
    else:
        enc.parallelImaging.accelerationFactor.kspace_encoding_step_1 = 1
        enc.parallelImaging.accelerationFactor.kspace_encoding_step_2 = 1

    mrdHead.encoding.append(enc)

    mrdHead.sequenceParameters                                  = ismrmrd.xsd.sequenceParametersType()

    return mrdHead

def GetDicomFiles(directory):
    """Get path to all DICOMs in a directory and its sub-directories"""
    for entry in os.scandir(directory):
        if entry.is_file() and (entry.path.lower().endswith(".dcm") or entry.path.lower().endswith(".ima")):
            yield entry.path
        elif entry.is_dir():
            yield from GetDicomFiles(entry.path)


def main(args):
    dsetsAll = []
    for entryPath in GetDicomFiles(args.folder):
        dsetsAll.append(pydicom.dcmread(entryPath))

    # Group by series number
    uSeriesNum = np.unique([dset.SeriesNumber for dset in dsetsAll])

    # Re-group series that were split during conversion from multi-frame to single-frame DICOMs
    if all(uSeriesNum > 1000):
        for i in range(len(dsetsAll)):
            dsetsAll[i].SeriesNumber = int(np.floor(dsetsAll[i].SeriesNumber / 1000))
    uSeriesNum = np.unique([dset.SeriesNumber for dset in dsetsAll])

    print("Found %d unique series from %d files in folder %s" % (len(uSeriesNum), len(dsetsAll), args.folder))

    print("Creating MRD XML header from file %s" % dsetsAll[0].filename)
    mrdHead = CreateMrdHeader(dsetsAll[0])
    print(mrdHead.toXML())

    imgAll = [None]*len(uSeriesNum)

    for iSer in range(len(uSeriesNum)):
        dsets = [dset for dset in dsetsAll if dset.SeriesNumber == uSeriesNum[iSer]]

        imgAll[iSer] = [None]*len(dsets)

        # Sort images by instance number, as they may be read out of order
        def get_instance_number(item):
            return item.InstanceNumber
        dsets = sorted(dsets, key=get_instance_number)

        # Build a list of unique geometric slice locations and trigger times.
        # SliceLocation can be absent/inconsistent; project ImagePositionPatient onto slice normal.
        slice_locs = np.asarray([_get_slice_location(dset) for dset in dsets], dtype=float)
        uSliceLoc = np.unique(slice_locs)
        if (uSliceLoc.size > 1) and (not np.isclose(slice_locs[0], uSliceLoc[0])):
            uSliceLoc = uSliceLoc[::-1]

        try:
            # This field may not exist for non-gated sequences
            trig_times = np.asarray([float(dset.TriggerTime) for dset in dsets], dtype=float)
            uTrigTime = np.unique(trig_times)
            if (uTrigTime.size > 1) and (not np.isclose(trig_times[0], uTrigTime[0])):
                uTrigTime = uTrigTime[::-1]
        except Exception:
            trig_times = np.zeros(len(dsets), dtype=float)
            uTrigTime = np.asarray([0.0], dtype=float)

        print("Series %d has %d images with %d slices and %d phases" % (uSeriesNum[iSer], len(dsets), len(uSliceLoc), len(uTrigTime)))

        for iImg in range(len(dsets)):
            tmpDset = dsets[iImg]

            # Create new MRD image instance.
            # pixel_array data has shape [row col], i.e. [y x].
            # from_array() should be called with 'transpose=False' to avoid warnings, and when called
            # with this option, can take input as: [cha z y x], [z y x], or [y x]
            tmpMrdImg = ismrmrd.Image.from_array(tmpDset.pixel_array, transpose=False)
            tmpMeta   = ismrmrd.Meta()

            try:
                tmpMrdImg.image_type                = imtype_map[tmpDset.ImageType[2]]
            except:
                print("Unsupported ImageType %s -- defaulting to IMTYPE_MAGNITUDE" % tmpDset.ImageType[2])
                tmpMrdImg.image_type                = ismrmrd.IMTYPE_MAGNITUDE

            row_spacing, col_spacing, slice_thickness = _get_pixel_spacing_and_thickness(tmpDset)
            row_dir, col_dir = _get_image_orientation(tmpDset)
            slice_dir = _normalize(np.cross(row_dir, col_dir))
            image_position = _get_image_position(tmpDset)

            tmpMrdImg.field_of_view            = (col_spacing*tmpDset.Columns, row_spacing*tmpDset.Rows, slice_thickness)
            tmpMrdImg.position                 = tuple(image_position)
            tmpMrdImg.read_dir                 = tuple(row_dir)
            tmpMrdImg.phase_dir                = tuple(col_dir)
            tmpMrdImg.slice_dir                = tuple(slice_dir)
            tmpMrdImg.acquisition_time_stamp   = _parse_acquisition_time_ms(tmpDset.get('AcquisitionTime', '000000.0'))
            try:
                tmpMrdImg.physiology_time_stamp[0] = round(int(tmpDset.TriggerTime/2.5))
            except:
                pass

            try:
                ImaAbsTablePosition = tmpDset.get_private_item(0x0019, 0x13, 'SIEMENS MR HEADER').value
                tmpMrdImg.patient_table_position = (ctypes.c_float(ImaAbsTablePosition[0]), ctypes.c_float(ImaAbsTablePosition[1]), ctypes.c_float(ImaAbsTablePosition[2]))
            except:
                pass

            tmpMrdImg.image_series_index     = uSeriesNum.tolist().index(tmpDset.SeriesNumber)
            tmpMrdImg.image_index            = tmpDset.get('InstanceNumber', 0)
            tmpMrdImg.slice                  = _closest_index(uSliceLoc, slice_locs[iImg])
            try:
                tmpMrdImg.phase              = _closest_index(uTrigTime, trig_times[iImg])
            except Exception:
                tmpMrdImg.phase              = 0

            try:
                res  = re.search(r'(?<=_v).*$',     tmpDset.SequenceName)
                venc = re.search(r'^\d+',           res.group(0))
                dir  = re.search(r'(?<=\d)[^\d]*$', res.group(0))

                tmpMeta['FlowVelocity']   = float(venc.group(0))
                tmpMeta['FlowDirDisplay'] = venc_dir_map[dir.group(0)]
            except:
                pass

            try:
                tmpMeta['ImageComments'] = tmpDset.ImageComments
            except:
                pass

            tmpMeta['SequenceDescription'] = tmpDset.SeriesDescription

            # Remove pixel data from pydicom class
            del tmpDset['PixelData']

            # Store the complete base64, json-formatted DICOM header so that non-MRD fields can be
            # recapitulated when generating DICOMs from MRD images
            tmpMeta['DicomJson'] = base64.b64encode(tmpDset.to_json().encode('utf-8')).decode('utf-8')

            tmpMrdImg.attribute_string = tmpMeta.serialize()
            imgAll[iSer][iImg] = tmpMrdImg

    # Create an MRD file
    print("Creating MRD file %s with group %s" % (args.outFile, args.outGroup))
    mrdDset = ismrmrd.Dataset(args.outFile, args.outGroup)
    mrdDset._file.require_group(args.outGroup)

    # Write MRD Header
    mrdDset.write_xml_header(bytes(mrdHead.toXML(), 'utf-8'))

    # Write all images
    for iSer in range(len(imgAll)):
        for iImg in range(len(imgAll[iSer])):
            mrdDset.append_image("image_%d" % imgAll[iSer][iImg].image_series_index, imgAll[iSer][iImg])

    mrdDset.close()

if __name__ == '__main__':
    """Basic conversion of a folder of DICOM files to MRD .h5 format"""

    parser = argparse.ArgumentParser(description='Convert DICOMs to MRD file',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('folder',            help='Input folder of DICOMs')
    parser.add_argument('-o', '--outFile',  help='Output MRD file')
    parser.add_argument('-g', '--outGroup', help='Group name in output MRD file')

    parser.set_defaults(**defaults)

    args = parser.parse_args()

    if args.outFile is None:
        args.outFile = os.path.basename(args.folder) + '.h5'

    main(args)
