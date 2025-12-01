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
              'I': ismrmrd.IMTYPE_IMAG,
              'DIXON': ismrmrd.IMTYPE_MAGNITUDE}

# Lookup table between DICOM and Siemens flow directions
venc_dir_map = {'rl'  : 'FLOW_DIR_R_TO_L',
                'lr'  : 'FLOW_DIR_L_TO_R',
                'ap'  : 'FLOW_DIR_A_TO_P',
                'pa'  : 'FLOW_DIR_P_TO_A',
                'fh'  : 'FLOW_DIR_F_TO_H',
                'hf'  : 'FLOW_DIR_H_TO_F',
                'in'  : 'FLOW_DIR_TP_IN',
                'out' : 'FLOW_DIR_TP_OUT'}

def CreateMrdHeader(dset, dsetsAll=None):
    """Create MRD XML header from a DICOM file
    
    Args:
        dset: Primary DICOM dataset to extract header info from
        dsetsAll: Optional list of all DICOM datasets to determine number of slices
    """

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
    
    # Use helper functions to get pixel spacing and slice thickness
    pixel_spacing = get_pixel_spacing(dset)
    slice_thickness = get_slice_thickness(dset)
    
    # Calculate number of slices if we have all datasets
    num_slices = 1
    if dsetsAll is not None and len(dsetsAll) > 0:
        # For Enhanced DICOM (multi-frame), check NumberOfFrames
        if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
            if hasattr(dset, 'NumberOfFrames'):
                num_slices = int(dset.NumberOfFrames)
            elif hasattr(dset, 'PerFrameFunctionalGroupsSequence'):
                num_slices = len(dset.PerFrameFunctionalGroupsSequence)
        else:
            # For standard DICOM, count unique slice locations across all files
            slice_locations = [get_slice_location(d) for d in dsetsAll]
            
            # If any slice locations are None, assume sequential numbering (fallback)
            if None in slice_locations:
                print("Warning: Some images missing slice location in header generation - assuming sequential slices")
                num_slices = len(dsetsAll)
            else:
                num_slices = len(np.unique(slice_locations))
    
    # Set matrix size z-dimension to number of slices
    encSpace.matrixSize.z = num_slices
    encSpace.fieldOfView_mm = ismrmrd.xsd.fieldOfViewMm()
    
    encSpace.fieldOfView_mm.x = pixel_spacing[0] * dset.Rows
    encSpace.fieldOfView_mm.y = pixel_spacing[1] * dset.Columns
    encSpace.fieldOfView_mm.z = slice_thickness * num_slices
    
    enc.encodedSpace                                            = encSpace
    enc.reconSpace                                              = encSpace
    enc.encodingLimits                                          = ismrmrd.xsd.encodingLimitsType()
    enc.parallelImaging                                         = ismrmrd.xsd.parallelImagingType()

    enc.parallelImaging.accelerationFactor                      = ismrmrd.xsd.accelerationFactorType()
    if dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        enc.parallelImaging.accelerationFactor.kspace_encoding_step_1 = dset.SharedFunctionalGroupsSequence[0].MRModifierSequence[0].ParallelReductionFactorInPlane
        enc.parallelImaging.accelerationFactor.kspace_encoding_step_2 = dset.SharedFunctionalGroupsSequence[0].MRModifierSequence[0].ParallelReductionFactorOutOfPlane
    else:
        enc.parallelImaging.accelerationFactor.kspace_encoding_step_1 = 1
        enc.parallelImaging.accelerationFactor.kspace_encoding_step_2 = 1

    mrdHead.encoding.append(enc)

    mrdHead.sequenceParameters                                  = ismrmrd.xsd.sequenceParametersType()

    return mrdHead

def GetDicomFiles(directory):
    """Get path to all DICOMs in a directory and its sub-directories"""
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.is_file() and (entry.path.lower().endswith(".dcm") or entry.path.lower().endswith(".ima")):
                yield entry.path
            elif entry.is_dir():
                yield from GetDicomFiles(entry.path)

def get_slice_location(dset):
    """Extract slice location from various DICOM formats"""

    def _extract_z(position):
        try:
            return float(position[2])
        except (TypeError, ValueError, IndexError):
            return None

    # Enhanced DICOM: prefer ImagePositionPatient from functional groups
    if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        try:
            if hasattr(dset, 'PerFrameFunctionalGroupsSequence') and len(dset.PerFrameFunctionalGroupsSequence) > 0:
                frame_seq = dset.PerFrameFunctionalGroupsSequence[0]
                if hasattr(frame_seq, 'PlanePositionSequence'):
                    pos = frame_seq.PlanePositionSequence[0].ImagePositionPatient
                    z = _extract_z(pos)
                    if z is not None:
                        return z
            if hasattr(dset, 'SharedFunctionalGroupsSequence'):
                if hasattr(dset.SharedFunctionalGroupsSequence[0], 'PlanePositionSequence'):
                    pos = dset.SharedFunctionalGroupsSequence[0].PlanePositionSequence[0].ImagePositionPatient
                    z = _extract_z(pos)
                    if z is not None:
                        return z
        except Exception:
            pass

    # Standard DICOM: prioritize ImagePositionPatient; SliceLocation can be quantized/constant
    if hasattr(dset, 'ImagePositionPatient'):
        z = _extract_z(dset.ImagePositionPatient)
        if z is not None:
            return z

    # Fall back to SliceLocation if no positional info is available
    if hasattr(dset, 'SliceLocation'):
        try:
            return float(dset.SliceLocation)
        except (TypeError, ValueError):
            pass

    # If all else fails, return None
    return None

def get_pixel_spacing(dset):
    """Extract pixel spacing from various DICOM formats"""
    # Check for standard PixelSpacing
    if hasattr(dset, 'PixelSpacing'):
        return dset.PixelSpacing
    
    # Check for enhanced DICOM with PerFrameFunctionalGroupsSequence
    if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        try:
            # Extract from PerFrameFunctionalGroupsSequence (should have only this frame's data)
            if hasattr(dset, 'PerFrameFunctionalGroupsSequence') and len(dset.PerFrameFunctionalGroupsSequence) > 0:
                frame_seq = dset.PerFrameFunctionalGroupsSequence[0]
                if hasattr(frame_seq, 'PixelMeasuresSequence'):
                    return frame_seq.PixelMeasuresSequence[0].PixelSpacing
            # Check SharedFunctionalGroupsSequence as an alternative
            if hasattr(dset, 'SharedFunctionalGroupsSequence'):
                if hasattr(dset.SharedFunctionalGroupsSequence[0], 'PixelMeasuresSequence'):
                    return dset.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0].PixelSpacing
        except:
            pass
    
    # If all else fails, return default values
    print(f"Warning: No pixel spacing found for file {dset.filename if hasattr(dset, 'filename') else 'unknown'}")
    return [1.0, 1.0]  # Default 1mm spacing

def get_slice_thickness(dset):
    """Extract slice thickness from various DICOM formats"""
    # Check for standard SliceThickness
    if hasattr(dset, 'SliceThickness'):
        return float(dset.SliceThickness)
    
    # Check for enhanced DICOM with PerFrameFunctionalGroupsSequence
    if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        try:
            # Extract from PerFrameFunctionalGroupsSequence (should have only this frame's data)
            if hasattr(dset, 'PerFrameFunctionalGroupsSequence') and len(dset.PerFrameFunctionalGroupsSequence) > 0:
                frame_seq = dset.PerFrameFunctionalGroupsSequence[0]
                if hasattr(frame_seq, 'PixelMeasuresSequence'):
                    return float(frame_seq.PixelMeasuresSequence[0].SliceThickness)
            # Check SharedFunctionalGroupsSequence as an alternative
            if hasattr(dset, 'SharedFunctionalGroupsSequence'):
                if hasattr(dset.SharedFunctionalGroupsSequence[0], 'PixelMeasuresSequence'):
                    return float(dset.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0].SliceThickness)
        except:
            pass
    
    # If all else fails, return default value
    print(f"Warning: No slice thickness found for file {dset.filename if hasattr(dset, 'filename') else 'unknown'}")
    return 1.0  # Default 1mm thickness

def get_image_position(dset):
    """Extract image position from various DICOM formats"""
    # Check for standard ImagePositionPatient
    if hasattr(dset, 'ImagePositionPatient'):
        return np.stack(dset.ImagePositionPatient)
    
    # Check for enhanced DICOM with PerFrameFunctionalGroupsSequence
    if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        try:
            # Extract from PerFrameFunctionalGroupsSequence (should have only this frame's data)
            if hasattr(dset, 'PerFrameFunctionalGroupsSequence') and len(dset.PerFrameFunctionalGroupsSequence) > 0:
                frame_seq = dset.PerFrameFunctionalGroupsSequence[0]
                if hasattr(frame_seq, 'PlanePositionSequence'):
                    return np.stack(frame_seq.PlanePositionSequence[0].ImagePositionPatient)
            # Check SharedFunctionalGroupsSequence as an alternative
            if hasattr(dset, 'SharedFunctionalGroupsSequence'):
                if hasattr(dset.SharedFunctionalGroupsSequence[0], 'PlanePositionSequence'):
                    return np.stack(dset.SharedFunctionalGroupsSequence[0].PlanePositionSequence[0].ImagePositionPatient)
        except:
            pass
    
    # If all else fails, return default values
    print(f"Warning: No image position found for file {dset.filename if hasattr(dset, 'filename') else 'unknown'}")
    return np.array([0.0, 0.0, 0.0])  # Default position at origin

def get_image_orientation(dset):
    """Extract image orientation from various DICOM formats"""
    # Check for standard ImageOrientationPatient
    if hasattr(dset, 'ImageOrientationPatient'):
        return np.stack(dset.ImageOrientationPatient)
    
    # Check for enhanced DICOM with PerFrameFunctionalGroupsSequence
    if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        try:
            # Extract from PerFrameFunctionalGroupsSequence (should have only this frame's data)
            if hasattr(dset, 'PerFrameFunctionalGroupsSequence') and len(dset.PerFrameFunctionalGroupsSequence) > 0:
                frame_seq = dset.PerFrameFunctionalGroupsSequence[0]
                if hasattr(frame_seq, 'PlaneOrientationSequence'):
                    return np.stack(frame_seq.PlaneOrientationSequence[0].ImageOrientationPatient)
            # Check SharedFunctionalGroupsSequence as an alternative
            if hasattr(dset, 'SharedFunctionalGroupsSequence'):
                if hasattr(dset.SharedFunctionalGroupsSequence[0], 'PlaneOrientationSequence'):
                    return np.stack(dset.SharedFunctionalGroupsSequence[0].PlaneOrientationSequence[0].ImageOrientationPatient)
        except:
            pass
    
    # If all else fails, return default values (axial orientation)
    print(f"Warning: No image orientation found for file {dset.filename if hasattr(dset, 'filename') else 'unknown'}")
    return np.array([1.0, 0.0, 0.0, 0.0, 1.0, 0.0])  # Default axial orientation

def get_acquisition_time(dset):
    """Extract acquisition time from various DICOM formats"""
    # Check for standard AcquisitionTime
    if hasattr(dset, 'AcquisitionTime') and dset.AcquisitionTime:
        return dset.AcquisitionTime
    
    # Check for enhanced DICOM - look in various functional groups
    if hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage':
        try:
            # Try to get from PerFrameFunctionalGroupsSequence
            if hasattr(dset, 'PerFrameFunctionalGroupsSequence'):
                if hasattr(dset.PerFrameFunctionalGroupsSequence[0], 'MRAcquisitionSequence'):
                    return dset.PerFrameFunctionalGroupsSequence[0].MRAcquisitionSequence[0].AcquisitionDateTime.split('T')[1]
                
            # Try SharedFunctionalGroupsSequence
            if hasattr(dset, 'SharedFunctionalGroupsSequence'):
                if hasattr(dset.SharedFunctionalGroupsSequence[0], 'MRAcquisitionSequence'):
                    return dset.SharedFunctionalGroupsSequence[0].MRAcquisitionSequence[0].AcquisitionDateTime.split('T')[1]
                
            # Try ContentTime as fallback
            if hasattr(dset, 'ContentTime'):
                return dset.ContentTime
        except:
            pass
    
    # Try other common time fields as fallbacks
    for time_field in ['AcquisitionDateTime', 'ContentTime', 'InstanceCreationTime', 'SeriesTime', 'StudyTime']:
        if hasattr(dset, time_field) and getattr(dset, time_field):
            # If it's a datetime field, extract just the time part
            time_value = getattr(dset, time_field)
            if 'T' in time_value:
                return time_value.split('T')[1]
            return time_value
    
    # If all else fails
    print(f"Warning: No acquisition time found for file {dset.filename if hasattr(dset, 'filename') else 'unknown'}")
    return "000000.000000"  # Midnight as default

def expand_enhanced_dicom(dset):
    """Expand Enhanced DICOM multi-frame file into individual frame datasets
    
    Args:
        dset: Enhanced DICOM dataset with multiple frames
        
    Returns:
        List of individual frame datasets (or original dataset if not Enhanced)
    """
    # Check if this is an Enhanced DICOM
    if not (hasattr(dset, 'SOPClassUID') and dset.SOPClassUID.name == 'Enhanced MR Image Storage'):
        return [dset]  # Not enhanced, return as-is
    
    if not hasattr(dset, 'NumberOfFrames'):
        return [dset]  # No frames info, return as-is
    
    num_frames = int(dset.NumberOfFrames)
    if num_frames <= 1:
        return [dset]  # Single frame, return as-is
    
    print(f"Expanding Enhanced DICOM with {num_frames} frames")
    
    # Pre-load the full pixel array once
    full_array = dset.pixel_array

    # OPTIMIZATION: Remove PixelData to save memory and copy time
    if 'PixelData' in dset:
        del dset['PixelData']
    if hasattr(dset, '_pixel_array'):
        del dset._pixel_array

    # OPTIMIZATION: Handle PerFrameFunctionalGroupsSequence efficiently
    # Detach it from the source to avoid deep copying it N times
    per_frame_seq = None
    if hasattr(dset, 'PerFrameFunctionalGroupsSequence'):
        per_frame_seq = dset.PerFrameFunctionalGroupsSequence
        del dset.PerFrameFunctionalGroupsSequence

    from pydicom.sequence import Sequence
    
    # Restore PerFrameFunctionalGroupsSequence to the source dset FIRST
    # This is needed so we can extract data from it for each frame
    if per_frame_seq:
        dset.PerFrameFunctionalGroupsSequence = per_frame_seq
    
    expanded_dsets = []
    for frame_idx in range(num_frames):
        # Create a shallow copy of the dataset
        frame_dset = dset.copy()

        # Keep only this frame's functional group
        if per_frame_seq:
            frame_dset.PerFrameFunctionalGroupsSequence = Sequence([per_frame_seq[frame_idx]])
        
        # Extract frame-specific pixel data
        if len(full_array.shape) >= 3:  # Multi-frame array
            frame_pixel_data = full_array[frame_idx]
        else:
            frame_pixel_data = full_array
        
        # Store frame-specific information WITHOUT keeping reference to source dataset
        # This prevents the entire multi-frame dataset from being kept in memory/storage
        object.__setattr__(frame_dset, '_frame_index', frame_idx)
        object.__setattr__(frame_dset, '_frame_pixel_array', frame_pixel_data)
        
        # Update InstanceNumber to be unique per frame
        if hasattr(dset, 'InstanceNumber'):
            frame_dset.InstanceNumber = dset.InstanceNumber * 1000 + frame_idx
        else:
            frame_dset.InstanceNumber = frame_idx
        
        expanded_dsets.append(frame_dset)
    
    return expanded_dsets

def main(args):
    dsetsAll = []
    for entryPath in GetDicomFiles(args.folder):
        dset = pydicom.dcmread(entryPath)
        # Expand Enhanced DICOM files into individual frames
        expanded = expand_enhanced_dicom(dset)
        dsetsAll.extend(expanded)

    # Group by series number
    uSeriesNum = np.unique([dset.SeriesNumber for dset in dsetsAll])

    # Re-group series that were split during conversion from multi-frame to single-frame DICOMs
    if all(uSeriesNum > 1000):
        for i in range(len(dsetsAll)):
            dsetsAll[i].SeriesNumber = int(np.floor(dsetsAll[i].SeriesNumber / 1000))
    uSeriesNum = np.unique([dset.SeriesNumber for dset in dsetsAll])

    print("Found %d unique series from %d files in folder %s" % (len(uSeriesNum), len(dsetsAll), args.folder))

    # Group datasets by series once so we can reuse the grouping below
    series_dict = {ser: [dset for dset in dsetsAll if dset.SeriesNumber == ser] for ser in uSeriesNum}

    # Use the largest series as the canonical one for header generation to avoid localizer bias
    primary_series_num = max(series_dict, key=lambda ser: len(series_dict[ser]))
    primary_series = series_dict[primary_series_num]

    print("Creating MRD XML header from file %s" % primary_series[0].filename)
    mrdHead = CreateMrdHeader(primary_series[0], primary_series)
    print(mrdHead.toXML())

    # Create an MRD file
    print("Creating MRD file %s with group %s" % (args.outFile, args.outGroup))
    mrdDset = ismrmrd.Dataset(args.outFile, args.outGroup)
    mrdDset._file.require_group(args.outGroup)

    # Write MRD Header
    mrdDset.write_xml_header(bytes(mrdHead.toXML(), 'utf-8'))

    for iSer in range(len(uSeriesNum)):
        dsets = series_dict[uSeriesNum[iSer]]

        # Sort images by instance number, as they may be read out of order
        def get_instance_number(item):
            return item.InstanceNumber
        dsets = sorted(dsets, key=get_instance_number)

        # Build a list of unique SliceLocation and TriggerTimes, as the MRD
        # slice and phase counters index into these
        slice_locations = [get_slice_location(dset) for dset in dsets]
        
        # If any slice locations are None, create artificial locations
        if None in slice_locations:
            print("Warning: Some images missing slice location - using sequential numbering")
            slice_locations = list(range(len(dsets)))
        
        uSliceLoc = np.unique(slice_locations)
        if slice_locations[0] != uSliceLoc[0]:
            uSliceLoc = uSliceLoc[::-1]

        try:
            # This field may not exist for non-gated sequences
            uTrigTime = np.unique([getattr(dset, 'TriggerTime', 0) for dset in dsets])
            if hasattr(dsets[0], 'TriggerTime') and dsets[0].TriggerTime != uTrigTime[0]:
                uTrigTime = uTrigTime[::-1]
        except:
            uTrigTime = np.zeros_like(uSliceLoc)

        print("Series %d has %d images with %d slices and %d phases" % (uSeriesNum[iSer], len(dsets), len(uSliceLoc), len(uTrigTime)))

        for iImg in range(len(dsets)):
            # Progress indicator
            if iImg % 10 == 0 or iImg == len(dsets) - 1:
                progress = (iImg + 1) / len(dsets) * 100
                print(f"  Processing image {iImg + 1}/{len(dsets)} ({progress:.1f}%)", end='\r' if iImg < len(dsets) - 1 else '\n')
            
            tmpDset = dsets[iImg]

            # Create new MRD image instance.
            # pixel_array data has shape [row col], i.e. [y x].
            # from_array() should be called with 'transpose=False' to avoid warnings, and when called
            # with this option, can take input as: [cha z y x], [z y x], or [y x]
            # Use frame-specific pixel array if this is an expanded Enhanced DICOM frame
            if hasattr(tmpDset, '_frame_pixel_array'):
                pixel_data = tmpDset._frame_pixel_array
            else:
                # For non-expanded datasets, access pixel_array directly
                # Note: This must happen before PixelData is deleted
                pixel_data = tmpDset.pixel_array
            
            tmpMrdImg = ismrmrd.Image.from_array(pixel_data, transpose=False)
            tmpMeta   = ismrmrd.Meta()
            
            # Immediately set the correct matrix size after creation
            # This is critical because from_array() defaults z dimension to 1
            head = tmpMrdImg.getHead()
            head.matrix_size[0] = int(tmpDset.Columns)
            head.matrix_size[1] = int(tmpDset.Rows) 
            head.matrix_size[2] = int(len(uSliceLoc))
            tmpMrdImg.setHead(head)

            try:
                tmpMrdImg.image_type = imtype_map[tmpDset.ImageType[2]]
            except:
                print("Unsupported ImageType %s -- defaulting to IMTYPE_MAGNITUDE" % tmpDset.ImageType[2])
                tmpMrdImg.image_type = ismrmrd.IMTYPE_MAGNITUDE

            try:
                # Get pixel spacing and slice thickness with proper handling for enhanced DICOM
                pixel_spacing = get_pixel_spacing(tmpDset)
                slice_thickness = get_slice_thickness(tmpDset)
                
                # pixel_spacing[0] is row spacing (y-direction), pixel_spacing[1] is column spacing (x-direction)
                # z-FOV should be slice_thickness * number of slices
                tmpMrdImg.field_of_view = (
                    pixel_spacing[1] * tmpDset.Columns,
                    pixel_spacing[0] * tmpDset.Rows,
                    slice_thickness * len(uSliceLoc)
                )
            except Exception as e:
                print(f"Error setting field_of_view: {e} - using defaults")
                tmpMrdImg.field_of_view = (
                    tmpDset.Columns,  # Default to 1mm spacing
                    tmpDset.Rows,
                    1.0  # Default slice thickness
                )
                
            try:
                # Get image position and orientation with proper handling for enhanced DICOM
                image_position = get_image_position(tmpDset)
                image_orientation = get_image_orientation(tmpDset)
                
                tmpMrdImg.position = tuple(image_position)
                tmpMrdImg.read_dir = tuple(image_orientation[0:3])
                tmpMrdImg.phase_dir = tuple(image_orientation[3:6])
                tmpMrdImg.slice_dir = tuple(np.cross(image_orientation[0:3], image_orientation[3:6]))
            except Exception as e:
                print(f"Error setting position/orientation: {e} - using defaults")
                # Default to standard orientation (axial)
                tmpMrdImg.position = (0.0, 0.0, 0.0)
                tmpMrdImg.read_dir = (1.0, 0.0, 0.0)
                tmpMrdImg.phase_dir = (0.0, 1.0, 0.0)
                tmpMrdImg.slice_dir = (0.0, 0.0, 1.0)
            
            try:
                # Get acquisition time with proper handling for enhanced DICOM
                acq_time = get_acquisition_time(tmpDset)
                
                # Parse the acquisition time string into hours, minutes, seconds
                if len(acq_time) >= 6:  # Make sure we have at least HHMMSS format
                    hours = int(acq_time[0:2])
                    minutes = int(acq_time[2:4])
                    seconds = float(acq_time[4:])
                    tmpMrdImg.acquisition_time_stamp = round((hours*3600 + minutes*60 + seconds)*1000/2.5)
                else:
                    # If format is unexpected, use a default timestamp
                    print(f"Warning: Unexpected acquisition time format: {acq_time}")
                    tmpMrdImg.acquisition_time_stamp = 0
            except Exception as e:
                print(f"Error setting acquisition_time_stamp: {e} - using default")
                tmpMrdImg.acquisition_time_stamp = 0
            
            try:
                tmpMrdImg.physiology_time_stamp[0] = round(int(getattr(tmpDset, 'TriggerTime', 0)/2.5))
            except:
                pass

            try:
                ImaAbsTablePosition = tmpDset.get_private_item(0x0019, 0x13, 'SIEMENS MR HEADER').value
                tmpMrdImg.patient_table_position = (ctypes.c_float(ImaAbsTablePosition[0]), ctypes.c_float(ImaAbsTablePosition[1]), ctypes.c_float(ImaAbsTablePosition[2]))
            except:
                pass

            tmpMrdImg.image_series_index     = uSeriesNum.tolist().index(tmpDset.SeriesNumber)
            tmpMrdImg.image_index            = tmpDset.get('InstanceNumber', 0)
            
            # Use the same slice location extraction for consistency
            loc = get_slice_location(tmpDset)
            if loc is not None:
                tmpMrdImg.slice = np.where(uSliceLoc == loc)[0][0]
            else:
                tmpMrdImg.slice = iImg % len(uSliceLoc)
                
            try:
                if hasattr(tmpDset, 'TriggerTime'):
                    tmpMrdImg.phase = uTrigTime.tolist().index(tmpDset.TriggerTime)
                else:
                    tmpMrdImg.phase = 0
            except:
                pass

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
            tmpMeta['TotalSlices'] = len(uSliceLoc)

            tmpMrdImg.attribute_string = tmpMeta.serialize()
            mrdDset.append_image("image_%d" % tmpMrdImg.image_series_index, tmpMrdImg)

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
