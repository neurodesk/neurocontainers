import ismrmrd
import os
import itertools
import logging
import traceback
import numpy as np
import numpy.fft as fft
import matplotlib.pyplot as plt
import xml.dom.minidom
import base64
import ctypes
import re
import mrdhelper
import constants
from time import perf_counter
import nibabel as nib
import subprocess
from scipy.ndimage import binary_erosion, binary_dilation, gaussian_filter
from skimage.measure import label

# Folder for debug output files
debugFolder = "/tmp/share/debug"

def process(connection, config, mrdHeader):
    logging.info("Config: \n%s", config)

    # mrdHeader should be xml formatted MRD header, but may be a string
    # if it failed conversion earlier
    try:
        # Disabled due to incompatibility between PyXB and Python 3.8:
        # https://github.com/pabigot/pyxb/issues/123
        # # logging.info("MRD header: \n%s", mrdHeader.toxml('utf-8'))

        logging.info("Incoming dataset contains %d encodings", len(mrdHeader.encoding))
        logging.info("First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3", 
            mrdHeader.encoding[0].trajectory, 
            mrdHeader.encoding[0].encodedSpace.matrixSize.x, 
            mrdHeader.encoding[0].encodedSpace.matrixSize.y, 
            mrdHeader.encoding[0].encodedSpace.matrixSize.z, 
            mrdHeader.encoding[0].encodedSpace.fieldOfView_mm.x, 
            mrdHeader.encoding[0].encodedSpace.fieldOfView_mm.y, 
            mrdHeader.encoding[0].encodedSpace.fieldOfView_mm.z)

    except:
        logging.info("Improperly formatted MRD header: \n%s", mrdHeader)

    # Continuously parse incoming data parsed from MRD messages
    currentSeries = 0
    acqGroup = []
    imgGroup = []
    waveformGroup = []
    try:
        for item in connection:
            # ----------------------------------------------------------
            # Raw k-space data messages
            # ----------------------------------------------------------
            if isinstance(item, ismrmrd.Acquisition):
                # Accumulate all imaging readouts in a group
                if (not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)):
                    acqGroup.append(item)

                # When this criteria is met, run process_raw() on the accumulated
                # data, which returns images that are sent back to the client.
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    logging.info("Processing a group of k-space data")
                    image = process_raw(acqGroup, connection, config, mrdHeader)
                    connection.send_image(image)
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                # When this criteria is met, run process_group() on the accumulated
                # data, which returns images that are sent back to the client.
                # e.g. when the series number changes:
                # KP: This is good for EPI but not sensible when there are multiple series arriving simultaneously.
                # if item.image_series_index != currentSeries:
                # logging.info("Processing a group of images because series index changed to %d", item.image_series_index)
                # currentSeries = item.image_series_index
                # image = process_image(imgGroup, connection, config, mrdHeader)
                # connection.send_image(image)
                # imgGroup = []

                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imgGroup.append(item)
                else:
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta['Keep_image_geometry']    = 1
                    item.attribute_string = tmpMeta.serialize()

                    connection.send_image(item)
                    continue

            # ----------------------------------------------------------
            # Waveform data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Waveform):
                waveformGroup.append(item)

            elif item is None:
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        # Extract raw ECG waveform data. Basic sorting to make sure that data 
        # is time-ordered, but no additional checking for missing data.
        # ecgData has shape (5 x timepoints)
        if len(waveformGroup) > 0:
            waveformGroup.sort(key = lambda item: item.time_stamp)
            ecgData = [item.data for item in waveformGroup if item.waveform_id == 0]
            if len(ecgData) > 0:
                ecgData = np.concatenate(ecgData,1)

        # Process any remaining groups of raw or image data.  This can 
        # happen if the trigger condition for these groups are not met.
        # This is also a fallback for handling image data, as the last
        # image in a series is typically not separately flagged.
        if len(acqGroup) > 0:
            logging.info("Processing a group of k-space data (untriggered)")
            image = process_raw(acqGroup, connection, config, mrdHeader)
            connection.send_image(image)
            acqGroup = []

        if len(imgGroup) > 0:
            logging.info("Processing a group of images (untriggered)")
            logging.info(mrdHeader)
            # raise Exception("Image group not empty at end of processing loop")
            image = process_image(imgGroup, connection, config, mrdHeader)
            connection.send_image(image)
            imgGroup = []

    except Exception as e:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()


def process_raw(acqGroup, connection, config, mrdHeader):
    if len(acqGroup) == 0:
        return []
    
    logging.info(f'-----------------------------------------------')
    logging.info(f'     process_raw called with {len(acqGroup)} readouts')
    logging.info(f'-----------------------------------------------')

    # Start timer
    tic = perf_counter()

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")

    # Format data into single [cha PE RO phs] array
    lin = [acquisition.idx.kspace_encode_step_1 for acquisition in acqGroup]
    phs = [acquisition.idx.phase                for acquisition in acqGroup]

    # Use the zero-padded matrix size
    data = np.zeros((acqGroup[0].data.shape[0], 
                     mrdHeader.encoding[0].encodedSpace.matrixSize.y, 
                     mrdHeader.encoding[0].encodedSpace.matrixSize.x, 
                     max(phs)+1), 
                    acqGroup[0].data.dtype)

    rawHead = [None]*(max(phs)+1)

    for acq, lin, phs in zip(acqGroup, lin, phs):
        if (lin < data.shape[1]) and (phs < data.shape[3]):
            # TODO: Account for asymmetric echo in a better way
            data[:,lin,-acq.data.shape[1]:,phs] = acq.data

            # center line of k-space is encoded in user[5]
            if (rawHead[phs] is None) or (np.abs(acq.getHead().idx.kspace_encode_step_1 - acq.getHead().idx.user[5]) < np.abs(rawHead[phs].idx.kspace_encode_step_1 - rawHead[phs].idx.user[5])):
                rawHead[phs] = acq.getHead()

    # Flip matrix in RO/PE to be consistent with ICE
    data = np.flip(data, (1, 2))

    logging.debug("Raw data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "raw.npy", data)

    # Fourier Transform
    data = fft.fftshift( data, axes=(1, 2))
    data = fft.ifft2(    data, axes=(1, 2))
    data = fft.ifftshift(data, axes=(1, 2))
    data *= np.prod(data.shape) # FFT scaling for consistency with ICE

    # Sum of squares coil combination
    # Data will be [PE RO phs]
    data = np.abs(data)
    data = np.square(data)
    data = np.sum(data, axis=0)
    data = np.sqrt(data)

    logging.debug("Image data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "img.npy", data)

    # Remove readout oversampling
    if mrdHeader.encoding[0].reconSpace.matrixSize.x != 0:
        offset = int((data.shape[1] - mrdHeader.encoding[0].reconSpace.matrixSize.x)/2)
        data = data[:,offset:offset+mrdHeader.encoding[0].reconSpace.matrixSize.x]

    # Remove phase oversampling
    if mrdHeader.encoding[0].reconSpace.matrixSize.y != 0:
        offset = int((data.shape[0] - mrdHeader.encoding[0].reconSpace.matrixSize.y)/2)
        data = data[offset:offset+mrdHeader.encoding[0].reconSpace.matrixSize.y,:]

    logging.debug("Image without oversampling is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "imgCrop.npy", data)

    # Measure processing time
    toc = perf_counter()
    strProcessTime = "Total processing time: %.2f ms" % ((toc-tic)*1000.0)
    logging.info(strProcessTime)

    # Send this as a text message back to the client
    connection.send_logging(constants.MRD_LOGGING_INFO, strProcessTime)

    # Format as ISMRMRD image data
    imagesOut = []
    for phs in range(data.shape[2]):
        # Create new MRD instance for the processed image
        # data has shape [PE RO phs], i.e. [y x].
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        tmpImg = ismrmrd.Image.from_array(data[...,phs], transpose=False)

        # Set the header information
        tmpImg.setHead(mrdhelper.update_img_header_from_raw(tmpImg.getHead(), rawHead[phs]))
        tmpImg.field_of_view = (ctypes.c_float(mrdHeader.encoding[0].reconSpace.fieldOfView_mm.x), 
                                ctypes.c_float(mrdHeader.encoding[0].reconSpace.fieldOfView_mm.y), 
                                ctypes.c_float(mrdHeader.encoding[0].reconSpace.fieldOfView_mm.z))
        tmpImg.image_index = phs

        # Set ISMRMRD Meta Attributes
        tmpMeta = ismrmrd.Meta()
        tmpMeta['DataRole']               = 'Image'
        tmpMeta['ImageProcessingHistory'] = ['FIRE', 'PYTHON']
        tmpMeta['Keep_image_geometry']    = 1

        xml = tmpMeta.serialize()
        logging.debug("Image MetaAttributes: %s", xml)
        tmpImg.attribute_string = xml
        imagesOut.append(tmpImg)

    # Call process_image() to invert image contrast
    imagesOut = process_image(imagesOut, connection, config, mrdHeader)

    return imagesOut


def process_image(imgGroup, connection, config, mrdHeader):
    if len(imgGroup) == 0:
        logging.debug("Empty imgGroup, returning")
        return []

    logging.info(f'-----------------------------------------------')
    logging.info(f'     process_image called with {len(imgGroup)} images')
    logging.info(f'-----------------------------------------------')

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")

    logging.debug("Processing data with %d images of type %s", len(imgGroup), ismrmrd.get_dtype_from_data_type(imgGroup[0].data_type))

    # Note: The MRD Image class stores data as [cha z y x]

    # Extract image data into a 5D array of size [img cha z y x]
    data = np.stack([img.data                              for img in imgGroup])
    head = [img.getHead()                                  for img in imgGroup]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in imgGroup]

    # Reformat data to [y x z cha img], i.e. [row col] for the first two dimensions
    # data = data.transpose((3, 4, 2, 1, 0))

    # Reformat data to [y x img cha z], i.e. [row ~col] for the first two dimensions
    data = data.transpose((3, 4, 0, 1, 2))

    # Display MetaAttributes for first image
    # KP: This needs to be tested on the scanner, testing with replayed DICOMs is no good because meta contains DicomJson inside XML
    logging.debug('Try logging meta')
    logging.debug("MetaAttributes[0]: %s", ismrmrd.Meta.serialize(meta[0]))

    # Optional serialization of ICE MiniHeader
    # logging.debug('Try logging minihead')
    if 'IceMiniHead' in meta[0]:
        logging.debug("IceMiniHead[0]: %s", base64.b64decode(meta[0]['IceMiniHead']).decode('utf-8'))


    logging.debug("Stebo: Original image data is size %s" % (data.shape,))
    # e.g. gre with 128x128x10 with phase and magnitude results in [128 128 1 1 1]
    # np.save(debugFolder + "/" + "imgOrig.npy", data)

    logging.debug('Do the afi stuff.')

    # Parameters come first
    tr_ratio   = 5
    nominal_fa = 60

    # Trying to get it from the header. This will not work with DICOM data but should work on the scanner.
    # Log them but don't use them. In future, we can set some more stuff automatically.
    TR_array = getattr(mrdHeader.sequenceParameters, "TR", None)
    flipAngle_deg = getattr(mrdHeader.sequenceParameters, "flipAngle_deg", None)
    logging.debug("TR_array is %s", TR_array)
    logging.debug("flipAngle_deg is %s", flipAngle_deg)

    opre_sendoriginal = mrdhelper.get_json_config_param(config, 'sendoriginal', default=False, type='bool')
    opre_interleaved = mrdhelper.get_json_config_param(config, 'interleaved', default=False, type='bool')
    opre_b1output = mrdhelper.get_json_config_param(config, 'b1output', default='pu', type='str')
    tr_ratio = mrdhelper.get_json_config_param(config, 'tr_ratio', default=5.0, type='float')
    nominal_fa = mrdhelper.get_json_config_param(config, 'nominal_fa', default=60.0, type='float')
    opre_brainmask = mrdhelper.get_json_config_param(config, 'brainmask', default=True, type='bool')
    opre_mask_fwhm = mrdhelper.get_json_config_param(config, 'maskfwhm', default=4.0, type='float')
    opre_mask_nerode = mrdhelper.get_json_config_param(config, 'masknerode', default=2, type='int')
    opre_mask_ndilate = mrdhelper.get_json_config_param(config, 'maskndilate', default=4, type='int')
    opre_mask_thresh = mrdhelper.get_json_config_param(config, 'mask_thresh', default=0.6, type='float')
    opre_signal_thresh = mrdhelper.get_json_config_param(config, 'signalthresh', default=0.1, type='float')
    opre_b1fwhm = mrdhelper.get_json_config_param(config, 'b1fwhm', default=6.0, type='float')

    voxel_sizes = (
        mrdHeader.encoding[0].encodedSpace.fieldOfView_mm.y / mrdHeader.encoding[0].encodedSpace.matrixSize.y,
        mrdHeader.encoding[0].encodedSpace.fieldOfView_mm.x / mrdHeader.encoding[0].encodedSpace.matrixSize.x,
        mrdHeader.encoding[0].encodedSpace.fieldOfView_mm.z / mrdHeader.encoding[0].encodedSpace.matrixSize.z
    )

    if opre_interleaved:
        # Convert to float to avoid integer division issues later
        data_tr1 = np.squeeze(data.astype(np.float32)[:,:,::2,0,0])
        data_tr2 = np.squeeze(data.astype(np.float32)[:,:,1::2,0,0])
    else:
        # KP: This would split the data into first half and second half but how is it on the scanner?
        data_tr1,data_tr2 = np.split(np.squeeze(data.astype(np.float32)),2,axis=2)

    # For debugging and masking write out with nibabel
    xform = np.eye(4)
    tr1_img = nib.nifti1.Nifti1Image(data_tr1, xform)
    tr2_img = nib.nifti1.Nifti1Image(data_tr2, xform)
    nib.save(tr1_img, 'nifti_tr1_image.nii')
    nib.save(tr2_img, 'nifti_tr2_image.nii')

    # Masking
    if opre_brainmask:
        subprocess.run(['bet2', 'nifti_tr1_image.nii', 'brain_tr1'], check=True)
        subprocess.run(['bet2', 'nifti_tr2_image.nii', 'brain_tr2'], check=True)
        mask1 = nib.load('brain_tr1_mask.nii.gz').get_fdata().astype(bool)
        mask2 = nib.load('brain_tr2_mask.nii.gz').get_fdata().astype(bool)
        # combined_mask = np.logical_or(mask1,mask2)
        combined_mask = mask1

        for _ in range(opre_mask_nerode):
            combined_mask = binary_erosion(combined_mask)

        # Keep largest connected component (the head)
        labeled = label(combined_mask)
        sizes = np.bincount(labeled.ravel())
        sizes[0] = 0  # ignore background
        combined_mask = labeled == np.argmax(sizes)

        for _ in range(opre_mask_ndilate):
            combined_mask = binary_dilation(combined_mask)

        # ---- Remove largest external component (background) ----
        inverted = ~combined_mask
        labeled = label(inverted)
        sizes = np.bincount(labeled.ravel())
        sizes[0] = 0
        outside = labeled == np.argmax(sizes)
        final_mask = ~outside

        # ---- Smooth and threshold the mask ----
        sigma_mask = (opre_mask_fwhm / (2 * np.sqrt(2 * np.log(2)))) / np.array(voxel_sizes[:3])
        smoothed_mask = gaussian_filter(final_mask.astype(np.float32), sigma=sigma_mask)
        final_mask = smoothed_mask > opre_mask_thresh

    # Processing of AFI data
    # We want acosd((r*n-1)./(n-r)) where r=image2/image1 and e.g. n=10
 
    # Create a mask for valid division (data_tr1 must not be near zero)
    valid_mask = data_tr1 > opre_signal_thresh

    # Initialize signal_ratio array
    signal_ratio = np.zeros_like(data_tr1, dtype=np.float32)

    # Compute signal ratio only where safe
    signal_ratio[valid_mask] = data_tr2[valid_mask] / data_tr1[valid_mask]
    
    # Calculate numerator and denominator
    numerator = tr_ratio * signal_ratio - 1.0
    denominator = tr_ratio - signal_ratio

    # Safely compute the ratio term
    epsilon = 1e-6
    ratio_term = np.zeros_like(numerator, dtype=np.float32)
    valid_denominator = np.abs(denominator) > epsilon
    ratio_term[valid_denominator] = numerator[valid_denominator] / denominator[valid_denominator]

    # Clip ratio term to be in the valid input range for arccos
    ratio_term_clipped = np.clip(ratio_term, -1.0, 1.0)

    # Compute the flip angle in degrees
    actual_fa = np.degrees(np.arccos(ratio_term_clipped))

    # Apply mask to B1 map first
    if opre_brainmask:
        actual_fa = actual_fa * final_mask

        if (opre_b1fwhm > 0.1):
            # Smooth both B1 map and mask
            sigma_vox = (opre_b1fwhm / (2 * np.sqrt(2 * np.log(2)))) / np.array(voxel_sizes[:3])
            actual_fa = gaussian_filter(actual_fa, sigma=sigma_vox)
            smoothed_mask = gaussian_filter(final_mask.astype(np.float32), sigma=sigma_vox)
            # Normalize to fix smoothing edge effects
            with np.errstate(invalid='ignore', divide='ignore'):
                actual_fa = np.divide(actual_fa, smoothed_mask)
                actual_fa[smoothed_mask == 0] = np.nan
    else:
        if (opre_b1fwhm > 0.1):
            # Smooth whole map without masking
            sigma_vox = (opre_b1fwhm / (2 * np.sqrt(2 * np.log(2)))) / np.array(voxel_sizes[:3])
            actual_fa = gaussian_filter(actual_fa, sigma=sigma_vox)    

    # Replace NaNs in actual_fa with 0.1 (for visualization or further processing)
    actual_fa = np.nan_to_num(actual_fa, nan=0.1)

    # Troubleshooting
    b1_img = nib.nifti1.Nifti1Image(actual_fa, xform)
    nib.save(b1_img, 'nifti_b1_image.nii')
    

    # And restore the other two dimensions
    actual_fa = actual_fa[..., np.newaxis, np.newaxis]

    if (opre_b1output == 'afa'):
        data = actual_fa
    else:
        data = actual_fa/nominal_fa*100.0

    # Reformat data
    logging.debug("shape of b1 map")
    logging.debug(data.shape)
    #data = data[:, :, :, None, None]
    data = data.transpose((0, 1, 4, 3, 2))

    # if mrdhelper.get_json_config_param(config, 'options') == 'complex':
        # Complex images are requested
        # data = data.astype(np.complex64)
        #  maxVal = data.max()
    # else:
        # Determine max value (12 or 16 bit)
    BitsStored = 12
        # if (mrdhelper.get_userParameterLong_value(mrdHeader, "BitsStored") is not None):
            # BitsStored = mrdhelper.get_userParameterLong_value(mrdHeader, "BitsStored")
    maxVal = 2**BitsStored - 1

        # Normalize and convert to int16
        # Nuh uh, no normalizing here!
    data = data.astype(np.float64)
    # data *= maxVal/data.max()
    data = np.around(data)
    data = data.astype(np.int16)

    currentSeries = 0

    # Re-slice back into 2D images
    imagesOut = [None] * data.shape[-1]

    logging.debug("KP350: data is size %s" % (data.shape,))
    has_nan = np.isnan(data).any()
    logging.debug("Contains NaN: %s" % has_nan)

    for iImg in range(data.shape[-1]):
        # Create new MRD instance for the inverted image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)

        # Create a copy of the original fixed header and update the data_type
        # (we changed it to int16 from all other types)
        oldHeader = head[iImg]
        oldHeader.data_type = imagesOut[iImg].data_type

        # Set the image_type to match the data_type for complex data
        if (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXFLOAT) or (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXDOUBLE):
            oldHeader.image_type = ismrmrd.IMTYPE_COMPLEX

        # Unused example, as images are grouped by series before being passed into this function now
        # oldHeader.image_series_index = currentSeries

        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        if mrdhelper.get_meta_value(meta[iImg], 'IceMiniHead') is not None:
            if mrdhelper.extract_minihead_bool_param(base64.b64decode(meta[iImg]['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                currentSeries += 1

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = meta[iImg]
        tmpMeta['DataRole']                       = 'Image'
        tmpMeta['ImageProcessingHistory']         = ['PYTHON', 'AFIB1']
        tmpMeta['WindowCenter']                   = str((maxVal+1)/2)
        tmpMeta['WindowWidth']                    = str((maxVal+1))
        # tmpMeta['SequenceDescriptionAdditional']  = 'FIRE'
        tmpMeta['SequenceDescriptionAdditional']  = 'AFI B1+ Map'
        tmpMeta['Keep_image_geometry']            = 1

        if ('parameters' in config) and ('options' in config['parameters']):
            # Example for sending ROIs
            if config['parameters']['options'] == 'roi':
                logging.info("Creating ROI_example")
                tmpMeta['ROI_example'] = create_example_roi(data.shape)

            # Example for setting colormap
            if config['parameters']['options'] == 'colormap':
                tmpMeta['LUTFileName'] = 'MicroDeltaHotMetal.pal'

        # Add image orientation directions to MetaAttributes if not already present
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(oldHeader.read_dir[0]), "{:.18f}".format(oldHeader.read_dir[1]), "{:.18f}".format(oldHeader.read_dir[2])]

        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(oldHeader.phase_dir[0]), "{:.18f}".format(oldHeader.phase_dir[1]), "{:.18f}".format(oldHeader.phase_dir[2])]

        metaXml = tmpMeta.serialize()
        logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
        logging.debug("Image data has %d elements", imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml

    # Send a copy of original (unmodified) images back too
    if opre_sendoriginal:
        stack = traceback.extract_stack()
        if stack[-2].name == 'process_raw':
            logging.warning('sendOriginal is true, but input was raw data, so no original images to return!')
        else:
            logging.info('Sending a copy of original unmodified images due to sendOriginal set to True')
            # In reverse order so that they'll be in correct order as we insert them to the front of the list
            for image in reversed(imgGroup):
                # Create a copy to not modify the original inputs
                tmpImg = image

                # Change the series_index to have a different series
                tmpImg.image_series_index = 99

                # Ensure Keep_image_geometry is set to not reverse image orientation
                tmpMeta = ismrmrd.Meta.deserialize(tmpImg.attribute_string)
                tmpMeta['Keep_image_geometry'] = 1
                tmpImg.attribute_string = tmpMeta.serialize()

                imagesOut.insert(0, tmpImg)

    return imagesOut

# Create an example ROI <3
def create_example_roi(img_size):
    t = np.linspace(0, 2*np.pi)
    x = 16*np.power(np.sin(t), 3)
    y = -13*np.cos(t) + 5*np.cos(2*t) + 2*np.cos(3*t) + np.cos(4*t)

    # Place ROI in bottom right of image, offset and scaled to 10% of the image size
    x = (x-np.min(x)) / (np.max(x) - np.min(x))
    y = (y-np.min(y)) / (np.max(y) - np.min(y))
    x = (x * 0.10*np.min(img_size[:2])) + (img_size[1]-0.2*np.min(img_size[:2]))
    y = (y * 0.10*np.min(img_size[:2])) + (img_size[0]-0.2*np.min(img_size[:2]))

    rgb = (1,0,0)  # Red, green, blue color -- normalized to 1
    thickness  = 1 # Line thickness
    style      = 0 # Line style (0 = solid, 1 = dashed)
    visibility = 1 # Line visibility (0 = false, 1 = true)

    roi = mrdhelper.create_roi(x, y, rgb, thickness, style, visibility)
    return roi
