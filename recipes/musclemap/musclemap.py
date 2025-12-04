import ismrmrd
import os
import itertools
import logging
import traceback
import numpy as np
import numpy.fft as fft
import xml.dom.minidom
import base64
import ctypes
import re
import mrdhelper
import constants
from time import perf_counter
import nibabel as nib
import subprocess
import SimpleITK as sitk


# Folder for debug output files
debugFolder = "/tmp/share/debug"


def process(connection, config, metadata):
    logging.info("Config: \n%s", config)

    # Metadata should be MRD formatted header, but may be a string
    # if it failed conversion earlier
    try:
        # Disabled due to incompatibility between PyXB and Python 3.8:
        # https://github.com/pabigot/pyxb/issues/123
        # # logging.info("Metadata: \n%s", metadata.toxml('utf-8'))

        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info(
            "First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3",
            metadata.encoding[0].trajectory,
            metadata.encoding[0].encodedSpace.matrixSize.x,
            metadata.encoding[0].encodedSpace.matrixSize.y,
            metadata.encoding[0].encodedSpace.matrixSize.z,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z,
        )

    except:
        logging.info("Improperly formatted metadata: \n%s", metadata)

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
                if (
                    not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)
                ):
                    acqGroup.append(item)

                # When this criteria is met, run process_raw() on the accumulated
                # data, which returns images that are sent back to the client.
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    logging.info("Processing a group of k-space data")
                    image = process_raw(acqGroup, connection, config, metadata)
                    connection.send_image(image)
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                # When this criteria is met, run process_group() on the accumulated
                # data, which returns images that are sent back to the client.
                # e.g. when the series number changes:
                if item.image_series_index != currentSeries:
                    logging.info("Processing a group of images because series index changed to %d", item.image_series_index)
                    currentSeries = item.image_series_index
                    image = process_image(imgGroup, connection, config, metadata)
                    connection.send_image(image)
                    imgGroup = []

                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imgGroup.append(item)
                else:
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta["Keep_image_geometry"] = 1
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

        if len(imgGroup) > 0:
            logging.info("Processing a group of images (untriggered)")
            image = process_image(imgGroup, connection, config, metadata)
            connection.send_image(image)
            imgGroup = []

    except Exception as e:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()

# from https://github.com/benoitberanger/openrecon-template/blob/main/app/i2i-save-original-images.py
def compute_nifti_affine(image_header, voxel_size):

    # Extract necessary fields
    position      = image_header.position
    read_dir      = image_header.read_dir
    phase_dir     = image_header.phase_dir
    slice_dir     = image_header.slice_dir

    # Convert from LPS to RAS
    position_ras  = [ -position[0],  -position[1],  position[2]]
    read_dir_ras  = [ -read_dir[0],  -read_dir[1],  read_dir[2]]
    phase_dir_ras = [-phase_dir[0], -phase_dir[1], phase_dir[2]]
    slice_dir_ras = [-slice_dir[0], -slice_dir[1], slice_dir[2]]

    # Construct rotation-scaling matrix
    rotation_scaling_matrix = np.column_stack([
        voxel_size[0] * np.array( read_dir_ras),
        voxel_size[1] * np.array(phase_dir_ras),
        voxel_size[2] * np.array(slice_dir_ras)
    ])

    # Construct affine matrix
    affine = np.eye(4)
    affine[:3, :3] = rotation_scaling_matrix
    affine[:3,  3] = position_ras

    return affine


def process_image(imgGroup, connection, config, metadata):
    if len(imgGroup) == 0:
        return []

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")

    # logging.debug(
    #     "Processing data with %d images of type %s", len(images), ismrmrd.get_dtype_from_data_type(images[0].data_type)
    # )

    # Note: The MRD Image class stores data as [cha z y x]

    # Extract image data into a 5D array of size [img cha z y x]
    data = np.stack([img.data for img in imgGroup])
    head = [img.getHead() for img in imgGroup]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in imgGroup]

    print("header length - should be as long as there are images:")
    print(len(head))

    matrix = np.array(head[0].matrix_size[:])

    #adjust the matrix size to the full 3D volume, it should be as many slices as in length(imgGroup)
    if matrix[2] != len(imgGroup):
        matrix[2] = len(imgGroup)    

    fov = np.array(head[0].field_of_view[:])

    #we also need to adjust fov z to be slice thickness * number of slices
    slice_thickness = fov[2]
    fov[2] = slice_thickness * len(imgGroup)

    voxelsize = fov/matrix

    print("matrix:")
    print(matrix)
    print("fov:")
    print(fov)
    print("voxelsize:") 
    print(voxelsize)

    crop_size = data.shape

    print("shape before transpose:")
    print(data.shape)

    # Reformat data to [y x z cha img], i.e. [row col] for the first two dimensions
    # data = data.transpose((3, 4, 2, 1, 0))

    # Reformat data to [y x img cha z], i.e. [row ~col] for the first two dimensions
    data = data.transpose((3, 4, 0, 1, 2))

    print("shape after initial transpose:")
    print(data.shape)

    # convert data to nifti using nibabel
    affine = compute_nifti_affine(head[0], voxelsize)

    print("affine matrix:")
    print(affine)

#     affine matrix: dcm -> hdf5 -> nifti:
# [[-8.00799981e-01  3.92151765e-12 -0.00000000e+00  2.49850006e+02]
#  [-3.92151743e-12 -8.00800025e-01  0.00000000e+00  2.05005005e+02]
#  [ 0.00000000e+00  0.00000000e+00  7.99999971e-01 -1.42800000e+03]
#  [ 0.00000000e+00  0.00000000e+00  0.00000000e+00  1.00000000e+00]]    

    # print("overwriting affine with identity for testing")
    # affine = np.eye(4)

    data = np.squeeze(data)
    print("shape before saving nifti and running mm_segment:")
    print(data.shape)
    # should be: 624 x 512 x 416
    # is: (512, 624, 416)

    new_img = nib.nifti1.Nifti1Image(data, affine)
    # check if /buildhostdirectory exists, if not create it:
    # if not os.path.exists("/buildhostdirectory"):
        # os.makedirs("/buildhostdirectory")
    nib.save(new_img, "/opt/input_fromDCM.nii.gz")

    # Extract UI parameters from JSON config
    bodyregion = mrdhelper.get_json_config_param(config, 'bodyregion', default='wholebody', type='str')
    chunksize = mrdhelper.get_json_config_param(config, 'chunksize', default='auto', type='str')
    spatialoverlap = mrdhelper.get_json_config_param(config, 'spatialoverlap', default=50, type='int')
    fastmodel = mrdhelper.get_json_config_param(config, 'fastmodel', default=True, type='bool')
    forcegpu = mrdhelper.get_json_config_param(config, 'forcegpu', default=False, type='bool')
    
    # Convert forcegpu boolean to Y/N string
    gpu_flag = "Y" if forcegpu else "N"
    
    logging.info(f"mm_segment parameters: bodyregion={bodyregion}, chunksize={chunksize}, spatialoverlap={spatialoverlap}, fastmodel={fastmodel}, forcegpu={forcegpu}")
    
    # Build mm_segment command with parameters
    mm_segment_cmd = [
        "mm_segment",
        "-i", "/opt/input_fromDCM.nii.gz",
        "-r", bodyregion,
        "-c", str(chunksize),
        "-s", str(spatialoverlap),
        "-g", gpu_flag
    ]
    
    if fastmodel:
        mm_segment_cmd.append("--fast")
    
    mm_segment_cmd.append("-v")
    
    # Run mm_segment
    logging.info(f"Running command: {' '.join(mm_segment_cmd)}")
    preprocess_result = subprocess.run(mm_segment_cmd, check=True)

    # This is just for debugging and can be removed later:
    # copy_result = subprocess.run(["cp", "/opt/input_fromDCM_dseg.nii.gz", "/buildhostdirectory/input_fromDCM_dseg.nii.gz"], check=True) 
    
    img = nib.load("/opt/input_fromDCM_dseg.nii.gz")
    data = img.get_fdata()

    print("maximum value in segmented data:")
    print(np.max(data))


    # transform labels if selected using 3 * (label_in // 10) + (label_in % 10):
    label_transform = mrdhelper.get_json_config_param(config, 'labeltransform', default=False, type='str')
    if label_transform is not None:  
        logging.info("Applying label transformation: 3 * (label_in // 10) + (label_in % 10)")
        data = 3 * (data // 10) + (data % 10)
        logging.info(f"Label transformation complete. New data range: [{data.min()}, {data.max()}]")


    print("maximum value in segmented data after transform:")
    print(np.max(data))

    # Reformat data
    print("shape after loading with nibabel")
    print(data.shape)

    if data.ndim == 2:
        data = data[:, :, None]

    data = data[..., None, None]
    data = data.transpose((0, 1, 4, 3, 2))

    print("shape after applying transpose")
    print(data.shape)

    # compare size of data to crop_size, if not identical do a center crop
    print("crop_size:")
    print(crop_size)

    print("data shape before crop:")
    print(data.shape)
    
    # crop_size is [img, cha, z, y, x]
    # data is [y, x, 1, 1, img]
    if data.shape[0] != crop_size[3] or data.shape[1] != crop_size[4]:
        crop_y = int((data.shape[0] - crop_size[3]) / 2)
        crop_x = int((data.shape[1] - crop_size[4]) / 2)
        data = data[crop_y : crop_y + crop_size[3], crop_x : crop_x + crop_size[4], ...]

    print("data shape after crop:")
    print(data.shape)

    if ("parameters" in config) and ("options" in config["parameters"]) and (config["parameters"]["options"] == "complex"):
        # Complex images are requested
        data = data.astype(np.complex64)
        maxVal = data.max()
    else:
        # Determine max value (12 or 16 bit)
        BitsStored = 12
        # if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
        #     BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
        maxVal = 2**BitsStored - 1

        # Normalize and convert to int16
        data = data.astype(np.float64)
        data *= maxVal / data.max()
        data = np.around(data)
        data = data.astype(np.int16)

    print("maximum value in segmented data after DICOM range adjustment:")
    print(np.max(data))

    currentSeries = 0

    # Re-slice back into 2D images
    imagesOut = [None] * data.shape[-1]

    print("data.shape before creating output images:")
    print(data.shape)

    print("header length - should be as many as images:")
    print(len(head))

    for iImg in range(data.shape[-1]):
        # Create new MRD instance for the segmented image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        # imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)
        imagesOut[iImg] = ismrmrd.Image.from_array(data[..., iImg].transpose((3, 2, 0, 1)), transpose=False)

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
        if mrdhelper.get_meta_value(meta[iImg], "IceMiniHead") is not None:
            if (
                mrdhelper.extract_minihead_bool_param(
                    base64.b64decode(meta[iImg]["IceMiniHead"]).decode("utf-8"), "BIsSeriesEnd"
                )
                is True
            ):
                currentSeries += 1

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = meta[iImg]
        tmpMeta["DataRole"] = "Image"
        tmpMeta["ImageProcessingHistory"] = ["PYTHON", "MUSCLEMAP"]
        tmpMeta["WindowCenter"] = str((maxVal + 1) / 2)
        tmpMeta["WindowWidth"] = str((maxVal + 1))
        tmpMeta["SequenceDescriptionAdditional"] = "OpenRecon"
        tmpMeta["Keep_image_geometry"] = 1

        # Add image orientation directions to MetaAttributes if not already present
        if tmpMeta.get("ImageRowDir") is None:
            tmpMeta["ImageRowDir"] = [
                "{:.18f}".format(oldHeader.read_dir[0]),
                "{:.18f}".format(oldHeader.read_dir[1]),
                "{:.18f}".format(oldHeader.read_dir[2]),
            ]

        if tmpMeta.get("ImageColumnDir") is None:
            tmpMeta["ImageColumnDir"] = [
                "{:.18f}".format(oldHeader.phase_dir[0]),
                "{:.18f}".format(oldHeader.phase_dir[1]),
                "{:.18f}".format(oldHeader.phase_dir[2]),
            ]

        metaXml = tmpMeta.serialize()
        # logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
        logging.debug("Image data has %d elements", imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml


     # Send a copy of original (unmodified) images back too if selected
    opre_sendoriginal = mrdhelper.get_json_config_param(config, 'sendoriginal', default=True, type='bool')
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



