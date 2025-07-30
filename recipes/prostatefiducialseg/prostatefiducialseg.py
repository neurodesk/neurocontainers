import ismrmrd
import logging
import traceback
import numpy as np
import base64
import mrdhelper
import constants
import nibabel as nib
import subprocess
import os

from scipy.ndimage.morphology import binary_fill_holes
from skimage.segmentation import find_boundaries
from skimage.morphology import dilation
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
        logging.info("First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3", 
            metadata.encoding[0].trajectory, 
            metadata.encoding[0].encodedSpace.matrixSize.x, 
            metadata.encoding[0].encodedSpace.matrixSize.y, 
            metadata.encoding[0].encodedSpace.matrixSize.z, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z)

    except Exception as e:
        logging.warning("Failed to parse metadata properly: %s", str(e))

    # Continuously parse incoming data parsed from MRD messages
    currentSeries = 0
    acqGroup = []
    imgGroup = []
    waveformGroup = []
    
    # Statistics tracking
    total_acquisitions = 0
    total_images = 0
    total_waveforms = 0
    
    try:
        logging.info("Starting data processing loop...")
        for item in connection:
            # ----------------------------------------------------------
            # Raw k-space data messages
            # ----------------------------------------------------------
            if isinstance(item, ismrmrd.Acquisition):
                total_acquisitions += 1
                
                # Log acquisition flags for debugging
                flags = []
                if item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT):
                    flags.append("NOISE")
                if item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION):
                    flags.append("PARALLEL_CAL")
                if item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA):
                    flags.append("PHASECORR")
                if item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA):
                    flags.append("NAVIGATION")
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    flags.append("LAST_IN_SLICE")
                
                if flags:
                    logging.debug("Acquisition %d flags: %s", total_acquisitions, ", ".join(flags))
                
                # Accumulate all imaging readouts in a group
                if (not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)):
                    acqGroup.append(item)
                    logging.debug("Added acquisition %d to group (group size: %d)", total_acquisitions, len(acqGroup))

                # When this criteria is met, run process_raw() on the accumulated
                # data, which returns images that are sent back to the client.
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    logging.info("Processing k-space group with %d acquisitions", len(acqGroup))
                    image = process_raw(acqGroup, connection, config, metadata)
                    connection.send_image(image)
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                total_images += 1
                logging.debug("Received image %d: series=%d, type=%s, shape=%s", 
                             total_images, item.image_series_index, item.image_type, item.data.shape)
                
                # When this criteria is met, run process_group() on the accumulated
                # data, which returns images that are sent back to the client.
                # e.g. when the series number changes:
                if item.image_series_index != currentSeries:
                    logging.info("Series change detected: %d -> %d, processing image group with %d images", 
                                currentSeries, item.image_series_index, len(imgGroup))
                    currentSeries = item.image_series_index
                    
                    if len(imgGroup) > 0:
                        image = process_image(imgGroup, connection, config, metadata)
                        connection.send_image(image)
                    imgGroup = []

                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imgGroup.append(item)
                    logging.debug("Added magnitude image to group (group size: %d)", len(imgGroup))
                else:
                    logging.debug("Sending non-magnitude image directly (type: %s)", item.image_type)
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta['Keep_image_geometry'] = 1
                    item.attribute_string = tmpMeta.serialize()

                    connection.send_image(item)
                    continue

            # ----------------------------------------------------------
            # Waveform data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Waveform):
                total_waveforms += 1
                waveformGroup.append(item)
                logging.debug("Received waveform %d: ID=%d, samples=%d", 
                             total_waveforms, item.waveform_id, item.number_of_samples)

            elif item is None:
                logging.info("Received end-of-stream signal")
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        # Extract raw ECG waveform data. Basic sorting to make sure that data 
        # is time-ordered, but no additional checking for missing data.
        # ecgData has shape (5 x timepoints)
        if len(waveformGroup) > 0:
            logging.info("Processing %d waveform items", len(waveformGroup))
            waveformGroup.sort(key = lambda item: item.time_stamp)
            ecgData = [item.data for item in waveformGroup if item.waveform_id == 0]
            if ecgData:
                ecgData = np.concatenate(ecgData, 1)
                logging.info("ECG data extracted: shape=%s", ecgData.shape)
            else:
                logging.warning("No ECG data found (waveform_id == 0)")

        # Process any remaining groups of raw or image data.  This can 
        # happen if the trigger condition for these groups are not met.
        # This is also a fallback for handling image data, as the last
        # image in a series is typically not separately flagged.
        if len(acqGroup) > 0:
            logging.info("Processing remaining k-space group with %d acquisitions (untriggered)", len(acqGroup))
            image = process_raw(acqGroup, connection, config, metadata)
            connection.send_image(image)
            acqGroup = []

        if len(imgGroup) > 0:
            logging.info("Processing remaining image group with %d images (untriggered)", len(imgGroup))
            image = process_image(imgGroup, connection, config, metadata)
            connection.send_image(image)
            imgGroup = []

        # Final statistics
        logging.info("=== Processing session completed ===")
        logging.info("Statistics - Acquisitions: %d, Images: %d, Waveforms: %d", 
                    total_acquisitions, total_images, total_waveforms)

    except Exception as e:
        logging.error("Fatal error in main processing loop: %s", str(e))
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        logging.info("Sending close signal to connection")
        connection.send_close()


def apply_homogeneity_correction(image_data):
    logging.info("Starting N4 bias field correction on data shape: %s", image_data.shape)
    
    try:
        # 1) NumPy → SITK
        logging.debug("Converting NumPy array to SimpleITK image")
        sitk_image = sitk.GetImageFromArray(image_data)
        
        # 2) Float32 input for N4
        logging.debug("Converting to Float32 for N4 processing")
        sitk_image = sitk.Cast(sitk_image, sitk.sitkFloat32)
        
        # 3) Otsu mask → UInt8
        logging.debug("Generating Otsu threshold mask")
        mask = sitk.OtsuThreshold(sitk_image, 0, 1, 200)
        mask = sitk.Cast(mask, sitk.sitkUInt8)
        
        # Log mask statistics
        mask_array = sitk.GetArrayFromImage(mask)
        mask_volume = np.sum(mask_array)
        total_volume = mask_array.size
        mask_percentage = (mask_volume / total_volume) * 100
        logging.info("Otsu mask covers %.1f%% of volume (%d/%d voxels)", 
                    mask_percentage, mask_volume, total_volume)
        
        # 4) N4 on floats + uint8 mask
        logging.debug("Running N4 bias field correction")
        corrected = sitk.N4BiasFieldCorrection(sitk_image, mask)
        
        # 5) Back to NumPy (still float)
        logging.debug("Converting corrected image back to NumPy")
        corrected_array = sitk.GetArrayFromImage(corrected)
        
        # Log intensity statistics
        original_stats = f"min={image_data.min():.2f}, max={image_data.max():.2f}, mean={image_data.mean():.2f}"
        corrected_stats = f"min={corrected_array.min():.2f}, max={corrected_array.max():.2f}, mean={corrected_array.mean():.2f}"
        logging.info("Intensity before correction: %s", original_stats)
        logging.info("Intensity after correction: %s", corrected_stats)
        
        return corrected_array
        
    except Exception as e:
        logging.error("Error in homogeneity correction: %s", str(e))
        logging.error("Returning original data unchanged")
        return image_data


def do_segmentation(data):
    logging.info("=== Starting segmentation pipeline ===")
    logging.info("Input data shape: %s, dtype: %s", data.shape, data.dtype)
    
    # ===== PRE-PREDICTION TRANSFORM =====
    # A) (Row,Col,Slice) to (Row,Col,Slice)
    order = (0, 1, 2)
    # B) (Row,Col,Slice) to (Row,Slice,Col)
    #order = (0, 2, 1)
    # C) (Row,Col,Slice) to (Col,Row,Slice)
    #order = (1, 0, 2)
    # D) (Row,Col,Slice) to (Col,Slice,Row)
    #order = (1, 2, 0)
    # E) (Row,Col,Slice) to (Slice,Row,Col)
    #order = (2, 0, 1)
    # F) (Row,Col,Slice) to (Slice,Col,Row)
    #order = (2, 1, 0)

    # --- Pre-Prediction Transform ---
    logging.info("Applying pre-prediction transform with order: %s", order)
    original_shape = data.shape
    data = np.transpose(data, order)
    logging.info("Shape after transpose: %s -> %s", original_shape, data.shape)
    # ====================================
    
    # ——— Homogeneity correction ———
    logging.info("Applying N4 bias‐field correction")
    data = apply_homogeneity_correction(data)

    # ——— Save to NIfTI for prediction ———
    logging.info("Saving preprocessed data to NIfTI format")
    try:
        new_img = nib.nifti1.Nifti1Image(data, np.eye(4))
        nib.save(new_img, 't1_from_h5.nii')
        
        # Verify file was created
        if os.path.exists('t1_from_h5.nii'):
            file_size = os.path.getsize('t1_from_h5.nii')
            logging.info("NIfTI file saved successfully: t1_from_h5.nii (%.2f MB)", file_size / (1024*1024))
        else:
            logging.error("Failed to create NIfTI file: t1_from_h5.nii")
            
    except Exception as e:
        logging.error("Error saving NIfTI file: %s", str(e))
        raise

    # ——— Run prediction script ———
    logging.info("Starting prediction with external script")
    
    try:
        # Run prediction
        cmd = ["simple_predict.py", "--input", "t1_from_h5.nii", "--model", "/opt/models/model.pth", "--output", "output"]
        logging.info("Running command: %s", " ".join(cmd))
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info("Prediction script completed successfully")
            if result.stdout:
                logging.debug("Prediction stdout: %s", result.stdout.strip())
        else:
            logging.error("Prediction script failed with return code: %d", result.returncode)
            if result.stderr:
                logging.error("Prediction stderr: %s", result.stderr.strip())
            if result.stdout:
                logging.error("Prediction stdout: %s", result.stdout.strip())
                
    except Exception as e:
        logging.error("Error running prediction script: %s", str(e))
        raise

    # ——— Load prediction results ———
    logging.info("Loading prediction results")
    try:
        segmentation_file = 'output/pred_seeds.nii.gz'
        t1_file = 't1_from_h5.nii'
        
        # Check if output files exist
        if not os.path.exists(segmentation_file):
            logging.error("Segmentation output file not found: %s", segmentation_file)
            raise FileNotFoundError(f"Missing segmentation file: {segmentation_file}")
            
        if not os.path.exists(t1_file):
            logging.error("T1 input file not found: %s", t1_file)
            raise FileNotFoundError(f"Missing T1 file: {t1_file}")
        
        logging.info('Loading segmentation image from: %s', segmentation_file)
        segmentation = nib.load(segmentation_file).get_fdata()
        logging.info("Segmentation loaded: shape=%s, dtype=%s", segmentation.shape, segmentation.dtype)
        
        logging.info('Loading T1 image from: %s', t1_file)
        data = nib.load(t1_file).get_fdata()
        logging.info("T1 data reloaded: shape=%s, dtype=%s", data.shape, data.dtype)
        
        # Log segmentation statistics
        unique_labels = np.unique(segmentation)
        logging.info("Segmentation contains %d unique labels: %s", len(unique_labels), unique_labels)
        
        for label in unique_labels:
            if label > 0:  # Skip background
                voxel_count = np.sum(segmentation == label)
                percentage = (voxel_count / segmentation.size) * 100
                logging.info("Label %d: %d voxels (%.2f%%)", label, voxel_count, percentage)
                
    except Exception as e:
        logging.error("Error loading prediction results: %s", str(e))
        raise

    # ——— Post-processing of segmentation ———
    logging.info("Starting post-processing of segmentation")
    
    try:
        # Create seed mask
        logging.debug("Creating seed mask from segmentation")
        seed_mask = (segmentation > 0)
        initial_seed_count = np.sum(seed_mask)
        logging.info("Initial seed mask: %d voxels", initial_seed_count)
        
        # Dilation
        logging.debug("Applying morphological dilation")
        seed_mask = dilation(seed_mask, footprint=np.ones((3, 3, 3)))
        dilated_seed_count = np.sum(seed_mask)
        logging.info("After dilation: %d voxels (+%d)", dilated_seed_count, dilated_seed_count - initial_seed_count)
        
        # Fill holes
        logging.debug("Filling holes in seed mask")
        seed_mask = binary_fill_holes(seed_mask)
        filled_seed_count = np.sum(seed_mask)
        logging.info("After hole filling: %d voxels (+%d)", filled_seed_count, filled_seed_count - dilated_seed_count)
        
        # Find boundaries
        logging.debug("Finding boundaries of seed mask")
        seed_mask = find_boundaries(seed_mask, mode='outer')
        boundary_count = np.sum(seed_mask)
        logging.info("Boundary mask: %d voxels", boundary_count)
        
        # Apply overlay to data
        logging.debug("Applying boundary overlay to original data")
        data_max = data.max()
        modified_voxels = 0
        
        for z in range(seed_mask.shape[2]):
            rows, cols = np.where(seed_mask[:, :, z])
            if len(rows) > 0:
                data[rows, cols, z] = data_max
                modified_voxels += len(rows)
                
        logging.info("Modified %d voxels with boundary overlay (max value: %.2f)", modified_voxels, data_max)
        
    except Exception as e:
        logging.error("Error in post-processing: %s", str(e))
        raise
    
    # ===== POST-PREDICTION TRANSFORM =====
    # Reverse the pre-prediction transform
    logging.info("Applying post-prediction transform (reversing order)")
    reverse_order = np.argsort(order)
    logging.info("Reverse transform order: %s", reverse_order)
    pre_reverse_shape = data.shape
    data = np.transpose(data, reverse_order)
    logging.info("Shape after reverse transpose: %s -> %s", pre_reverse_shape, data.shape)
    # =====================================

    return data


def process_image(images, connection, config, metadata):
    if len(images) == 0:
        logging.warning("process_image called with empty image list")
        return []

    logging.info("=== Starting image processing ===")
    logging.info("Processing %d images", len(images))

    # Create folder, if necessary
    # if not os.path.exists(debugFolder):
    #     os.makedirs(debugFolder)
    #     logging.debug("Created folder " + debugFolder + " for debug output files")

    # Log image types and sizes
    for i, img in enumerate(images):
        logging.debug("Image %d: type=%s, shape=%s, series=%d", 
                     i, ismrmrd.get_dtype_from_data_type(img.data_type), 
                     img.data.shape, img.image_series_index)

    # Note: The MRD Image class stores data as [cha z y x]

    # Extract image data into a 5D array of size [img cha z y x]
    logging.info("Extracting image data into 5D array")
    data = np.stack([img.data for img in images])
    head = [img.getHead() for img in images]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in images]

    # Diagnostic info
    matrix = np.array(head[0].matrix_size[:]) 
    fov = np.array(head[0].field_of_view[:])
    voxelsize = fov/matrix
    read_dir = np.array(images[0].read_dir)
    phase_dir = np.array(images[0].phase_dir)
    slice_dir = np.array(images[0].slice_dir)
    
    logging.info(f'MRD computed matrix [x y z] : {matrix}')
    logging.info(f'MRD computed fov     [x y z] : {fov}')
    logging.info(f'MRD computed voxel   [x y z] : {voxelsize}')
    logging.info(f'MRD read_dir         [x y z] : {read_dir}')
    logging.info(f'MRD phase_dir        [x y z] : {phase_dir}')
    logging.info(f'MRD slice_dir        [x y z] : {slice_dir}')

    # (Slice, 1, 1, Row, Col)
    logging.debug("Original image data before transposing is %s", data.shape) 

    # Reformat data to [y x img cha z], i.e. [row ~col] for the first two dimensions
    # data = data.transpose((3, 4, 2, 1, 0))
    # t1.h5 is 40 x 1 x 1 x 320 x 320
    # data = data.transpose((3, 4, 0, 1, 2))
    # after resorting it should be: 320 x 320 x 40 x 1 x 1

    # (Row, Col, 1, 1, Slice)
    logging.info("Transposing data from (Slice, 1, 1, Row, Col) to (Row, Col, 1, 1, Slice)")
    data = data.transpose((3, 4, 2, 1, 0))
    
    # Display MetaAttributes for first image
    # logging.debug("MetaAttributes[0]: %s", ismrmrd.Meta.serialize(meta[0]))

    # Optional serialization of ICE MiniHeader
    # if 'IceMiniHead' in meta[0]:
    #     logging.debug("IceMiniHead[0]: %s", base64.b64decode(meta[0]['IceMiniHead']).decode('utf-8'))

    logging.debug("Image data after transposing is %s", data.shape)

    # convert data to nifti using nibabel
    # prostatefiducialseg needs 3D data:
    # data = np.squeeze(data)
    logging.info("Squeezing to 3D data for segmentation")
    original_5d_shape = data.shape
    data = data[:,:,0,0,:]
    logging.info("Squeezed from %s to %s", original_5d_shape, data.shape)

    # (Row, Col, Slice)
    logging.debug("Final 3D shape: %s", data.shape)

    logging.info("Config: \n%s", config)

    # Run segmentation
    logging.info("Starting segmentation process")
    data = do_segmentation(data)

    # Transpose from (Row,Col,Slice) to (Row,Col,Slice,1,1)
    logging.info("Reshaping data back to 5D format")
    data = data[:, :, :, None, None]
    # Transpose from (Row,Col,Slice,1,1) to (Row,Col,1,1,Slice)
    data = data.transpose((0, 1, 3, 4, 2))
    logging.debug("Final 5D shape for output: %s", data.shape)

    # ===== ISMRMRD PREPARATION =====
    # Data MUST BE (Row, Col, 1, 1, Slice) at this point

    # Determine max value (12 or 16 bit)
    BitsStored = 12
    # if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
    #     BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
    maxVal = 2**BitsStored - 1
    logging.info("Using %d-bit storage, max value: %d", BitsStored, maxVal)

    # Normalize Data and convert to int16
    logging.info("Normalizing and converting data to int16")
    data_min_before = data.min()
    data_max_before = data.max()
    data = data.astype(np.float64)
    data *= maxVal/data.max()
    data = np.around(data)
    data = data.astype(np.int16)
    logging.info("Data range before normalization: [%.2f, %.2f]", data_min_before, data_max_before)
    logging.info("Data range after normalization: [%d, %d]", data.min(), data.max())

    currentSeries = 0

    # Re-slice image data back into 2D images
    logging.info("Creating %d output MRD images", data.shape[-1])
    imagesOut = [None] * data.shape[-1]
    
    for iImg in range(data.shape[-1]):
        logging.debug("Processing output image %d/%d", iImg + 1, data.shape[-1])
        
        # Create new MRD instance for the final image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)

        # Create a copy of the original fixed header and update the data_type
        # (we changed it to int16 from all other types)
        oldHeader = head[iImg]
        oldHeader.data_type = imagesOut[iImg].data_type

        # Unused example, as images are grouped by series before being passed into this function now
        # oldHeader.image_series_index = currentSeries+1

        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        if mrdhelper.get_meta_value(meta[iImg], 'IceMiniHead') is not None:
            if mrdhelper.extract_minihead_bool_param(base64.b64decode(meta[iImg]['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                currentSeries += 1
                logging.debug("Incremented series number to %d based on ICE MiniHead", currentSeries)

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = meta[iImg]
        tmpMeta['DataRole'] = 'Image'
        tmpMeta['ImageProcessingHistory'] = ['PYTHON', 'PROSTATEFIDUCIALSEG']
        tmpMeta['WindowCenter'] = str((maxVal+1)/2)
        tmpMeta['WindowWidth'] = str((maxVal+1))
        tmpMeta['SequenceDescriptionAdditional'] = 'OpenRecon'
        tmpMeta['Keep_image_geometry'] = 1

        # Example for sending ROIs
        logging.debug("Creating example ROI for image %d", iImg)
        tmpMeta['ROI_example'] = create_example_roi(data.shape)

        # Add image orientation directions to MetaAttributes if not already present
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(oldHeader.read_dir[0]), "{:.18f}".format(oldHeader.read_dir[1]), "{:.18f}".format(oldHeader.read_dir[2])]

        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(oldHeader.phase_dir[0]), "{:.18f}".format(oldHeader.phase_dir[1]), "{:.18f}".format(oldHeader.phase_dir[2])]

        metaXml = tmpMeta.serialize()
        logging.debug("Image %d data has %d elements", iImg, imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml

    logging.info("Successfully created %d output images", len(imagesOut))
    
    return imagesOut


# Create an example ROI <3
def create_example_roi(img_size):
    logging.debug("Creating example ROI for image size: %s", img_size)
    
    t = np.linspace(0, 2*np.pi)
    x = 16*np.power(np.sin(t), 3)
    y = -13*np.cos(t) + 5*np.cos(2*t) + 2*np.cos(3*t) + np.cos(4*t)

    # Place ROI in bottom right of image, offset and scaled to 10% of the image size
    x = (x-np.min(x)) / (np.max(x) - np.min(x))
    y = (y-np.min(y)) / (np.max(y) - np.min(y))
    x = (x * 0.08*img_size[0]) + 0.82*img_size[0]
    y = (y * 0.10*img_size[1]) + 0.80*img_size[1]

    rgb = (1,0,0)  # Red, green, blue color -- normalized to 1
    thickness = 1  # Line thickness
    style = 0      # Line style (0 = solid, 1 = dashed)
    visibility = 1 # Line visibility (0 = false, 1 = true)

    logging.debug("ROI parameters: color=%s, thickness=%d, style=%d, visibility=%d", rgb, thickness, style, visibility)

    roi = mrdhelper.create_roi(x, y, rgb, thickness, style, visibility)
    logging.debug("Successfully created ROI with %d points", len(x))
    
    return roi