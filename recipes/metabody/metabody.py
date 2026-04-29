import ismrmrd
import logging
import traceback
import numpy as np
import base64
import mrdhelper
import constants
import nibabel as nib
import subprocess

from skimage.segmentation import find_boundaries
import SimpleITK as sitk
import os


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
                if (not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)):
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
            ecgData = np.concatenate(ecgData,1)

        # Process any remaining groups of raw or image data.  This can 
        # happen if the trigger condition for these groups are not met.
        # This is also a fallback for handling image data, as the last
        # image in a series is typically not separately flagged.
        if len(acqGroup) > 0:
            logging.info("Processing a group of k-space data (untriggered)")
            image = process_raw(acqGroup, connection, config, metadata)
            connection.send_image(image)
            acqGroup = []

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



def process_image(images, connection, config, metadata):
    if len(images) == 0:
        return []

    # Create folder, if necessary
    # if not os.path.exists(debugFolder):
    #     os.makedirs(debugFolder)
    #     logging.debug("Created folder " + debugFolder + " for debug output files")

    # logging.debug("Processing data with %d images of type %s", len(images), ismrmrd.get_dtype_from_data_type(images[0].data_type))

    # Note: The MRD Image class stores data as [cha z y x]

    # Extract image data into a 5D array of size [img cha z y x]
    data = np.stack([img.data                              for img in images])
    head = [img.getHead()                                  for img in images]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in images]

    # Diagnostic info
    matrix    = np.array(head[0].matrix_size  [:]) 
    fov       = np.array(head[0].field_of_view[:])
    voxelsize = fov/matrix
    read_dir  = np.array(images[0].read_dir )
    phase_dir = np.array(images[0].phase_dir)
    slice_dir = np.array(images[0].slice_dir)
    logging.info(f'MRD computed maxtrix [x y z] : {matrix   }')
    logging.info(f'MRD computed fov     [x y z] : {fov      }')
    logging.info(f'MRD computed voxel   [x y z] : {voxelsize}')
    logging.info(f'MRD read_dir         [x y z] : {read_dir }')
    logging.info(f'MRD phase_dir        [x y z] : {phase_dir}')
    logging.info(f'MRD slice_dir        [x y z] : {slice_dir}')

    logging.debug("Original image data before transposing is %s" % (data.shape,))
    
    # Transpose to [x y img z cha] and keep only [x y img]
    # It looks like MRD images might be [img cha z x y]
    data = data.transpose((3, 4, 0, 2, 1))
    data = np.squeeze(data[..., 0, 0])

    logging.debug("Original image data after transposing is %s" % (data.shape,))

    # Turn into 4D fMRI data
    n_slices = np.unique([img.slice for img in images]).shape[0]
    n_repetitions = np.unique([img.repetition for img in images]).shape[0]
    # New data with the correct 4D dimensions
    new_data = np.zeros((data.shape[0], data.shape[1], n_slices, n_repetitions))
    
    # Determine order for stacking when returning images
    slices = [img.slice for img in images]
    repetitions = [img.repetition for img in images]
    slice_diff = np.nansum(np.diff(slices)[:5])
    rep_diff = np.nansum(np.diff(repetitions)[:5])
    # If the sum of the first few differences in slice indices is 0,
    # then we can assume that slices are stacked within repetitions
    logging.debug(f'Slices: {slices[:10]} (sum: {slice_diff}); Repetitions: {repetitions[:10]} (sum: {rep_diff})')
    if slice_diff > 0 and rep_diff == 0:
        logging.info("Detected repetition-major ordering of images (stack all slices of repetition 0, then all slices of repetition 1, etc)")
        orderinfo = 'slices'
    elif slice_diff == 0 and rep_diff > 0:
        logging.info("Detected slice-major ordering of images (stack all repetitions of slice 0, then all repetitions of slice 1, etc)")
        orderinfo = 'repetitions'
    else:
        # They might be interleaved or in some other unexpected order
        show_idx = min(10, len(slices))
        logging.warning("Could not determine ordering of slices and repetitions - unexpected pattern detected. Defaulting to repetition-major ordering.")
        logging.warning('Slices: %s;  Repetitions: %s', slices[:show_idx], repetitions[:show_idx])
        orderinfo = 'slices'

    for img in images:
        # Assuming that slices of repetitions are stacked (all slices
        # of rep 0, then all slices of rep 1, etc)
        new_data[:, :, img.slice, img.repetition] = img.data[0, 0, :, :]
    data = new_data

    logging.debug("Transformed to 4D: %s (slices: %d; repetitions: %d)" % (data.shape, n_slices, n_repetitions))

    # The affine matrix can be reconstructed from the metadata
    affine = np.eye(4)
    meta_voxelsize = np.zeros(3)
    # Voxel sizes from the FoV ('voxelsize') are stored in a different
    # order in MRD images. Use metadata for correct orientation
    meta_voxelsize[0] = float(meta[0].get('PixelSpacing', ['0'])[0])
    meta_voxelsize[1] = float(meta[0].get('PixelSpacing', ['0', '0'])[1])
    meta_voxelsize[2] = float(meta[0].get('SliceThickness', '0'))
    
    affine[:3, 3] = np.array(head[0].position)  # Translation
    affine[:3, 0] = np.array(head[0].read_dir ) * voxelsize[0]  # X direction
    affine[:3, 1] = np.array(head[0].phase_dir) * voxelsize[1]  # Y direction
    affine[:3, 2] = np.array(head[0].slice_dir) * voxelsize[2]  # Z direction

    logging.debug("Voxel size from metadata: %s" % (meta_voxelsize,))
    logging.debug("Voxel size from FoV: %s" % (voxelsize,))

    new_img = nib.nifti1.Nifti1Image(data, affine=affine)
    nib.save(new_img, 'nifti_from_h5.nii')
    logging.info('Saved NIfTI image for AFNI processing')

    ## WRITE AFNI SCRIPTS HERE!!!!
    logging.info('Running AFNI processing')
    subprocess.run(["/opt/code/afni_processing.sh", "--input", "nifti_from_h5.nii", "--output", "output_afni"])

    logging.info('Running image transformation for showing stats')
    stat_labels, stat_img = show_stats('output_afni/output_image.nii', 'output_afni/stats.nii')

    logging.info("Config: \n%s", config)

    logging.info('Processing done')

    logging.info('Loading Output image')
    # data_img = nib.load('output_image.nii')
    # data = data_img.get_fdata()
    stat_data = stat_img.get_fdata()
    data = np.concatenate([stat_data, data], axis=-1)
    logging.info("Output image data shape before transposing: %s", data.shape)

    # The output is 4D image with the 4th dimension being different
    # stats maps and repetitions. Transpose it to [cha x y rep slice]
    # for easier reslicing into 2D images. Follow the pattern the
    # images came in - all slices of one rep/stat then all slices of
    # the next rep/stat, etc.
    data = data[:, :, :, :, None]
    data = data.transpose((4, 0, 1, 3, 2))
    n_slices = data.shape[-1]
    n_reps  = data.shape[-2]  # includes number of stats maps as well
    logging.info("Output image data shape: %s", data.shape)

    # Determine max value (12 or 16 bit)
    BitsStored = 12
    # if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
    #     BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
    maxVal = 2**BitsStored - 1

    # Normalize Data and convert to int16
    data = data.astype(np.float64)
    data *= maxVal/data.max()
    data = np.around(data)
    data = data.astype(np.int16)

    currentSeries = 0

    # Precompute indices based on orderinfo
    if orderinfo == 'slices':
        # Stack all slices of repetition 0, then all slices of repetition 1, etc
        outer_range = range(n_reps)
        inner_range = range(n_slices)
        get_idx = lambda i, j: j + i * n_slices
        get_data = lambda data, i, j: data[..., i, j]
        get_rep = lambda i, j: i
    else:
        # Stack all repetitions of slice 0, then all repetitions of slice 1, etc
        outer_range = range(n_slices)
        inner_range = range(n_reps)
        get_idx = lambda i, j: j + i * n_reps
        get_data = lambda data, i, j: data[..., j, i]
        get_rep = lambda i, j: j

    # Re-slice image data back into 2D images
    imagesOut = [None] * (n_reps * n_slices)
    for i in outer_range:
        for j in inner_range:
            img_idx = get_idx(i, j)
            # Make sure that index that gets headers and meta stays in
            # range. Use the first header/meta for stats images
            head_meta_idx = 0 if ((n_reps * n_slices) - img_idx) > len(head) else len(head) - ((n_reps * n_slices) - img_idx)
            # Create new MRD instance for the final image
            # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
            # from_array() should be called with 'transpose=False' to avoid warnings, and when called
            # with this option, can take input as: [cha z y x], [z y x], or [y x]
            # imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)
            # imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)
            imagesOut[img_idx] = ismrmrd.Image.from_array(get_data(data, i, j), transpose=False)
            # outputOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)

            # Create a copy of the original fixed header and update the data_type
            # (we changed it to int16 from all other types)
            # logging.debug("Head index for img %d is %d (out of %d headers)", img_idx, head_meta_idx, len(head))
            oldHeader = head[head_meta_idx]
            oldHeader.data_type = imagesOut[img_idx].data_type

            # Unused example, as images are grouped by series before being passed into this function now
            # oldHeader.image_series_index = currentSeries+1

            # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
            if mrdhelper.get_meta_value(meta[head_meta_idx], 'IceMiniHead') is not None:
                if mrdhelper.extract_minihead_bool_param(base64.b64decode(meta[head_meta_idx]['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                    currentSeries += 1

            rep_no = get_rep(i, j)
            # Stat maps are first in the output
            img_comment = stat_labels[rep_no] if rep_no <= len(stat_labels)-1 else 'fMRI'

            imagesOut[img_idx].setHead(oldHeader)
            # Create a copy of the original ISMRMRD Meta attributes and update
            tmpMeta = meta[head_meta_idx]
            tmpMeta['DataRole']                       = 'Image'
            tmpMeta['ImageProcessingHistory']         = ['PYTHON', 'METABODY']
            tmpMeta['WindowCenter']                   = str((maxVal+1)/2)
            tmpMeta['WindowWidth']                    = str((maxVal+1))
            tmpMeta['SequenceDescriptionAdditional']  = 'OpenRecon'
            tmpMeta['Keep_image_geometry']            = 1
            tmpMeta['ImageComments']                  = img_comment

            #     # Example for setting colormap
            #     if config['parameters']['options'] == 'colormap':
            #         tmpMeta['LUTFileName'] = 'MicroDeltaHotMetal.pal'

            # Add image orientation directions to MetaAttributes if not already present
            if tmpMeta.get('ImageRowDir') is None:
                tmpMeta['ImageRowDir'] = ["{:.18f}".format(oldHeader.read_dir[0]), "{:.18f}".format(oldHeader.read_dir[1]), "{:.18f}".format(oldHeader.read_dir[2])]

            if tmpMeta.get('ImageColumnDir') is None:
                tmpMeta['ImageColumnDir'] = ["{:.18f}".format(oldHeader.phase_dir[0]), "{:.18f}".format(oldHeader.phase_dir[1]), "{:.18f}".format(oldHeader.phase_dir[2])]

            metaXml = tmpMeta.serialize()
            # logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
            # logging.debug("Image data has %d elements", imagesOut[img_idx].data.size)

            imagesOut[img_idx].attribute_string = metaXml

    return imagesOut


def show_stats(img_path, stats_img_path, output_path='./'):
    img = nib.load(img_path)
    data = img.get_fdata()
    data = data[..., 0]
    norm_data = normalise_data(data)

    # Set threshold for showing voxels
    thresh = 0.9
    # Max value for masking (should be 10% higher than normalised max)
    mask_val = 4095 * (1 - thresh)

    # Set to more usual values for MRI image but reserve the top 10% 
    # for showing stats
    norm_data = norm_data * (4095 * thresh)
    logging.info(f'Normalized data to range: {norm_data.min():.2f} - {norm_data.max():.2f}')

    stats_img = nib.load(stats_img_path)
    stats_data = stats_img.get_fdata()
    stats_data = stats_data[..., 0, :]

    # Get stats labels from AFNI.
    result = subprocess.run(
        ["3dinfo", "-label", stats_img_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True
    )
    labels = result.stdout.strip().split('|')
    logging.info(f'Labels: {labels}')

    # Create pairs of coefficients and their t-stat values.
    coefs_stats = []
    for lab in labels:
        if 'Tstat' in lab:
            coef = [ii for ii in labels if ii == lab.replace('Tstat', 'Coef')]
            if len(coef) != 1:
                print(f'Found wrong number of coefficients: {len(coef)}')
            coef = coef[0]
            coefs_stats.append((coef, lab))

    logging.info(f'Coef & Tstat pairs: {coefs_stats}')

    output_data = []
    output_labels = []
    for coef_label, tstat_label in coefs_stats:
        coef_idx = labels.index(coef_label)
        stat_idx = labels.index(tstat_label)

        # Find all data that have coefficients above a certain threshold
        #  (top 20% for example) and set their values to above the max value
        # of the normalised data.
        current_stats_data = np.squeeze(stats_data[..., stat_idx])
        thresh = np.quantile(current_stats_data, 0.9)

        above_idx = current_stats_data >= thresh
        data_copy = norm_data.copy()
        data_copy[above_idx] = 1.1
        output_data.append(data_copy)
        output_labels.append(f'{coef_label}_thresh90')

    output_data = np.stack(output_data, axis=-1)
    output_img = nib.nifti1.Nifti1Image(output_data, img.affine)
    nib.save(output_img, os.path.join(output_path, 'output_image.nii'))

    return output_labels, output_img


def normalise_data(data):
    min_val = np.min(data)
    max_val = np.max(data)
    normalized_data = (data - min_val) / (max_val - min_val)

    return normalized_data

    
