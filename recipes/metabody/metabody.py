import copy
import logging
import os
import subprocess
import traceback

import ismrmrd
import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np

import constants
import mrdhelper


# Folder for debug output files
debugFolder = "/tmp/share/debug"
ORIGINAL_SERIES_INDEX = 99
PROCESSED_SERIES_INDEX = ORIGINAL_SERIES_INDEX + 1
RGB_IMAGE_TYPE = 6
DEFAULT_REPETITION_TIME_SECONDS = 1.0


def get_repetition_time_seconds(image_meta):
    repetition_time_ms = image_meta.get('RepetitionTime')
    if isinstance(repetition_time_ms, (list, tuple)):
        repetition_time_ms = repetition_time_ms[0] if repetition_time_ms else None

    try:
        repetition_time_seconds = float(repetition_time_ms) / 1000
    except (TypeError, ValueError):
        logging.warning(
            'RepetitionTime is missing or invalid; using %.3f seconds',
            DEFAULT_REPETITION_TIME_SECONDS,
        )
        return DEFAULT_REPETITION_TIME_SECONDS

    if repetition_time_seconds <= 0:
        logging.warning(
            'RepetitionTime must be positive; using %.3f seconds',
            DEFAULT_REPETITION_TIME_SECONDS,
        )
        return DEFAULT_REPETITION_TIME_SECONDS

    return repetition_time_seconds


def copy_original_images(images):
    images_out = []
    for image in images:
        copied_image = copy.deepcopy(image)
        copied_image.image_series_index = ORIGINAL_SERIES_INDEX

        copied_meta = ismrmrd.Meta.deserialize(copied_image.attribute_string)
        copied_meta['Keep_image_geometry'] = 1
        copied_image.attribute_string = copied_meta.serialize()
        images_out.append(copied_image)

    return images_out


def get_processed_series_index(images):
    used_series_indices = {int(image.image_series_index) for image in images}
    series_index = PROCESSED_SERIES_INDEX
    while series_index in used_series_indices:
        series_index += 1
    return series_index


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

    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()



def process_image(images, connection, config, metadata):
    if len(images) == 0:
        return []

    send_originals = mrdhelper.get_json_config_param(
        config, 'sendOriginal', default=True
    )
    colormap = mrdhelper.get_json_config_param(
        config, 'colormap', default='seismic'
    )
    do_rgb = colormap != 'none'

    # Note: The MRD Image class stores data as [cha z y x]
    # Extract image data into a 5D array of size [img cha z y x]
    data = []
    head = []
    meta = []
    header_index_by_slice = {}
    slices = []
    repetitions = []
    for img_idx, img in enumerate(images):
        data.append(img.data)
        head.append(img.getHead())
        meta.append(ismrmrd.Meta.deserialize(img.attribute_string))
        header_index_by_slice.setdefault(img.slice, img_idx)
        slices.append(img.slice)
        repetitions.append(img.repetition)
    data = np.stack(data)
    slice_ids = sorted(set(slices))
    repetition_ids = sorted(set(repetitions))
    slice_positions = {
        slice_id: position for position, slice_id in enumerate(slice_ids)
    }
    repetition_positions = {
        repetition_id: position
        for position, repetition_id in enumerate(repetition_ids)
    }
    n_slices = len(slice_ids)
    n_repetitions = len(repetition_ids)

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
    logging.info(f'Number of slices             : {n_slices}')
    logging.info(f'First 10 slice numbers       : {slices[:10]}')
    logging.info(f'Number of repetitions        : {n_repetitions}')
    logging.info(f'First 10 repetition numbers  : {repetitions[:10]}')

    logging.debug("Original image data before transposing is %s" % (data.shape,))
    # Turn into 4D fMRI data
    # New data with the correct 4D dimensions
    # It looks like MRD images might be [img cha z y x]
    # Transpose to [y x rep slc]
    new_data = np.zeros((data.shape[-2], data.shape[-1], n_slices, n_repetitions))
    for img in images:
        # Use dense positions so sparse MRD counters do not become array indices.
        slice_position = slice_positions[img.slice]
        repetition_position = repetition_positions[img.repetition]
        new_data[:, :, slice_position, repetition_position] = img.data[0, 0, :, :]
    data = new_data

    logging.debug("Original image data after transposing is %s" % (data.shape,))
    logging.debug("Transformed to 4D: %s (slices: %d; repetitions: %d)" % (data.shape, n_slices, n_repetitions))

    # The affine matrix can be reconstructed from the metadata
    affine = np.eye(4)
    meta_voxelsize = np.zeros(3)
    # Voxel sizes from the FoV ('voxelsize') are stored in a different
    # order in MRD images. Use metadata for correct orientation
    meta_voxelsize[0] = float(meta[0].get('PixelSpacing', ['0'])[0])
    meta_voxelsize[1] = float(meta[0].get('PixelSpacing', ['0', '0'])[1])
    meta_voxelsize[2] = float(meta[0].get('SliceThickness', '0'))
    
    first_slice_header = head[header_index_by_slice[slice_ids[0]]]
    affine[:3, 3] = np.array(first_slice_header.position)  # Translation
    affine[:3, 0] = np.array(head[0].phase_dir ) * voxelsize[1]  # Y direction
    affine[:3, 1] = np.array(head[0].read_dir) * voxelsize[0]  # X direction
    affine[:3, 2] = np.array(head[0].slice_dir) * voxelsize[2]  # Z direction

    logging.debug("Voxel size from metadata: %s" % (meta_voxelsize,))
    logging.debug("Voxel size from FoV: %s" % (voxelsize,))

    repetition_time_seconds = get_repetition_time_seconds(meta[0])
    new_img = nib.nifti1.Nifti1Image(data, affine=affine)
    zooms = list(new_img.header.get_zooms())
    zooms[3] = repetition_time_seconds
    new_img.header.set_zooms(zooms)
    new_img.header.set_xyzt_units(xyz='mm', t='sec')
    nib.save(new_img, 'nifti_from_h5.nii')
    logging.info('Saved NIfTI image for AFNI processing')

    ## WRITE AFNI SCRIPTS HERE!!!!
    logging.info('Running AFNI processing')
    try:
        subprocess.run(
            [
                "/opt/code/afni_processing.sh",
                "--input",
                "nifti_from_h5.nii",
                "--output",
                "output_afni",
                "--tr",
                f"{repetition_time_seconds:g}",
            ],
            check=True,
        )

        logging.info('Running image transformation for showing stats')
        stat_labels, stat_img = show_stats(
            'output_afni/output_image.nii',
            'output_afni/stats.nii',
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.error('AFNI processing failed:\n%s', traceback.format_exc())
        if send_originals:
            logging.warning(
                'Returning original images because AFNI produced no output'
            )
            return copy_original_images(images)
        raise

    stat_data = stat_img.get_fdata()

    logging.info("Config: \n%s", config)

    logging.info('Processing done')

    if not send_originals:
        logging.debug('`sendOriginal` is false')
    else:
        logging.debug('`sendOriginal` is true')
    data = stat_data
    logging.info("Output image data shape before transposing: %s", data.shape)

    # The output is 4D image with the 4th dimension being different
    # stats maps. Transpose it to [cha y x stat_map slice] for easier
    # reslicing into 2D images. Follow the pattern the images came
    # in.
    data = data[:, :, :, :, None]
    data = data.transpose((4, 0, 1, 3, 2))
    n_slices = data.shape[-1]
    # Stat maps take the place of repetitions in the output
    n_stats = data.shape[-2]  # includes number of stats maps as well
    logging.info("Output image data shape: %s", data.shape)

    # Determine max value (12 or 16 bit)
    BitsStored = 12
    # if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
    #     BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
    maxVal = 2**BitsStored - 1

    # Normalize data
    data = data.astype(np.float64)
    data *= maxVal/data.max()
    data = np.around(data)

    if do_rgb:
        logging.info(f'Converting data into RGB using colormap "{colormap}"')

        # Normalize to (0.0, 1.0) as expected by get_cmap()
        data = data.astype(np.float64)
        data -= data.min()
        data *= 1/data.max()

        # Apply colormap
        cmap = plt.get_cmap(colormap)
        rgb = cmap(data)

        # Remove alpha channel. The input shape is
        # [cha y x rep slice 4] where the last dimension is RGBA.
        # `cha` can be replaced by RGB and become `z` (see the required
        # shape when creating the ISMRMRD Image with `cha` below).
        rgb = rgb[...,0:-1]
        rgb = rgb.transpose((5, 0, 1, 2, 3, 4))
        # rgb = rgb.squeeze()

        # MRD RGB images must be uint16 in range (0, 255)
        rgb *= 255
        data = rgb.astype(np.uint16)
        # np.save(debugFolder + "/" + "imgRGB.npy", data)
    else:
        data = data.astype(np.int16)

    # Re-slice image data back into 2D images.
    # Preallocate outputs for speed and efficiency.
    imagesOut = [None] * (n_stats * n_slices)
    image_details = {
        'slice': [None] * (n_stats * n_slices),
        'repetition': [None] * (n_stats * n_slices),
        'x': [None] * (n_stats * n_slices),
        'y': [None] * (n_stats * n_slices)
    }
    # The header determines the assembly of images not their order in
    # the output list.
    processed_series_index = get_processed_series_index(images)
    logging.info(
        'Processed images use image_series_index=%d',
        processed_series_index,
    )
    out_idx = 0
    for stat in range(n_stats):
        for slice_position, slice_id in enumerate(slice_ids):
            header_index = header_index_by_slice[slice_id]

            # Create new MRD instance for the final image
            # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
            # from_array() should be called with 'transpose=False' to avoid warnings, and when called
            # with this option, can take input as: [cha z y x], [z y x], or [y x]
            img_data = data[..., stat, slice_position]
            # logging.debug(f'Output image data shape for slice {slc} and stat {stat}: {img_data.shape}')
            # if do_rgb:
            #     # Make sure RGB takes place of 'cha'.
            #     img_data = np.expand_dims(img_data, axis=1)
            imagesOut[out_idx] = ismrmrd.Image.from_array(img_data, transpose=False)

            # Create a copy of the original fixed header and update the data_type
            # (we changed it to int16 from all other types)
            # logging.debug("Head index for img %d is %d (out of %d headers)", idx, head_meta_idx, len(head))
            oldHeader = copy.deepcopy(head[header_index])

            oldHeader.data_type = imagesOut[out_idx].data_type
            oldHeader.image_series_index = processed_series_index
            oldHeader.slice = slice_id
            oldHeader.repetition = stat
            oldHeader.image_index = out_idx + 1
            if do_rgb:
                # Set RGB parameters
                # To be defined as ismrmrd.IMTYPE_RGB
                oldHeader.image_type = RGB_IMAGE_TYPE
                # RGB "channels".  This is set by from_array, but need to
                # be explicit as we're copying the old header instead.
                # Otherwise the data itself gets amended.
                oldHeader.channels = 3
            imagesOut[out_idx].setHead(oldHeader)

            # Stat maps are first in the output
            img_comment = stat_labels[stat] if stat <= len(stat_labels)-1 else 'fMRI'
            # Create a copy of the original ISMRMRD Meta attributes and update
            tmpMeta = ismrmrd.Meta.deserialize(meta[header_index].serialize())
            tmpMeta['DataRole']                       = 'Image'
            tmpMeta['ImageProcessingHistory']         = ['PYTHON', 'METABODY']
            tmpMeta['WindowCenter']                   = str((maxVal+1)/2)
            tmpMeta['WindowWidth']                    = str((maxVal+1))
            tmpMeta['SequenceDescriptionAdditional']  = 'OpenRecon'
            tmpMeta['Keep_image_geometry']            = 1
            tmpMeta['ImageComments']                  = img_comment

            # Example for setting RGB
            if do_rgb:
                tmpMeta['SequenceDescriptionAdditional']  = 'FIRE_RGB'
                tmpMeta['ImageProcessingHistory'].append('RGB')

                # RGB images have no windowing
                del tmpMeta['WindowCenter']
                del tmpMeta['WindowWidth']

                # RGB images shouldn't undergo further processing, e.g. orientation or distortion correction
                tmpMeta['InternalSend'] = 1

                tmpMeta['LUTFileName'] = 'MicroDeltaHotMetal.pal'

            # Add image orientation directions to MetaAttributes if not already present
            if tmpMeta.get('ImageRowDir') is None:
                tmpMeta['ImageRowDir'] = ["{:.18f}".format(oldHeader.read_dir[0]), "{:.18f}".format(oldHeader.read_dir[1]), "{:.18f}".format(oldHeader.read_dir[2])]

            if tmpMeta.get('ImageColumnDir') is None:
                tmpMeta['ImageColumnDir'] = ["{:.18f}".format(oldHeader.phase_dir[0]), "{:.18f}".format(oldHeader.phase_dir[1]), "{:.18f}".format(oldHeader.phase_dir[2])]

            metaXml = tmpMeta.serialize()
            # logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
            # logging.debug("Image data has %d elements", imagesOut[out_idx].data.size)

            imagesOut[out_idx].attribute_string = metaXml
            # For debugging and troubleshooting.
            image_details['slice'][out_idx] = int(slice_id)
            image_details['repetition'][out_idx] = stat
            image_details['x'][out_idx] = imagesOut[out_idx].data.shape[-1]
            image_details['y'][out_idx] = imagesOut[out_idx].data.shape[-2]

            out_idx += 1

    # Send a copy of original (unmodified) images back too
    if send_originals:
        logging.info(
            'Sending a copy of original unmodified images due to '
            'sendOriginal set to True'
        )
        imagesOut = copy_original_images(images) + imagesOut

    n_out_imgs = len(imagesOut)
    x_not_same = np.any(np.diff(image_details['x']) != 0)
    y_not_same = np.any(np.diff(image_details['y']) != 0)
    logging.debug(f'Outputting {n_out_imgs} images with shape ({image_details["x"][0]}, {image_details["y"][0]})')
    logging.debug(f'Slices: {image_details["slice"][:10]}; Repetitions: {image_details["repetition"][:10]}')
    if x_not_same:
        logging.warning('Output images have different x dimensions!')
    if y_not_same:
        logging.warning('Output images have different y dimensions!')

    logging.debug('Example header:')
    for field_name, field_type in oldHeader._fields_:
        var = getattr(oldHeader, field_name)
        if hasattr(var, '_length_'):
            retstr = '  %s: (%s), ' % (field_name, ', '.join((str(v) for v in var)))
        else:
            retstr = '  %s: %s, ' % (field_name, var)
        logging.debug(retstr)

    return imagesOut


def show_stats(img_path, stats_img_path, output_path='./'):
    img = nib.load(img_path)
    data = img.get_fdata()
    data = data[..., 0]
    norm_data = normalise_data(data)

    # Set threshold for showing voxels
    thresh = 0.9
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
