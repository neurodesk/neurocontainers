import ismrmrd
import os
import logging
import traceback
import numpy as np
import xml.dom.minidom
import base64
import mrdhelper
import constants
import nibabel as nib
from scipy.ndimage import binary_erosion, binary_dilation
import subprocess

# Folder for debug output files
debugFolder = "/tmp/share/debug"

def log_array_info(arr, label):
    """
    Some verbose logging
    """
    logging.info("B0MAP_LOG: %s array info:", label)
    logging.info("  type: %s", type(arr))
    logging.info("  dtype: %s", arr.dtype)
    logging.info("  ndim: %d", arr.ndim)
    logging.info("  shape: %s", arr.shape)
    logging.info("  size (total elements): %d", arr.size)

    # Length of each dimension
    for axis, dim in enumerate(arr.shape):
        logging.info("  axis %d length: %d", axis, dim)

    # Safe reductions
    if arr.size > 0:
        logging.info("  min: %s", np.min(arr))
        logging.info("  max: %s", np.max(arr))
        logging.info("  mean: %s", np.mean(arr))
    else:
        logging.info("B0MAP_LOG:  array is empty; min/max/mean skipped")

def get_MiniHeader_fromItem(item):
    """
    Docstring for get_MiniHeader_fromItem
    :param item: the incoming item coming in from connection.

    A Wrapper for the base64.b64decode((ismrmrd.Meta.deserialize(item.attribute_string))['IceMiniHead']).decode('utf-8')
    For easier comprehension.
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    return base64.b64decode(meta['IceMiniHead']).decode('utf-8')

def get_TE_fromItem(item):
    """
    get_TE_fromItem(item):
    :param item: the incoming item coming in from connection.
    
    Extracts the TE of a item/slice from the IceMiniHeader and returns the value as a float.
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    EchoTime = mrdhelper.extract_minihead_double_param(base64.b64decode(meta['IceMiniHead']).decode('utf-8'), 'TE')
    return float(EchoTime)

def get_PrimaryKey_fromItem(item):
    """
    get_PrimaryKey_fromItem(item)
    :param item: the incoming item coming in from connection.

    This function finds a suitable parameter to act as a primary key and returns the value of that key.
    There is three ranked options for a Primary Key:
        1. NumberInSeries from the IceMiniHeader
        2. SliceNo from the IceMiniHeader
        3. item.slice from the ImageHeader class of https://github.com/ismrmrd/ismrmrd-python > ismrmrd/image.py
    NOTE: SliceNo runs opposite to NumberInSeries and item.slice, i.e. NumberInSeries = 0 then SliceNo = 63

    COMMENTS:
        In all testing performed this function has been sufficient and consistent for the complete series,
        despite the possibility of two slices in a image having a different PrimaryKey i.e. 
            first slice has a PrimaryKey value from NumberInSeries
            second slice has a PrimaryKey value from SliceNo
        This has not been seen in testing.
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    IceMiniHeader = base64.b64decode(meta['IceMiniHead']).decode('utf-8')
    if mrdhelper.extract_minihead_long_param(IceMiniHeader, 'NumberInSeries') is not None:
        return int(mrdhelper.extract_minihead_long_param(IceMiniHeader, 'NumberInSeries'))
    elif mrdhelper.extract_minihead_long_param(IceMiniHeader, 'SliceNo') is not None:
        return int(mrdhelper.extract_minihead_long_param(IceMiniHeader, 'SliceNo'))
    else:
        try:
            S = item.slice
            return int(S)
        except Exception as e:
            logging.info("B0MAP_ERROR: Failed to find a primary Key %s", e)
        return None
    
def get_PrimaryKey_fromItem_LogOnly(item):
    """
    Almost the same as get_PrimaryKey_fromItem, but just logs.
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    IceMiniHeader = base64.b64decode(meta['IceMiniHead']).decode('utf-8')
    if mrdhelper.extract_minihead_long_param(IceMiniHeader, 'NumberInSeries') is not None:
        logging.info("B0MAP_LOG: PrimaryKey is NumberInSeries")
        return None
    elif mrdhelper.extract_minihead_long_param(IceMiniHeader, 'SliceNo') is not None:
        logging.info("B0MAP_LOG: PrimaryKey is NumberInSeries")
        return None
    else:
        try:
            S = item.slice
            logging.info("B0MAP_LOG: PrimaryKey is item.slice")
            return int(S)
        except Exception as e:
            logging.info("B0MAP_ERROR: Failed to find a primary Key %s", e)
        return None

def get_IceMiniHeader_Double_fromItem(item,param):
    """
    Docstring for get_IceMiniHeader_Double_fromItem

    :param item: the incoming item coming in from connection.
    :param param: a str text of the double param you are looking for.
        
    This is a wrapper function to make the code more comprehendable.
    
    Extracts the double paramater from the IceMiniHeader and returns the param as a float
    e.g.
        get_IceMiniHeader_Double_fromItem(item,'RescaleIntercept')
    
    NOTE:
    if you are unsure whether your desired parameter is a Double or Long or String, etc. it is recommended to 
    log the complete IceMiniHeader in a test/logging container e.g.
        for item in connection:
            if isinstance(item, ismrmrd.Image):
                logging.info("IceMiniHead: %s", base64.b64decode((ismrmrd.Meta.deserialize(item.attribute_string))['IceMiniHead']).decode('utf-8'))
                or using get_MiniHeader_fromItem:
                logging.info("IceMiniHead: %s", get_MiniHeader_fromItem(item))                
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    Double = mrdhelper.extract_minihead_double_param(base64.b64decode(meta['IceMiniHead']).decode('utf-8'), str(param))
    return float(Double)

def get_IceMiniHeader_Long_fromItem(item,param):
    """
    Docstring for get_IceMiniHeader_Long_fromItem

    :param item: the incoming item coming in from connection.
    :param param: a str text of the long param you are looking for.
    
    This is a wrapper function to make the code more comprehendable.
    
    Extracts the long paramater from the IceMiniHeader and returns the param as a float
    e.g.
        get_IceMiniHeader_Long_fromItem(item,'BitsStored')
    
    NOTE:
    if you are unsure whether your desired parameter is a Double or Long or String, etc. it is recommended to 
    log the complete IceMiniHeader in a test/logging container e.g.
        for item in connection:
            if isinstance(item, ismrmrd.Image):
                logging.info("IceMiniHead: %s", base64.b64decode((ismrmrd.Meta.deserialize(item.attribute_string))['IceMiniHead']).decode('utf-8'))
                or using get_MiniHeader_fromItem:
                logging.info("IceMiniHead: %s", get_MiniHeader_fromItem(item))        
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    long = mrdhelper.extract_minihead_long_param(base64.b64decode(meta['IceMiniHead']).decode('utf-8'), str(param))
    return float(long)

def get_IceMiniHeader_String_fromItem(item,param):
    """
    Docstring for get_IceMiniHeader_String_fromItem
    
    :param item: the incoming item coming in from connection.
    :param param: a str text of the long param you are looking for.
    
    This is a wrapper function to make the code more comprehendable.
    
    Extracts the string paramater from the IceMiniHeader and returns the param as a string
    e.g.
        get_IceMiniHeader_String_fromItem(item, "SequenceDescription")
    
    NOTE:
    if you are unsure whether your desired parameter is a Double or Long or String, etc. it is recommended to 
    log the complete IceMiniHeader in a test/logging container e.g.
        for item in connection:
            if isinstance(item, ismrmrd.Image):
                logging.info("IceMiniHead: %s", base64.b64decode((ismrmrd.Meta.deserialize(item.attribute_string))['IceMiniHead']).decode('utf-8'))
                or using get_MiniHeader_fromItem:
                logging.info("IceMiniHead: %s", get_MiniHeader_fromItem(item))        
    """
    meta = ismrmrd.Meta.deserialize(item.attribute_string)
    string = mrdhelper.extract_minihead_string_param(base64.b64decode(meta['IceMiniHead']).decode('utf-8'), str(param))
    return str(string)

def process_b0map(Te1Group, PhGroup,TEs, connection, config, metadata,currentSeries):
    """
    
    :param Te1Group: A list of the TE1 or magnitude 1 slices
    :param PhGroup: A list of the phase slices
    :param TEs: [float(TE1),float(TE2)]
    :param connection: the connection class to the MRD Server
    :param config: JSON configs and user defined params as selected in the inline card.
    :param metadata: metadata of the sequence
    :param currentSeries: an int value from process.

    Constructs a B0map from a list of TE1 and Phase items, a relevant params 
    and returns a list B0map items with mean and std in the image comment, 
    AND an int value of the currentSeries for - for item in connection: - consistency
    
    Steps to construct B0map:

    Step 0. Determining Additional Parameters
        B0map Series Number 
        delta Echo Time
        Number of Erosions
        Number of Dilations
        Rescale Intercept
        Rescale Slope
        BitsStored

    Step 1. Sorting Groups
        Sort Groups so Slices are in a suitable order for image-image operations:
            - Sorting using a PrimaryKey based method (get_PrimaryKey_fromItem func)
            - creates (key,item) pairs and sorts ascendingly based on key
            - unpacks pairs back to groups that are now sorted ascendingly based on PrimaryKey

    Step 2. Break Groups into head, data, img arrays
        Unpacks Groups into relevant head, data and img arrays that are used
        for calculating a B0map and its meta and header info.


    Step 3. transpose Data arrays

    Step 4. Make Frequency Map
        4.0 Raw Phase Data
        4.1 Rescaled Phase Data (PHdata_Rescale = PHdata*RescaleSlope + RescaleIntercept)
        4.2 Convert to Radians (PHdata_inRad = PHdata_Rescale*(np.pi/np.abs(RescaleIntercept)) ) 
        4.3 Convert to Frequency (Fdata = PHdata_inRad/(2*np.pi * delTE*1e-3) )

    Step 5. Make BMask (bet2 and scipy.ndimage)

    Step 6. Make B0Map 
        6.0 Make B0Map (FData*BMask)
        6.1 Compensate for machine rescaling (B0map_compensate = (B0map - RescaleIntercept)/RescaleSlope)
        6.2 Machine output: (expected_machine_out = B0map_compensate*RescaleSlope + RescaleIntercept) ((Inverse of 6.1))

    Step 7. Calculate STD, Mean

    Step 8. Prepare B0map for MRD Server
        0. Add char,img dims back. NOTE: Not necessary as they are removed again later
        1. Copy Phase metadata to B0map metadata
        2. Apply B0mapseries Number
        3. add STDNoZero and MeanNoZero to Image Comments
        4. Add ImageHistory
    
    Step 9. returns a list of B0map items with appropriate metadata 
        AND the an updated int value of the currentSeries for the - for item in connection: - loop consistency 
    """

    # Check if Groups are empty
    if (len(Te1Group) == 0) or (len(PhGroup) == 0):
        return []
    
    # Indicate beginning of B0map Processing
    logging.info("B0MAP_LOG: Processing B0map...")
    
    # Step 0. Determining Additional Parameters
    b0mapSeries = max(item.image_series_index for item in (Te1Group + PhGroup)) + 30 # ensures no clashing of series numbers
    delTE = TEs[1]-TEs[0]
    xform = np.eye(4)
    Num_ero = mrdhelper.get_json_config_param(config, 'NumEro', default=3, type='int')
    Num_Dil = mrdhelper.get_json_config_param(config, 'NumDil', default=2, type='int')
    Verbose = False
    FracIntens = 0.4
    FracIntens = float(mrdhelper.get_json_config_param(config, 'FracIntens', default=0.4, type='float'))
    try:
        Verbose = bool(mrdhelper.get_json_config_param(config, 'VerboseLogging', default=True, type='bool'))
        if Verbose:
            logging.info("B0MAP_LOG: READING JSON CONFIGS... Verbose Logging - Enabled")
        else:
            logging.info("B0MAP_LOG: READING JSON CONFIGS... Verbose Logging - Disabled")
    except Exception as e:
        logging.error(f"B0MAP_ERROR: READING JSON CONFIGS... Failed to config: Verbose Logging:\n{e}")
        logging.info("B0MAP_LOG: READING JSON CONFIGS... Resorting to default value: Verbose Logging - Disabled")
    
    try:
        FracIntens = round(float(mrdhelper.get_json_config_param(config, 'FracIntens', default=0.4, type='float')),1)
        logging.info(f"B0MAP_LOG: READING JSON CONFIGS... Fractional Intensity: {FracIntens}")
    except Exception as e:
        logging.error(f"B0MAP_ERROR: READING JSON CONFIGS... Failed to config: Fractional Intensity:\n{e}")
        logging.info("B0MAP_LOG: READING JSON CONFIGS... Resorting to default value: Fractional Intensity - 0.4")
    
    # Finding Scaling Params of Phase Image
    
    RescaleIntercept = -4096 # Fix this!!

    if (get_IceMiniHeader_Double_fromItem(PhGroup[0],'RescaleIntercept') is not None):
        RescaleIntercept = get_IceMiniHeader_Double_fromItem(PhGroup[0],'RescaleIntercept') # If works replace tmp3 with RescaleIntercept
        logging.info("B0MAP_LOG: Rescale Intercept: %d found in PhGroup[0] IceHeader", RescaleIntercept)# If works replace tmp3 with RescaleIntercept
    else:
        logging.info("B0MAP_ERROR: Rescale Intercept - not found in PhGroup[0] IceHeader")
    RescaleSlope = 2 # Fix This!!
    if (get_IceMiniHeader_Double_fromItem(PhGroup[0],'RescaleSlope') is not None):
        RescaleSlope = get_IceMiniHeader_Double_fromItem(PhGroup[0],'RescaleSlope')# If works replace tmp3 with RescaleSlope
        logging.info("B0MAP_LOG: Rescale Slope: %d found in PhGroup[0] IceHeader", RescaleSlope)# If works replace tmp3 with RescaleSlope
    else:
        logging.info("B0MAP_ERROR: Rescale Slope - not found in PhGroup[0] IceHeader")

    BitsStored = 12
    if (get_IceMiniHeader_Long_fromItem(Te1Group[0],'BitsStored') is not None):
        BitsStored = int(get_IceMiniHeader_Long_fromItem(Te1Group[0],'BitsStored'))
        logging.info("B0MAP_LOG: BitsStored: %d found in IceHeader", BitsStored)
    elif (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
        BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
        logging.info("B0MAP_LOG: BitsStored: %d found in metadata",BitsStored)
    
    logging.info(f"B0MAP_LOG: Params Collected:\n b0mapSeries: {b0mapSeries}\n delTE: {delTE}\n Num_ero: {Num_ero}\n Num_Dil: {Num_Dil} \nRescale Intercept: {RescaleIntercept} \nRescale Slope: {RescaleSlope}\nBitsStored: {BitsStored}")

    # Step 1. Sorting Groups
    
    # Log the PrimaryKey list for each group.
    if Verbose:
        try:
            logging.info("B0MAP_LOG: Determining Primary Keys from Slices...")
            TE1Iorder = [get_PrimaryKey_fromItem(item) for item in Te1Group]
            PHIorder = [get_PrimaryKey_fromItem(item) for item in PhGroup]

            logging.info(f"B0MAP_LOG: Key List of Groups\nTE1: {TE1Iorder} \nPH:{PHIorder}")
        except Exception as e:
            logging.error("B0MAP_ERROR: Failed to bulk parse primary key from groups, Sorting Groups...")

    def group_with_key(items, label):
        """
        Docstring for group_with_key
    
        :param items: a list of items from connection (from the process function)
        :param label: an identifiable string for logging purposes.
        
        Extract primary keys from a list of items and pairs each item with its key, and returns
        a list of (key,item) tuples.

        NOTE: This complicated function that avoids an expensive O(nlog(n)) calculation.
        """
        keyed = []
        for item in items:
            try:
                key = get_PrimaryKey_fromItem(item)
                keyed.append((key, item))
            except Exception as e:
                logging.error("B0MAP_ERROR: Failed to extract primary key for %s image: %s",label, e)
        return keyed

    def validate_keys(keyed_items, label):
        """
        Docstring for validate_keys
        
        :param keyed_items: a list of (key,item) tuples generated from the group_with_key function
        :param label: an identifiable string for logging purposes.
        
        This function does a simple check to see if there is any duplicated primary keys. 
        NOTE: it does not check whether there is any missing 
        """
        keys = [k for k, _ in keyed_items]
        if len(keys) != len(set(keys)):
            logging.error("B0MAP_ERROR: Duplicate primary keys detected in %s group: %s",label, keys)
        else:
            logging.info("B0MAP_LOG: %s group is correctly keyed with keys %s", label, keys)
    
    # Extracts PrimaryKeys from items, and pairs each with its key
    # in a list (key,item) tuples and call it {GROUP_NAME}_keyed
    # Also check whether there is any duplicate keys.
    Te1_keyed = group_with_key(Te1Group, "TE1")
    validate_keys(Te1_keyed, "TE1")

    Ph_keyed  = group_with_key(PhGroup,  "PH")
    validate_keys(Ph_keyed,  "PH")

    #Sort items ascendingly, and unpack tuples back into a sorted (ascendingly) list of items for each group.
    Te1Group = [item for _, item in sorted(Te1_keyed, key=lambda x: x[0])]
    PhGroup  = [item for _, item in sorted(Ph_keyed,  key=lambda x: x[0])]
    
    if Verbose:
        logging.info("B0MAP_LOG: After Sorting Groups\nTE1: %s\nPH:%s",[get_PrimaryKey_fromItem(i) for i in Te1Group],[get_PrimaryKey_fromItem(i) for i in PhGroup],)

    # Step 2. Break Groups into head, data, img arrays
    logging.info(f"B0MAP_LOG: Unpacking Groups into head, data meta arrays...")
    
    # Note: The MRD Image class stores data as [cha z y x]
    # Extract image data into a 5D array of size [img cha z y x]
    T1data = np.stack([item.data                              for item in Te1Group])
    #T1head = [item.getHead()                                  for item in Te1Group] #NOT USED
    #T1meta = [ismrmrd.Meta.deserialize(item.attribute_string) for item in Te1Group] #NOT USED

    PHdata = np.stack([item.data                              for item in PhGroup])
    PHhead = [item.getHead()                                  for item in PhGroup]
    PHmeta = [ismrmrd.Meta.deserialize(item.attribute_string) for item in PhGroup]


    
    # Step 3. transpose Data arrays
    logging.info(f"B0MAP_LOG: Transposing data arrays...")
    # Reformat data to [y x z cha img], i.e. [row col] for the first two dimensions
    logging.info("Original shape: %s", T1data.shape)
    tmp = T1data.transpose((3, 4, 2, 1, 0))
    logging.info("After transpose: %s", tmp.shape)
    T1data, PHdata = [data.transpose((3, 4, 2, 1, 0))[:, :, 0, 0, :] for data in [T1data, PHdata]]
    cen_e = tuple(s//2 for s in T1data.shape) # center element
    logging.info("After slice and transpose: %s", T1data.shape)
    logging.info(f"B0MAP_LOG: center element; PHdata{cen_e}: {PHdata[cen_e]}")
    logging.info(f"B0MAP_LOG: center element; T1data{cen_e}: {T1data[cen_e]}")
    if Verbose:
        log_array_info(T1data, "T1data")
        log_array_info(PHdata, "PHdata")


    # Some Early Processing
    PHdata = PHdata.astype(np.float64)
    T1data = T1data.astype(np.float64)
    
    #Step 4. Make Frequency Map
    logging.info(f"B0MAP_LOG: Making Frequency map...")
    logging.info(f"B0MAP_LOG: Values used for Frequency Map:: RescaleSlope: {RescaleSlope}, RescaleIntercept: {RescaleIntercept}, delTe: {delTE}")

    # 0. Raw Data
    try:
        logging.info(f"B0MAP_LOG: center element; PHdata{cen_e}: {PHdata[cen_e]}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: could not log center element of PHdata\n{e}")
    # 1. Rescale
    PHdata_Rescale = PHdata*RescaleSlope + RescaleIntercept
    try:
        logging.info(f"B0MAP_LOG: center element; PHdata_Rescale{cen_e}: {PHdata_Rescale[cen_e]}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: could not log center element of PHdata_Rescale\n{e}")
    # 2. Convert to radians
    PHdata_inRad = PHdata_Rescale*(np.pi/np.abs(RescaleIntercept))
    try:
        logging.info(f"B0MAP_LOG: center element; PHdata_inRad{cen_e}: {PHdata_inRad[cen_e]}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: could not log center element of PHdata_inRad\n{e}")
    # 3. Convert to Frequency
    Fdata = PHdata_inRad/(2*np.pi * delTE*1e-3)
    try:
        logging.info(f"B0MAP_LOG: center element; Fdata{cen_e}: {Fdata[cen_e]}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: could not log center element of Fdata\n{e}")

    # Step 5. Make BMask (Uses bet2 and scipy.ndimage)
    logging.info(f"B0MAP_LOG: Making BrainMask...")
    logging.info(f"B0MAP_LOG: T1data.shape: {T1data.shape}")
    T1data = T1data.transpose(1,0,2) # to [x,y,z]
    logging.info(f"T1data.transpose(1,0,2).shape: {T1data.shape}")
    logging.info(f"B0MAP_LOG: Attempting to save T1data.transpose(1,0,2) as nifti...")
    try:
        nib.save(nib.nifti1.Nifti1Image(T1data,xform),'temp_t1.nii')
        if os.path.isfile('temp_t1.nii'):
            logging.info("B0MAP_LOG: successfully saved T1data.transpose(1,0,2) as nifti!")
        else:
            logging.error("B0MAP_ERROR: Cannot find T1data.transpose(1,0,2) as nifti!")
    except Exception as e:
        logging.error(f"B0MAP_ERROR: Failed to save T1data.transpose(1,0,2) as nifti.\n{e}")

    
    logging.info(f"B0MAP_LOG: Attempting to run: bet2 temp_t1.nii temp_bm -m -n -f {round(FracIntens,1)} ...")
    try: 
        subprocess.run(["bet2","temp_t1.nii","temp_bm","-m","-n","-f",f"{round(FracIntens,1)}"],check=True)
        if os.path.isfile('-n.nii.gz'):
            logging.info("B0MAP_LOG: successfully saved -n.nii-gz brain mask (weird bug)")
        else:
            logging.error("B0MAP_ERROR: Cannot find -n.nii.gz")
    except Exception as e:
        logging.error(f"B0MAP_ERROR: Failed to run: bet2 temp_t1.nii temp_bm -m -n -f 0.4 ... \n{e}")
    
    BMask = None
    try:
        logging.info(f"B0MAP_LOG: Importing Brain Mask...")
        BMask_load = nib.load("-n.nii.gz")
        BMask = BMask_load.get_fdata()
        try:
            logging.info(f"B0MAP_LOG: center element; BMask_Raw{cen_e}: {BMask[cen_e]}")
        except Exception as e:
            logging.log(f"B0MAP_LOG: could not log center element of BMask_Raw\n{e}")
        logging.info(f"B0MAP_LOG: Applying {Num_ero} Erosions and {Num_Dil} Dilations...")
        try: 
                BMask = binary_erosion(BMask, iterations=Num_ero)
        except Exception as e:
            logging.info(f"B0MAP_ERROR: could not Erode B0Mask_Raw\n{e}")
        try: 
                BMask = binary_dilation(BMask,iterations=Num_Dil)
        except Exception as e:
            logging.info(f"B0MAP_ERROR: could not Dilate B0Mask_Raw\n{e}")
        logging.info(f"B0MAP_LOG: BMask.shape: {BMask.shape}")
        BMask = BMask.transpose((1,0,2)).astype(np.float64)
        logging.info(f"B0MAP_LOG: After Transpose BMask.shape: {BMask.shape}")
        try:
            logging.info(f"B0MAP_LOG: center element; BMask{cen_e}: {BMask[cen_e]}")
        except Exception as e:
            logging.info(f"B0MAP_ERROR: could not log center element of BMask\n{e}")
    except Exception as e:
        logging.info(f"B0MAP_LOG: Failed to import Brain Mask\n{e}")


    # Step 6. Make B0Map (FData*BMask)
    logging.info(f"B0MAP_LOG: Making B0Map...")
    logging.info("Original Fdata shape: %s", Fdata.shape)
    tmp = Fdata[:,:,None,None,:]
    logging.info("After Fdata Adding: %s", tmp.shape)
    if BMask is None:
        logging.info("B0MAP_ERROR: Failed to calculate BrainMask resorting to B0map with brain mask...")
        B0map = (Fdata).astype(np.float64)
    else:
        logging.info("B0MAP_ERROR: Multiplying BMask with Fdata...")
        B0map = ((BMask*Fdata)).astype(np.float64)
    try:
        logging.info(f"B0MAP_LOG: center element; B0map{cen_e}: {B0map[cen_e]}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: could not log center element of B0map\n{e}")
    # 6.1 Compensate for Machine rescaling.
    B0map_uncompensate = B0map
    B0map_compensate = (B0map - RescaleIntercept)/RescaleSlope
    try:
        logging.info(f"B0MAP_LOG: center element; B0map_compensate{cen_e}: {B0map_compensate[cen_e]}")
        # Optional. Predict value in MR Viewer
        expected_machine_out = B0map_compensate[cen_e]*RescaleSlope + RescaleIntercept
        logging.info(f"B0MAP_LOG: center element; expected_machine_out{cen_e}: {expected_machine_out}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: could not log center element of B0map_compensate\n{e}")
    B0map = (B0map_compensate[:,:,None,None,:]).astype(np.float32)

    # Step 7. Find STD, Mean, Also STDNoZero and MeanNoZero
    logging.info(f"B0MAP_LOG: Calculating stats of B0Map...")
    B0max = np.max(B0map_uncompensate[B0map_uncompensate != 0]) #Can fail if all zero Array, find a better way
    B0min = np.min(B0map_uncompensate[B0map_uncompensate != 0]) #Can fail if all zero Array, find a better way
    B0mean = np.mean(B0map_uncompensate)
    B0std = np.std(B0map_uncompensate)
    B0meanNoZero = np.mean(B0map_uncompensate[B0map_uncompensate != 0 ]) #Can fail if all zero Array, find a better way
    B0stdNoZero = np.std(B0map_uncompensate[B0map_uncompensate != 0 ]) #Can fail if all zero Array, find a better way
    B0meanMaskMethod = np.mean(Fdata[BMask.astype(bool)])
    B0stdMaskMethod = np.std(Fdata[BMask.astype(bool)])
    logging.info(f"B0MAP_LOG: Min(!0): {B0min}, Max: {B0max}")
    logging.info(f"B0MAP_LOG: Mean: {B0mean:.5f}, STD: {B0std:.5f}")
    logging.info(f"B0MAP_LOG:(No Zero) Mean: {B0meanNoZero:.5f}, STD: {B0stdNoZero:.5f}")
    logging.info(f"B0MAP_LOG:(Mask Method) Mean: {B0meanMaskMethod:.5f}, STD: {B0stdMaskMethod:.5f}")

    # Step 8. Prepare B0map for MRD Server
    logging.info(f"B0MAP_LOG: Preparing B0map for injector...")
    InitialItem = 0
    B0mapOut = [None] * B0map.shape[-1]
    logging.info(f"B0MAP_LOG: length B0map.shape[-1]: {B0map.shape[-1]}")
    for item in range(B0map.shape[-1]):
        # Create new MRD instance for the inverted image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        tmp = B0map[...,item].transpose((3,2,0,1)) # Transpose from [y,x,char,z,(img)] to [z,char,y,x,(img)] (img) -> ..,item indicate this the (img^{th}) item 
        logging.info("B0MAP_LOG: After transpose: %s", tmp.shape)
        tmp2 = B0map[...,item].transpose((3,2,0,1))[0,0,:,:] # Crops from [z,char,y,x,(img)] to [y,x,(img)] -> ..,item indicate this the (img^{th}) item
        B0mapOut[item] = ismrmrd.Image.from_array(tmp2,transpose = False)
        tmp = tmp[0,0,:,:]
        logging.info("B0MAP_LOG: After cut: %s", tmp.shape)

        if item == InitialItem:
            try:
                logging.info("B0MAP_LOG: Assessing ismmrd mapping to np.astype...")
                logging.info(f"B0MAP_LOG: B0mapOut[0].data_type is {B0mapOut[item].data_type}")
                logging.info(f"B0MAP_LOG: B0map[...,item].dtype is {B0map[...,item].dtype}")
                logging.info(f"B0MAP_LOG: logging tmpMeta: {PHmeta[item]}")
                logging.info(f"B0MAP_LOG: logging B0Header: {PHhead[item]}")
            except Exception as e:
                logging.info(f"B0MAP_ERROR: Failed to assess imsmrd mapping to np.astype:\n{e}")
        
        B0Header = PHhead[item]
        B0Header.data_type = B0mapOut[item].data_type
        B0Header.image_series_index = b0mapSeries
        tmpMeta = PHmeta[item]
        
        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        try:
            if mrdhelper.get_meta_value(tmpMeta, 'IceMiniHead') is not None:
                if mrdhelper.extract_minihead_bool_param(base64.b64decode(tmpMeta['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                    currentSeries = b0mapSeries
        except Exception as e:
            logging.info(f"B0MAP_ERROR: Failed to update CurrentSeries to b0mapSeries:\n{e}")
        B0mapOut[item].setHead(B0Header)
        # Create a copy of the original ISMRMRD Meta attributes from TE1 and update where possible
        tmpMeta['DataRole']                       = 'Image'
        tmpMeta['ImageProcessingHistory']         = ['PYTHON', 'B0MAP']
        tmpMeta['WindowCenter']                   = str(np.round(((B0max - B0min)/2)))
        tmpMeta['WindowWidth']                    = str((B0max)+1)
        tmpMeta['SequenceDescriptionAdditional']  = 'OPENRECON_B0MAP'
        tmpMeta['Keep_image_geometry']            = 1
        tmpMeta['ImageComments']                  = f"Mean: {B0meanMaskMethod:.3f}, STD: {B0stdMaskMethod:.3f}" # Change the image comments to mean and SD
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(B0Header.read_dir[0]), "{:.18f}".format(B0Header.read_dir[1]), "{:.18f}".format(B0Header.read_dir[2])]
                 
        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(B0Header.phase_dir[0]), "{:.18f}".format(B0Header.phase_dir[1]), "{:.18f}".format(B0Header.phase_dir[2])]
             
        metaXml = tmpMeta.serialize()
                 
        B0mapOut[item].attribute_string = metaXml
    logging.info(f"B0MAP_LOG: Returning B0Map...")
    return B0mapOut, currentSeries

def process(connection, config, metadata):
    logging.info("B0MAP_LOG: Config: \n%s", config)

    # Metadata should be MRD formatted header, but may be a string
    # if it failed conversion earlier
    try:
        logging.info("B0MAP_LOG: Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info("B0MAP_LOG: First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3", 
            metadata.encoding[0].trajectory, 
            metadata.encoding[0].encodedSpace.matrixSize.x, 
            metadata.encoding[0].encodedSpace.matrixSize.y, 
            metadata.encoding[0].encodedSpace.matrixSize.z, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z)

    except Exception as e:
        logging.info("B0MAP_LOG: Improperly formatted metadata: \n%s", metadata, e)

    # Continuously parse incoming data parsed from MRD messages
    currentSeries = 0
    imgGroup = []
    Te1, Te2 = 3.06,4.08
    SendOriginals = True
    Verbose = False
    try:
        SendOriginals = bool(mrdhelper.get_json_config_param(config, 'SendOriginals', default=True, type='bool'))
        if SendOriginals:
            logging.info("B0MAP_LOG: READING JSON CONFIGS... Sending Original Images - Enabled")
        else:
            logging.info("B0MAP_LOG: READING JSON CONFIGS... Sending Original Images - Disabled")
    except Exception as e:
        logging.error(f"B0MAP_ERROR: READING JSON CONFIGS... Failed to config: Send Original Images:\n{e}")
        logging.info("B0MAP_LOG: READING JSON CONFIGS... Resorting to default value: Send Original Image - Enabled")
    try:
        Verbose = bool(mrdhelper.get_json_config_param(config, 'VerboseLogging', default=True, type='bool'))
        if Verbose:
            logging.info("B0MAP_LOG: READING JSON CONFIGS... Verbose Logging - Enabled")
        else:
            logging.info("B0MAP_LOG: READING JSON CONFIGS... Verbose Logging - Disabled")
    except Exception as e:
        logging.error(f"B0MAP_ERROR: READING JSON CONFIGS... Failed to config: Verbose Logging:\n{e}")
        logging.info("B0MAP_LOG: READING JSON CONFIGS... Resorting to default value: Verbose Logging - Disabled")
    
    logging.info("B0MAP_LOG:  READING JSON CONFIGS... Echo Time values...")
    try:
        Te1, Te2 = float(mrdhelper.get_json_config_param(config, 'TE1', type='str')), float(mrdhelper.get_json_config_param(config, 'TE2', type='str'))
        logging.info(f"B0MAP_LOG: READING JSON CONFIGS... Echo Time values: TE1: {Te1}, TE2: {Te2}")
    except Exception as e:
        logging.info(f"B0MAP_ERROR: READING JSON CONFIGS... Failed to config: Echo Time values:\n{e}")
        logging.info(f"B0MAP_LOG: READING JSON CONFIGS... Resorting to default values: TE1: {Te1}, TE2: {Te2}")

    
    sliceMaximum = metadata.encoding[0].encodingLimits.slice.maximum
    logging.info("B0MAP_LOG: sliceMaximum found: %d",sliceMaximum)

    # Finding TEs 
    # ranked options:
    # 1. metadata.sequenceParameters.TE
    # 2. User defined parameters in Inline Card
    # 3. Default values Te1 = 3.06, Te2 = 4.08
    try:
        if (metadata.sequenceParameters.TE is not None):
            TEs = metadata.sequenceParameters.TE
            logging.info(f"B0MAP_LOG: TE List collected: {TEs} from metadata.sequenceParameters.TE")
        else: 
            Te1, Te2 = mrdhelper.get_json_config_param(config, 'TE1', default=10.0, type='float'), mrdhelper.get_json_config_param(config, 'TE2', default=12.5, type='float') 
            logging.info(f"B0MAP_LOG: Could not find TE List, Using User defined JSON Parameters TE1 = {Te1}, TE2 = {Te2}")
        if len(TEs)==2:
            Te1, Te2 = float(min(TEs)), float(max(TEs))
            logging.info(f"B0MAP_LOG: TE List is length 2 assigned Te1={Te1} and Te2={Te2} based on metadata.sequenceParameters.TE")
        else:
            logging.info(f"B0MAP_ERROR: TE List collect is not equal to 2; len(TEs) == {len(TEs)}")
            Te1, Te2 = mrdhelper.get_json_config_param(config, 'TE1', default=10.0, type='float'), mrdhelper.get_json_config_param(config, 'TE2', default=12.5, type='float')
            logging.info(f"B0MAP_LOG: Using User defined JSON Parameters TE1 = {Te1}, TE2 = {Te2}")
    except Exception as e:
        logging.info("B0MAP_ERROR: Failed to find TE's from metadata: %s", e)
    
    Te1Group = [] # a list to keep with example convention
    Te1Tally = set() # a set to keep track of items collected via a primary key,
                  # These are separate to stay inline with i2i.py example in case of data structure mismatch.
    
    
    PhGroup = []
    PhTally = set()

    b0mapProcessed = False
    initial = True
    try:
        for item in connection:
            # ----------------------------------------------------------
            # Raw k-space data messages
            # ----------------------------------------------------------
            if isinstance(item, ismrmrd.Acquisition):
                raise Exception("Raw k-space data is not supported by this module")

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                if Verbose:
                    if initial:
                        get_PrimaryKey_fromItem_LogOnly(item)
                        logging.info("B0MAP_LOG: Initial Search for SequenceDescription in item IceMiniHeader")
                        try:
                            seqdes = get_IceMiniHeader_String_fromItem(item, "SequenceDescription")
                            logging.info(f"B0MAP_LOG SequenceDescription Search - logging get_IceMiniHeader_String_fromItem(item,SequenceDescription): \n {seqdes}")
                        except Exception as e:
                            logging.info(f"B0MAP_LOG SequenceDescription Search - Failed to collect get_IceMiniHeader_String_fromItem(item,SequenceDescription) \n {e}")

                # When this criteria is met, run process_group() on the accumulated
                # data, which returns images that are sent back to the client.
                # e.g. when the series number changes:
                if item.image_series_index != currentSeries:
                    logging.info("B0MAP_LOG: Processing a group of images of size %d because series index changed to %d", len(imgGroup), item.image_series_index)
                    currentSeries = item.image_series_index
                    image = process_image(imgGroup, connection, config, metadata)
                    connection.send_image(image)
                    imgGroup = []




                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0) or (item.image_type is ismrmrd.IMTYPE_PHASE):
                    if SendOriginals:
                        imgGroup.append(item)
                    if (not get_IceMiniHeader_String_fromItem(item,"SequenceDescription")):
                        logging.info("B0MAP_LOG: cannot find sequencedescription to determine whether _ND or not")
                    if ((item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0)) and (get_IceMiniHeader_String_fromItem(item,"SequenceDescription") and get_IceMiniHeader_String_fromItem(item,"SequenceDescription").endswith("_ND")): #06-01 changed and not endswith _ND to and endswith _ND
                        te = get_TE_fromItem(item)
                        key = get_PrimaryKey_fromItem(item)
                        #added rounding to "encourage" process_b0map trigger
                        te = np.around(te,1)
                        if (te==np.around(Te1,1)) and not (key in Te1Tally):
                            Te1Tally.add(key)
                            Te1Group.append(item)
                            if Verbose:
                                logging.info(f"B0MAP_LOG: Collecting series index {key} for Te1Group")
                    if (item.image_type is ismrmrd.IMTYPE_PHASE) and not (get_PrimaryKey_fromItem(item) in PhTally) and (get_IceMiniHeader_String_fromItem(item,"SequenceDescription") and get_IceMiniHeader_String_fromItem(item,"SequenceDescription").endswith("_ND")): #06-01 changed and not endswith _ND to and endswith _ND
                        PhTally.add(get_PrimaryKey_fromItem(item))
                        PhGroup.append(item)
                        if Verbose:
                            logging.info(f"B0MAP_LOG: Collecting series index {key} for PhGroup")
                
                else:
                    logging.info("B0MAP_LOG: Unknown image slice, sending through unmodified...")
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta['Keep_image_geometry']    = 1
                    item.attribute_string = tmpMeta.serialize()

                    connection.send_image(item)
                    continue

                if not b0mapProcessed and (len(Te1Tally) == (sliceMaximum+1) 
                    and (len(PhTally) == (sliceMaximum+1))):
                    logging.info("B0MAP_LOG: Processing b0map because size of Te1Group and PhGroup is %d", (sliceMaximum+1))
                    logging.info(f"Te1Group Set: {Te1Tally}, len(Te1Group): {len(Te1Group)}")
                    logging.info(f"PhGroup Set: {PhTally}, len(PhGroup): {len(PhGroup)}")
                    TEs = [Te1,Te2]
                    B0map, currentSeries = process_b0map(Te1Group, PhGroup,TEs, connection, config, metadata,currentSeries)
                    connection.send_image(B0map)
                    
                    #Clean up
                    Te1Group.clear()
                    PhGroup.clear()
                    Te1Tally.clear()
                    PhTally.clear()

                    b0mapProcessed = True

            elif item is None:
                break

            else:
                raise logging.exception("B0MAP_ERROR: Unsupported data type %s", type(item).__name__)

        # Process any remaining groups of image data.  This can 
        # happen if the trigger condition for these groups are not met.
        # This is also a fallback for handling image data, as the last
        # image in a series is typically not separately flagged.
        if len(imgGroup) > 0:
            logging.info("B0MAP_LOG: Processing a group of images (untriggered)")
            image = process_image(imgGroup, connection, config, metadata)
            connection.send_image(image)
            imgGroup = []

        if not b0mapProcessed and (len(Te1Tally) > 0 
                    or (len(PhTally) > 0)):
                    logging.info("B0MAP_LOG: Processing b0map (untriggered)")
                    TEs = [Te1,Te2]
                    B0map, currentSeries = process_b0map(Te1Group, PhGroup,TEs, connection, config, metadata,currentSeries)
                    connection.send_image(B0map)

                    #Clean up
                    Te1Group.clear()
                    PhGroup.clear()
                    Te1Tally.clear()
                    PhTally.clear()

                    b0mapProcessed = True


    except Exception as e:
        logging.error(f"B0MAP_ERROR: shutting down connection... {traceback.format_exc()}")
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
        # Close connection without sending MRD_MESSAGE_CLOSE message to signal failure
        connection.shutdown_close()

    finally:
        try:
                logging.info("BOMAP_LOG: Closing Connection Without B0mapSent")
                connection.send_close()
        except Exception as e:
            logging.error(f"B0MAP_ERROR: Failed to send close message! \n {e}")

def process_image(images, connection, config, metadata):
    """
    The standard process_image funciton provided from the sdk.
    """
    if len(images) == 0:
        return []

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")

    logging.debug("Processing data with %d images of type %s", len(images), ismrmrd.get_dtype_from_data_type(images[0].data_type))

    # Note: The MRD Image class stores data as [cha z y x]

    # Extract image data into a 5D array of size [img cha z y x]
    data = np.stack([img.data                              for img in images])
    head = [img.getHead()                                  for img in images]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in images]

    # Reformat data to [y x z cha img], i.e. [row col] for the first two dimensions
    data = data.transpose((3, 4, 2, 1, 0))

    # Display MetaAttributes for first image
    logging.debug("MetaAttributes[0]: %s", ismrmrd.Meta.serialize(meta[0]))

    # Optional serialization of ICE MiniHeader
    if 'IceMiniHead' in meta[0]:
        logging.debug("IceMiniHead[0]: %s", base64.b64decode(meta[0]['IceMiniHead']).decode('utf-8'))

    logging.debug("Original image data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "imgOrig.npy", data)

    if ('parameters' in config) and ('options' in config['parameters']) and (config['parameters']['options'] == 'complex'):
        # Complex images are requested
        data = data.astype(np.complex64)
        maxVal = data.max()
    else:
        # Determine max value (12 or 16 bit)
        BitsStored = 12
        if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
            BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
        maxVal = 2**BitsStored - 1

        # Normalize and convert to int16
        data = data.astype(np.float64)
        data *= maxVal/data.max()
        data = np.around(data)
        data = data.astype(np.int16)

    currentSeries = 0

    # Re-slice back into 2D images
    imagesOut = [None] * data.shape[-1]
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

        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        if mrdhelper.get_meta_value(meta[iImg], 'IceMiniHead') is not None:
            if mrdhelper.extract_minihead_bool_param(base64.b64decode(meta[iImg]['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                currentSeries += 1

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = meta[iImg]
        tmpMeta['DataRole']                       = 'Image'
        tmpMeta['ImageProcessingHistory']         = ['PYTHON']
        tmpMeta['WindowCenter']                   = str((maxVal+1)/2)
        tmpMeta['WindowWidth']                    = str((maxVal+1))
        tmpMeta['SequenceDescriptionAdditional']  = 'OPENRECON'
        tmpMeta['Keep_image_geometry']            = 1

        # Add image orientation directions to MetaAttributes if not already present
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(oldHeader.read_dir[0]), "{:.18f}".format(oldHeader.read_dir[1]), "{:.18f}".format(oldHeader.read_dir[2])]

        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(oldHeader.phase_dir[0]), "{:.18f}".format(oldHeader.phase_dir[1]), "{:.18f}".format(oldHeader.phase_dir[2])]

        metaXml = tmpMeta.serialize()
        logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
        logging.debug("Image data has %d elements", imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml

    return imagesOut
