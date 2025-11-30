#!/usr/bin/env python3
"""
NIfTI to ISMRMRD Converter
Converts NIfTI files to ISMRMRD format for testing the OpenRecon pipeline
adapted from: https://github.com/jlautman1/open-recon-fetal-brain-measurements/blob/main/nifti_to_ismrmrd_converter.py
"""

import os
import sys
import argparse
import re
import numpy as np
import nibabel as nib
import json
from pathlib import Path

try:
    import ismrmrd
    print("✅ Successfully imported ismrmrd module")
except ImportError as e:
    print(f"❌ Failed to import ismrmrd: {e}")
    print("   Creating mock ISMRMRD classes for testing...")
    
    # Create mock ISMRMRD classes for testing
    class MockImage:
        def __init__(self, data):
            self.data = data.astype(np.complex64)  # ISMRMRD typically uses complex data
            self.meta = {}
            self.attribute_string = ""
            self.image_type = 1  # IMTYPE_MAGNITUDE
            self.image_index = 0
            self.image_series_index = 1
        
        @classmethod
        def from_array(cls, data):
            return cls(data)
    
    class MockMeta:
        def __init__(self):
            self._data = {}
        
        def __setitem__(self, key, value):
            self._data[key] = value
        
        def __getitem__(self, key):
            return self._data[key]
        
        def get(self, key, default=None):
            return self._data.get(key, default)
        
        def serialize(self):
            return json.dumps(self._data)
    
    # Create a mock ismrmrd module
    import types
    ismrmrd = types.ModuleType('ismrmrd')
    ismrmrd.Image = MockImage
    ismrmrd.Meta = MockMeta
    IMTYPE_MAGNITUDE = 1


def extract_orientation_from_affine(affine, shape):
    """
    Extract position and direction vectors from NIfTI affine matrix
    
    The affine matrix transforms from voxel coordinates to world coordinates:
    [x_world]   [r11 r12 r13 tx]   [i]
    [y_world] = [r21 r22 r23 ty] * [j]
    [z_world]   [r31 r32 r33 tz]   [k]
    [   1   ]   [ 0   0   0   1]   [1]
    
    Returns:
        position: [x, y, z] position of the first voxel center
        read_dir: [x, y, z] direction vector for readout (columns)
        phase_dir: [x, y, z] direction vector for phase encoding (rows)
        slice_dir: [x, y, z] direction vector for slice (slices)
    """
    print("🧭 Extracting orientation from affine matrix...")
    print(f"   Affine matrix:\n{affine}")
    
    # Extract the rotation/scaling part and translation
    # First 3 columns are the direction vectors scaled by voxel size
    rotation_scale = affine[:3, :3]
    translation = affine[:3, 3]
    
    # Extract direction vectors (columns of the rotation matrix)
    # These need to be normalized to get unit direction vectors
    col0 = rotation_scale[:, 0]  # First axis (usually X/readout)
    col1 = rotation_scale[:, 1]  # Second axis (usually Y/phase)
    col2 = rotation_scale[:, 2]  # Third axis (usually Z/slice)
    
    # Calculate voxel sizes from the direction vectors
    voxel_size_x = np.linalg.norm(col0)
    voxel_size_y = np.linalg.norm(col1)
    voxel_size_z = np.linalg.norm(col2)
    
    print(f"   Voxel sizes from affine: [{voxel_size_x:.4f}, {voxel_size_y:.4f}, {voxel_size_z:.4f}] mm")
    
    # Normalize to get unit direction vectors
    read_dir = col0 / voxel_size_x if voxel_size_x > 0 else col0
    phase_dir = col1 / voxel_size_y if voxel_size_y > 0 else col1
    slice_dir = col2 / voxel_size_z if voxel_size_z > 0 else col2
    
    # Position is the translation (position of first voxel)
    position = translation
    
    print(f"   Position: [{position[0]:.4f}, {position[1]:.4f}, {position[2]:.4f}] mm")
    print(f"   Read direction:  [{read_dir[0]:.4f}, {read_dir[1]:.4f}, {read_dir[2]:.4f}]")
    print(f"   Phase direction: [{phase_dir[0]:.4f}, {phase_dir[1]:.4f}, {phase_dir[2]:.4f}]")
    print(f"   Slice direction: [{slice_dir[0]:.4f}, {slice_dir[1]:.4f}, {slice_dir[2]:.4f}]")
    
    return {
        'position': position.tolist(),
        'read_dir': read_dir.tolist(),
        'phase_dir': phase_dir.tolist(),
        'slice_dir': slice_dir.tolist(),
        'voxel_size': [voxel_size_x, voxel_size_y, voxel_size_z]
    }


def extract_metadata_from_filename(nifti_path):
    """Extract patient and series metadata from NIfTI filename"""
    filename = os.path.basename(nifti_path)
    print(f"🏷️ Extracting metadata from filename: {filename}")
    
    # Default metadata
    metadata = {
        'config': 'openrecon',
        'enable_measurements': True,
        'enable_reporting': True,
        'confidence_threshold': 0.5,
        'PatientName': 'TEST^PATIENT',
        'StudyDescription': 'OPENRECON TEST',
        'SeriesDescription': 'TEST_SERIES',
        'PixelSpacing': [0.8, 0.8],
        'SliceThickness': 0.8,
        'PatientID': 'TESTPAT001',
        'SeriesNumber': 1
    }
    
    # Try to parse filename format: Pat[PatientID]_Se[SeriesNumber]_Res[X]_[Y]_Spac[Z].nii.gz
    if filename.startswith('Pat') and '_Se' in filename:
        try:
            # Remove either .nii.gz or .nii extension flexibly
            base_name = re.sub(r'\.nii(\.gz)?$', '', filename, flags=re.IGNORECASE)
            parts = base_name.split('_')
            for part in parts:
                if part.startswith('Pat'):
                    patient_id = part[3:]  # Remove 'Pat' prefix
                    metadata['PatientID'] = patient_id
                    metadata['PatientName'] = f'PATIENT^{patient_id}'
                elif part.startswith('Se'):
                    series_num = int(part[2:])  # Remove 'Se' prefix
                    metadata['SeriesNumber'] = series_num
                elif part.startswith('Res'):
                    # Next part should be the Y resolution
                    idx = parts.index(part)
                    if idx + 1 < len(parts):
                        x_res = float(part[3:])  # Remove 'Res' prefix
                        y_res = float(parts[idx + 1])
                        metadata['PixelSpacing'] = [x_res, y_res]
                elif part.startswith('Spac'):
                    slice_thickness = float(part[4:])  # Remove 'Spac' prefix
                    metadata['SliceThickness'] = slice_thickness
            
            print(f"✅ Parsed metadata from filename:")
            print(f"   Patient ID: {metadata['PatientID']}")
            print(f"   Series: {metadata['SeriesNumber']}")
            print(f"   Resolution: {metadata['PixelSpacing']}")
            print(f"   Slice thickness: {metadata['SliceThickness']}")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not parse filename completely: {e}")
            print("   Using default metadata values")
    
    return metadata


def convert_nifti_to_ismrmrd(nifti_path, output_path=None):
    """Convert NIfTI file to ISMRMRD format"""
    
    if not os.path.exists(nifti_path):
        raise FileNotFoundError(f"NIfTI file not found: {nifti_path}")
    
    print(f"🔄 Converting NIfTI to ISMRMRD format")
    print(f"   Input: {nifti_path}")
    
    # Load NIfTI data
    print("📖 Loading NIfTI file...")
    nii = nib.load(nifti_path)
    data = nii.get_fdata()
    affine = nii.affine
    
    print(f"📐 Original data shape: {data.shape}")
    print(f"🔢 Value range: {data.min():.2f} - {data.max():.2f}")
    print(f"📊 Data type: {data.dtype}")
    
    # Extract orientation information from affine matrix
    orientation_info = extract_orientation_from_affine(affine, data.shape)
    
    # Normalize data to reasonable range for medical imaging
    if data.max() > 4095:  # If values are very high, normalize
        data = (data / data.max()) * 4095
        print(f"🔧 Normalized data to range: {data.min():.2f} - {data.max():.2f}")
    
    # Ensure we have 3D data
    if len(data.shape) == 2:
        data = data[:, :, np.newaxis]
        print(f"📝 Expanded 2D to 3D: {data.shape}")
    elif len(data.shape) == 4:
        data = data[:, :, :, 0]  # Take first volume
        print(f"📝 Reduced 4D to 3D: {data.shape}")
    
    # Create ISMRMRD Image object
    print("🏗️ Creating ISMRMRD Image object...")
    
    # For magnitude/T2W images, keep as real float32 data
    if np.iscomplexobj(data):
        # If complex, take magnitude
        ismrmrd_data = np.abs(data).astype(np.float32)
        print("🔧 Converted complex data to magnitude (float32)")
    else:
        # Keep as real float32
        ismrmrd_data = data.astype(np.float32)
    
    print(f"📊 ISMRMRD data type: {ismrmrd_data.dtype}")
    print(f"📐 NIfTI data shape: {ismrmrd_data.shape}")
    
    # Create ISMRMRD image - no transpose, keep data as-is
    # Let ISMRMRD handle storage format internally
    try:
        ismrmrd_image = ismrmrd.Image.from_array(ismrmrd_data, transpose=False)
    except TypeError:
        # Older versions don't support transpose parameter
        ismrmrd_image = ismrmrd.Image.from_array(ismrmrd_data)
    
    print(f"📐 ISMRMRD image data shape: {ismrmrd_image.data.shape}")
    
    # Set basic image properties
    if hasattr(ismrmrd_image, 'image_type'):
        ismrmrd_image.image_type = IMTYPE_MAGNITUDE if 'IMTYPE_MAGNITUDE' in globals() else 1
    if hasattr(ismrmrd_image, 'image_series_index'):
        ismrmrd_image.image_series_index = 1
    if hasattr(ismrmrd_image, 'image_index'):
        ismrmrd_image.image_index = 0
    
    # Extract metadata from filename
    metadata = extract_metadata_from_filename(nifti_path)
    
    # Add orientation information to metadata
    metadata['position'] = orientation_info['position']
    metadata['read_dir'] = orientation_info['read_dir']
    metadata['phase_dir'] = orientation_info['phase_dir']
    metadata['slice_dir'] = orientation_info['slice_dir']
    
    # Update pixel spacing from actual affine-derived voxel sizes
    voxel_size = orientation_info['voxel_size']
    print(f"🔧 Voxel spacing from affine matrix: {voxel_size}")
    
    print(f"📏 Original data shape: {ismrmrd_data.shape}")
    print(f"📏 ISMRMRD image.data shape: {ismrmrd_image.data.shape}")
    
    # Check if ISMRMRD image has matrix_size attribute
    if hasattr(ismrmrd_image, 'matrix_size'):
        print(f"📏 ISMRMRD matrix_size attribute: {ismrmrd_image.matrix_size}")
    
    # CRITICAL: ISMRMRD stores data in reversed/transposed order (column-major Fortran order)
    # Original data: [624, 512, 416] but ISMRMRD reads it as [416, 512, 624]
    # So we need to reverse the FOV array to match: [Z_fov, Y_fov, X_fov]
    field_of_view = [
        ismrmrd_data.shape[2] * voxel_size[2],  # Z: 416 * 0.8 = 332.8 (stored as first dimension)
        ismrmrd_data.shape[1] * voxel_size[1],  # Y: 512 * 0.8 = 409.6 (middle dimension)
        ismrmrd_data.shape[0] * voxel_size[0]   # X: 624 * 0.8 = 499.2 (stored as last dimension)
    ]
    
    metadata['PixelSpacing'] = [voxel_size[0], voxel_size[1]]
    metadata['SliceThickness'] = voxel_size[2]
    metadata['field_of_view'] = field_of_view
    
    print(f"📏 Voxel spacing: [{voxel_size[0]}, {voxel_size[1]}, {voxel_size[2]}] mm")
    print(f"📏 Field of view (reversed for ISMRMRD): {field_of_view} mm")
    print(f"📏 Target matrix: 624 x 512 x 416")
    print(f"📏 ISMRMRD stores as: 416 x 512 x 624")
    print(f"📏 Expected voxel: [{field_of_view[0]/416:.8f}, {field_of_view[1]/512:.8f}, {field_of_view[2]/624:.8f}] (should be 0.8 each)")
    
    # Set image metadata
    if hasattr(ismrmrd_image, 'meta'):
        ismrmrd_image.meta = metadata
    
    # Set field_of_view on the image header if available
    if hasattr(ismrmrd_image, 'field_of_view'):
        ismrmrd_image.field_of_view[:] = field_of_view
    
    # Set position and orientation on the image header if available
    if hasattr(ismrmrd_image, 'position'):
        ismrmrd_image.position[:] = orientation_info['position']
        print(f"✅ Set image position: {orientation_info['position']}")
    
    if hasattr(ismrmrd_image, 'read_dir'):
        ismrmrd_image.read_dir[:] = orientation_info['read_dir']
        print(f"✅ Set read direction: {orientation_info['read_dir']}")
    
    if hasattr(ismrmrd_image, 'phase_dir'):
        ismrmrd_image.phase_dir[:] = orientation_info['phase_dir']
        print(f"✅ Set phase direction: {orientation_info['phase_dir']}")
    
    if hasattr(ismrmrd_image, 'slice_dir'):
        ismrmrd_image.slice_dir[:] = orientation_info['slice_dir']
        print(f"✅ Set slice direction: {orientation_info['slice_dir']}")
    
    # Create XML metadata string for ISMRMRD
    meta_obj = ismrmrd.Meta()
    for key, value in metadata.items():
        if isinstance(value, (list, tuple)):
            meta_obj[key] = list(value)
        else:
            meta_obj[key] = str(value)
    
    meta_obj['DataRole'] = 'Image'
    meta_obj['ImageProcessingHistory'] = ['NIfTI_CONVERSION']
    meta_obj['Keep_image_geometry'] = 1
    meta_obj['orientation_extracted'] = 'true'
    
    if hasattr(ismrmrd_image, 'attribute_string'):
        ismrmrd_image.attribute_string = meta_obj.serialize()
    
    print(f"✅ Successfully created ISMRMRD Image")
    print(f"   Data shape: {ismrmrd_image.data.shape}")
    print(f"   Data type: {ismrmrd_image.data.dtype}")
    
    # Save to file if requested
    if output_path:
        print(f"💾 Saving to: {output_path}")
        
        # Remove existing file if it exists to avoid corruption
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"🗑️  Removed existing file: {output_path}")
        
        try:
            # Try to save as proper ISMRMRD HDF5 file
            # Check if we have the real ismrmrd module (not mock)
            if hasattr(ismrmrd, 'Dataset'):
                # Use ismrmrd.Dataset directly
                dset = ismrmrd.Dataset(output_path, '/dataset', create_if_needed=True)
                # Write the image using ISMRMRD Dataset
                dset.append_image("image_0", ismrmrd_image)
                dset.close()
                print(f"✅ Saved ISMRMRD HDF5 file to {output_path}")
            else:
                # Try importing from h5py directly
                import h5py
                
                # Ensure parent directory exists
                os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                
                # Create HDF5 file with ISMRMRD structure
                print(f"📝 Creating HDF5 file with h5py...")
                f = h5py.File(output_path, 'w')
                
                # Create dataset group
                grp = f.create_group('dataset')
                
                # Create image dataset
                img_data = ismrmrd_image.data
                grp.create_dataset('data', data=img_data, compression='gzip')
                print(f"   Data shape: {img_data.shape}, dtype: {img_data.dtype}")
                
                # Add metadata as attributes
                for key, value in metadata.items():
                    if isinstance(value, (list, tuple)):
                        grp.attrs[key] = json.dumps(value)
                    else:
                        grp.attrs[key] = str(value)
                
                grp.attrs['original_nifti_path'] = nifti_path
                
                # Explicitly close the file
                f.close()
                
                print(f"✅ Saved HDF5 file to {output_path}")
            
        except (ImportError, AttributeError, Exception) as e:
            print(f"❌ Could not save as HDF5: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    return ismrmrd_image, metadata


def main():
    """Main function to test the converter"""
    print("🧪 NIfTI to ISMRMRD Converter")
    print("=" * 50)
    
    # CLI arguments
    parser = argparse.ArgumentParser(description="Convert a NIfTI file to ISMRMRD format for OpenRecon testing")
    parser.add_argument(
        "-i", "--input",
        dest="nifti_file",
        help="Path to the NIfTI file to convert, e.g. Pat[PatientID]_Se[SeriesNumber]_Res[X]_[Y]_Spac[Z].nii.gz",
        default="Pat[PatientID]_Se[SeriesNumber]_Res[X]_[Y]_Spac[Z].nii.gz"
    )
    parser.add_argument(
        "-o", "--output",
        dest="output_path",
        help="Optional output path for serialized ISMRMRD data (pickle)",
        default="test_ismrmrd_output.h5"
    )
    args = parser.parse_args()

    # Resolve inputs
    nifti_file = args.nifti_file
    output_path = args.output_path
    
    if not os.path.exists(nifti_file):
        print(f"❌ Test file not found: {nifti_file}")
        print("   Please check the file path")
        return False
    
    try:
        # Convert NIfTI to ISMRMRD
        print(f"➡️  Using input: {nifti_file}")
        print(f"➡️  Output path: {output_path}")
        ismrmrd_image, metadata = convert_nifti_to_ismrmrd(nifti_file, output_path)
        
        print("\n📋 Conversion Summary:")
        print(f"   Input file: {nifti_file}")
        print(f"   Output data shape: {ismrmrd_image.data.shape}")
        print(f"   Patient ID: {metadata.get('PatientID', 'Unknown')}")
        print(f"   Series: {metadata.get('SeriesNumber', 'Unknown')}")
        print(f"   PixelSpacing: {metadata.get('PixelSpacing', 'Unknown')}")
        print(f"   SliceThickness: {metadata.get('SliceThickness', 'Unknown')}")
        print(f"\n🧭 Orientation Information:")
        print(f"   Position: {metadata.get('position', 'Unknown')}")
        print(f"   Read direction: {metadata.get('read_dir', 'Unknown')}")
        print(f"   Phase direction: {metadata.get('phase_dir', 'Unknown')}")
        print(f"   Slice direction: {metadata.get('slice_dir', 'Unknown')}")
        
        return ismrmrd_image, metadata
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    result = main()
    if result:
        print("\n🎉 Conversion completed successfully!")
    else:
        print("\n💥 Conversion failed!")
