python enhanceddicom2mrd.py \
-o input_fromDCM.h5 
- dicom_data

rm recipes/musclemap/input_fromDCM.h5 \
    && source .venv/bin/activate \
    && python recipes/musclemap/enhanceddicom2mrd.py -o recipes/musclemap/input_fromDCM.h5 recipes/musclemap/dicom_data