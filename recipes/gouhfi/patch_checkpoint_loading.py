from pathlib import Path
path = Path('/opt/GOUHFI/nnunetv2/inference/predict_from_raw_data.py')
source = path.read_text()
old = "map_location=torch.device('cpu'))"
new = "map_location=torch.device('cpu'), weights_only=False)"
assert source.count(old) == 1, 'Review upstream checkpoint loader before applying compatibility patch'
path.write_text(source.replace(old, new))
