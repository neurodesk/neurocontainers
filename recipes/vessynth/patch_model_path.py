from pathlib import Path

path = Path('/opt/VesSynth/vessynth_test.py')
source = path.read_text()
old = "model_path = './models/'"
new = "model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models', '')"
if source.count(old) != 1:
    raise RuntimeError('Upstream model_path changed; review the launcher patch')
path.write_text(source.replace(old, new))
