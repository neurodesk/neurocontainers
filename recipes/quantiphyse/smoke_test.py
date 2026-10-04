"""Exercise the installed ASL plugin, numerical backend and graphical controls."""
import os
from pathlib import Path

import numpy as np
import quantiphyse.qpmain
from PySide2 import QtWidgets
from quantiphyse.data import DataGrid, ImageVolumeManagement
from quantiphyse.gui.main_window import MainWindow
from quantiphyse.utils import get_plugins, set_local_file_path
from quantiphyse_basil.process import AslPreprocProcess
from quantiphyse_fabber import FabberProcess

app = QtWidgets.QApplication([])
set_local_file_path()
processes = {process.__name__ for process in get_plugins("processes")}
assert {"AslPreprocProcess", "OxaslProcess", "FabberProcess"} <= processes
ivm = ImageVolumeManagement()
grid = DataGrid((2, 2, 2), np.eye(4))
pairs = np.full((2, 2, 2, 8), 90.0)
pairs[..., 1::2] = 100.0
ivm.add(pairs, grid=grid, name="pairs")
AslPreprocProcess(ivm).run({
    "data": "pairs", "iaf": "tc", "order": "lrt", "tis": [1.5],
    "rpts": [4], "taus": [1.8], "casl": True, "diff": True,
    "output-name": "difference",
})
np.testing.assert_allclose(ivm.data["difference"].raw(), 10.0)
assert ivm.data["difference"].raw().shape == (2, 2, 2, 4)

api = FabberProcess.api()
assert "aslrest" in api.get_models(model_group="asl")
options = {
    "model": "aslrest", "ti1": 1.5, "ti2": 2.0, "ti3": 2.5,
    "tau": 1.8, "casl": True, "inctiss": True, "infertiss": True, "inferart": False,
    "method": "vb", "noise": "white", "max-iterations": 20,
    "save-mean": True,
}
signal = np.asarray(api.model_evaluate(options, {"ftiss": 50.0}, 3))
assert signal.shape == (3,) and np.all(np.isfinite(signal))
options["data"] = np.broadcast_to(signal, (2, 2, 2, 3)).copy()
options["mask"] = np.ones((2, 2, 2))
fit = api.run(options)
flow = fit.data["mean_ftiss"]
assert np.all(np.isfinite(flow)) and np.all(flow > 0)
np.testing.assert_allclose(flow, 50.0, rtol=0.001)
print("ASL tissue-flow fit:", float(np.mean(flow)))

window = MainWindow()
app.processEvents()
widgets = [widget for group in window.widget_groups.values() for widget in group]
classes = {type(widget).__name__ for widget in widgets}
assert {"AslPreprocWidget", "OxaslWidget", "FabberModellingWidget"} <= classes
assert window.isVisible()
window.ivm.add(pairs, grid=grid, name="pairs")
for action in window.findChildren(QtWidgets.QAction):
    widget = getattr(action, "widget", None)
    if type(widget).__name__ == "OxaslWidget":
        action.trigger()
        app.processEvents()
        assert window.current_widget is widget and widget.isVisible()
        break
else:
    raise AssertionError("ASL analysis menu action is missing")
if os.environ.get("QUANTIPHYSE_SCREENSHOT"):
    assert window.grab().save(str(Path(os.environ["QUANTIPHYSE_SCREENSHOT"])))
print("GUI ASL controls:", sorted(classes & {"AslPreprocWidget", "OxaslWidget", "FabberModellingWidget"}))
window.ivl.multiview = True
window.close()
app.processEvents()
print("ASL smoke test passed")
