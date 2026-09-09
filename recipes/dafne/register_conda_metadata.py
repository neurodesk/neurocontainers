"""Expose installed Conda PyQt/VTK binaries to Python dependency resolution."""

import json
import sys
import sysconfig
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from PyQt5 import QtCore
from vtkmodules import vtkCommonCore


prefix = Path(sys.prefix)
for name, conda_name, module, installed_version in (
    ("PyQt5", "pyqt", QtCore, QtCore.PYQT_VERSION_STR),
    ("vtk", "vtk-base", vtkCommonCore, vtkCommonCore.vtkVersion.GetVTKVersion()),
):
    try:
        recorded_version = version(name)
    except PackageNotFoundError:
        records = [
            json.loads(path.read_text())
            for path in (prefix / "conda-meta").glob(f"{conda_name}-*.json")
        ]
        module_path = str(Path(module.__file__).relative_to(prefix))
        if not any(
            record["name"] == conda_name
            and record["version"] == installed_version
            and module_path in record["files"]
            for record in records
        ):
            raise RuntimeError(f"No matching Conda ownership record for {name}")
        metadata = Path(sysconfig.get_path("purelib")) / f"{name}-{installed_version}.dist-info"
        metadata.mkdir()
        (metadata / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {installed_version}\n"
        )
        (metadata / "INSTALLER").write_text("conda\n")
        recorded_version = version(name)
    if recorded_version != installed_version:
        raise RuntimeError(f"{name} metadata disagrees with its installed binary")
    print(f"{name} {recorded_version}: native package metadata verified")
