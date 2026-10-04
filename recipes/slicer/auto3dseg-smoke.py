"""Check installed extensions; optionally run the quick CT model on a real volume.

Set SLICER_AUTO3DSEG_INPUT to a CT volume and SLICER_AUTO3DSEG_OUTPUT to the
output labelmap path for inference. The declared abdominal quick model is verified
against its SHA256 before extraction. SLICER_AUTO3DSEG_DOWNLOAD_SAMPLE=1 uses
Slicer's checksum-verified CTLiver sample, SHA256
e16eae0ae6fefa858c5c11e58f0f1bb81834d81b7102e021571056324ef6f37e.
The model downloads on first use into MONAI_AUTO3DSEG_CACHE_DIR. Reuse the input and cache without network access to
check offline inference. This checks execution and geometry, not clinical quality.
"""

import hashlib
import os
import tempfile
import traceback
import zipfile
from pathlib import Path

import slicer


MODEL_ID = "abdominal-organs-3mm-v2.0.0"
MODEL_URL = "https://github.com/lassoan/SlicerMONAIAuto3DSeg/releases/download/Models/" + MODEL_ID + ".zip"
MODEL_SHA256 = "0d0fc443decbac8e10dc0c2b1c574069f0bc620999140341210ca4dfab25d07c"


def verify_model(model_path: Path) -> None:
    expected = {
        "labels.csv": "7abf9bf8a4c7455a4ad013e91d549444e30199e855e6509dd23a2339d9bb675b",
        "model.pt": "3a5ed8cbb7bc4d54534e97c1421853fe16219664ac91c693d86cc4423537a3d5",
    }
    for name, expected_digest in expected.items():
        digest = hashlib.sha256()
        with (model_path / name).open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        assert digest.hexdigest() == expected_digest, f"Cached {name} digest changed"


def prepare_model(logic) -> None:
    try:
        model_path = logic._modelPath(MODEL_ID)
        verify_model(model_path)
        return
    except RuntimeError:
        pass
    import requests

    assert logic.model(MODEL_ID)["url"] == MODEL_URL
    with tempfile.TemporaryDirectory() as temp_dir:
        archive = Path(temp_dir) / "model.zip"
        digest = hashlib.sha256()
        with requests.get(MODEL_URL, stream=True, timeout=60) as response:
            response.raise_for_status()
            with archive.open("wb") as stream:
                for block in response.iter_content(1024 * 1024):
                    stream.write(block)
                    digest.update(block)
        assert digest.hexdigest() == MODEL_SHA256, "Quick model archive digest changed"
        with zipfile.ZipFile(archive) as stream:
            stream.extractall(logic.modelsPath / MODEL_ID)
    verify_model(logic._modelPath(MODEL_ID))


def check() -> None:
    import MONAILabel
    import MONAIAuto3DSeg
    import PyTorchUtils
    import monai
    import torch
    import vtk
    from MONAIAuto3DSegLib.dependency_handler import SlicerPythonDependencies

    assert MONAILabel and PyTorchUtils and monai and torch
    assert hasattr(slicer.modules, "monaiauto3dseg")
    assert hasattr(slicer.modules, "pytorchutils")
    SlicerPythonDependencies().setupPythonRequirements()
    logic = MONAIAuto3DSeg.MONAIAuto3DSegLogic()
    cache = Path(os.environ["MONAI_AUTO3DSEG_CACHE_DIR"]).resolve()
    assert logic.fileCachePath.resolve() == cache
    assert not str(cache).startswith("/opt/")
    cache.mkdir(parents=True, exist_ok=True)
    probe = cache / "write-probe"
    probe.write_text("writable")
    probe.unlink()
    print("SLICER_AUTO3DSEG_IMPORTS_OK", flush=True)

    input_path = os.environ.get("SLICER_AUTO3DSEG_INPUT")
    if not input_path:
        return
    if not Path(input_path).exists() and os.environ.get("SLICER_AUTO3DSEG_DOWNLOAD_SAMPLE") == "1":
        import SampleData
        sample = SampleData.downloadSample("CTLiver")
        assert slicer.util.saveNode(sample, input_path)
        slicer.mrmlScene.RemoveNode(sample)
    volume = slicer.util.loadVolume(input_path)
    assert volume is not None
    segmentation = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLSegmentationNode")
    prepare_model(logic)
    logic.process([volume], segmentation, MODEL_ID, cpu=True)
    assert segmentation.GetSegmentation().GetNumberOfSegments() > 0
    assert segmentation.GetNodeReferenceID(segmentation.GetReferenceImageGeometryReferenceRole()) == volume.GetID()
    labelmap = slicer.mrmlScene.AddNewNodeByClass("vtkMRMLLabelMapVolumeNode")
    assert slicer.modules.segmentations.logic().ExportAllSegmentsToLabelmapNode(
        segmentation, labelmap, slicer.vtkSegmentation.EXTENT_REFERENCE_GEOMETRY
    )
    data = slicer.util.arrayFromVolume(labelmap)
    assert data.shape == slicer.util.arrayFromVolume(volume).shape
    assert (data != 0).any(), "Inference produced an empty labelmap"
    input_matrix, output_matrix = vtk.vtkMatrix4x4(), vtk.vtkMatrix4x4()
    volume.GetIJKToRASMatrix(input_matrix)
    labelmap.GetIJKToRASMatrix(output_matrix)
    for row in range(4):
        for col in range(4):
            assert abs(input_matrix.GetElement(row, col) - output_matrix.GetElement(row, col)) < 1e-6
    output_path = os.environ["SLICER_AUTO3DSEG_OUTPUT"]
    assert slicer.util.saveNode(labelmap, output_path)
    print(f"SLICER_AUTO3DSEG_INFERENCE_OK nonzero_voxels={int((data != 0).sum())}", flush=True)


try:
    check()
except Exception:
    traceback.print_exc()
    slicer.app.exit(1)
else:
    slicer.app.exit(0)
