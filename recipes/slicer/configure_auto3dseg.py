"""Configure the pinned extension for an immutable container and external cache."""

import sys
from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    source = path.read_text()
    if source.count(old) != 1:
        raise RuntimeError(f"{path}: upstream integration point changed")
    path.write_text(source.replace(old, new))


module_dir = Path(sys.argv[1])
replace_once(
    module_dir / "MONAIAuto3DSegLib" / "model_database.py",
    'self.fileCachePath = Path.home().joinpath(f"{self.DEFAULT_CACHE_DIR_NAME}")',
    'self.fileCachePath = Path(os.environ["MONAI_AUTO3DSEG_CACHE_DIR"]).resolve()',
)
replace_once(
    module_dir / "MONAIAuto3DSegLib" / "dependency_handler.py",
    "slicer.util.pip_install(monaiInstallString)",
    '''from importlib import import_module
        from importlib.metadata import version as package_version
        from packaging import version
        if upgrade:
            raise RuntimeError("Update the container to upgrade the preinstalled engine")
        if version.parse(package_version("monai")) < version.parse("1.3"):
            raise RuntimeError("The preinstalled MONAI engine is incompatible")
        for module in ("fire", "yaml", "nibabel", "nrrd", "psutil", "tensorboard", "skimage", "itk", "tqdm"):
            import_module(module)''',
)
