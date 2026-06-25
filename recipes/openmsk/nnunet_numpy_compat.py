"""Compatibility aliases for model checkpoints pickled with newer NumPy.

The packaged nnU-Net checkpoint references NumPy 2's ``numpy._core`` package,
but the OpenMSK image keeps NumPy 1.24 for TensorFlow/DOSMA compatibility.
Loading this module as ``sitecustomize`` makes those pickle imports resolve to
the equivalent NumPy 1 modules without changing the global NumPy version.
"""

from __future__ import annotations

import importlib
import sys


def _alias_module(alias: str, target: str) -> None:
    if alias in sys.modules:
        return
    try:
        module = importlib.import_module(target)
    except Exception:
        return
    sys.modules.setdefault(alias, module)


_alias_module("numpy._core", "numpy.core")

try:
    import numpy as _numpy

    if "numpy._core" in sys.modules and not hasattr(_numpy, "_core"):
        _numpy._core = sys.modules["numpy._core"]
except Exception:
    pass

for _module_name in (
    "_multiarray_umath",
    "arrayprint",
    "defchararray",
    "einsumfunc",
    "fromnumeric",
    "function_base",
    "getlimits",
    "memmap",
    "multiarray",
    "numeric",
    "numerictypes",
    "overrides",
    "records",
    "shape_base",
    "umath",
):
    _alias_module(f"numpy._core.{_module_name}", f"numpy.core.{_module_name}")
