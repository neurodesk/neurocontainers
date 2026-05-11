"""Checkout-friendly launcher for the src-layout build3 package."""

from __future__ import annotations

import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
sys.modules["build3"].__path__.append(str(SRC / "build3"))

from build3.cli import main


raise SystemExit(main())
