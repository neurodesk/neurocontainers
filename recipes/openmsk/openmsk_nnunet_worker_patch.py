#!/usr/bin/env python3
"""Patch nnunet_knee_inference to run with scanner-safe worker counts."""

from __future__ import annotations

from pathlib import Path
import sys


PREDICT_FLAGS = """                    '--disable_tta',
                    '--disable_progress_bar',
                    '-npp', os.environ.get('OPENMSK_NNUNET_NUM_PROCESSES_PREPROCESSING', '1'),
                    '-nps', os.environ.get('OPENMSK_NNUNET_NUM_PROCESSES_EXPORT', '1'),
                    '-device', 'cuda'
"""

UPSTREAM_FLAGS = """                    '--disable_tta',
                    '-device', 'cuda'
"""


def patch_inference_file(path: Path) -> bool:
    text = path.read_text()
    if "OPENMSK_NNUNET_NUM_PROCESSES_PREPROCESSING" in text:
        return False

    occurrences = text.count(UPSTREAM_FLAGS)
    if occurrences != 3:
        raise RuntimeError(
            f"Expected 3 nnUNetv2_predict command blocks in {path}, found {occurrences}"
        )

    path.write_text(text.replace(UPSTREAM_FLAGS, PREDICT_FLAGS))
    return True


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} /path/to/inference.py", file=sys.stderr)
        return 2

    path = Path(argv[1])
    changed = patch_inference_file(path)
    print(f"{'patched' if changed else 'already patched'} {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
