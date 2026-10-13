import hashlib
import sys
from importlib.resources import files
from pathlib import Path


root = Path(str(files("mede")))
profile = root / "dicom_deid/deid_options/basicProfile.dicom"
deidentifier = root / "dicom_deidentification.py"
for path, expected in zip([profile, deidentifier], sys.argv[1:], strict=True):
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise SystemExit(
            f"MEDE source changed at {path}; "
            "review the private-tag repair before updating"
        )

profile.write_text(
    profile.read_text().rstrip()
    + "\nREMOVE contains:^[0-9A-F]{3}[13579BDF][0-9A-F]{4}$\n"
)
deidentifier.write_text(
    deidentifier.read_text().replace(
        "        DeidRecipe, _ = _load_deid()\n",
        '        if "rtnSafePrivOpt" in (recipe or []):\n'
        '            raise ValueError("rtnSafePrivOpt is unsupported: '
        'no validated safe-private allowlist is available")\n'
        "        DeidRecipe, _ = _load_deid()\n",
    )
)
