#!/opt/miniconda-latest/envs/totalsegmentator/bin/python
import os
import stat
import sys
from pathlib import Path

if not os.environ.get("TOTALSEG_HOME_DIR"):
    cache = Path(f"/tmp/totalsegmentator-{os.geteuid()}")
    try:
        cache.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = cache.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        sys.exit(f"Unsafe cache directory: {cache}. Set TOTALSEG_HOME_DIR to a private writable directory.")
    os.environ["TOTALSEG_HOME_DIR"] = str(cache)

executable = Path("/opt/miniconda-latest/envs/totalsegmentator/bin") / Path(sys.argv[0]).name
os.execv(executable, [str(executable), *sys.argv[1:]])
