#!/opt/moosez/venv/bin/python
import os
import stat
import sys
from pathlib import Path


def _ensure_private_cache_dir() -> Path:
    cache = Path(f"/tmp/moosez-{os.geteuid()}")
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
        sys.exit(
            f"Unsafe cache directory: {cache}. "
            "Set MOOSEZ_MODELS_DIR to a private writable directory."
        )
    return cache


cache_dir = None
for env_var, subdir in (
    ("MOOSEZ_MODELS_DIR", "models/nnunet_trained_models"),
    ("MPLCONFIGDIR", "mpl"),
    ("XDG_CACHE_HOME", "xdg"),
    ("TORCH_HOME", "torch"),
):
    if not os.environ.get(env_var):
        if cache_dir is None:
            cache_dir = _ensure_private_cache_dir()
        os.environ[env_var] = str(cache_dir / subdir)

executable = Path("/opt/moosez/venv/bin") / Path(sys.argv[0]).name
os.execv(executable, [str(executable), *sys.argv[1:]])
