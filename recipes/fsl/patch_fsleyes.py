import hashlib
import importlib.metadata
import sys
from pathlib import Path


version, source_hash = sys.argv[1:]
package = importlib.metadata.distribution("fsleyes")
if package.version != version:
    raise SystemExit(f"Expected FSLeyes {version}, found {package.version}")
path = Path(package.locate_file("fsleyes/filtermain.py"))
source = path.read_bytes()
if hashlib.sha256(source).hexdigest() != source_hash:
    raise SystemExit("FSLeyes filtermain.py changed; review the shutdown patch")
original = b"""            os.close(fd)
            os.close(piper)
            os.dup2(oldfd, fd)
            os.close(oldfd)"""
replacement = b"""            os.dup2(oldfd, fd)
            fout.close()"""
if source.count(original) != 1:
    raise SystemExit("Expected exactly one FSLeyes stream cleanup block")
path.write_bytes(source.replace(original, replacement))
