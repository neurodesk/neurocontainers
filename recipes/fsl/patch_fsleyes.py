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
    raise SystemExit("FSLeyes filtermain.py changed; review the stream filter patch")
# FSLeyes 1.20.2 loses filtered output written to a pipe or file:
# - the reader stops once shutdown begins, discarding lines it already
#   buffered; read until EOF, which follows the writer restoring the stream.
# - the writer prints into a block-buffered file object that is never flushed
#   before its descriptor is closed; write each line unchanged and flush it.
patches = {
    b"""                if line != '':
                    q.put(line)
                if fin.closed or die.is_set():
                    break""": b"""                if line == '':
                    break
                q.put(line)""",
    b"""            if skip < 0:
                print(line, file=fout)""": b"""            if skip < 0:
                fout.write(line)
                fout.flush()""",
}
for original, replacement in patches.items():
    if source.count(original) != 1:
        raise SystemExit("Expected exactly one FSLeyes stream filter block")
    source = source.replace(original, replacement)
path.write_bytes(source)
