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
#   buffered, and the writer stops before the reader reaches EOF, which only
#   follows the writer restoring the stream. An unterminated final line is
#   returned at that EOF, so the writer restores the stream first, then
#   drains until the reader queues None at EOF.
# - a process started by FSLeyes may inherit the pipe and delay EOF
#   indefinitely, so the writer stops after a second without input and
#   the caller no longer joins the reader.
# - the writer prints into a block-buffered file object that is never flushed
#   before its descriptor is closed; write each line unchanged and flush it.
patches = {
    b"""                if line != '':
                    q.put(line)
                if fin.closed or die.is_set():
                    break
        finally:
            os.close(piper)""": b"""                if line == '':
                    break
                q.put(line)
        finally:
            q.put(None)
            os.close(piper)""",
    b"""        skip = 0
        alive.wait()
        while True:
            try:
                line = q.get(timeout=0.25)
            except queue.Empty:
                if die.is_set(): break
                else:            continue
""": b"""        skip     = 0
        restored = False
        alive.wait()
        while True:
            if die.is_set() and not restored:
                try:
                    os.dup2(origfd, fd)
                except Exception:
                    pass
                restored = True
            try:
                line = q.get(timeout=1 if restored else 0.25)
            except queue.Empty:
                if restored: break
                else:        continue
            if line is None:
                break
""",
    b"""            if skip < 0:
                print(line, file=fout)

        # Restore the original stream
        try:
            os.dup2(origfd, fd)
            os.close(origfd)
        except Exception:
            pass""": b"""            if skip < 0:
                fout.write(line)
                fout.flush()

        try:
            os.close(origfd)
        except Exception:
            pass""",
    b"""        wtstdout.join()
        rtstdout.join()
        wtstderr.join()
        rtstderr.join()""": b"""        wtstdout.join()
        wtstderr.join()""",
}
for original, replacement in patches.items():
    if source.count(original) != 1:
        raise SystemExit("Expected exactly one FSLeyes stream filter block")
    source = source.replace(original, replacement)
path.write_bytes(source)
