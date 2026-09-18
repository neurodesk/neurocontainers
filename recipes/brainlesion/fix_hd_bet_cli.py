"""Make brain extraction usable: revive the `hd-bet` command, and stop it
reporting success after it has failed.

Two defects in brainles_hd_bet 0.0.11, both upstream, both landing on anyone
who uses this bundle for brain extraction -- which is the one part of the
pipeline the container is meant to provide end to end.

1. The `hd-bet` command cannot start. `hd_bet.py` opens with

       from brainles_hd_bet.utils import maybe_mkdir_p, subfiles

   and `utils.py` never defines `maybe_mkdir_p`, so every invocation --
   including `hd-bet --help` -- dies with an ImportError before argument
   parsing. The library entry point `run_hd_bet()` imports only names that do
   exist and works fine, so the extractor itself is sound and only its command
   is dead. The helper is restored here with the semantics its one call site
   needs (`maybe_mkdir_p(output_dir)` before writing into it), matching the
   nnU-Net helper of the same name it was taken from.

2. A failed extraction exits 0. `run_hd_bet` wraps its loader in

       except RuntimeError: print(...); continue
       except AssertionError as e: print(e); continue

   so an unreadable or missing input prints a message, writes nothing, and
   returns normally. A pipeline that checks the exit code -- the normal thing
   to check -- treats a total failure as success and carries on with a mask
   that does not exist. The handlers now re-raise as RuntimeError with the
   original message, so failure is visible to a caller and to a shell.

Kept as patches rather than a fork: together they are a handful of lines, and
pinning a fork would strand the recipe on this release. Each target is asserted
to exist first, so an upstream fix or rename fails the build here instead of
silently leaving the defect in place.
"""

from importlib.util import find_spec
from pathlib import Path

package_spec = find_spec("brainles_hd_bet")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_hd_bet is not installed")

package_dir = Path(next(iter(package_spec.submodule_search_locations)))

# --- 1. the helper the CLI imports and the package never defines -------------

utils_path = package_dir / "utils.py"
utils_source = utils_path.read_text()

if "def maybe_mkdir_p(" in utils_source:
    raise RuntimeError(
        "brainles_hd_bet.utils already defines maybe_mkdir_p; upstream has "
        "fixed this and the patch should be dropped"
    )
if "import os" not in utils_source:
    raise RuntimeError("brainles_hd_bet.utils no longer imports os; review this patch")

utils_source += '''

# --- added by the neurocontainers recipe -------------------------------------
# hd_bet.py (the console script) imports this name, and utils.py never defined
# it, so the `hd-bet` command failed on import before parsing any argument.
# Same behaviour as the nnU-Net helper it was taken from: create the directory
# and every parent, and do not complain if it is already there.
def maybe_mkdir_p(directory):
    os.makedirs(directory, exist_ok=True)
    return directory
'''
utils_path.write_text(utils_source)

# --- 2. failure must not look like success -----------------------------------

run_path = package_dir / "run.py"
run_source = run_path.read_text()

old_handlers = """            except RuntimeError:
                print("\\nERROR\\nCould not read file", in_fname, "\\n")
                continue
            except AssertionError as e:
                print(e)
                continue
"""
new_handlers = """            # neurocontainers recipe: upstream printed and continued, so a
            # brain extraction that produced nothing still returned normally
            # and exited 0. Any pipeline checking the exit code took a total
            # failure for a success and carried on without a mask.
            except RuntimeError as e:
                raise RuntimeError(
                    "hd-bet could not read %s: %s" % (in_fname, e)
                ) from e
            except AssertionError as e:
                raise RuntimeError(
                    "hd-bet rejected %s: %s" % (in_fname, e)
                ) from e
"""

if old_handlers not in run_source:
    raise RuntimeError(
        "brainles_hd_bet.run error handling changed; review the silent-failure fix"
    )

run_path.write_text(run_source.replace(old_handlers, new_handlers, 1))

print("hd-bet CLI import repaired; failed extractions now raise")
