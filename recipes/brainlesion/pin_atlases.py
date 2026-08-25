"""Stop the baked atlases being replaced from Zenodo at run time.

`bake_atlases.py` puts the registration atlases inside the installed package,
which is read-only once the image is built. That fixes the first-run download,
but not the version check that happens on every run after it.

`ZenodoRecord.fetch()` queries Zenodo before it looks at what is already on
disk, and `ATLASES_RECORD_ID = "15236131"` is a Zenodo *concept* DOI, which by
design always resolves to the newest version. When the remote version differs
from the local one, `fetch()` does:

    shutil.rmtree(self.target_dir / latest_local)
    return self._download(metadata, archive_url)

and `_download` calls `folder.mkdir(parents=True, exist_ok=True)` inside the
image. So the day upstream publishes a new atlas release, `preprocessor` fails
on its own defaults, on every node, with the error this recipe exists to
remove -- and the container does not have to change for it to happen:

    OSError: [Errno 30] Read-only file system: '.../atlases/15236131_v2.1.0'

Pointing `bake_atlases.py` at a versioned record is not enough on its own,
because `fetch_atlases()` builds its `ZenodoRecord` from the installed
package's `ATLASES_RECORD_ID` and would still query the concept record.

So the fix is here, at run time: a copy that is already present wins, and
Zenodo is not consulted at all. That is the same bargain the rest of this
bundle makes for its model weights -- what ships in the image is what runs,
and moving to a newer release is a rebuild, not something that happens to a
user mid-session. It also makes the readme's offline claim true by
construction rather than by the `RequestException` handler happening to be
reached.

Deliberately not conditioned on the target directory being read-only.
`apptainer exec --writable-tmpfs` -- which the release tests use -- overlays a
writable tmpfs, so a writability probe would report that the package directory
can be written and quietly restore the defect in exactly the environment that
is supposed to catch it.

Only affects a record whose files are already on disk. `fetch_synthstrip()`
uses the same class against a directory this recipe does not bake, so it keeps
upstream's download behaviour untouched.
"""

from importlib.util import find_spec
from pathlib import Path

package_spec = find_spec("brainles_preprocessing")
if package_spec is None or package_spec.submodule_search_locations is None:
    raise RuntimeError("brainles_preprocessing is not installed")

zenodo_path = Path(next(iter(package_spec.submodule_search_locations))) / "utils" / "zenodo.py"
source = zenodo_path.read_text()

replacements = [
    # Consult the disk before the network, and stop there when it answers.
    (
        '''    def fetch(self) -> Path:
        """Fetch the latest version of the record from Zenodo or from local storage."""
        zenodo_response = self._get_metadata_and_archive_url()
''',
        '''    def fetch(self) -> Path:
        """Fetch the latest version of the record from Zenodo or from local storage."""
        baked = self._baked_copy()
        if baked is not None:
            return baked
        zenodo_response = self._get_metadata_and_archive_url()
''',
    ),
    # The check itself, placed with the other private helpers.
    (
        """    def _glob_pattern(self) -> str:
""",
        '''    def _baked_copy(self) -> Path | None:
        """Return a copy that is already on disk, without asking Zenodo.

        Added by the neurocontainers recipe. Upstream asks Zenodo first and
        deletes the local copy when the versions differ, which cannot work
        when the local copy lives in a read-only image -- and the record id is
        a concept DOI, so the versions differ the moment upstream publishes.
        """
        name = self._get_latest_version_folder_name(
            list(self.target_dir.glob(self._glob_pattern()))
        )
        if not name:
            return None
        logger.info(
            f"Using the {self.label} baked into this container ({name}). "
            "The Zenodo lookup is disabled here, so nothing is downloaded or "
            "replaced at run time; a newer release needs a rebuild."
        )
        return self.target_dir / name

    def _glob_pattern(self) -> str:
''',
    ),
]

for old, new in replacements:
    if old not in source:
        raise RuntimeError(
            f"brainles_preprocessing zenodo layout changed; review this patch:\n{old!r}"
        )
    source = source.replace(old, new, 1)

zenodo_path.write_text(source)
print("zenodo lookup pinned off for records already present on disk")
