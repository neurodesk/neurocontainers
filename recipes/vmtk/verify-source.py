import hashlib
import json
from pathlib import Path
import re
import sys
import tarfile


(
    archive, version, helper_commit, helper_directory, output,
    source_directory, patch_file, manifest_file, source_commit,
) = sys.argv[1:]
with tarfile.open(archive) as source:
    root = source.getnames()[0].split("/")[0]
    cmake = source.extractfile(f"{root}/CMakeLists.txt").read().decode()
    installed_version = ".".join(
        re.search(rf"set\(VMTK_VERSION_{part} (\d+)\)", cmake)[1]
        for part in ("MAJOR", "MINOR", "PATCH")
    )
    assert installed_version == version, (installed_version, version)
    commit = source.pax_headers["comment"]
    assert commit == source_commit, (commit, source_commit)
    helpers = source.extractfile(f"{root}/CMake/CMakeLists.txt").read().decode()

embedded_commit = re.search(r'set\(vtkAddon_version "([a-f0-9]{40})"\)', helpers)[1]
assert embedded_commit == helper_commit, (embedded_commit, helper_commit)
hashes = dict(re.findall(r'set\((\S+)_expected_hash "([a-f0-9]{64})"\)', helpers))
assert len(hashes) == 4, hashes
for filename, expected in hashes.items():
    actual = hashlib.sha256((Path(helper_directory) / filename).read_bytes()).hexdigest()
    assert actual == expected, (filename, actual, expected)

manifest = json.loads(Path(manifest_file).read_text())
archive_hash = hashlib.sha256(Path(archive).read_bytes()).hexdigest()
assert manifest["commit"] == commit
assert manifest["version"] == version
assert manifest["archive_sha256"] == archive_hash
patch = Path(patch_file).read_bytes()
assert hashlib.sha256(patch).hexdigest() == manifest["patch_sha256"]

# Match exact lines and hashes. No offsets, fuzz, reversed patch or partial write.
lines = patch.decode().splitlines(keepends=True)
position = 0
staged = {}
while position < len(lines):
    assert lines[position].startswith("--- a/")
    relative = lines[position][6:].rstrip("\n")
    assert relative in manifest["files"] and relative not in staged
    assert lines[position + 1] == f"+++ b/{relative}\n"
    original = (Path(source_directory) / relative).read_bytes()
    assert hashlib.sha256(original).hexdigest() == manifest["files"][relative]["before"]
    old = original.decode().splitlines(keepends=True)
    result = []
    cursor = 0
    position += 2
    while position < len(lines) and lines[position].startswith("@@ "):
        match = re.fullmatch(r"@@ -(\d+),(\d+) \+(\d+),(\d+) @@\n", lines[position])
        assert match, lines[position]
        old_start, old_count, new_start, new_count = map(int, match.groups())
        assert old_start - 1 >= cursor
        result.extend(old[cursor:old_start - 1])
        cursor = old_start - 1
        assert len(result) == new_start - 1
        consumed = produced = 0
        position += 1
        while consumed < old_count or produced < new_count:
            line = lines[position]
            assert line[0] in " +-"
            if line[0] in " -":
                assert old[cursor] == line[1:], (relative, cursor + 1)
                cursor += 1
                consumed += 1
            if line[0] in " +":
                result.append(line[1:])
                produced += 1
            position += 1
        assert (consumed, produced) == (old_count, new_count)
    result.extend(old[cursor:])
    data = "".join(result).encode()
    assert hashlib.sha256(data).hexdigest() == manifest["files"][relative]["after"]
    staged[relative] = data
assert staged.keys() == manifest["files"].keys()
for relative, data in staged.items():
    (Path(source_directory) / relative).write_bytes(data)

Path(output).write_text(json.dumps({
    "version": installed_version,
    "commit": commit,
    "archive_sha256": archive_hash,
    "vtkaddon_commit": helper_commit,
    "vtkaddon_sha256": hashes,
    "sizing_patch": manifest,
}, indent=2) + "\n")
