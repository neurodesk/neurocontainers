#!/usr/bin/env bash
set -euo pipefail

source_archive=$1
rust_archive=$2
source_patch=$3
upstream_version=$4
rust_version=$5
core_commit=$6
patch_sha256=$7
script_sha256=$8
core_patch=$9
core_patch_sha256=${10}
printf '%s  %s\n' "$patch_sha256" "$source_patch" "$script_sha256" "$0" \
    "$core_patch_sha256" "$core_patch" | sha256sum -c -
build_root=$(mktemp -d /tmp/qsmxt-build.XXXXXX)
trap 'rm -rf "$build_root"' EXIT

mkdir "$build_root/source" "$build_root/toolchain"
tar -xzf "$source_archive" --strip-components=1 -C "$build_root/source"
tar -xJf "$rust_archive" --strip-components=1 -C "$build_root/toolchain"
"$build_root/toolchain/install.sh" --prefix="$build_root/rust" --disable-ldconfig \
    --components=rustc,cargo,rust-std-x86_64-unknown-linux-gnu
export PATH="$build_root/rust/bin:$PATH"
export CARGO_HOME="$build_root/cargo"
export CARGO_TARGET_DIR="$build_root/target"
export CARGO_NET_GIT_FETCH_WITH_CLI=true
test "$(rustc --version | cut -d' ' -f2)" = "$rust_version"
cd "$build_root/source"
patch --batch --fuzz=0 -p1 < "$source_patch"

# The release workflow sets these two package versions from its tag. Update only
# their lock entries so --locked retains every upstream dependency and checksum.
python3 - "$upstream_version" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]
for name in ("Cargo.toml", "crates/qsmxt-config/Cargo.toml"):
    path = Path(name)
    text, count = re.subn(r'^version = "0\.0\.0"', f'version = "{version}"', path.read_text(), count=1, flags=re.M)
    assert count == 1, name
    path.write_text(text)
path = Path("Cargo.lock")
text = path.read_text()
for name in ("qsmxt", "qsmxt-config"):
    old = f'name = "{name}"\nversion = "0.0.0"'
    assert text.count(old) == 1, name
    text = text.replace(old, f'name = "{name}"\nversion = "{version}"')
path.write_text(text)
PY

# Patch only the pinned checkout inside this build's private Cargo directory.
# Keeping its git source identity retains the complete upstream lockfile.
cargo fetch --locked
core_source=$(python3 - "$core_commit" <<'PYCORE'
import json
from pathlib import Path
import subprocess
import sys

metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--locked", "--format-version", "1"], text=True))
core = next(p for p in metadata["packages"] if p["name"] == "qsm-core")
assert core["source"].endswith("#" + sys.argv[1]), core["source"]
source = Path(core["manifest_path"]).parent
assert subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() == sys.argv[1]
subprocess.run(["git", "-C", str(source), "diff", "--exit-code"], check=True, stdout=subprocess.DEVNULL)
print(source)
PYCORE
)
patch --batch --fuzz=0 -p1 -d "$core_source" < "$core_patch"
cargo test --manifest-path "$core_source/Cargo.toml" --release --locked --jobs 4 --features parallel --lib
cargo test --release --locked --jobs 4 --bin qsmxt single_echo_handoff
cargo build --release --locked --jobs 4 --bin qsmxt
install -m 755 "$CARGO_TARGET_DIR/release/qsmxt" /opt/qsmxt/qsmxt
python3 - "$source_archive" "$source_patch" "$upstream_version" "$0" "$core_commit" "$core_patch" <<'PY'
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tomllib

def sha256(path):
    return hashlib.file_digest(Path(path).open("rb"), "sha256").hexdigest()

source, patch, version, build_script, core_commit, core_patch = sys.argv[1:]
lock = tomllib.loads(Path("Cargo.lock").read_text())
core = next(p for p in lock["package"] if p["name"] == "qsm-core")
assert core["source"].endswith("#" + core_commit), core["source"]
info = {
    "upstream_version": version,
    "source_sha256": sha256(source),
    "patch_sha256": sha256(patch),
    "build_script_sha256": sha256(build_script),
    "cargo_lock_sha256": sha256("Cargo.lock"),
    "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
    "qsm_core_version": core["version"],
    "qsm_core_source": core["source"],
    "qsm_core_patch_sha256": sha256(core_patch),
    "qsm_core_patched": True,
    "binary_sha256": sha256("/opt/qsmxt/qsmxt"),
    "handoff_tests_passed": 2,
}
Path("/opt/qsmxt/native-build.json").write_text(json.dumps(info, indent=2) + "\n")
PY
