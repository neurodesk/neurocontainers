#!/usr/bin/env bash
set -euo pipefail

version="${APPTAINER_VERSION:?Set APPTAINER_VERSION}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
  echo "Invalid Apptainer version: $version" >&2
  exit 1
fi
if [[ "$(uname -s)" != Linux ]]; then
  echo "Apptainer is only supported on Linux runners." >&2
  exit 1
fi
arch="$(dpkg --print-architecture)"
case "$arch/$(uname -m)" in
  amd64/x86_64)
    asset="apptainer_${version}_amd64.deb"
    machine='Advanced Micro Devices X86-64'
    ;;
  arm64/aarch64)
    asset="apptainer-${version}.tar.gz"
    machine='AArch64'
    ;;
  *)
    echo "Unsupported native Apptainer architecture: $arch/$(uname -m)" >&2
    exit 1
    ;;
esac

cache_dir="${RUNNER_TEMP:-/tmp}/apptainer/${version}/${arch}"
mkdir -p "$cache_dir"
work_dir="$(mktemp -d "$cache_dir/install.XXXXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
curl_options=(--fail --show-error --silent --location --proto '=https' --proto-redir '=https'
  --retry 5 --retry-delay 5 --retry-all-errors)
headers=(-H 'Accept: application/vnd.github+json')
if [[ -n "${GH_TOKEN:-}" ]]; then
  headers+=(-H "Authorization: Bearer $GH_TOKEN")
fi
curl "${curl_options[@]}" "${headers[@]}" \
  "https://api.github.com/repos/apptainer/apptainer/releases/tags/v${version}" \
  -o "$work_dir/release.json"
digest="$(python3 - "$work_dir/release.json" "$version" "$asset" <<'PY'
import json
import re
import sys

with open(sys.argv[1]) as stream:
    release = json.load(stream)
version, name = sys.argv[2:]
if release.get("tag_name") != f"v{version}" or release.get("draft"):
    sys.exit("Unexpected Apptainer release metadata")
assets = [asset for asset in release["assets"] if asset["name"] == name]
if len(assets) != 1 or not re.fullmatch(r"sha256:[0-9a-f]{64}", assets[0].get("digest") or ""):
    sys.exit(f"No SHA-256 verified official release asset: {name}")
print(assets[0]["digest"].removeprefix("sha256:"))
PY
)"
artifact="$cache_dir/$asset"
if [[ ! -f "$artifact" ]] || ! echo "$digest  $artifact" | sha256sum --check --status; then
  rm -f "$artifact"
  curl "${curl_options[@]}" \
    "https://github.com/apptainer/apptainer/releases/download/v${version}/${asset}" \
    -o "$work_dir/$asset"
  echo "$digest  $work_dir/$asset" | sha256sum --check
  mv "$work_dir/$asset" "$artifact"
fi
printf 'Verified official Apptainer asset %s sha256:%s\n' "$asset" "$digest"

if [[ "$arch" == amd64 ]]; then
  if [[ "$(dpkg-deb -f "$artifact" Package)" != apptainer ||
        "$(dpkg-deb -f "$artifact" Version)" != "$version" ||
        "$(dpkg-deb -f "$artifact" Architecture)" != "$arch" ]]; then
    echo "Apptainer package metadata does not match $version/$arch" >&2
    exit 1
  fi
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends --allow-downgrades binutils "$artifact"
else
  # The official release has no ARM64 deb. Use its source and Ubuntu's runtime helpers.
  export GOTOOLCHAIN=local GOMAXPROCS=2
  if [[ "$(go env GOHOSTOS)/$(go env GOHOSTARCH)" != linux/arm64 ]]; then
    echo "A native Linux ARM64 Go toolchain is required." >&2
    exit 1
  fi
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends \
    build-essential pkg-config libseccomp-dev uidmap fakeroot cryptsetup \
    squashfs-tools squashfuse fuse-overlayfs fuse2fs gocryptfs apparmor
  tar -xzf "$artifact" -C "$work_dir"
  (
    cd "$work_dir/apptainer-$version"
    ./mconfig --prefix=/usr/local --sysconfdir=/etc --without-suid
    make -C builddir -j2
    sudo make -C builddir install
    # Match the deb's scoped userns permission on Ubuntu 23.10+ without changing sysctls.
    if [[ -r /proc/sys/kernel/apparmor_restrict_unprivileged_userns ]] &&
       [[ "$(cat /proc/sys/kernel/apparmor_restrict_unprivileged_userns)" == 1 ]]; then
      sed 's|/usr/libexec/|/usr/local/libexec/|' dist/debian/apparmor-userns > "$work_dir/apptainer.apparmor"
      sudo install -m 644 "$work_dir/apptainer.apparmor" /etc/apparmor.d/apptainer
      sudo apparmor_parser -r /etc/apparmor.d/apptainer
    fi
  )
fi

installed="$(apptainer --version)"
if [[ "$installed" != "apptainer version $version" ]]; then
  echo "Expected Apptainer $version, got: $installed" >&2
  exit 1
fi
installed_machine="$(LC_ALL=C readelf -h "$(command -v apptainer)" | sed -n 's/^ *Machine: *//p')"
if [[ "$installed_machine" != "$machine" ]]; then
  echo "Expected $machine Apptainer executable, got: $installed_machine" >&2
  exit 1
fi
printf '%s (%s)\n' "$installed" "$installed_machine"
printf 'apptainer-version=%s\n' "${installed#apptainer version }" >> "$GITHUB_OUTPUT"
