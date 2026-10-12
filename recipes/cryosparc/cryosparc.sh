#!/bin/bash
# Neurodesk launcher for CryoSPARC (Structura Biotechnology).
#
# CryoSPARC is licence-gated and must not be redistributed, so this container
# ships no CryoSPARC code. `cryosparc install` downloads the official packages
# with the user's own licence ID into a writable directory the user chooses;
# the other subcommands run that installation inside this container.
set -euo pipefail

LAUNCHER_VERSION=1.0.0
DOWNLOAD_BASE=${CRYOSPARC_DOWNLOAD_BASE:-https://get.cryosparc.com}
DEFAULT_PORT=61000
MARKER=.neurodesk-cryosparc

die() {
  printf 'cryosparc: error: %s\n' "$*" >&2
  exit 1
}

note() {
  printf 'cryosparc: %s\n' "$*" >&2
}

usage() {
  cat <<EOF
Neurodesk CryoSPARC launcher ${LAUNCHER_VERSION}

This container provides the operating system and tools CryoSPARC needs. It
does not contain CryoSPARC: you install it with your own licence ID into a
writable directory, then start and stop it from there.

Usage:
  cryosparc install --license ID --dir PATH --email EMAIL [options]
  cryosparc start|stop|restart|status [--dir PATH]
  cryosparc help

Install options:
  --license ID       CryoSPARC licence ID (or set CRYOSPARC_LICENSE_ID).
                     Request one at https://cryosparc.com/download
  --dir PATH         Writable directory for the installation, database and
                     downloads (or set CRYOSPARC_DIR). Keep the same absolute
                     path for later sessions; CryoSPARC cannot be moved.
  --version VER      CryoSPARC release to install, e.g. v4.7.1 (default: latest).
                     Releases before v4.4.0 need a system CUDA and are refused.
  --port N           First of ten consecutive ports (default: ${DEFAULT_PORT}).
  --hostname NAME    Master hostname (default: localhost, which survives
                     container restarts on a single workstation).
  --email EMAIL      Login email of the initial CryoSPARC user (standalone).
  --username NAME    Initial user name (default: your login name).
  --firstname NAME   Initial user's first name (default: the user name).
  --lastname NAME    Initial user's last name (default: the user name).
  --ssd PATH         Particle cache directory on fast local disk (default: none).
  --master-only      Install only the master (no worker, no GPU required).
                     Connect workers or cluster lanes yourself afterwards.

The initial user's password is read from CRYOSPARC_INITIAL_PASSWORD, or the
installer prompts for it when run in a terminal.

After installation:
  export CRYOSPARC_DIR=PATH
  cryosparc start          # then open http://localhost:PORT in a browser
  cryosparcm ...           # upstream master CLI from the installation
  cryosparcw ...           # upstream worker CLI from the installation
  cryosparc stop

Run with GPU access (e.g. 'apptainer exec --nv') for the worker.
CryoSPARC is free for non-profit academic use and requires a commercial
licence otherwise; see https://guide.cryosparc.com/licensing
EOF
}

resolve_dir() {
  local dir=$1
  [[ -n "$dir" ]] || die "no installation directory given; use --dir PATH or set CRYOSPARC_DIR"
  realpath -m -- "$dir"
}

installed_dir() {
  local dir
  dir=$(resolve_dir "$1")
  [[ -f "$dir/$MARKER" && -x "$dir/cryosparc_master/bin/cryosparcm" ]] ||
    die "no CryoSPARC installation found in $dir; run 'cryosparc install' first"
  printf '%s\n' "$dir"
}

marker_value() {
  sed -n "s/^$2=//p" "$1/$MARKER" | head -n 1
}

normalise_version() {
  local version=$1
  if [[ "$version" == latest ]]; then
    printf 'latest\n'
    return
  fi
  [[ "$version" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] ||
    die "invalid --version '$version'; use 'latest' or a release such as v4.7.1"
  local major=${BASH_REMATCH[1]} minor=${BASH_REMATCH[2]}
  if (( major < 4 || (major == 4 && minor < 4) )); then
    die "CryoSPARC $version needs a system CUDA toolkit, which this container does not provide; choose v4.4.0 or newer"
  fi
  printf 'v%s.%s.%s\n' "${BASH_REMATCH[1]}" "$minor" "${BASH_REMATCH[3]}"
}

check_licence() {
  local licence=$1 response
  if ! response=$(curl -fsS --retry 2 --max-time 30 "$DOWNLOAD_BASE/checklicenseexists/$licence" 2>&1); then
    die "could not reach the CryoSPARC licence server at $DOWNLOAD_BASE ($response); CryoSPARC needs internet access to install and verify its licence"
  fi
  [[ "$response" =~ \"success\"[[:space:]]*:[[:space:]]*true ]] ||
    die "licence ID $licence was not accepted by the CryoSPARC licence server; check the ID in your licence email"
}

download_package() {
  local kind=$1 version=$2 licence=$3 dir=$4 archive
  archive="$dir/downloads/cryosparc_${kind}.tar.gz"
  note "downloading cryosparc_${kind} ${version}"
  curl -fL --retry 3 --progress-bar -o "$archive" "$DOWNLOAD_BASE/download/${kind}-${version}/$licence" ||
    die "download of cryosparc_${kind} ${version} failed; check that the release exists and that your licence is valid"
  tar -xzf "$archive" -C "$dir" && [[ -f "$dir/cryosparc_${kind}/install.sh" ]] ||
    die "the downloaded cryosparc_${kind} package is not a CryoSPARC archive"
  rm -f "$archive"
}

cmd_install() {
  local licence=${CRYOSPARC_LICENSE_ID:-} dir=${CRYOSPARC_DIR:-} version=latest
  local port=$DEFAULT_PORT hostname=localhost email='' username firstname='' lastname=''
  local ssd='' master_only=false
  username=$(id -un 2>/dev/null || printf 'cryosparc')

  while (( $# )); do
    case $1 in
      --license|--licence) licence=${2-}; shift 2 || die "$1 needs a value" ;;
      --dir) dir=${2-}; shift 2 || die "$1 needs a value" ;;
      --version) version=${2-}; shift 2 || die "$1 needs a value" ;;
      --port) port=${2-}; shift 2 || die "$1 needs a value" ;;
      --hostname) hostname=${2-}; shift 2 || die "$1 needs a value" ;;
      --email) email=${2-}; shift 2 || die "$1 needs a value" ;;
      --username) username=${2-}; shift 2 || die "$1 needs a value" ;;
      --firstname) firstname=${2-}; shift 2 || die "$1 needs a value" ;;
      --lastname) lastname=${2-}; shift 2 || die "$1 needs a value" ;;
      --ssd) ssd=${2-}; shift 2 || die "$1 needs a value" ;;
      --master-only) master_only=true; shift ;;
      -h|--help) usage; return 0 ;;
      *) die "unknown install option '$1'; see 'cryosparc help'" ;;
    esac
  done
  firstname=${firstname:-$username}
  lastname=${lastname:-$username}

  [[ -n "$licence" ]] || die "no licence ID given; use --license ID or set CRYOSPARC_LICENSE_ID (request one at https://cryosparc.com/download)"
  [[ "$licence" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]] ||
    die "'$licence' is not a CryoSPARC licence ID; expected the form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  version=$(normalise_version "$version")
  [[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1024 && port <= 65525 )) ||
    die "invalid --port '$port'; choose 1024-65525 so that ten consecutive ports are available"
  # CryoSPARC refuses base ports whose range overlaps the ephemeral ports.
  local ephemeral_low ephemeral_high
  if read -r ephemeral_low ephemeral_high 2>/dev/null </proc/sys/net/ipv4/ip_local_port_range &&
      (( port <= ephemeral_high && port + 9 >= ephemeral_low )); then
    die "ports $port-$((port + 9)) overlap this host's ephemeral port range $ephemeral_low-$ephemeral_high; choose another --port, e.g. $DEFAULT_PORT"
  fi
  getent hosts "$hostname" >/dev/null || die "hostname '$hostname' does not resolve inside the container"
  # CryoSPARC v4 installers check the hostname with ping, which needs the
  # host to allow unprivileged ICMP (sysctl net.ipv4.ping_group_range).
  ping -c 1 -W 2 "$hostname" >/dev/null 2>&1 ||
    note "warning: 'ping $hostname' fails here; CryoSPARC v4 installers may stop at their hostname check (ask your administrator about net.ipv4.ping_group_range)"
  if ! $master_only; then
    [[ -n "$email" ]] || die "--email is required to create the initial CryoSPARC user"
    command -v nvidia-smi >/dev/null && nvidia-smi -L >/dev/null 2>&1 ||
      die "no NVIDIA GPU is visible; run the container with GPU access (e.g. 'apptainer exec --nv') or use --master-only"
    if [[ -z "${CRYOSPARC_INITIAL_PASSWORD:-}" && ! -t 0 ]]; then
      die "set CRYOSPARC_INITIAL_PASSWORD or run in a terminal to enter the initial user's password"
    fi
  fi
  if [[ $(id -u) -eq 0 ]]; then
    die "CryoSPARC must not be installed or run as root; use your own user account"
  fi
  [[ -z "$ssd" ]] || ssd=$(realpath -m -- "$ssd")

  dir=$(resolve_dir "$dir")
  if [[ -f "$dir/$MARKER" ]]; then
    die "CryoSPARC is already installed in $dir; use 'cryosparc start --dir $dir'"
  fi
  if [[ -e "$dir/cryosparc_master" || -e "$dir/cryosparc_worker" ]]; then
    die "$dir contains files from an incomplete installation; remove $dir/cryosparc_master, $dir/cryosparc_worker and $dir/cryosparc_database, then retry"
  fi
  mkdir -p "$dir" 2>/dev/null && [[ -w "$dir" ]] ||
    die "$dir is not writable; choose a directory you can write to (the container image itself is read-only)"

  check_licence "$licence"
  mkdir -p "$dir/downloads"
  download_package master "$version" "$licence" "$dir"
  $master_only || download_package worker "$version" "$licence" "$dir"
  rmdir "$dir/downloads" 2>/dev/null || true

  # The upstream installer appends its bin directory to ~/.bashrc. Give it a
  # private HOME so the user's own shell startup files are left untouched.
  local installer_home="$dir/.installer-home"
  mkdir -p "$installer_home"
  local args=(--license "$licence" --hostname "$hostname" --dbpath "$dir/cryosparc_database" --port "$port" --yes)
  if ! $master_only; then
    args+=(--standalone --worker_path "$dir/cryosparc_worker"
           --initial_email "$email" --initial_username "$username"
           --initial_firstname "$firstname" --initial_lastname "$lastname")
    [[ -n "${CRYOSPARC_INITIAL_PASSWORD:-}" ]] && args+=(--initial_password "$CRYOSPARC_INITIAL_PASSWORD")
    if [[ -n "$ssd" ]]; then
      args+=(--ssdpath "$ssd")
    else
      args+=(--nossd)
    fi
  fi
  # A container's hostname can differ from the configured master hostname
  # (localhost by default) and change between sessions. The standalone
  # installer starts the master itself, so the override must already apply.
  note "running the CryoSPARC installer in $dir/cryosparc_master"
  if ! (cd "$dir/cryosparc_master" &&
      HOME=$installer_home CRYOSPARC_FORCE_HOSTNAME=true ./install.sh "${args[@]}"); then
    # The standalone installer starts the master before installing the worker;
    # do not leave its services holding the ports after a later failure.
    if [[ -x "$dir/cryosparc_master/bin/cryosparcm" && -f "$dir/cryosparc_master/config.sh" ]]; then
      note "stopping CryoSPARC services started by the failed installer"
      CRYOSPARC_FORCE_HOSTNAME=true "$dir/cryosparc_master/bin/cryosparcm" stop >/dev/null 2>&1 || true
    fi
    die "the CryoSPARC installer failed; see its output above"
  fi

  local config="$dir/cryosparc_master/config.sh"
  [[ -f "$config" ]] || die "the CryoSPARC installer did not write $config"
  grep -q '^export CRYOSPARC_FORCE_HOSTNAME=' "$config" ||
    printf 'export CRYOSPARC_FORCE_HOSTNAME=true\n' >>"$config"

  printf 'launcher=%s\nversion=%s\nhostname=%s\nport=%s\nmode=%s\n' \
    "$LAUNCHER_VERSION" "$version" "$hostname" "$port" "$($master_only && echo master || echo standalone)" >"$dir/$MARKER"

  note "CryoSPARC is installed in $dir"
  note "next: export CRYOSPARC_DIR=$dir; cryosparc start; open http://$hostname:$port"
}

cmd_service() {
  local action=$1 dir=${CRYOSPARC_DIR:-}
  shift
  while (( $# )); do
    case $1 in
      --dir) dir=${2-}; shift 2 || die "$1 needs a value" ;;
      -h|--help) usage; return 0 ;;
      *) die "unknown $action option '$1'; see 'cryosparc help'" ;;
    esac
  done
  dir=$(installed_dir "$dir")
  [[ $(id -u) -ne 0 ]] || die "CryoSPARC must not be run as root; use your own user account"
  "$dir/cryosparc_master/bin/cryosparcm" "$action"
  if [[ "$action" == start || "$action" == restart ]]; then
    note "CryoSPARC is running at http://$(marker_value "$dir" hostname):$(marker_value "$dir" port)"
    note "stop it with 'cryosparc stop' before leaving this session"
  fi
}

# Shims named cryosparcm/cryosparcw forward to the upstream CLIs.
case $(basename "$0") in
  cryosparcm|cryosparcw)
    tool=$(basename "$0")
    package=master
    [[ "$tool" == cryosparcw ]] && package=worker
    dir=$(installed_dir "${CRYOSPARC_DIR:-}")
    [[ -x "$dir/cryosparc_$package/bin/$tool" ]] || die "$dir has no cryosparc_$package installation"
    exec "$dir/cryosparc_$package/bin/$tool" "$@"
    ;;
esac

command=${1:-help}
(( $# )) && shift
case $command in
  install) cmd_install "$@" ;;
  start|stop|restart|status) cmd_service "$command" "$@" ;;
  help|-h|--help) usage ;;
  --version|version) printf 'Neurodesk CryoSPARC launcher %s\n' "$LAUNCHER_VERSION" ;;
  *) usage >&2; die "unknown command '$command'" ;;
esac
