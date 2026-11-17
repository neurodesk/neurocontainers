#!/bin/bash
# Neurodesk launcher for nextPYP (Bartesaghi lab, Duke University).
#
# nextPYP runs as two Apptainer images. This container ships the web server
# image (nextPYP.sif), upstream's start/stop script and host processor. The
# ~43 GB processing image (pyp.sif) is too large to bake in, so `nextpyp
# install` fetches it once into a directory the user chooses and lays that
# directory out like upstream's single-user installation. `nextpyp start` and
# `nextpyp stop` then run upstream's own script from that directory.
set -euo pipefail

NEXTPYP_VERSION=@NEXTPYP_VERSION@
DIST=/opt/nextpyp-dist
DOWNLOAD_BASE=${NEXTPYP_DOWNLOAD_BASE:-https://nextpyp.app/files/pyp/$NEXTPYP_VERSION}
DEFAULT_PORT=8080
MARKER=.neurodesk-nextpyp

die() {
  printf 'nextpyp: error: %s\n' "$*" >&2
  exit 1
}

note() {
  printf 'nextpyp: %s\n' "$*" >&2
}

usage() {
  cat <<EOF
Neurodesk nextPYP launcher for nextPYP ${NEXTPYP_VERSION}

nextPYP is a web application for cryo-EM and cryo-ET processing. Install it
once into a writable directory, then start and stop it from there.

Usage:
  nextpyp install --dir PATH [--pyp-sif FILE] [--port N] [--scratch PATH] [--bind PATH]...
  nextpyp start|stop|status [--dir PATH]
  nextpyp version
  nextpyp help

Install options:
  --dir PATH       Writable directory for the installation, database, projects
                   and pyp.sif (or set NEXTPYP_DIR). Keep the same path for
                   later sessions. Every parent directory must be group- and
                   world-executable (chmod go+x), which nextPYP enforces.
  --pyp-sif FILE   Use an existing pyp.sif for nextPYP ${NEXTPYP_VERSION} instead of
                   downloading it (about 43 GB). The file is linked, not copied,
                   so it must stay in place.
  --port N         Web server port (default: ${DEFAULT_PORT}).
  --scratch PATH   Fast scratch space for processing jobs (default: PATH/scratch).
  --bind PATH      Folder with your data to make visible to nextPYP. Repeatable.

Running install again resumes an interrupted pyp.sif download and upgrades an
installation made with an older container. It keeps an existing config.toml;
edit PATH/config.toml to change the port, binds or scratch space later.

After installation:
  export NEXTPYP_DIR=PATH
  nextpyp start            # keeps running; open http://localhost:PORT in a browser
  nextpyp status           # from another terminal
  nextpyp stop             # or press Ctrl-C where 'nextpyp start' runs

Run the container with GPU access (e.g. 'apptainer exec --nv') for GPU jobs.
EOF
}

resolve_dir() {
  [[ -n "$1" ]] || die "no installation directory given; use --dir PATH or set NEXTPYP_DIR"
  realpath -m -- "$1"
}

installed_dir() {
  local dir
  dir=$(resolve_dir "$1")
  [[ -f "$dir/$MARKER" && -f "$dir/config.toml" ]] ||
    die "no nextPYP installation found in $dir; run 'nextpyp install --dir $dir' first"
  printf '%s\n' "$dir"
}

installed_version() {
  [[ -f "$1/$MARKER" ]] || return 0
  sed -n 's/^version=//p' "$1/$MARKER" | head -n 1
}

# nextPYP quotes paths in single-quoted TOML strings and passes them to
# Apptainer bind lists, which split on ':' and ','.
check_path() {
  [[ "$1" != *[\'\":,]* ]] || die "path '$1' contains a quote, colon or comma, which nextPYP cannot use"
}

# Upstream's start script refuses to run unless the shared folder and all of
# its parents are group- and world-executable. Check before a long download.
check_traversable() {
  local path=$1 perms
  while [[ "$path" != / ]]; do
    if [[ -e "$path" ]]; then
      perms=$(stat -L -c %A "$path")
      [[ "${perms:6:1}" != - && "${perms:9:1}" != - ]] ||
        die "$path has mode $perms; nextPYP needs every parent of its folders to be group- and world-executable. Run 'chmod go+x $path' or choose another directory"
    fi
    path=$(dirname "$path")
  done
}

web_setting() { #(dir, key)
  awk -v key="$2" '
    /^[[:space:]]*\[/ { in_web = ($0 ~ /^[[:space:]]*\[web\][[:space:]]*$/) }
    in_web && $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      sub(/^[^=]*=[[:space:]]*/, ""); sub(/[[:space:]]*#.*$/, ""); gsub(/["'\''[:space:]]/, "")
      print; exit
    }' "$1/config.toml"
}

web_url() {
  local host port
  host=$(web_setting "$1" host)
  port=$(web_setting "$1" port)
  [[ -z "$host" || "$host" == 0.0.0.0 ]] && host=127.0.0.1
  printf 'http://%s:%s\n' "$host" "${port:-$DEFAULT_PORT}"
}

write_config() { #(dir, port, scratch, binds...)
  local dir=$1 port=$2 scratch=$3
  shift 3
  local bind_list='' bind
  for bind in "$@"; do
    bind_list+="${bind_list:+, }'$bind'"
  done
  cat >"$dir/config.toml" <<EOF
# nextPYP configuration, written by the Neurodesk nextpyp launcher.
# Reference: https://nextpyp.app/files/pyp/latest/docs/reference/config.html
# Restart nextPYP after editing: nextpyp stop; nextpyp start

[pyp]

# fast storage for temporary files, ideally local to the compute node
scratch = '$scratch'

# folders with your data, made visible to nextPYP
binds = [$bind_list]


[web]

# web server port; the server listens on localhost only
port = $port

localDir = '$dir/local'
sharedDir = '$dir/shared'
sharedExecDir = '$dir/sharedExec'
workflowDirs = ['$dir/workflows']

# single-user mode: no login
auth = "none"
EOF
}

ensure_pyp_sif() { #(dir, existing file or empty)
  local dir=$1 source=$2
  local target="$dir/sharedExec/containers/pyp.sif"

  if [[ -n "$source" ]]; then
    source=$(realpath -e -- "$source" 2>/dev/null) || die "pyp.sif not found: $2"
    [[ -f "$source" && -r "$source" ]] || die "$source is not a readable file"
    apptainer sif list "$source" >/dev/null 2>&1 || die "$source is not an Apptainer SIF image"
    [[ ! -e "$target" || -L "$target" ]] ||
      die "$target already holds a downloaded pyp.sif; delete it first to use $source instead"
    ln -sfn "$source" "$target"
    note "using pyp.sif at $source"
    return
  fi

  if [[ -e "$target" && "$(installed_version "$dir")" == "$NEXTPYP_VERSION" ]]; then
    note "pyp.sif for nextPYP $NEXTPYP_VERSION is already installed"
    return
  fi
  [[ ! -L "$target" ]] ||
    die "$target links to $(readlink "$target"), which is not for nextPYP $NEXTPYP_VERSION; pass --pyp-sif with a $NEXTPYP_VERSION image or delete the link to download one"

  local url="$DOWNLOAD_BASE/pyp.sif" part="$target.part" size have free
  size=$(curl -fsSIL --max-time 60 "$url" 2>/dev/null | tr -d '\r' |
    sed -n 's/^[Cc]ontent-[Ll]ength:[[:space:]]*//p' | tail -n 1) || true
  if [[ "$size" =~ ^[0-9]+$ ]]; then
    have=$(stat -c %s "$part" 2>/dev/null || echo 0)
    free=$(( $(df -Pk "$(dirname "$target")" | awk 'NR == 2 {print $4}') * 1024 ))
    (( size - have <= free )) ||
      die "pyp.sif needs $(( (size - have) / 1000**3 + 1 )) GB but $(dirname "$target") has only $(( free / 1000**3 )) GB free"
  fi
  note "downloading pyp.sif for nextPYP $NEXTPYP_VERSION (about 43 GB) from $url"
  note "if the download stops, run the same install command again to resume it"
  curl -fL --retry 5 --retry-delay 10 -C - --progress-bar -o "$part" "$url" ||
    die "download of $url failed; run the install command again to resume"
  apptainer sif list "$part" >/dev/null 2>&1 || {
    rm -f "$part"
    die "the downloaded pyp.sif is not an Apptainer SIF image; run the install command again"
  }
  mv -f "$part" "$target"
}

cmd_install() {
  local dir=${NEXTPYP_DIR:-} pyp_sif='' port=$DEFAULT_PORT scratch=''
  local binds=()
  while (( $# )); do
    case $1 in
      --dir) dir=${2-}; shift 2 || die "$1 needs a value" ;;
      --pyp-sif) pyp_sif=${2-}; shift 2 || die "$1 needs a value" ;;
      --port) port=${2-}; shift 2 || die "$1 needs a value" ;;
      --scratch) scratch=${2-}; shift 2 || die "$1 needs a value" ;;
      --bind) binds+=("$(realpath -m -- "${2-}")"); shift 2 || die "$1 needs a value" ;;
      -h|--help) usage; return 0 ;;
      *) die "unknown install option '$1'; see 'nextpyp help'" ;;
    esac
  done
  [[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1024 && port <= 65535 )) ||
    die "invalid --port '$port'; choose 1024-65535"

  dir=$(resolve_dir "$dir")
  scratch=$(realpath -m -- "${scratch:-$dir/scratch}")
  local path
  for path in "$dir" "$scratch" "${binds[@]}"; do
    check_path "$path"
  done
  for path in "${binds[@]}"; do
    [[ -d "$path" ]] || die "--bind folder $path does not exist"
  done
  mkdir -p -m 755 "$dir" 2>/dev/null && [[ -w "$dir" ]] ||
    die "$dir is not writable; choose a directory you can write to (the container image itself is read-only)"
  check_traversable "$dir"

  local previous
  previous=$(installed_version "$dir")
  if [[ -n "$previous" && "$previous" != "$NEXTPYP_VERSION" ]]; then
    note "upgrading the installation in $dir from nextPYP $previous to $NEXTPYP_VERSION"
  fi

  # Upstream's single-user folder layout.
  local sub
  for sub in local/containers local/logs local/db local/sock \
      shared/batch shared/log shared/users shared/os-users shared/sessions \
      shared/groups shared/user-processors sharedExec/containers workflows; do
    install -d -m 755 "$dir/$sub"
  done
  chmod 755 "$dir/local" "$dir/shared" "$dir/sharedExec"
  install -m 644 "$DIST"/workflows/*.toml "$dir/workflows/"

  if [[ -f "$dir/config.toml" ]]; then
    note "keeping the existing $dir/config.toml"
  else
    write_config "$dir" "$port" "$scratch" "${binds[@]}"
  fi

  ensure_pyp_sif "$dir" "$pyp_sif"
  printf 'version=%s\n' "$NEXTPYP_VERSION" >"$dir/$MARKER"

  note "nextPYP $NEXTPYP_VERSION is installed in $dir"
  note "next: export NEXTPYP_DIR=$dir; nextpyp start; open $(web_url "$dir")"
}

# Puts this container's upstream files where upstream's script expects them in
# the installation directory and confines Apptainer state to that directory.
prepare_runtime() { #(dir)
  local dir=$1
  install -m 755 "$DIST/nextpyp" "$dir/nextpyp"
  ln -sfn "$DIST/nextPYP.sif" "$dir/nextPYP.sif"
  ln -sfn "$DIST/host-processor" "$dir/nextpyp-host-processor"
  export APPTAINER_CONFIGDIR="$dir/local/apptainer"
  mkdir -p "$APPTAINER_CONFIGDIR"
  # Binds of an outer Neurodesk container need not exist for the inner one.
  unset APPTAINER_BIND APPTAINER_BINDPATH SINGULARITY_BIND SINGULARITY_BINDPATH
}

instance_running() {
  apptainer instance list nextPYP 2>/dev/null | grep -qw nextPYP
}

wait_for_web() { #(dir, url)
  local dir=$1 url=$2 deadline=$(( SECONDS + ${NEXTPYP_START_TIMEOUT:-300} ))
  note "waiting for the web server at $url"
  until curl -fs -o /dev/null --max-time 5 "$url"; do
    if ! instance_running; then
      note "error: nextPYP stopped while starting; see the logs in $dir/local/logs"
      return 1
    fi
    if (( SECONDS >= deadline )); then
      note "error: nextPYP did not answer at $url; see the logs in $dir/local/logs"
      return 1
    fi
    sleep 2
  done
}

stop_services() { #(dir)
  (cd "$1" && ./nextpyp stop) >/dev/null 2>&1 || true
}

# The services run in the background of this container. When the container is
# a SIF file mounted with squashfuse, its file system disappears for them once
# the command that started them exits, so `start` stays in the foreground and
# stops nextPYP when it is interrupted.
run_foreground() { #(dir, url)
  local dir=$1 url=$2
  trap 'trap "" INT TERM HUP; note "stopping nextPYP"; stop_services "$dir"; exit 0' INT TERM HUP
  (cd "$dir" && ./nextpyp start)
  if ! wait_for_web "$dir" "$url"; then
    stop_services "$dir"
    exit 1
  fi
  note "nextPYP is running at $url"
  note "keep this command running; press Ctrl-C or run 'nextpyp stop' elsewhere to stop nextPYP"
  while instance_running; do
    sleep 5 &
    wait $!
  done
  # Reaps a host processor left behind if the web server exited by itself.
  stop_services "$dir"
  note "nextPYP has stopped"
}

cmd_service() {
  local action=$1 dir=${NEXTPYP_DIR:-}
  shift
  while (( $# )); do
    case $1 in
      --dir) dir=${2-}; shift 2 || die "$1 needs a value" ;;
      -h|--help) usage; return 0 ;;
      *) die "unknown $action option '$1'; see 'nextpyp help'" ;;
    esac
  done
  dir=$(installed_dir "$dir")
  prepare_runtime "$dir"
  local url
  url=$(web_url "$dir")

  case $action in
    start)
      local installed
      installed=$(installed_version "$dir")
      [[ "$installed" == "$NEXTPYP_VERSION" ]] ||
        die "$dir holds nextPYP $installed but this container runs $NEXTPYP_VERSION; run 'nextpyp install --dir $dir' to upgrade it"
      [[ -e "$dir/sharedExec/containers/pyp.sif" ]] ||
        die "$dir/sharedExec/containers/pyp.sif is missing; run 'nextpyp install --dir $dir'"
      if instance_running; then
        note "nextPYP is already running at $url"
        return 0
      fi
      run_foreground "$dir" "$url"
      ;;
    stop)
      if ! instance_running && ! compgen -G "$dir/local/sock/*-processor-*" >/dev/null; then
        note "nextPYP is not running"
        return 0
      fi
      (cd "$dir" && ./nextpyp stop)
      ;;
    status)
      if instance_running && curl -fsS -o /dev/null --max-time 5 "$url"; then
        printf 'nextPYP %s is running at %s\n' "$(installed_version "$dir")" "$url"
      elif instance_running; then
        printf 'nextPYP is starting or not responding at %s\n' "$url"
      else
        printf 'nextPYP is not running (installation: %s)\n' "$dir"
        return 3
      fi
      ;;
  esac
}

command=${1:-help}
(( $# )) && shift
case $command in
  install) cmd_install "$@" ;;
  start|stop|status) cmd_service "$command" "$@" ;;
  help|-h|--help) usage ;;
  version|--version) printf 'nextPYP %s\n' "$NEXTPYP_VERSION" ;;
  *) usage >&2; die "unknown command '$command'" ;;
esac
