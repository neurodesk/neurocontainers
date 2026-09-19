#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'HELP'
Usage: start-docker.sh [--no-build] [ACSRSS_IMAGE_TAG]
       start-docker.sh --list

Build acsrss if it is not already running, then launch acsrss and quickgrid.
--no-build uses the existing acsrss image. --list shows local images and containers.
Environment overrides: ACSRSS_PORT=9002 QUICKGRID_PORT=9003 QUICKGRID_IMAGE=...
If multiple quickgrid images exist, set QUICKGRID_IMAGE explicitly.
Requires repository Python dependencies when building (activate its environment).
HELP
}

build=true
list=false
image_tag=''
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --no-build) build=false ;;
    --list) list=true ;;
    -*) usage >&2; exit 1 ;;
    *) [[ -z "$image_tag" ]] || { usage >&2; exit 1; }; image_tag=$arg ;;
  esac
done

docker_cmd=(docker)
if ! docker info >/dev/null 2>&1; then
  echo 'Docker is unavailable to the current user; trying sudo.'
  docker_cmd=(sudo docker)
  "${docker_cmd[@]}" info >/dev/null
fi
if "$list"; then
  "${docker_cmd[@]}" image ls
  "${docker_cmd[@]}" ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
  exit 0
fi

recipe_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$recipe_dir/../.." && pwd)
cd "$repo_root"
# The release is a top-level scalar; launching existing images needs no Python packages.
version=$(awk '$1 == "version:" {print $2; exit}' "$recipe_dir/build.yaml")
image_tag=${image_tag:-acsrss:$version}
acsrss_port=${ACSRSS_PORT:-9002}
quickgrid_port=${QUICKGRID_PORT:-9003}
for port in "$acsrss_port" "$quickgrid_port"; do
  if [[ ! "$port" =~ ^[1-9][0-9]{0,4}$ ]] || (( port > 65535 )); then
    echo "Invalid TCP port: $port" >&2; exit 1
  fi
done
if [[ "$acsrss_port" == "$quickgrid_port" ]]; then
  echo 'ACSRSS_PORT and QUICKGRID_PORT must differ.' >&2; exit 1
fi

quickgrid_image=${QUICKGRID_IMAGE:-}
if [[ -z "$quickgrid_image" ]]; then
  images=$("${docker_cmd[@]}" image ls --format '{{.Repository}}:{{.Tag}}')
  mapfile -t candidates < <(printf '%s\n' "$images" | awk -F: '$1 ~ /(^|\/)quickgrid$/ && $2 != "<none>"' | sort -u)
  if [[ ${#candidates[@]} -ne 1 ]]; then
    echo 'Set QUICKGRID_IMAGE to one local image from start-docker.sh --list.' >&2
    exit 1
  fi
  quickgrid_image=${candidates[0]}
fi
"${docker_cmd[@]}" image inspect "$quickgrid_image" >/dev/null

# Reuse only running containers with the requested image and exact public port mapping.
check_container() {
  local name=$1 image=$2 port=$3 actual wanted state bindings
  if "${docker_cmd[@]}" container inspect "$name" >/dev/null 2>&1; then
    actual=$("${docker_cmd[@]}" inspect --format '{{.Image}}' "$name")
    wanted=$("${docker_cmd[@]}" image inspect --format '{{.Id}}' "$image")
    state=$("${docker_cmd[@]}" inspect --format '{{.State.Running}}' "$name")
    bindings=$("${docker_cmd[@]}" port "$name" 9002/tcp 2>/dev/null || true)
    if [[ "$actual" == "$wanted" && "$state" == true ]] && \
       printf '%s\n' "$bindings" | grep -Fxq "0.0.0.0:$port"; then
      return 0
    fi
    echo "$name exists with a different image, port, or stopped state. Stop/remove it explicitly first." >&2
    exit 1
  fi
  # Check Docker port reservations, including stopped containers that we do not change.
  local published
  published=$("${docker_cmd[@]}" ps --format '{{.Ports}}')
  if [[ "$published" == *":$port->"* ]]; then
    echo "Host port $port is already published by another Docker container." >&2
    exit 1
  fi
  return 1
}
acsrss_running=false
quickgrid_running=false
if check_container acsrss-fire "$image_tag" "$acsrss_port"; then acsrss_running=true; fi
if check_container quickgrid-fire "$quickgrid_image" "$quickgrid_port"; then quickgrid_running=true; fi

if ! "$acsrss_running" && "$build"; then
  python_cmd=${PYTHON:-python3}
  echo "Staging acsrss $version..."
  "$python_cmd" -m builder stage "$recipe_dir" --architecture x86_64 --recreate --download
  build_dir="$repo_root/build/acsrss"
  context_file=$(mktemp)
  trap 'rm -f -- "$context_file"' EXIT
  "$python_cmd" -m builder staged-context-args "$build_dir" > "$context_file"
  mapfile -d '' -t context_args < "$context_file"
  "${docker_cmd[@]}" buildx build --load --platform linux/amd64 \
    --file "$build_dir/acsrss_${version}.Dockerfile" --tag "$image_tag" \
    --build-context "neurocontainer-cache=$build_dir/cache" \
    "${context_args[@]}" "$build_dir"
fi
"${docker_cmd[@]}" image inspect "$image_tag" >/dev/null

# Docker performs the final atomic port reservation, also detecting non-Docker listeners.
if ! "$acsrss_running"; then
  "${docker_cmd[@]}" run --detach --rm --name acsrss-fire \
    --publish "$acsrss_port:9002" --entrypoint python "$image_tag" \
    /opt/code/python-ismrmrd-server/main.py -H 0.0.0.0 -p 9002 -d acsrss
fi
if ! "$quickgrid_running"; then
  "${docker_cmd[@]}" run --detach --rm --name quickgrid-fire \
    --publish "$quickgrid_port:9002" --entrypoint python "$quickgrid_image" \
    /opt/code/python-ismrmrd-server/main.py -H 0.0.0.0 -p 9002 -d quickgrid
fi

printf 'FIRE endpoints on this host:\n  acsrss:    TCP %s (config=acsrss)\n  quickgrid: TCP %s (config=quickgrid)\n' "$acsrss_port" "$quickgrid_port"
echo "Logs: ${docker_cmd[*]} logs -f CONTAINER_NAME"
echo "Stop: ${docker_cmd[*]} stop CONTAINER_NAME"
