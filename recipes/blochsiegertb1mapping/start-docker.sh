#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'HELP'
Usage: start-docker.sh [--no-build] [IMAGE_TAG]
       start-docker.sh --list

Build blochsiegertb1mapping and launch its FIRE development server.
--no-build uses an existing image. --list shows local images and containers.
Environment overrides: BLOCHSIEGERTB1MAPPING_PORT=9002 PYTHON=python3
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
version=$(awk '$1 == "version:" {gsub(/[\047\042]/, "", $2); print $2; exit}' "$recipe_dir/build.yaml")
image_tag=${image_tag:-blochsiegertb1mapping:$version}
port=${BLOCHSIEGERTB1MAPPING_PORT:-9002}
name=blochsiegertb1mapping-fire
if [[ ! "$port" =~ ^[1-9][0-9]{0,4}$ ]] || (( port > 65535 )); then
  echo "Invalid TCP port: $port" >&2; exit 1
fi

if "${docker_cmd[@]}" container inspect "$name" >/dev/null 2>&1; then
  actual=$("${docker_cmd[@]}" inspect --format '{{.Image}}' "$name")
  wanted=$("${docker_cmd[@]}" image inspect --format '{{.Id}}' "$image_tag")
  state=$("${docker_cmd[@]}" inspect --format '{{.State.Running}}' "$name")
  bindings=$("${docker_cmd[@]}" port "$name" 9002/tcp 2>/dev/null || true)
  if [[ "$actual" == "$wanted" && "$state" == true ]] && \
     printf '%s\n' "$bindings" | grep -Fxq "0.0.0.0:$port"; then
    echo "$name is already running on TCP $port (config=blochsiegertb1mapping)."
    exit 0
  fi
  echo "$name exists with a different image, port, or stopped state. Stop/remove it explicitly first." >&2
  exit 1
fi
published=$("${docker_cmd[@]}" ps --format '{{.Ports}}')
if [[ "$published" == *":$port->"* ]]; then
  echo "Host port $port is already published by another Docker container." >&2
  exit 1
fi

if "$build"; then
  python_cmd=${PYTHON:-python3}
  "$python_cmd" -m builder stage "$recipe_dir" --architecture x86_64 --recreate --download
  build_dir="$repo_root/build/blochsiegertb1mapping"
  context_file=$(mktemp)
  trap 'rm -f -- "$context_file"' EXIT
  "$python_cmd" -m builder staged-context-args "$build_dir" > "$context_file"
  mapfile -d '' -t context_args < "$context_file"
  "${docker_cmd[@]}" buildx build --load --platform linux/amd64 \
    --file "$build_dir/blochsiegertb1mapping_${version}.Dockerfile" --tag "$image_tag" \
    --build-context "neurocontainer-cache=$build_dir/cache" \
    "${context_args[@]}" "$build_dir"
fi
"${docker_cmd[@]}" image inspect "$image_tag" >/dev/null
"${docker_cmd[@]}" run --detach --rm --name "$name" \
  --publish "$port:9002" --entrypoint python "$image_tag" \
  /opt/code/python-ismrmrd-server/main.py -H 0.0.0.0 -p 9002 -d blochsiegertb1mapping

echo "FIRE endpoint on this host: TCP $port (config=blochsiegertb1mapping)"
echo "Logs: ${docker_cmd[*]} logs -f $name"
echo "Stop: ${docker_cmd[*]} stop $name"
