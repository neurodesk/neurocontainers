#!/bin/bash
# Neurodesk launcher for SmartScope (automated cryo-EM grid screening).
#
# Upstream runs SmartScope as six docker-compose services. This launcher runs
# the same services from one read-only image: MariaDB, Redis, the Django app
# (gunicorn), its Celery worker and nginx. Everything they write lives in one
# instance directory chosen by the user, because the image is read-only and
# the home directory may be unavailable at runtime.
set -euo pipefail

APP_DIR=/opt/smartscope
VENV=$APP_DIR/.venv
STATIC_ROOT=/opt/shared/static
MARKER=.smartscope-neurodesk
DEFAULT_PORT=48000
SERVICES=(mariadb redis web worker nginx)
declare -A PROCESS=([mariadb]=mariadbd [redis]=redis-server [web]=gunicorn [worker]=celery [nginx]=nginx)

die() {
  printf 'smartscope: error: %s\n' "$*" >&2
  exit 1
}

note() {
  printf 'smartscope: %s\n' "$*" >&2
}

smartscope_version() {
  cat "$APP_DIR/VERSION"
}

usage() {
  cat <<EOF
SmartScope $(smartscope_version) (Neurodesk launcher)

SmartScope screens cryo-EM grids automatically through SerialEM. This
container runs its database, cache, web app and worker from one writable
instance directory.

Usage:
  smartscope init [--dir PATH] [--port N]   create an instance (once)
  smartscope run [--dir PATH]               start, stay in the foreground, Ctrl-C stops
  smartscope start [--dir PATH]             start in the background
  smartscope stop [--dir PATH]
  smartscope status [--dir PATH]
  smartscope.py COMMAND [ARGS]              SmartScope command-line interface
  manage.py COMMAND [ARGS]                  Django management commands
  smartscope help | --version

PATH defaults to \$SMARTSCOPE_DIR. It must be writable, keep the same path
in later sessions, and contain only letters, digits and . _ - + @ /
The web interface listens on http://127.0.0.1:PORT (default ${DEFAULT_PORT});
Redis uses PORT+1 on 127.0.0.1. Settings live in PATH/smartscope.env.

init sets the 'admin' password from SMARTSCOPE_ADMIN_PASSWORD, or generates
one and stores it in PATH/auth/admin_password.
EOF
}

resolve_dir() {
  local dir=$1
  [[ -n "$dir" ]] || die "no instance directory given; use --dir PATH or set SMARTSCOPE_DIR"
  dir=$(realpath -m -- "$dir")
  [[ "$dir" =~ ^[A-Za-z0-9._/@+-]+$ ]] ||
    die "'$dir' contains spaces or special characters; choose a path made of letters, digits and . _ - + @ /"
  # Unix socket paths are limited to 107 bytes.
  (( ${#dir} <= 80 )) || die "'$dir' is longer than 80 characters; choose a shorter path"
  printf '%s\n' "$dir"
}

instance_dir() {
  local dir
  dir=$(resolve_dir "$1")
  [[ -f "$dir/$MARKER" ]] || die "no SmartScope instance in $dir; run 'smartscope init --dir $dir' first"
  printf '%s\n' "$dir"
}

parse_dir_option() {
  local command=$1 dir=${SMARTSCOPE_DIR:-}
  shift
  while (( $# )); do
    case $1 in
      --dir) dir=${2-}; shift 2 || die "$1 needs a value" ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown $command option '$1'; see 'smartscope help'" ;;
    esac
  done
  instance_dir "$dir"
}

random_secret() {
  python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
}

# Exports the environment every SmartScope process expects. Values the user
# may change come from smartscope.env; paths and secrets owned by the
# instance are set afterwards so the file cannot break them.
load_env() {
  local dir=$1
  export SMARTSCOPE_DIR=$dir
  set -a
  # shellcheck disable=SC1091
  source "$dir/smartscope.env"
  set +a
  export SMARTSCOPE_PORT=${SMARTSCOPE_PORT:-$DEFAULT_PORT}
  export SMARTSCOPE_LISTEN=${SMARTSCOPE_LISTEN:-127.0.0.1}
  [[ "$SMARTSCOPE_PORT" =~ ^[0-9]+$ ]] && (( SMARTSCOPE_PORT >= 1024 && SMARTSCOPE_PORT <= 65534 )) ||
    die "SMARTSCOPE_PORT=$SMARTSCOPE_PORT in $dir/smartscope.env must be 1024-65534"

  RUN=$dir/run
  LOGS=$dir/logs
  export APP=$APP_DIR/
  export CONFIG=$dir/config/
  export LOGDIR=$LOGS/
  export TEMPDIR=$dir/tmp/
  export MPLCONFIGDIR=$dir/tmp/matplotlib
  export DJANGO_SETTINGS_MODULE=Smartscope.core.settings.server_docker
  export USE_SSL=false
  export DEFAULT_UMASK=${DEFAULT_UMASK:-002}
  # Upstream's settings treat any non-empty DEBUG value as true.
  export DEBUG=
  export MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_DATABASE=smartscope MYSQL_USER=smartscope
  export MYSQL_SOCKET=$RUN/mysqld.sock
  MYSQL_PASSWORD=$(<"$dir/auth/db_password")
  export MYSQL_PASSWORD
  export REDIS_HOST=127.0.0.1 REDIS_PORT=$((SMARTSCOPE_PORT + 1))
  REDIS_PASSWORD=$(<"$dir/auth/redis_password")
  export REDIS_PASSWORD
  SECRET_KEY=$(<"$dir/auth/secret_key")
  export SECRET_KEY
}

write_settings() {
  local dir=$1 port=$2
  cat >"$dir/smartscope.env" <<EOF
# SmartScope settings for this instance, read by every smartscope command.
# Restart SmartScope after editing. Any upstream SmartScope variable
# (https://docs.smartscope.org/advanced_setup/environment/) can be added.

# Web interface port. Redis uses SMARTSCOPE_PORT+1.
SMARTSCOPE_PORT=$port
# Address nginx listens on. Use 0.0.0.0 to serve other computers, and add
# this computer's name to ALLOWED_HOSTS.
SMARTSCOPE_LISTEN=127.0.0.1
ALLOWED_HOSTS=localhost,127.0.0.1
TIMEZONE=UTC

# Where SmartScope writes screening sessions. Point this at a large data
# location bound into the container if needed.
AUTOSCREENDIR=$dir/data
SCRATCH_DIR=$dir/scratch
# Image bank for the fake_scope test microscope.
TEST_FILES=$dir/testfiles
EXTERNAL_PLUGINS_DIRECTORY=$dir/external_plugins

WORKER_CONCURRENCY=4
LOGLEVEL=INFO
EOF
}

# Copies upstream's default configuration into the instance, as upstream's
# entrypoint does whenever the SmartScope version changes.
sync_config() {
  local config=$1 version
  version=$(smartscope_version)
  [[ "$(cat "$config/version" 2>/dev/null)" == "$version" ]] && return
  mkdir -p "$config/plugins" "$config/protocols"
  find "$APP_DIR/config/smartscope" -maxdepth 1 -type f -exec cp {} "$config/" \;
  printf '%s\n' "$version" >"$config/version"
}

pid_of() {
  local file=$RUN/$1.pid pid
  [[ -f "$file" ]] || return 1
  pid=$(<"$file")
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null &&
    grep -qa -- "${PROCESS[$1]}" "/proc/$pid/cmdline" 2>/dev/null || return 1
  printf '%s\n' "$pid"
}

# Starts a service in its own session, so stopping it reaches every process
# it spawned (gunicorn workers, celery children, SmartScope runs). The
# previous run's log is kept as NAME.log.1.
spawn() {
  local name=$1
  shift
  [[ ! -f "$LOGS/$name.log" ]] || mv -f "$LOGS/$name.log" "$LOGS/$name.log.1"
  setsid "$@" >>"$LOGS/$name.log" 2>&1 </dev/null &
  printf '%s\n' "$!" >"$RUN/$name.pid"
}

wait_for() {
  local name=$1 seconds=$2
  shift 2
  local i pid
  pid=$(<"$RUN/$name.pid")
  for (( i = 0; i < seconds * 2; i++ )); do
    "$@" >/dev/null 2>&1 && return 0
    # Not pid_of: right after spawn the child may not have exec'd yet.
    kill -0 "$pid" 2>/dev/null ||
      die "$name exited during startup; see $LOGS/$name.log"$'\n'"$(tail -n 20 "$LOGS/$name.log" 2>/dev/null)"
    sleep 0.5
  done
  die "$name did not become ready within ${seconds}s; see $LOGS/$name.log"
}

stop_service() {
  local name=$1 pid i
  if pid=$(pid_of "$name"); then
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    for (( i = 0; i < 60; i++ )); do
      kill -0 -- "-$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 -- "-$pid" 2>/dev/null; then
      note "$name did not stop within 30s; killing it"
      kill -KILL -- "-$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$RUN/$name.pid"
}

stop_all() {
  local i
  for (( i = ${#SERVICES[@]} - 1; i >= 0; i-- )); do
    stop_service "${SERVICES[i]}"
  done
}

port_in_use() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

mariadb_root() {
  mariadb --no-defaults --socket="$RUN/mysqld.sock" -u root "$@"
}

start_mariadb() {
  local dir=$1
  local args=(--no-defaults --datadir="$dir/db" --socket="$RUN/mysqld.sock"
              --pid-file="$RUN/mysqld.pid" --skip-networking --tmpdir="$dir/tmp"
              --log-error="$LOGS/mariadb.log"
              --character-set-server=utf8mb4 --collation-server=utf8mb4_general_ci)
  (( $(id -u) != 0 )) || args+=(--user=root)
  spawn mariadb mariadbd "${args[@]}"
  wait_for mariadb 120 mariadb-admin --no-defaults --socket="$RUN/mysqld.sock" -u root ping
}

start_redis() {
  cat >"$RUN/redis.conf" <<EOF
bind 127.0.0.1
port $REDIS_PORT
requirepass $REDIS_PASSWORD
dir $RUN
save ""
appendonly no
daemonize no
logfile ""
EOF
  chmod 600 "$RUN/redis.conf"
  spawn redis redis-server "$RUN/redis.conf"
  wait_for redis 30 env REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -h 127.0.0.1 -p "$REDIS_PORT" ping
}

start_web() {
  (cd "$APP_DIR" && "$VENV/bin/manage.py" migrate --noinput >>"$LOGS/migrate.log" 2>&1) ||
    die "database migration failed; see $LOGS/migrate.log"$'\n'"$(tail -n 20 "$LOGS/migrate.log")"
  rm -f "$RUN/gunicorn.sock"
  (cd "$APP_DIR" && spawn web "$VENV/bin/gunicorn" -c "$APP_DIR/config/docker/gunicorn.conf.py" \
    --bind "unix:$RUN/gunicorn.sock" --pid "$RUN/gunicorn.pid" --no-control-socket)
  wait_for web 120 test -S "$RUN/gunicorn.sock"
}

start_worker() {
  (cd "$APP_DIR" && spawn worker "$VENV/bin/celery" -A Smartscope.tasks.app worker -n long@%h \
    -Q smartscope --loglevel="${LOGLEVEL:-INFO}" --concurrency="${WORKER_CONCURRENCY:-4}")
  wait_for worker 60 grep -q 'ready\.' "$LOGS/worker.log"
}

start_nginx() {
  local user_line=''
  (( $(id -u) != 0 )) || user_line='user root;'
  mkdir -p "$RUN/nginx"
  cat >"$RUN/nginx.conf" <<EOF
$user_line
worker_processes 1;
daemon off;
pid $RUN/nginx.pid;
error_log $LOGS/nginx-error.log warn;
events { worker_connections 512; }
http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    access_log $LOGS/nginx-access.log;
    client_body_temp_path $RUN/nginx/client_body;
    proxy_temp_path $RUN/nginx/proxy;
    fastcgi_temp_path $RUN/nginx/fastcgi;
    uwsgi_temp_path $RUN/nginx/uwsgi;
    scgi_temp_path $RUN/nginx/scgi;
    client_max_body_size 200M;
    map \$http_upgrade \$connection_upgrade {
        default upgrade;
        '' close;
    }
    upstream smartscope {
        server unix:$RUN/gunicorn.sock;
    }
    server {
        listen $SMARTSCOPE_LISTEN:$SMARTSCOPE_PORT;
        server_name _;
        location / {
            proxy_pass http://smartscope;
            proxy_set_header Host \$http_host;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_redirect off;
        }
        location /websocket/ {
            proxy_pass http://smartscope;
            proxy_set_header Host \$http_host;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_http_version 1.1;
            proxy_set_header Upgrade \$http_upgrade;
            proxy_set_header Connection \$connection_upgrade;
        }
        location /static/ {
            alias $STATIC_ROOT/;
        }
    }
}
EOF
  spawn nginx nginx -p "$RUN/nginx" -c "$RUN/nginx.conf" -e "$LOGS/nginx-error.log"
  wait_for nginx 30 curl -fsS -o /dev/null "http://127.0.0.1:$SMARTSCOPE_PORT/login/"
}

start_all() {
  local dir=$1 name running=()
  load_env "$dir"
  for name in "${SERVICES[@]}"; do
    pid_of "$name" >/dev/null && running+=("$name")
  done
  if (( ${#running[@]} )); then
    die "SmartScope is already running from $dir (${running[*]}); run 'smartscope stop' first"
  fi
  local port
  for port in "$SMARTSCOPE_PORT" "$REDIS_PORT"; do
    port_in_use "$port" && die "port $port is in use; set another SMARTSCOPE_PORT in $dir/smartscope.env"
  done
  [[ "$SMARTSCOPE_LISTEN" == 127.0.0.1 ]] || note "listening on $SMARTSCOPE_LISTEN: other computers can reach this SmartScope"
  mkdir -p "$MPLCONFIGDIR"
  sync_config "$dir/config"
  trap 'stop_all' EXIT
  start_mariadb "$dir"
  start_redis
  start_web
  start_worker
  start_nginx
  trap - EXIT
  note "SmartScope $(smartscope_version) is running at http://localhost:$SMARTSCOPE_PORT (logs in $LOGS)"
}

cmd_init() {
  local dir=${SMARTSCOPE_DIR:-} port=$DEFAULT_PORT
  while (( $# )); do
    case $1 in
      --dir) dir=${2-}; shift 2 || die "$1 needs a value" ;;
      --port) port=${2-}; shift 2 || die "$1 needs a value" ;;
      -h|--help) usage; return 0 ;;
      *) die "unknown init option '$1'; see 'smartscope help'" ;;
    esac
  done
  [[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1024 && port <= 65534 )) || die "invalid --port '$port'; choose 1024-65534"
  dir=$(resolve_dir "$dir")
  [[ ! -f "$dir/$MARKER" ]] || die "$dir already holds a SmartScope instance; use 'smartscope start --dir $dir'"
  if [[ -d "$dir" && -n "$(ls -A "$dir")" ]]; then
    die "$dir is not empty; choose a new or empty directory (remove it to retry a failed init)"
  fi
  mkdir -p "$dir" 2>/dev/null && [[ -w "$dir" ]] ||
    die "$dir is not writable; choose a directory you can write to (the container image is read-only)"

  mkdir -p "$dir"/{auth,db,run,logs,tmp/matplotlib,config,data,scratch,testfiles,external_plugins,backups,fake_scope/raw,fake_scope/movies}
  chmod 700 "$dir/auth" "$dir/db" "$dir/run"
  local admin_password=${SMARTSCOPE_ADMIN_PASSWORD:-$(random_secret)}
  (
    umask 077
    random_secret >"$dir/auth/db_password"
    random_secret >"$dir/auth/redis_password"
    random_secret >"$dir/auth/secret_key"
    printf '%s\n' "$admin_password" >"$dir/auth/admin_password"
  )
  write_settings "$dir" "$port"
  load_env "$dir"
  sync_config "$dir/config"

  note "creating the database in $dir/db"
  mariadb-install-db --no-defaults --datadir="$dir/db" --auth-root-authentication-method=normal \
    --skip-test-db >"$LOGS/mariadb-install.log" 2>&1 ||
    die "mariadb-install-db failed; see $LOGS/mariadb-install.log"
  trap 'stop_all' EXIT
  start_mariadb "$dir"
  # root has no password but is reachable only through the socket in the
  # owner-only run directory (MariaDB listens on no TCP port). The tables in
  # upstream's initial database use utf8mb4_general_ci, and Django's foreign
  # keys to them need the same collation.
  mariadb_root <<EOF
CREATE DATABASE smartscope CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE USER 'smartscope'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON smartscope.* TO 'smartscope'@'localhost';
EOF
  mariadb_root smartscope <"$APP_DIR/config/docker/initialdb.sql"
  # Point upstream's sample fake-scope microscope at this instance.
  mariadb_root smartscope <<EOF
UPDATE microscope SET scope_path='$dir/fake_scope' WHERE serialem_IP='xxx.xxx.xxx.xxx';
UPDATE detector d JOIN microscope m ON d.microscope_id_id = m.microscope_id
  SET d.frames_directory='$dir/fake_scope/movies/' WHERE m.serialem_IP='xxx.xxx.xxx.xxx';
EOF
  (cd "$APP_DIR" && "$VENV/bin/manage.py" migrate --noinput >>"$LOGS/migrate.log" 2>&1) ||
    die "database migration failed; see $LOGS/migrate.log"
  (cd "$APP_DIR" && "$VENV/bin/manage.py" shell -c "
import os
from django.contrib.auth.models import User
user = User.objects.get(username='admin')
user.set_password(open(os.path.join(os.environ['SMARTSCOPE_DIR'], 'auth', 'admin_password')).read().strip())
user.save()
" >>"$LOGS/migrate.log" 2>&1) || die "could not set the admin password; see $LOGS/migrate.log"
  stop_all
  trap - EXIT
  printf 'smartscope=%s\nport=%s\n' "$(smartscope_version)" "$port" >"$dir/$MARKER"

  note "SmartScope instance created in $dir"
  if [[ -n "${SMARTSCOPE_ADMIN_PASSWORD:-}" ]]; then
    note "log in as 'admin' with the password from SMARTSCOPE_ADMIN_PASSWORD"
  else
    note "log in as 'admin'; the password is in $dir/auth/admin_password"
  fi
  note "next: export SMARTSCOPE_DIR=$dir; smartscope run"
}

cmd_run() {
  local dir
  dir=$(parse_dir_option run "$@")
  start_all "$dir"
  note "press Ctrl-C to stop"
  trap 'note "stopping"; stop_all; exit 0' INT TERM
  trap 'stop_all' EXIT
  local name
  while true; do
    for name in "${SERVICES[@]}"; do
      pid_of "$name" >/dev/null || die "$name stopped unexpectedly; see $LOGS/$name.log"
    done
    sleep 5 &
    wait $! || true
  done
}

cmd_start() {
  local dir
  dir=$(parse_dir_option start "$@")
  start_all "$dir"
  note "the services run only while this container runs; in a plain 'apptainer exec'"
  note "use 'smartscope run' instead, or start them inside an apptainer instance"
  note "stop them with 'smartscope stop' before leaving this session"
}

cmd_stop() {
  local dir
  dir=$(parse_dir_option stop "$@")
  load_env "$dir"
  stop_all
  note "SmartScope in $dir is stopped"
}

cmd_status() {
  local dir name pid stopped=0
  dir=$(parse_dir_option status "$@")
  load_env "$dir"
  for name in "${SERVICES[@]}"; do
    if pid=$(pid_of "$name"); then
      printf '%-8s running (pid %s)\n' "$name" "$pid"
    else
      printf '%-8s stopped\n' "$name"
      stopped=1
    fi
  done
  (( stopped )) || printf 'web interface: http://localhost:%s\n' "$SMARTSCOPE_PORT"
  return "$stopped"
}

# smartscope.py and manage.py run upstream's commands with the instance's
# environment, so they reach its database and cache.
case $(basename "$0") in
  smartscope.py|manage.py)
    tool=$(basename "$0")
    dir=$(instance_dir "${SMARTSCOPE_DIR:-}")
    load_env "$dir"
    exec "$VENV/bin/$tool" "$@"
    ;;
esac

command=${1:-help}
(( $# )) && shift
case $command in
  init) cmd_init "$@" ;;
  run) cmd_run "$@" ;;
  start) cmd_start "$@" ;;
  stop) cmd_stop "$@" ;;
  status) cmd_status "$@" ;;
  help|-h|--help) usage ;;
  version|--version) printf 'SmartScope %s\n' "$(smartscope_version)" ;;
  *) usage >&2; die "unknown command '$command'" ;;
esac
