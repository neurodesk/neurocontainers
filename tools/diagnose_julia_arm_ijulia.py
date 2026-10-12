"""Temporary diagnostic branch only. Instrumented observations, not a speed benchmark."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import zipfile

SIF = 'julia_arm64_1.13.1_20261012.simg'
SHA256 = '1851d06cb4de6cb59308fec16734077156e1cc092ef5a0f901718c835f3aabd1'
HEAD = '2db0f5fb5b87e659fa8afc71d3abc8e690a3557a'
LIMIT = 120
ACTIVE = set()


def extract(archive, output):
    output.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as bundle:
        for name in (SIF, 'manifest.json'):
            members = [m for m in bundle.infolist() if Path(m.filename).name == name]
            if len(members) != 1:
                raise ValueError(f'Expected exactly one {name}, got {len(members)}')
            # Never use archive paths as destinations; never extract the Docker archive.
            with bundle.open(members[0]) as source, (output / name).open('wb') as dest:
                shutil.copyfileobj(source, dest, 8 * 1024 * 1024)
    manifest = json.loads((output / 'manifest.json').read_text())
    assert manifest['head_sha'] == HEAD
    assert manifest['architecture'] == 'aarch64'
    assert manifest['sif'] == SIF and manifest['sif_sha256'] == SHA256
    digest = hashlib.sha256()
    with (output / SIF).open('rb') as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b''):
            digest.update(block)
    assert digest.hexdigest() == SHA256, 'Exact candidate SIF digest mismatch'
    (output / 'verified.json').write_text(json.dumps({'sif_sha256': digest.hexdigest(), 'head': HEAD}))


def stop_group(pid):
    # Each test owns one new session/process group, including compiler workers.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            return
        if sig == signal.SIGTERM:
            time.sleep(1)


def interrupted(signum, frame):
    raise KeyboardInterrupt(f'Received signal {signum}')


def observe(command, prefix):
    started = time.monotonic()
    timed_out = False
    with prefix.with_suffix('.log').open('w') as log, prefix.with_suffix('.processes.txt').open('w') as stats:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        ACTIVE.add(child.pid)
        try:
            while child.poll() is None:
                elapsed = time.monotonic() - started
                if elapsed >= LIMIT:
                    timed_out = True
                    break
                stats.write(f'elapsed={elapsed:.3f} load={os.getloadavg()}\n')
                stats.flush()
                snapshot = subprocess.check_output(['ps', '-eo', 'pid,ppid,pgid,sid,pcpu,pmem,etime,time,stat,args'], text=True)
                for line in snapshot.splitlines():
                    fields = line.split(None, 3)
                    if len(fields) >= 3 and fields[2] == str(child.pid):
                        stats.write(line + '\n')
                subprocess.run(['ps', '-eo', 'pid,ppid,pcpu,pmem,stat,comm'], stdout=stats, check=True)
                time.sleep(min(2, max(0, LIMIT - elapsed)))
        finally:
            elapsed = time.monotonic() - started
            stop_group(child.pid)
            child.wait(timeout=10)
            ACTIVE.discard(child.pid)
    result = {'command': command, 'elapsed_seconds': elapsed, 'timeout_seconds': LIMIT,
              'timed_out': timed_out, 'exit_code': child.returncode,
              'import_completed': 'IJulia loaded' in prefix.with_suffix('.log').read_text()}
    prefix.with_suffix('.json').write_text(json.dumps(result, indent=2))
    return result


METADATA = r'''
println("VERSION=", VERSION)
println("HOME=", homedir())
println("ENV_JULIA_DEPOT_PATH=", get(ENV, "JULIA_DEPOT_PATH", "<unset>"))
println("DEPOT_PATH=", repr(DEPOT_PATH))
println("LOAD_PATH=", repr(LOAD_PATH))
println("active_project=", Base.active_project())
println("cpu=", Sys.CPU_NAME, " threads=", Threads.nthreads())
println("ZMQ_preferences=", Base.get_preferences(Base.UUID("c2297ded-f4af-51ae-bb23-16f91089e4e1")))
for depot in DEPOT_PATH
    envpath = joinpath(depot, "environments", "v$(VERSION.major).$(VERSION.minor)")
    for file in ("Project.toml", "LocalPreferences.toml", "Manifest.toml")
        path = joinpath(envpath, file)
        if isfile(path)
            println("FILE=", path)
            println(read(path, String))
        end
    end
end
using IJulia
println("IJulia_path=", pathof(IJulia))
println("ZMQ_path=", pathof(IJulia.ZMQ))
println("frozen_launcher=", read("/usr/local/bin/julia", String))
for file in readdir(dirname(pathof(IJulia.ZMQ)); join=true)
    if isfile(file) && endswith(file, ".jl")
        text = read(file, String)
        if occursin("precompile", text) || occursin("load_preference", text)
            println("ZMQ_SOURCE=", file)
            println(text)
        end
    end
end
println("IJulia loaded")
'''


def run(output):
    assert os.uname().machine == 'aarch64', 'Native ARM required'
    assert json.loads((output / 'verified.json').read_text())['sif_sha256'] == SHA256
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    with (output / 'host.txt').open('w') as dest:
        for cmd in (['uname', '-a'], ['lscpu'], ['uptime'], ['nproc'], ['free', '-m'], ['apptainer', '--version']):
            subprocess.run(cmd, stdout=dest, stderr=subprocess.STDOUT, check=True)
    results = []
    modes = ['launcher', 'direct-tilde', 'direct-canonical']
    try:
        for repetition in range(5):
            # Rotate order to expose drift without parallel compilation contention.
            order = modes[repetition % 3:] + modes[:repetition % 3]
            for mode in order:
                case = output / f'{repetition + 1}-{mode}'
                case.mkdir()
                home = case / 'home'
                home.mkdir(mode=0o700)
                # --contain supplies private HOME/tmp; avoid host bind mounts and env.
                base = ['apptainer', 'exec', '--cleanenv', '--contain', '--home', f'{home}:/home/diagnostic',
                        '--pwd', '/home/diagnostic', '--env', 'JULIA_DEBUG=loading']
                if mode == 'direct-canonical':
                    base += ['--env', 'JULIA_DEPOT_PATH=/home/diagnostic/.julia:/opt/julia_depot']
                # Inherit the literal image ENV for both other modes. The wrapper
                # treats this default value as default even when explicitly set.
                executable = '/usr/local/bin/julia' if mode == 'launcher' else '/opt/julia-latest/bin/julia'
                command = base + [str(output / SIF), executable, '-e']
                for phase in ('cold', 'warm'):
                    result = observe(command + ['using IJulia; println("IJulia loaded")'], case / phase)
                    results.append({'repetition': repetition + 1, 'mode': mode, 'phase': phase, **result})
                    (output / 'results.json').write_text(json.dumps(results, indent=2))
                # Only inspect package preferences AFTER both imports, so metadata
                # collection cannot warm the cold package import.
                observe(command + [METADATA], case / 'metadata')
                with (case / 'cache-files.txt').open('w') as dest:
                    for path in sorted(home.rglob('*')):
                        if path.is_file():
                            info = path.stat()
                            dest.write(f'{path.relative_to(home)} size={info.st_size} mtime={info.st_mtime}\n')
    finally:
        for pid in list(ACTIVE):
            stop_group(pid)
    failures = [r for r in results if r['timed_out'] or r['exit_code'] != 0 or not r['import_completed']]
    print(json.dumps({'observations': len(results), 'failed_imports': len(failures)}))
    return bool(failures)


if __name__ == '__main__':
    if sys.argv[1] == 'extract':
        extract(Path(sys.argv[2]), Path(sys.argv[3]).resolve())
    elif sys.argv[1] == 'run':
        sys.exit(run(Path(sys.argv[2]).resolve()))
    else:
        raise SystemExit('Expected extract or run')
