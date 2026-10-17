#!/usr/bin/env bash
set -euo pipefail
[[ $(uname -m) == aarch64 ]] || { echo 'Native aarch64 only; no emulation.' >&2; exit 2; }
if [[ ${1:-} != --bounded ]]; then
  exec timeout --signal=TERM --kill-after=60s 480m bash "$0" --bounded
fi
export R10_HERE=/opt/brainles-cblas-inputs
export R10_WORK=/tmp/brainles-cblas-build
export R10_VENDOR=OPENBLAS R10_JOBS=2
export RUNTIME_PYTHON=/opt/miniconda-py311_24.9.2-0/envs/brainles/bin/python
export EVIDENCE=/opt/brainles-cblas-evidence
echo 'BrainLes CBLAS phase=admission'
[[ ! -e $R10_WORK && ! -e $EVIDENCE ]] || { echo 'Fresh build and evidence directories required.' >&2; exit 2; }
mkdir "$R10_WORK" "$EVIDENCE"
mkdir "$R10_WORK/logs"
phase=admission
started=$SECONDS
finish() {
  status=$?
  trap - EXIT
  set +e
  for file in CMakeCache.txt compile_commands.json; do
    if [[ -f $R10_WORK/pytorch/build/$file ]]; then
      cp "$R10_WORK/pytorch/build/$file" "$EVIDENCE/$file"
    fi
  done
  [[ ! -d $R10_WORK/logs ]] || cp -a "$R10_WORK/logs/." "$EVIDENCE/"
  if [[ -f $EVIDENCE/compile_commands.json ]]; then
    "$RUNTIME_PYTHON" -c 'import json,os,pathlib; p=pathlib.Path(os.environ["EVIDENCE"]); commands=json.loads((p/"compile_commands.json").read_text()); (p/"gemm-compile-command.json").write_text(json.dumps([c for c in commands if c["file"].endswith("/src/cpu/gemm/gemm.cpp")],indent=2)+"\n")'
  fi
  for file in /sys/fs/cgroup/memory.{events,current,peak,swap.current}; do
    [[ ! -r $file ]] || { printf '%s\n' "$file"; cat "$file"; }
  done > "$EVIDENCE/final-memory.txt"
  printf 'phase=%s status=%s elapsed_seconds=%s release=DIAGNOSTIC_BLOCKED merge_eligible=false\n' \
    "$phase" "$status" "$((SECONDS-started))" | tee "$EVIDENCE/outcome.txt"
  if ((status != 0)); then
    for file in "$EVIDENCE/"*.log; do
      [[ -f $file ]] || continue
      printf '\n%s\n' "${file##*/}"
      tail -n 25 "$file"
    done
    [[ ! -f $EVIDENCE/CMakeCache.txt ]] || grep -E 'DNNL_BLAS|CBLAS|OpenBLAS|BLAS_openblas|Python_|CMAKE_(C|CXX)_COMPILER' "$EVIDENCE/CMakeCache.txt" | head -n 60
    [[ ! -f $EVIDENCE/gemm-compile-command.json ]] || head -c 8000 "$EVIDENCE/gemm-compile-command.json"
  fi
  exit "$status"
}
trap finish EXIT
trap 'exit 143' TERM
step() {
  phase=$1
  printf 'BrainLes CBLAS phase=%s elapsed_seconds=%s\n' "$phase" "$((SECONDS-started))"
}
cp "$R10_HERE/pins.json" "$EVIDENCE/pins.json"
"$RUNTIME_PYTHON" - <<'PY' > "$R10_WORK/logs/admission.log" 2>&1
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

assert sys.version_info[:2] == (3, 11)
root = Path(os.environ['R10_HERE'])
pins = json.loads((root / 'pins.json').read_text())
assert pins['status'] == 'DIAGNOSTIC_BLOCKED' and pins['merge_eligible'] is False
for kind, records in pins['tools'].items():
    for record in records:
        path = root / kind / record['filename']
        assert hashlib.file_digest(path.open('rb'), 'sha256').hexdigest() == record['sha256'], path
for name, digest in pins['headers']['sha256'].items():
    assert hashlib.sha256((root / 'headers' / name).read_bytes()).hexdigest() == digest
assert hashlib.file_digest((root / 'reference.whl').open('rb'), 'sha256').hexdigest() == pins['wheel']['sha256']
for source in [pins['torch'], pins['acl']]:
    assert source['repository'].startswith('https://github.com/')
    assert len(source['commit']) == 40 and all(c in '0123456789abcdef' for c in source['commit'])
free = shutil.disk_usage(os.environ['R10_WORK']).free
assert free > 0
facts = {'affinity': sorted(os.sched_getaffinity(0)), 'disk_free_bytes': free,
         'hidden_cgroup_ancestry': 'unknown', 'capacity_for_build': 'unknown'}
for file in ['/proc/meminfo', '/proc/self/cgroup', '/proc/self/mountinfo']:
    try:
        facts[file] = Path(file).read_text()
    except OSError as error:
        facts[file] = str(error)
mount = Path('/sys/fs/cgroup')
groups = set()
try:
    memberships = [line.split(':', 2)[2] for line in facts['/proc/self/cgroup'].splitlines() if line.startswith('0::')]
    if len(memberships) != 1 or not memberships[0].startswith('/') or '..' in Path(memberships[0]).parts:
        raise ValueError('missing or unsupported unified membership')
    mounts = {}
    for line in facts['/proc/self/mountinfo'].splitlines():
        fields, filesystem = line.split(' - ', 1)
        fields = fields.split()
        if len(fields) < 6 or not filesystem.split():
            raise ValueError('malformed mountinfo')
        if any('\\' in field for field in fields[3:5]):
            raise ValueError('escaped mount paths are unsupported')
        mounts[fields[0]] = (fields[1], fields[3], Path(fields[4]), filesystem.split()[0])
    candidates = [key for key, entry in mounts.items() if entry[2] == mount]
    if len(candidates) != 1:
        raise ValueError('missing or stacked cgroup mount')
    selected = candidates[0]
    if mounts[selected][1] != '/' or mounts[selected][3] != 'cgroup2':
        raise ValueError('nonroot or non-cgroup2 mount is unsupported')
    ancestors = set()
    parent = mounts[selected][0]
    while parent in mounts:
        if parent in ancestors or parent == selected:
            raise ValueError('cyclic mount ancestry')
        ancestors.add(parent)
        parent = mounts[parent][0]
    if any(key != selected and (entry[2] == mount or mount in entry[2].parents
           or (entry[2] in mount.parents and key not in ancestors))
           for key, entry in mounts.items()):
        raise ValueError('overlapping mount visibility is unknown')
    leaf = mount / memberships[0].lstrip('/')
    if not leaf.is_dir():
        raise ValueError('membership directory is unavailable')
    groups.update([leaf, *(parent for parent in leaf.parents if parent == mount or mount in parent.parents)])
    facts['cgroup_mapping'] = {'status': 'supported', 'mount_id': selected, 'leaf': str(leaf)}
except (OSError, ValueError) as error:
    facts['cgroup_mapping'] = {'status': 'unknown', 'reason': str(error)}
facts['enforced_memory_groups'] = sorted(str(group) for group in groups)
for group in sorted(groups | {mount}):
    for name in ['memory.max', 'memory.high', 'memory.current', 'memory.events', 'memory.swap.max', 'memory.swap.current', 'memory.peak']:
        path = group / name
        try:
            facts[str(path)] = path.read_text()
        except OSError as error:
            facts[str(path)] = str(error)
print(json.dumps(facts, indent=2), flush=True)
for group in groups:
    limit = facts.get(str(group / 'memory.max'), '').strip()
    current = facts.get(str(group / 'memory.current'), '').strip()
    if limit.isdecimal() and current.isdecimal():
        assert int(current) < int(limit), ('Observable finite memory limit exhausted', str(group))
PY
step tools
apt-get -y --no-download --no-remove --allow-downgrades \
  -o Dir::Etc::sourcelist=/dev/null -o Dir::Etc::sourceparts=- \
  install "$R10_HERE"/debs/*.deb > "$R10_WORK/logs/debian-tools.log" 2>&1
"$RUNTIME_PYTHON" -m venv --without-pip "$R10_WORK/python"
export BUILD_PYTHON=$R10_WORK/python/bin/python
"$RUNTIME_PYTHON" -m pip --python "$BUILD_PYTHON" install --no-index --no-deps "$R10_HERE"/wheels/*.whl > "$R10_WORK/logs/python-tools.log" 2>&1
"$BUILD_PYTHON" -m pip check >> "$R10_WORK/logs/python-tools.log" 2>&1
mkdir "$R10_WORK/bin"
ln -s /usr/bin/gcc-13 "$R10_WORK/bin/gcc"
ln -s /usr/bin/g++-13 "$R10_WORK/bin/g++"
export PATH="$R10_WORK/bin:$R10_WORK/python/bin:$PATH"
for cmd in cmake ninja gcc g++ git scons readelf; do command -v "$cmd"; done >> "$R10_WORK/logs/tools.log"
[[ $(gcc -dumpfullversion) == 13.3.0 && $(g++ -dumpfullversion) == 13.3.0 ]]
"$BUILD_PYTHON" - <<'PY' >> "$R10_WORK/logs/tools.log" 2>&1
import importlib.metadata as md
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig

pins = json.loads((Path(os.environ['R10_HERE']) / 'pins.json').read_text())
for record in pins['tools']['wheels']:
    assert md.version(record['name']) == record['version'], record
for record in pins['tools']['debs']:
    actual = subprocess.check_output(['dpkg-query', '-W', '-f=${Version}', record['name']], text=True)
    assert actual == record['version'], (record, actual)
assert sys.version_info[:2] == (3, 11)
assert md.version('numpy') == '2.0.1'
assert 'v4.7.0' in subprocess.check_output(['scons', '--version'], text=True)
assert subprocess.check_output(['cmake', '--version'], text=True).split()[2] == md.version('cmake')
assert subprocess.check_output(['gcc', '-dumpmachine'], text=True).strip() == 'aarch64-linux-gnu'
code = "import json,sys,sysconfig; print(json.dumps({'base':sys.prefix,'include':sysconfig.get_path('include'),'lib':sysconfig.get_config_var('LIBDIR')+'/'+sysconfig.get_config_var('LDLIBRARY'),'soabi':sysconfig.get_config_var('SOABI')}))"
runtime = json.loads(subprocess.check_output([os.environ['RUNTIME_PYTHON'], '-c', code], text=True))
assert Path(sys.base_prefix).samefile(runtime['base'])
assert Path(sysconfig.get_path('include')).samefile(runtime['include'])
assert sysconfig.get_config_var('SOABI') == runtime['soabi']
assert (Path(runtime['include']) / 'Python.h').is_file()
assert Path(runtime['lib']).is_file()
(Path(os.environ['EVIDENCE']) / 'python.json').write_text(json.dumps(runtime, indent=2)+'\n')
print(json.dumps(runtime, indent=2))
PY
"$BUILD_PYTHON" -m pip freeze > "$R10_WORK/logs/build-environment.txt"
"$RUNTIME_PYTHON" -m pip list --format=json > "$EVIDENCE/packages-before.json"
step sources
"$BUILD_PYTHON" - <<'PY' > "$R10_WORK/logs/sources.log" 2>&1
import json
import os
from pathlib import Path
import subprocess

pins = json.loads((Path(os.environ['R10_HERE']) / 'pins.json').read_text())
root = Path(os.environ['R10_WORK'])
for key, directory in [('torch', 'pytorch'), ('acl', 'acl')]:
    dest = root / directory
    subprocess.run(['git', 'init', str(dest)], check=True)
    git = ['git', '-C', str(dest)]
    subprocess.run(git + ['remote', 'add', 'origin', pins[key]['repository']], check=True)
    subprocess.run(git + ['-c', 'protocol.allow=never', '-c', 'protocol.https.allow=always', 'fetch', '--depth=1', 'origin', pins[key]['commit']], check=True)
    subprocess.run(git + ['checkout', '--detach', 'FETCH_HEAD'], check=True)
    subprocess.run(git + ['-c', 'protocol.allow=never', '-c', 'protocol.https.allow=always', 'submodule', 'update', '--init', '--recursive', '--jobs', '2'], check=True)
    assert (dest / '.git').is_dir()
    assert subprocess.check_output(git + ['rev-parse', 'HEAD'], text=True).strip() == pins[key]['commit']
    assert not subprocess.check_output(git + ['status', '--porcelain', '--untracked-files=all'], text=True).strip()
    status = subprocess.check_output(git + ['submodule', 'status', '--recursive'], text=True)
    assert all(line.startswith(' ') for line in status.splitlines()), status
    subprocess.run(git + ['submodule', 'foreach', '--recursive', 'test -z "$(git status --porcelain --untracked-files=all)"'], check=True)
    with (Path(os.environ['EVIDENCE']) / 'submodules.txt').open('a') as output:
        output.write(key+' '+pins[key]['commit']+'\n'+status)
for directory, key in [('third_party/ideep', 'ideep_commit'), ('third_party/ideep/mkl-dnn', 'onednn_commit')]:
    actual = subprocess.check_output(['git', '-C', str(root / 'pytorch' / directory), 'rev-parse', 'HEAD'], text=True).strip()
    assert actual == pins[key], (key, actual)
PY
step provider
mkdir -p "$R10_WORK/provider/lib" "$R10_WORK/provider/include"
cp "$R10_HERE"/headers/*.h "$R10_WORK/provider/include/"
"$BUILD_PYTHON" - <<'PY' > "$R10_WORK/logs/provider.log" 2>&1
import ctypes
import hashlib
import json
import os
from pathlib import Path
import zipfile

pins = json.loads((Path(os.environ['R10_HERE']) / 'pins.json').read_text())
root = Path(os.environ['R10_WORK'])
with zipfile.ZipFile(Path(os.environ['R10_HERE']) / 'reference.whl') as archive:
    for name, record in pins['provider_members'].items():
        data = archive.read(name)
        assert hashlib.sha256(data).hexdigest() == record['sha256']
        (root / 'provider/lib' / Path(name).name).write_bytes(data)
lib = ctypes.CDLL(str(root / 'provider/lib/libopenblas.so.0'))
lib.openblas_get_config.restype = ctypes.c_char_p
config = lib.openblas_get_config().decode()
assert 'OpenBLAS 0.3.33' in config and 'USE_OPENMP' in config
assert 'USE64BITINT' not in config and 'NO_CBLAS' not in config
assert getattr(lib, 'cblas_sgemm')
print(config)
PY
ln -s libopenblas.so.0 "$R10_WORK/provider/lib/libopenblas.so"
export MAX_JOBS=$R10_JOBS CMAKE_BUILD_PARALLEL_LEVEL=$R10_JOBS
export CC=gcc CXX=g++
export TH_BINARY_BUILD=1 USE_CUDA=0 USE_CUDNN=OFF USE_CUSPARSELT=OFF
export USE_GFLAGS=OFF USE_GLOG=OFF USE_GLOO=ON USE_MPI=OFF USE_NCCL=OFF
export USE_NNPACK=ON USE_OPENMP=ON USE_ROCM=OFF USE_XCCL=OFF USE_XPU=OFF
export USE_MKLDNN=1 USE_MKLDNN_ACL=1 BLAS=OpenBLAS
export OpenBLAS_HOME=$R10_WORK/provider ACL_ROOT_DIR=$R10_WORK/acl
export CMAKE_BUILD_TYPE=Release PYTORCH_BUILD_NUMBER=1 USE_NINJA=1
PYTORCH_BUILD_VERSION=$("$BUILD_PYTHON" -c 'import json,os; print(json.load(open(os.environ["R10_HERE"]+"/pins.json"))["torch"]["version"])')
export PYTORCH_BUILD_VERSION
unset CMAKE_FRESH CMAKE_ONLY
export LD_LIBRARY_PATH="$R10_WORK/provider/lib:$R10_WORK/acl/build${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
step acl
(
  cd "$R10_WORK/acl"
  scons -j"$R10_JOBS" Werror=0 debug=0 neon=1 opencl=0 embed_kernels=0 os=linux arch=armv8a build=native multi_isa=1 fixed_format_kernels=1 openmp=1 cppthreads=0
) > "$R10_WORK/logs/acl-build.log" 2>&1
step configure
(
  cd "$R10_WORK/pytorch"
  CMAKE_ONLY=1 "$BUILD_PYTHON" setup.py build
  cmake -S . -B build \
    -DDNNL_BLAS_VENDOR=OPENBLAS \
    -DCMAKE_PROJECT_Torch_INCLUDE="$R10_HERE/provider.cmake" \
    -DBLAS_openblas_LIBRARY="$R10_WORK/provider/lib/libopenblas.so.0" \
    -DOpenBLAS_LIB="$R10_WORK/provider/lib/libopenblas.so.0" \
    -DOpenBLAS_INCLUDE_DIR="$R10_WORK/provider/include" \
    -DBLAS_INCLUDE_DIR="$R10_WORK/provider/include" \
    -DPython_EXECUTABLE="$BUILD_PYTHON" \
    -DPython_INCLUDE_DIR="$("$BUILD_PYTHON" -c 'import sysconfig; print(sysconfig.get_path("include"))')" \
    -DPython_LIBRARY="$("$BUILD_PYTHON" -c 'import json,os; print(json.load(open(os.environ["EVIDENCE"]+"/python.json"))["lib"])')"
  "$BUILD_PYTHON" "$R10_HERE/check-config.py"
) > "$R10_WORK/logs/configure.log" 2>&1
"$BUILD_PYTHON" - <<'PY' >> "$R10_WORK/logs/configure.log" 2>&1
import json
import os
from pathlib import Path
import re

import numpy

root = Path(os.environ['R10_WORK'])
cache = dict(re.findall(r'^([^:#/][^:]*):[^=]+=(.*)$', (root/'pytorch/build/CMakeCache.txt').read_text(), re.M))
expected = json.loads((Path(os.environ['EVIDENCE'])/'python.json').read_text())
assert Path(cache['Python_EXECUTABLE']).samefile(os.environ['BUILD_PYTHON'])
assert Path(cache['Python_INCLUDE_DIR']).samefile(expected['include'])
assert Path(cache['Python_LIBRARY']).samefile(expected['lib'])
assert Path(cache['Python_NumPy_INCLUDE_DIR']).samefile(numpy.get_include())
for key, suffix in [('CMAKE_C_COMPILER', 'gcc-13'), ('CMAKE_CXX_COMPILER', 'g++-13')]:
    assert Path(cache[key]).samefile('/usr/bin/'+suffix)
PY
step wheel
(cd "$R10_WORK/pytorch" && "$BUILD_PYTHON" setup.py bdist_wheel) > "$R10_WORK/logs/torch-build.log" 2>&1
cp "$R10_WORK/pytorch"/dist/torch-*.whl "$EVIDENCE/"
"$BUILD_PYTHON" - <<'PY' > "$R10_WORK/logs/wheel.log" 2>&1
import hashlib
import json
import os
from pathlib import Path
import zipfile
from packaging.utils import parse_wheel_filename
from packaging.tags import sys_tags

wheels = list(Path(os.environ['EVIDENCE']).glob('torch-*.whl'))
assert len(wheels) == 1
wheel = wheels[0]
name, version, _, tags = parse_wheel_filename(wheel.name)
assert str(version) == os.environ['PYTORCH_BUILD_VERSION']
assert tags & set(sys_tags())
assert all(tag.interpreter == 'cp311' and tag.abi == 'cp311' and tag.platform.endswith('_aarch64') for tag in tags)
print(wheel.name, hashlib.file_digest(wheel.open('rb'), 'sha256').hexdigest())
PY
step install
"$RUNTIME_PYTHON" -m pip install --no-index --no-deps --force-reinstall "$EVIDENCE"/torch-*.whl > "$R10_WORK/logs/install.log" 2>&1
R10_INSTALLED_LIB=$("$RUNTIME_PYTHON" -c "import sysconfig; print(sysconfig.get_path('purelib')+'/torch/lib')")
[[ $R10_INSTALLED_LIB == /opt/* ]]
cp "$R10_WORK/provider/lib/"*.so.* "$R10_INSTALLED_LIB/"
cp "$R10_WORK/acl/build/"libarm_compute*.so "$R10_INSTALLED_LIB/"
sha256sum "$R10_INSTALLED_LIB/"libarm_compute*.so > "$EVIDENCE/acl-installed.sha256"
readelf -d "$R10_INSTALLED_LIB/"*.so* > "$EVIDENCE/installed-elf.txt"
cp "$R10_WORK/pytorch/build/CMakeCache.txt" "$R10_WORK/pytorch/build/compile_commands.json" "$EVIDENCE/"
cp -a "$R10_WORK/logs/." "$EVIDENCE/"
rm -rf "$R10_WORK/pytorch" "$R10_WORK/acl" "$R10_WORK/provider" "$R10_WORK/python" "$R10_WORK/bin"
unset LD_LIBRARY_PATH
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
step import
"$RUNTIME_PYTHON" - <<'PY' > "$R10_WORK/logs/import.log" 2>&1
import hashlib
import json
import os
from pathlib import Path
import torch

pins = json.loads((Path(os.environ['R10_HERE'])/'pins.json').read_text())
assert torch.version.git_version == pins['torch']['commit']
assert torch.__version__ == pins['torch']['version']
maps = Path('/proc/self/maps').read_text()
(Path(os.environ['EVIDENCE'])/'installed-maps.txt').write_text(maps)
loaded = {Path(line.split()[-1]).resolve() for line in maps.splitlines() if line.split()[-1].startswith('/')}
for name, record in pins['provider_members'].items():
    path = Path(torch.__file__).parent/'lib'/Path(name).name
    assert hashlib.file_digest(path.open('rb'),'sha256').hexdigest() == record['sha256']
    assert path.resolve() in loaded, ('Provider not observed at expected installed path', str(path))
    print(str(path), record['sha256'])
assert not any('/tmp/brainles-cblas-build/' in str(path) for path in loaded)
print(torch.__config__.show())
PY
"$RUNTIME_PYTHON" -m pip check > "$R10_WORK/logs/pip-check.log" 2>&1
"$RUNTIME_PYTHON" -m pip list --format=json > "$EVIDENCE/packages-after.json"
"$RUNTIME_PYTHON" - <<'PY' > "$R10_WORK/logs/package-preservation.log" 2>&1
import json
import os
from pathlib import Path

root = Path(os.environ['EVIDENCE'])
def packages(name):
    return {r['name'].lower().replace('_','-'): r['version'] for r in json.loads((root/name).read_text()) if r['name'].lower() != 'torch'}
assert packages('packages-before.json') == packages('packages-after.json')
print('All runtime package versions except Torch unchanged')
PY
step complete
cp -a "$R10_WORK/logs/." "$EVIDENCE/"
rm -rf "$R10_WORK"
echo 'Build/import feasibility only. Original SIF fulltest and deployment remain unexecuted. DIAGNOSTIC_BLOCKED; merge_eligible=false.'
