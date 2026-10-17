import json
import os
from pathlib import Path
import re
import shlex

root = Path(os.environ['R10_WORK'])
build = root / 'pytorch/build'
cache = {}
for line in (build / 'CMakeCache.txt').read_text().splitlines():
    match = re.match(r'([^:#/][^:]*):[^=]+=(.*)', line)
    if match:
        cache[match[1]] = match[2]
assert cache['DNNL_BLAS_VENDOR'] == os.environ['R10_VENDOR']
assert Path(cache['OpenBLAS_LIB']).samefile(root / 'provider/lib/libopenblas.so.0')
assert Path(cache['BLAS_openblas_LIBRARY']).samefile(root / 'provider/lib/libopenblas.so.0')
assert cache['CMAKE_BUILD_TYPE'] == 'Release'
for key in ['USE_MKLDNN', 'USE_MKLDNN_ACL', 'USE_OPENMP', 'USE_NNPACK', 'USE_GLOO']:
    assert cache[key].upper() in {'ON', '1', 'TRUE'}, (key, cache[key])
for key in ['USE_CUDA', 'USE_MPI', 'USE_NCCL', 'USE_XPU']:
    assert cache[key].upper() in {'OFF', '0', 'FALSE'}, (key, cache[key])
commands = json.loads((build / 'compile_commands.json').read_text())
gemm = [entry for entry in commands if entry['file'].endswith('/src/cpu/gemm/gemm.cpp')]
assert len(gemm) == 1
has_cblas = '-DUSE_CBLAS' in gemm[0]['command'].split()
assert has_cblas == (os.environ['R10_VENDOR'] == 'OPENBLAS')
if has_cblas:
    assert cache['CBLAS_WORKS'] == '1'
    arguments = shlex.split(gemm[0]['command'])
    include_paths: list[Path] = []
    for index, arg in enumerate(arguments):
        if arg in {'-I', '-isystem'}:
            include_paths.append(Path(arguments[index + 1]))
        elif arg.startswith('-I'):
            include_paths.append(Path(arg[2:]))
        elif arg.startswith('-isystem'):
            include_paths.append(Path(arg[len('-isystem'):]))
    assert any(path.exists() and path.samefile(root / 'provider/include')
               for path in include_paths)
(root / 'logs/gemm-compile-command.json').write_text(json.dumps(gemm, indent=2) + '\n')
print('Generated oneDNN GEMM compile contract passed', os.environ['R10_VENDOR'])
