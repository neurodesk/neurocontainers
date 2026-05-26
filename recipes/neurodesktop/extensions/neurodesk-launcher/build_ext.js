/**
 * Build helper for the neurodesk-launcher JupyterLab extension.
 *
 * This script finds the JupyterLab staging directory via Python, then calls
 * build-labextension with the correct --core-path. This avoids shell-specific
 * quoting issues in package manager wrappers.
 */

'use strict';

const { execFileSync } = require('child_process');

function getStagingPath() {
  const pythonCmd = [
    'import importlib.util, pathlib',
    'spec = importlib.util.find_spec("jupyterlab")',
    'print(pathlib.Path(next(iter(spec.submodule_search_locations))) / "staging")'
  ].join('; ');
  const candidates = ['python3', 'python'];
  for (const py of candidates) {
    try {
      const result = execFileSync(py, ['-c', pythonCmd], { encoding: 'utf8' });
      const path = result.trim();
      if (path) {
        return path;
      }
    } catch (_) {
      // Try the next Python executable.
    }
  }
  return null;
}

const corePath = getStagingPath();
if (!corePath) {
  console.error('ERROR: Could not determine JupyterLab staging path. Is jupyterlab installed?');
  process.exit(1);
}

console.log('Building neurodesk-launcher with --core-path: ' + corePath);

let buildLabextensionPath;
try {
  buildLabextensionPath = require.resolve('@jupyterlab/builder/lib/build-labextension');
} catch (error) {
  console.error('ERROR: Could not resolve @jupyterlab/builder/lib/build-labextension:', error.message);
  process.exit(1);
}

try {
  execFileSync(
    'node',
    [buildLabextensionPath, '--core-path', corePath, '.'],
    { stdio: 'inherit', cwd: __dirname }
  );
} catch (error) {
  process.exit(error.status || 1);
}
