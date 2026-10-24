# VS Code setup

`install.yaml` installs the recipe's declared `download` artifact with apt.
The recipe owns its editor version, architecture, URL, checksum and update policy.
`extensions.yaml` installs the five Python/Jupyter extensions into
`/opt/vscode-extensions`. Its optional `vscode_extra_extensions` input is a list
of extension IDs. System packages and editor installation must precede it.

The four consumers retain their own launchers. All select the shared extension
directory, create writable user data below `XDG_CONFIG_HOME` or `TMPDIR` with a
`/tmp` fallback, forward every argument, and use `exec` so exit status and signals
reach the editor process. They inherit the recipe's Python/Conda environment and
do not activate a different environment. Only the `code` recipe retains its
existing `--no-sandbox` flag; the other launchers use the editor's sandbox defaults.
Callers may override the default user data directory with `--user-data-dir`.
