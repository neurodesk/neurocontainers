from builder.recipe import compile_recipe
from builder.tests.template_helpers import write_fragment_recipe as write_recipe


def test_builtin_cached_archive_extracts_without_network(tmp_path, monkeypatch):
    import io
    import subprocess
    import tarfile
    from builder.ir import RunWithMounts

    archive = tmp_path / "archive.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        info = tarfile.TarInfo("fixture/bin/example")
        payload = b"#!/bin/sh\nprintf 'fixture version\\n'\n"
        info.size = len(payload)
        info.mode = 0o755
        output.addfile(info, io.BytesIO(payload))
    install = tmp_path / "installed"
    recipe, _ = write_recipe(
        tmp_path,
        [
            {
                "template": {
                    "name": "example",
                    "version": "1.0.0",
                    "install_path": str(install),
                }
            }
        ],
    )
    monkeypatch.setattr(
        "builder.template_backend._load_template",
        lambda name: {
            "binaries": {
                "directives": [
                    {
                        "file": {
                            "name": "archive",
                            "url": "https://example.invalid/tool-{{ self.version }}.tar.gz",
                        }
                    },
                    {
                        "run": [
                            "mkdir -p {{ self.install_path }}",
                            "tar -xzf {{ get_file('archive') }} -C {{ self.install_path }} --strip-components 1",
                        ]
                    },
                ]
            }
        },
    )
    result = compile_recipe(recipe, architecture="x86_64")
    download = result.staging_plan.files["archive"]
    assert download.url == "https://example.invalid/tool-1.0.0.tar.gz"
    run = next(
        item
        for item in result.definition.directives
        if isinstance(item, RunWithMounts) and "tar -xzf" in item.command
    )
    cached = next(
        word
        for word in run.command.split()
        if word.startswith("/.neurocontainer-cache/")
    )
    # Exercise the cached extraction with downloads prohibited.
    command = run.command.replace(cached, str(archive))
    prefix = "curl() { return 97; }; "
    subprocess.run(["bash", "-ec", prefix + command], check=True)
    output = subprocess.check_output([str(install / "bin/example")], text=True)
    assert output == "fixture version\n"
