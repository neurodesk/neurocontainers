load("//lib/pypi.star", "build_run_fs", "get_wheel")

base = define.plan(
    builder = "ubuntu@jammy",
    packages = [
        query("python3-pip"),
        query("python3-packaging"),
    ],
    tags = ["level3", "defaults"],
)

top = get_wheel(base, "radtract", "==0.2.3")

run_fs = build_run_fs(top)

radtract_vm = define.build_vm(
    [
        base.add_packages([
            query("libglu1-mesa-dev"),
            query("libxrender1"),
        ]),
        run_fs,
        directive.run_command("/init -run-scripts /wheels/scripts.json"),
        directive.run_command("pip install --no-deps /wheels/numpy-1.25.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"),
        directive.run_command("login -f root"),
    ],
    storage_size = 4 * 1024,
)

radtract_root = define.build_fs([
    base.add_packages([
        query("libglu1-mesa-dev"),
        query("libxrender1"),
    ]).set_tags([
        "level3",
        "defaults",
        "noScripts",
    ]),
    run_fs,
    directive.builtin("init", "/init"),
], "tar")
