load("//lib/pypi.star", "build_fs_for_requirements")

requirements = "https://surfer.nmr.mgh.harvard.edu/docs/synthstrip/requirements/requirements.txt"

files = [
    "https://github.com/freesurfer/freesurfer/raw/v7.4.1/mri_synthstrip/mri_synthstrip",
    "https://surfer.nmr.mgh.harvard.edu/docs/synthstrip/requirements/synthstrip.1.pt",
    "https://surfer.nmr.mgh.harvard.edu/docs/synthstrip/requirements/synthstrip.nocsf.1.pt",
]

base = define.plan(
    builder = "ubuntu@jammy",
    packages = [
        query("python3-pip"),
        query("python3-packaging"),
    ],
    tags = ["level3", "defaults"],
)

requirements_fs = build_fs_for_requirements(base, define.fetch_http(requirements))

def synthstrip_fs(ctx, script, data_1, data_2):
    ret = filesystem()

    ret["usr/bin/mri_synthstrip"] = file(script, executable = True)

    ret["freesurfer/models/synthstrip.1.pt"] = data_1
    ret["freesurfer/models/synthstrip.nocsf.1.pt"] = data_2

    return ctx.archive(ret)

synthstrip_root = define.build_fs([
    base.set_tags([
        "level3",
        "defaults",
        "noScripts",
    ]),
    requirements_fs,
    define.build(synthstrip_fs, *[define.fetch_http(file) for file in files]),
    directive.builtin("init", "/init"),
], "tar")
