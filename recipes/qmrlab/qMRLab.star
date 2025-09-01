load("//lib/octave.star", "add_octave_packages", "build_octave_package")

qmr_lab_url = "https://github.com/qMRLab/qMRLab/archive/v2.4.2.tar.gz"

octave_forge = "https://downloads.sourceforge.net/project/octave/Octave%20Forge%20Packages/Individual%20Package%20Releases/"

pkg_image = build_octave_package(
    octave_forge + "image-2.14.0.tar.gz",
    "image-2.14.0",
)

pkg_io = build_octave_package(
    octave_forge + "io-2.6.0.tar.gz",
    "io-2.6.0",
)

pkg_struct = build_octave_package(
    octave_forge + "struct-1.0.18.tar.gz",
    "struct-1.0.18",
)

pkg_statistics = build_octave_package(
    "https://github.com/gnu-octave/statistics/archive/refs/tags/release-1.6.7.tar.gz",
    "statistics-1.6.7",
)

pkg_optim = build_octave_package(
    octave_forge + "optim-1.6.2.tar.gz",
    "optim-1.6.2",
    depends = [pkg_struct, pkg_statistics],
    additional_queries = [query("openblas-dev")],
)

vm_test = define.build_vm(
    directives = [
        define.plan(
            builder = "alpine@3.20",
            packages = [
                query("octave"),
                query("texinfo"),
            ],
            tags = ["level3", "defaults"],
        ),
    ] + add_octave_packages([
        pkg_image,
        pkg_io,
        pkg_struct,
        pkg_statistics,
        pkg_optim,
    ]) + [
        directive.run_command("interactive"),
    ],
)

qmr_lab = add_octave_packages([
    pkg_image,
    pkg_io,
    pkg_struct,
    pkg_statistics,
    pkg_optim,
]) + [
    define.read_archive(define.fetch_http(qmr_lab_url), ".tar.gz"),
]

octave_deps = [
    query("octave"),
    query("octave-dev"),
    query("build-base"),
    query("texinfo"),
    query("curl"),
    query("mesa-dri-gallium"),
    query("font-noto"),
    query("adwaita-icon-theme"),
    query("faenza-icon-theme"),
    query("bash"), # Needed for NeruoDesk
]

qmr_lab_root = define.build_fs([
    define.plan(
        builder = "alpine@3.20",
        packages = octave_deps,
        tags = ["level3", "defaults", "noScripts"],
    ),
] + qmr_lab + [
    directive.builtin("init", "/init"),
], "tar")

qmr_lab_test = define.build_vm(
    directives = [
        define.plan(
            builder = "alpine@3.20",
            packages = octave_deps,
            tags = ["level3", "defaults"],
        ),
    ] + qmr_lab + [directive.run_command("interactive")],
)
