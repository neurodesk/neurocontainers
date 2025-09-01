bnv_binaries = "https://www.nitrc.org/frs/download.php/11585/BrainNetViewer20191031_sd_Linux_x64_compiled2018a.zip"
mcr_installer = "https://ssd.mathworks.com/supportfiles/downloads/R2018a/deployment_files/R2018a/installers/glnxa64/MCR_R2018a_glnxa64_installer.zip"

mcr_installed = define.build_vm(
    directives = [
        define.plan(
            builder = "ubuntu@jammy",
            packages = [
                query("openjdk-17-jdk"),
                query("bash"),
            ],
            tags = ["level3", "defaults"],
        ),
        directive.archive(define.read_archive(define.fetch_http(mcr_installer), ".zip"), target = "/mcr"),
        directive.run_command("/mcr/install -destinationFolder /opt/matlab -mode silent -agreeToLicense yes"),
        directive.run_command("tar caf /matlab.tar.gz /opt/matlab"),
    ],
    storage_size = 16 * 1024,
    output = "/matlab.tar.gz",
)

mcr_installed_archive = define.read_archive(mcr_installed, ".tar.gz")

bnv_root = define.build_fs([
    define.plan(
        builder = "ubuntu@jammy",
        packages = [
            query("openjdk-17-jdk"),
            query("bash"),
        ],
        tags = ["level3", "defaults", "noScripts"],
    ),
    mcr_installed_archive,
    directive.builtin("init", "/init"),
    define.read_archive(define.fetch_http(bnv_binaries), ".zip"),
], "tar")
