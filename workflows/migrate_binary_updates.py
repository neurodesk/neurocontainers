#!/usr/bin/env python3
"""Apply the initial binary recipe conversions from verified artifact baselines.

Run with --cache pointing at the JSON sidecars produced by the artifact audit.
Already migrated recipes are left alone. No network requests are made here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
MIRROR = "https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/build/"
RUNTIME_SUBDIR = "{{ {'2018a': 'v94', '2019b': 'v97', '2020a': 'v98', '2020b': 'v99', '2021a': 'v910', '2021b': 'v911', '2022a': 'v912', '2022b': 'v913', '2023a': 'v914'}.get(context.matlab_version, 'R' + context.matlab_version) }}"


class Document:
    def __init__(self, text: str):
        self.text = text

    @property
    def data(self):
        return yaml.safe_load(self.text)

    def node(self, path):
        node = yaml.compose(self.text)
        for key in path:
            if isinstance(node, yaml.MappingNode):
                node = next(value for name, value in node.value if name.value == key)
            else:
                node = node.value[key]
        return node

    def set(self, path, value):
        try:
            node = self.node(path)
        except StopIteration:
            parent = self.node(path[:-1])
            rendered = yaml.safe_dump({path[-1]: value}, sort_keys=False, width=120)
            indent = parent.start_mark.column
            block = "".join(" " * indent + line + "\n" for line in rendered.rstrip().splitlines())
            at = parent.end_mark.index - parent.end_mark.column
            self.text = self.text[:at] + block + self.text[at:]
            return
        if isinstance(value, str):
            rendered = json.dumps(value, ensure_ascii=False)
        else:
            rendered = yaml.safe_dump(value, sort_keys=False, width=120).rstrip()
            if rendered.endswith("\n..."):
                rendered = rendered[:-4]
            rendered = rendered.replace("\n", "\n" + " " * node.start_mark.column)
        end = node.end_mark.index
        if isinstance(node, (yaml.MappingNode, yaml.SequenceNode)):
            rendered += "\n" + " " * node.end_mark.column
        self.text = self.text[:node.start_mark.index] + rendered + self.text[end:]

    def file(self, name, **values):
        files = self.data.get("files", [])
        for index, file in enumerate(files):
            if file["name"] == name:
                for key, value in values.items():
                    self.set(("files", index, key), value)
                return
        files.append({"name": name, **values})
        self.set(("files",), files)

    def variables(self, **values):
        existing = self.data.get("variables", {})
        self.set(("variables",), {**existing, **values})

    def insert_directive(self, index, value):
        sequence = self.node(("build", "directives"))
        node = sequence.value[index]
        at = self.text.rfind("\n", 0, node.end_mark.index) + 1
        indent = sequence.start_mark.column
        block = yaml.safe_dump([value], sort_keys=False, width=120)
        block = "".join(" " * indent + line + "\n" for line in block.rstrip().splitlines())
        self.text = self.text[:at] + block + self.text[at:]


def listing(name, url, pattern, file, variables=None, **options):
    target = {"file": file}
    if variables:
        target["variables"] = variables
    return {"id": name, "method": "artifact_listing", "url": url,
            "version_regex": pattern, "download_base": url.split("?")[0],
            **options, "target": target}


def mirror(prefix, pattern, file, variables=None, **options):
    source = listing("standalone", MIRROR.rstrip("/") + "?format=json&prefix=" + prefix,
                     pattern, file, variables, **options)
    source["download_base"] = MIRROR
    return source


def digest(file, url):
    return {"id": "binary", "method": "http_digest", "url": url, "target": {"file": file}}


def commit(name, repo, variable):
    return {"id": name, "method": "github_commit", "repo": repo,
            "target": {"variable": variable}}


def separate_version(doc, suite, recipe):
    version = str(doc.data["version"])
    doc.set(("variables",), {"upstream_version": version, **doc.data.get("variables", {})})
    if "readme" in doc.data:
        node = doc.node(("readme",))
        doc.text = (doc.text[:node.start_mark.index].replace("{{ context.version }}", "{{ context.upstream_version }}")
                    + doc.text[node.start_mark.index:node.end_mark.index]
                    + doc.text[node.end_mark.index:].replace("{{ context.version }}", "{{ context.upstream_version }}"))
    else:
        doc.text = doc.text.replace("{{ context.version }}", "{{ context.upstream_version }}")
    doc.text = doc.text.replace(f"/opt/{recipe}-{version}", f"/opt/{recipe}-{{{{ context.upstream_version }}}}")
    if suite:
        suite.set(("upstream_version",), version)
        suite.text = suite.text.replace("${version}", "${upstream_version}")
        suite.text = suite.text.replace(f"/opt/{recipe}-{version}", f"/opt/{recipe}-${{upstream_version}}")


def runtime(doc, suite, version, old_root, install_root="/opt/MCR"):
    doc.variables(matlab_version=version, matlab_runtime_subdir=RUNTIME_SUBDIR)
    doc.text = doc.text.replace(old_root, "/opt/matlab-runtime")
    directives = doc.data["build"]["directives"]
    for index, directive in enumerate(directives):
        if directive.get("template", {}).get("name") == "matlabmcr":
            doc.set(("build", "directives", index, "template", "version"), "{{ context.matlab_version }}")
            doc.set(("build", "directives", index, "template", "install_path"), install_root)
            doc.insert_directive(index, {"run": [
                f"ln -s {install_root}/{{{{ context.matlab_runtime_subdir }}}} /opt/matlab-runtime"
            ]})
            break
    if suite:
        suite.set(("matlab_version",), version)
        suite.text = suite.text.replace(old_root, "/opt/matlab-runtime")
        suite.text = suite.text.replace("R" + version, "R${matlab_version}")


def baseline(doc, cached, key, name=None):
    value = cached[key]
    file = name or key.split(":", 1)[1]
    doc.file(file, url=value["url"], sha256=value["sha256"])
    return file


def migrate(recipe, cached):
    path = ROOT / "recipes" / recipe / "build.yaml"
    doc = Document(path.read_text())
    if doc.data.get("auto_update", {}).get("method") == "sources":
        return None
    before = doc.text
    suite_path = path.with_name("fulltest.yaml")
    suite = Document(suite_path.read_text()) if suite_path.exists() else None
    separate_version(doc, suite, recipe)
    sources = []
    simple_mirrors = {
        "eeglab": ("eeglab", r"eeglab(?P<version>\d{4}\.\d+(?:\.\d+)?)_mcr(?P<runtime>\d{4}[ab])\.tar\.gz$", "eeglab2020_0_mcr2020a_tar_gz", "2020a", "/opt/MCR/v98"),
        "brainstorm": ("brainstorm", r"brainstorm(?P<version>\d+\.\d+)_mcr(?P<runtime>\d{4}[ab])\.tar\.gz$", "brainstorm3_211130_mcr2020a_tar_gz", "2020a", "/opt/MCR/v98"),
        "fieldtrip": ("fieldtrip", r"fieldtrip(?P<version>\d{8})_mcr(?P<runtime>\d{4}[ab])\.tar\.gz$", "fieldtrip_1", "2020b", "/opt/MCR/v99"),
    }
    if recipe in simple_mirrors:
        prefix, pattern, file, release, old_root = simple_mirrors[recipe]
        baseline(doc, cached, recipe + ":" + file)
        runtime(doc, suite, release, old_root)
        sources = [mirror(prefix, pattern, file, {"upstream_version": "version", "matlab_version": "runtime"})]
    elif recipe in {"trackvis", "terastitcher", "diffusiontoolkit"}:
        prefix, pattern, file = {
            "trackvis": ("TrackVis", r"TrackVis_v(?P<version>\d+(?:\.\d+)+)_x86_64\.tar\.gz$", "TrackVis_v"),
            "terastitcher": ("TeraStitcher", r"TeraStitcher-portable-(?P<version>\d+(?:\.\d+)+)-Linux\.tar\.gz$", "TeraStitcher_portable_1_11_10_Linux_tar_gz"),
            "diffusiontoolkit": ("Diffusion_Toolkit", r"Diffusion_Toolkit_v(?P<version>\d+(?:\.\d+)+)_x86_64\.tar\.gz$", "archive"),
        }[recipe]
        baseline(doc, cached, recipe + ":" + file)
        sources = [mirror(prefix, pattern, file, {"upstream_version": "version"})]
        if recipe == "diffusiontoolkit":
            doc.set(("build", "directives", 2, "run"), [
                'tar -xz -C /opt/{{ context.name }}-{{ context.upstream_version }}/ --strip-components 1 -f {{ get_file("archive") }}'
            ])
    elif recipe == "samsrfx":
        file = baseline(doc, cached, "samsrfx:samsrf_v10_004_zip")
        runtime(doc, suite, "2023b", "/opt/MCR-2023b/R2023b")
        sources = [mirror("samsrf", r"samsrf_(?P<version>v\d+(?:\.\d+)+)\.zip$", file, {"upstream_version": "version"})]
    elif recipe == "physio":
        file = baseline(doc, cached, "physio:spm12r8224_physioR2021a_standalone_MCRv99_MatlabR2020b_Linux_tar_gz")
        runtime(doc, suite, "2020b", "/opt/mcr/v99", "/opt/mcr")
        doc.variables(physio_release="2021a", spm_revision="8224")
        doc.text = doc.text.replace("SPM_REVISION: r2021a", 'SPM_REVISION: "r{{ context.spm_revision }}"')
        sources = [mirror("spm12", r"spm12r(?P<spm_revision>\d+)_physioR(?P<version>\d{4}[ab])_standalone_MCRv99_MatlabR(?P<runtime>2020b)_Linux\.tar\.gz$", file,
                          {"physio_release": "version", "spm_revision": "spm_revision", "matlab_version": "runtime"}, version_scheme="year_letter")]
    elif recipe == "mipav":
        file = doc.data["build"]["directives"][2].get("file")
        directives = doc.data["build"]["directives"]
        directives = [entry for entry in directives if "file" not in entry]
        doc.set(("build", "directives"), directives)
        baseline(doc, cached, "mipav:installer")
        doc.variables(artifact_version="11_3_3")
        sources = [mirror("mipav", r"mipav_unix_(?P<version>\d+_\d+_\d+)\.sh$", "installer", {"artifact_version": "version"})]
    elif recipe == "conn":
        file = baseline(doc, cached, "conn:conn22a_glnxa64_zip")
        runtime(doc, suite, "2022a", "/opt/MCR-2022a/v912")
        sources = [listing("standalone", "https://www.nitrc.org/frs/?group_id=279", r"conn(?P<version>\d+[a-z](?:\d+)?)_glnxa64\.zip$", file, {"upstream_version": "version"}, version_scheme="year_letter")]
    elif recipe == "spm12":
        file = baseline(doc, cached, "spm12:spm12_r7771_Linux_R2019b_zip")
        runtime(doc, suite, "2019b", "/opt/mcr/v97", "/opt/mcr")
        doc.variables(spm_revision="7771")
        doc.text = doc.text.replace("SPM_REVISION: r7771", 'SPM_REVISION: "r{{ context.spm_revision }}"')
        sources = [listing("standalone", "https://www.fil.ion.ucl.ac.uk/spm/download/restricted/bids/", r"spm12_r(?P<version>\d+)_Linux_R(?P<runtime>\d{4}[ab])\.zip$", file,
                           {"spm_revision": "version", "matlab_version": "runtime"})]
    elif recipe == "noddi":
        baseline(doc, cached, "noddi:standalone", "noddi_source")
        doc.text = doc.text.replace("NODDI_singularity-{{ context.noddi_source_revision }}", "NODDI_singularity-main")
        runtime(doc, suite, "2020b", "/usr/local/MATLAB/v99", "/usr/local/MATLAB")
        source = digest("noddi_source", cached["noddi:standalone"]["url"])
        source["matlab_readme"] = "NODDI_singularity-main/ss_noddi_App/readme.txt"
        source["target"]["variables"] = {"matlab_version": "runtime"}
        sources = [source]
    elif recipe in {"mfcsc", "startrack", "lcmodel"}:
        file = {"mfcsc": "mfcsc_binary", "startrack": "startrack_linux_zip", "lcmodel": "lcm_64_tar"}[recipe]
        baseline(doc, cached, recipe + ":" + file)
        sources = [digest(file, cached[recipe + ":" + file]["url"])]
        if recipe == "lcmodel":
            sources.append(commit("basis-sets", "mr-science-lab/mrs-basis-sets", "mrs_basis_sets_commit"))
    elif recipe == "gingerale":
        directives = doc.data["build"]["directives"]
        directives = [entry for entry in directives if "file" not in entry]
        doc.set(("build", "directives"), directives)
        baseline(doc, cached, "gingerale:gingerale.jar")
        sources = [digest("gingerale.jar", cached["gingerale:gingerale.jar"]["url"])]
    elif recipe == "ashs":
        baseline(doc, cached, "ashs:archive")
        doc.set(("build", "directives", 1, "run"), [
            'mkdir -p /opt/ashs-{{ context.upstream_version }}',
            'unzip -q {{ get_file("archive") }} -d /opt',
            'ASHS_DIR=$(find /opt -maxdepth 1 -mindepth 1 -type d -name "ashs-fastashs*" | head -n1)',
            'test -n "$ASHS_DIR"',
            'rmdir /opt/ashs-{{ context.upstream_version }}',
            'ln -s "$ASHS_DIR" /opt/ashs-{{ context.upstream_version }}',
            'ln -s /opt/ashs-{{ context.upstream_version }} /opt/ashs',
        ])
        sources = [listing("standalone", "https://www.nitrc.org/frs/?group_id=370", r"ashs-fastashs_(?P<version>\d+(?:\.\d+)+)_\d+\.zip$", "archive", {"upstream_version": "version"})]
    elif recipe == "mgltools":
        baseline(doc, cached, "mgltools:491")
        doc.file("491", url="https://ccsb.scripps.edu/download/491/")
        source = listing("standalone", "https://ccsb.scripps.edu/mgltools/downloads/", r"mgltools_Linux-x86_64_(?P<version>\d+(?:\.\d+)+)\.tar\.gz(?: Patch \d+)? \(Linux 64 tarball installer [^)]+\)", "491", {"upstream_version": "version"})
        source["download_base"] = "https://ccsb.scripps.edu/"
        sources = [source]
    elif recipe == "bcbtoolkit":
        baseline(doc, cached, "bcbtoolkit:bcbtoolkit_archive")
        doc.variables(wrapper_revision="70754272d545ca324122b2f15ebf96e6344b50bb")
        doc.text = doc.text.replace("BCBToolKit/master/", "BCBToolKit/{{ context.wrapper_revision }}/")
        sources = [digest("bcbtoolkit_archive", cached["bcbtoolkit:bcbtoolkit_archive"]["url"]),
                   commit("wrappers", "chrisfoulon/BCBToolKit", "wrapper_revision")]
        if suite is None:
            suite = Document("name: bcbtoolkit\nversion: 0.0.0\ntests: []\n")
            suite = Document(yaml.safe_dump({"name": "bcbtoolkit", "version": "0.0.0", "tests": [{"name": "BCB command line launchers", "command": "test -x /opt/BCBToolKit/jre/bin/java && test -r /opt/BCBToolKit/sources.jar && command -v run_disco.sh && command -v tractotron_cli.sh", "expected_exit_code": 0}]}, sort_keys=False))
    elif recipe == "brainnetviewer":
        baseline(doc, cached, "brainnetviewer:brainnetviewer_zip")
        directives = doc.data["build"]["directives"]
        directives[1] = {"template": {"name": "matlabmcr", "version": "2018a", "install_path": "/opt/matlab"}}
        directives[2] = {"run": ['unzip -q {{ get_file("brainnetviewer_zip") }} -d /']}
        doc.set(("build", "directives"), directives)
        doc.set(("files",), [f for f in doc.data["files"] if f["name"] != "mcr_installer"])
        runtime(doc, suite, "2018a", "/opt/matlab/v94", "/opt/matlab")
        doc.variables(artifact_date="20191031")
        sources = [listing("standalone", "https://www.nitrc.org/frs/?group_id=504", r"BrainNetViewer(?P<version>\d{8})_sd_Linux_x64_compiled(?P<runtime>\d{4}[ab])\.zip$", "brainnetviewer_zip", {"artifact_date": "version", "matlab_version": "runtime"})]
        wrapper = path.with_name("brainnetviewer")
        wrapper.write_text('#!/bin/bash\nexec /run_BrainNet.sh /opt/matlab-runtime "$@"\n')
    elif recipe == "spm12bi":
        baseline(doc, cached, "spm12bi:standalone")
        runtime(doc, suite, "2019b", "/opt/mcr/v97", "/opt/mcr")
        for i, directive in enumerate(doc.data["build"]["directives"]):
            if "run" in directive and any("wget --no-check" in str(command) for command in directive["run"]):
                doc.set(("build", "directives", i, "run"), ['unzip -q {{ get_file("standalone") }} -d /opt'])
        sources = [digest("standalone", cached["spm12bi:standalone"]["url"])]
    elif recipe == "tgvqsm":
        baseline(doc, cached, "tgvqsm:source")
        doc.variables(bet2_revision="adec315977486917d995ae2d1449f3efc616e2bc", dcm2niix_version="v1.0.20260724")
        doc.file("bet2", url="https://github.com/liangfu/bet2/archive/{{ context.bet2_revision }}.tar.gz")
        doc.file("miniconda", url="https://repo.anaconda.com/miniconda/Miniconda2-4.6.14-Linux-x86_64.sh")
        directives = doc.data["build"]["directives"]
        directives[1] = {"run": ['mkdir -p /bet2/build', 'tar -xzf {{ get_file("bet2") }} --strip-components 1 -C /bet2']}
        directives[4]["template"]["version"] = "{{ context.dcm2niix_version }}"
        directives[6] = {"run": ['cp {{ get_file("miniconda") }} /Miniconda2-4.6.14-Linux-x86_64.sh']}
        commands = directives[8]["run"]
        commands[-2:] = ['unzip -q {{ get_file("source") }} -d /tmp/tgvqsm', 'mv /tmp/tgvqsm/* /opt/tgvqsm-source', 'rmdir /tmp/tgvqsm']
        doc.set(("build", "directives"), directives)
        doc.text = doc.text.replace("/TGVQSM-master-011045626121baa8bfdd6633929974c732ae35e3", "/opt/tgvqsm-source").replace("/opt/dcm2niix-latest", "/opt/dcm2niix-{{ context.dcm2niix_version }}")
        sources = [digest("source", cached["tgvqsm:source"]["url"]), commit("bet2", "liangfu/bet2", "bet2_revision"), {"id": "dcm2niix", "method": "github_release", "repo": "rordenlab/dcm2niix", "target": {"variable": "dcm2niix_version", "value": "tag"}}]
    elif recipe == "convert3d":
        baseline(doc, cached, "convert3d:archive")
        doc.set(("build", "directives"), [
            {"install": ["ca-certificates", "curl"]},
            {"environment": {"C3DPATH": "/opt/convert3d-nightly", "PATH": "/opt/convert3d-nightly/bin:$PATH"}},
            {"run": ['mkdir -p /opt/convert3d-nightly', 'tar -xzf {{ get_file("archive") }} -C /opt/convert3d-nightly --strip-components 1']},
        ])
        sources = [digest("archive", cached["convert3d:archive"]["url"])]
    elif recipe == "minc":
        baseline(doc, cached, "minc:archive")
        doc.variables(volgenmodel_revision="9e2d8d9cb9e42925a612c9c81bd081c095879aec")
        doc.file("volgenmodel", url="https://github.com/CAIsr/volgenmodel-nipype/archive/{{ context.volgenmodel_revision }}.tar.gz")
        doc.file("beast", url="https://packages.bic.mni.mcgill.ca/tgz/beast-library-1.1.tar.gz")
        for suffix in ("a", "c"):
            doc.file("mni_09" + suffix, url="https://www.bic.mni.mcgill.ca/~vfonov/icbm/2009/mni_icbm152_nlin_sym_09" + suffix + "_minc2.zip")
        template = yaml.safe_load((ROOT / "builder/templates/minc.yaml").read_text())["binaries"]
        environment = template["directives"][0]["environment"]
        environment = {k: v.replace("{{ self.install_path }}", "/opt/minc-{{ context.upstream_version }}") for k, v in environment.items()}
        doc.set(("build", "directives"), [{"install": template["dependencies"]["apt"]}, {"environment": environment}, {"run": [
            'cd / && ar p {{ get_file("archive") }} data.tar.gz | tar -xz',
            'ln -s /opt/minc/{{ context.upstream_version }} /opt/minc-{{ context.upstream_version }}',
            'mkdir -p /opt/minc-{{ context.upstream_version }}/volgenmodel-nipype',
            'tar -xzf {{ get_file("volgenmodel") }} -C /opt/minc-{{ context.upstream_version }}/volgenmodel-nipype --strip-components 1',
            'tar -xzf {{ get_file("beast") }} -C /opt/minc-{{ context.upstream_version }}/share',
            'unzip -q {{ get_file("mni_09a") }} -d /opt/minc-{{ context.upstream_version }}/share/icbm152_model_09a',
            'unzip -q {{ get_file("mni_09c") }} -d /opt/minc-{{ context.upstream_version }}/share/icbm152_model_09c',
        ]}])
        sources = [listing("standalone", "https://packages.bic.mni.mcgill.ca/minc-toolkit/Debian/", r"minc-toolkit-(?P<version>\d+(?:\.\d+)+)-\d+-Ubuntu_18\.04-x86_64\.deb$", "archive", {"upstream_version": "version"}), commit("volgenmodel", "CAIsr/volgenmodel-nipype", "volgenmodel_revision")]
    else:
        return None
    if recipe in {"conn", "samsrfx", "startrack"}:
        member, variable = {
            "conn": ("readme.txt", "matlab_version"),
            "samsrfx": ("samsrf/readme.txt", "matlab_version"),
            "startrack": ("ST_Linux_x86/readme.txt", "matlab_runtime_version"),
        }[recipe]
        sources[0]["matlab_readme"] = member
        sources[0]["target"].setdefault("variables", {})[variable] = "runtime"
    doc.set(("auto_update",), {"method": "sources", "sources": sources})
    yaml.safe_load(doc.text)
    path.write_text(doc.text)
    if suite:
        yaml.safe_load(suite.text)
        suite_path.write_text(suite.text)
    return {"recipe": recipe, "before": hashlib.sha256(before.encode()).hexdigest(),
            "after": hashlib.sha256(doc.text.encode()).hexdigest(), "sources": sources}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=Path("/tmp/neuro-binary-migration.json"))
    args = parser.parse_args()
    cached = {}
    for path in args.cache.glob("*.json"):
        value = json.loads(path.read_text())
        if "sha256" in value:
            cached[value["name"]] = value
    recipes = "eeglab brainstorm fieldtrip trackvis terastitcher diffusiontoolkit samsrfx physio mipav conn spm12 noddi mfcsc startrack lcmodel gingerale ashs mgltools bcbtoolkit brainnetviewer spm12bi tgvqsm convert3d minc".split()
    changes = []
    for recipe in recipes:
        try:
            result = migrate(recipe, cached)
            if result:
                changes.append(result)
                print(recipe)
        except KeyError as error:
            print(f"{recipe}: awaiting verified baseline {error}")
    previous = json.loads(args.report.read_text()) if args.report.exists() else []
    args.report.write_text(json.dumps(previous + changes, indent=2) + "\n")


if __name__ == "__main__":
    main()
