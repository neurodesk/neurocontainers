#!/usr/bin/env python3
"""Migrate the local and image-wrapper recipe cohort to source plans.

The migration is intentionally declarative and idempotent. It refuses to replace
an unexpected base image or clone command so concurrent recipe edits are not lost.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
RECIPES = ROOT / "recipes"

BASES = {
    "ubuntu22": (
        "ubuntu:22.04",
        "ubuntu",
        "22.04",
        "sha256:2edbbc5dc405e9612ba3584ce95480277e3eb374407b5505fe26f17df77c7dbc",
    ),
    "ubuntu24": (
        "ubuntu:24.04",
        "ubuntu",
        "24.04",
        "sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517",
    ),
    "python312": (
        "python:3.12.0-slim",
        "python",
        "3.12.0-slim",
        "sha256:19a6235339a74eca01227b03629f63b6f5020abc21142436eced6ec3a9839a76",
    ),
    "neurodebian": (
        "neurodebian:bookworm-non-free",
        "neurodebian",
        "bookworm-non-free",
        "sha256:bc694492031b8ec54b86f338766ccbc0353f961669de65b1ad53857d6c275158",
    ),
    "buildkit": (
        "moby/buildkit:latest",
        "moby/buildkit",
        "latest",
        "sha256:6c2fa84a6b61ccd72899dde4239f8d5717f05f9a8ca6f3cad185fb1a95a94de3",
    ),
}

LOCAL = {
    "afib1": ("ubuntu22", True),
    "arfiproc": ("ubuntu22", False),
    "b0map": ("ubuntu22", True),
    "blochsiegertb1mapping": ("ubuntu22", False),
    "cbsb0stats": ("python312", True),
    "openreconexample": ("ubuntu22", True),
    "openreconi2iexample": ("ubuntu22", False),
    "sodiumgridding": ("ubuntu22", False),
    "sodiumgriddingptpi": ("ubuntu22", False),
    "sodiumnufft": ("ubuntu22", False),
    "template": ("neurodebian", False),
    "workshopdemo": ("ubuntu24", False),
    "condaenvs": ("ubuntu24", False),
    "builder": ("buildkit", False),
    "neurocommand": ("ubuntu24", False),
}

REMOTE_FILES = {
    "sodiumgridding": {
        "sodium_trajectory_n50": (
            "f656853f4bd2bd47e51314203df13791b3dff71cfd1e725fcc06973cb4b00e6e"
        ),
        "sodium_trajectory_n28": (
            "6281acf3ceced7c6251d90a45b9a6d8b50ff8135f12bf64dc7375a12bb79b8c6"
        ),
    },
    "sodiumgriddingptpi": {
        "sodiumgridding_ptpi_trajectory_n28_g50_p5": (
            "b1c133a3400bc057ae9f78c60160c1868e36d4d602b117cf84f4131501ad1624"
        ),
        "sodiumgridding_ptpi_trajectory_n28_g70_p5": (
            "15970ccac46c9e2edfda1be4e33ebd338bf8913b3d42513334b2bb6df57687a2"
        ),
        "sodiumgridding_ptpi_trajectory_n50_g70_p5": (
            "8b2420e35dbb220d6d73d59a1d41da76dc0273309e5c868eaae8549a871800d8"
        ),
    },
    "sodiumnufft": {
        "sodium_trajectory_n50": (
            "f656853f4bd2bd47e51314203df13791b3dff71cfd1e725fcc06973cb4b00e6e"
        ),
        "sodium_trajectory_n28": (
            "6281acf3ceced7c6251d90a45b9a6d8b50ff8135f12bf64dc7375a12bb79b8c6"
        ),
    },
    "neurocommand": {
        "cvmfs_release_latest_all_deb": (
            "88f4bb658c2c85e77aec39181f61dea8ab641b3481a0db2b00f453beabb05395"
        ),
    },
}

MUTABLE_IMAGES = {
    "dhcpstructuralpipeline": (
        "biomedia/dhcp-structural-pipeline:latest",
        "biomedia/dhcp-structural-pipeline",
        "latest",
        "sha256:318a11ba9d70fdca2fb96200afabe0109722c9072bc71b9644f65c6c10de57b3",
    ),
    "fetalsegmentation": (
        "fetalsvrtk/segmentation:general_auto_amd",
        "fetalsvrtk/segmentation",
        "general_auto_amd",
        "sha256:d0b3e19f7dd0b3d01fef6a41464487b5c127867646b5238c4eee79b02eecc268",
    ),
    "fetalsynthseg": (
        "vzalevskyi/fetalsynthseg:latest",
        "vzalevskyi/fetalsynthseg",
        "latest",
        "sha256:380b4ce81cb13f67ce8a09391e557f964eb6b1befbdbb405dd8e08dc474c620f",
    ),
    "linda": (
        "dorianps/linda",
        "dorianps/linda",
        "latest",
        "sha256:abe94d551bf6f7f2937f04a2bb5a1b3b7cd25a0f6e2cf684602ea9fa397c78a3",
    ),
    "openads": (
        "docker.io/sljhlab/openads:gpu",
        "sljhlab/openads",
        "gpu",
        "sha256:bd3508a0278538bbf10b7b0a090085b7b584860cef7f3c1fed6183707fc6223d",
    ),
    "svrtk": (
        "fetalsvrtk/svrtk:general_auto_amd",
        "fetalsvrtk/svrtk",
        "general_auto_amd",
        "sha256:d0ee22d6476277d172276ca33ee2a3f6334e1bf773c789d6012c8c23804460de",
    ),
}

TAGGED_IMAGES = {
    "bidsapphcppipelines": (
        "bids/hcppipelines:v4.3.0-3",
        "bids/hcppipelines",
        "v4.3.0-3",
        r"v(?P<version>\d+\.\d+\.\d+-\d+)",
        "dockerhub",
    ),
    "cpac": (
        "fcpindi/c-pac:release-v{{ context.version }}.post1.dev3",
        "fcpindi/c-pac",
        "release-v1.8.7.post1.dev3",
        r"release-v(?P<version>\d+\.\d+\.\d+\.post\d+\.dev\d+)",
        "dockerhub",
    ),
    "lesymap": (
        "dorianps/lesymap:20220701",
        "dorianps/lesymap",
        "20220701",
        r"(?P<version>\d{8})",
        "dockerhub",
    ),
    "nftsim": (
        "ghcr.io/farwa-abbas/nftsim:1.0.2",
        "https://ghcr.io/v2/farwa-abbas/nftsim/tags/list",
        "1.0.2",
        r"(?P<version>\d+\.\d+\.\d+)",
        "oci",
    ),
    "oshyx": (
        "jerync/oshyx_0.4:20220614",
        "jerync/oshyx_0.4",
        "20220614",
        r"(?P<version>\d{8})",
        "dockerhub",
    ),
}

SOFTWARE_VERSIONS = {
    "bidsapphcppipelines": (
        "4.3.0",
        '    expected_output_contains: "v4.3.0"',
        '    expected_output_contains: "v${software_version}"',
    ),
    "cpac": (
        "1.8.7",
        "    cpac version",
        "    cpac version\n  expected_output_contains: \"${software_version}\"",
    ),
    "lesymap": (
        "0.0.0.9221",
        '    expected_output_contains: "0.0.0.9221"',
        '    expected_output_contains: "${software_version}"',
    ),
    "linda": (
        "0.5.1",
        '    expected_output_contains: "0.5.1"',
        '    expected_output_contains: "${software_version}"',
    ),
    "oshyx": (
        "0.4",
        '    expected_output_contains: "OSHy-X v0.4"',
        '    expected_output_contains: "OSHy-X v${software_version}"',
    ),
}


def replace_top_level(text: str, key: str, replacement: str) -> str:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*(?:\n|$)", text)
    if match is None:
        raise ValueError(f"missing top-level {key}")
    next_key = re.search(r"(?m)^[A-Za-z_][A-Za-z0-9_-]*:\s*", text[match.end() :])
    end = match.end() + (next_key.start() if next_key else len(text[match.end() :]))
    return (
        text[: match.start()]
        + replacement.rstrip()
        + "\n\n"
        + text[end:].lstrip("\n")
    )


def ensure_variables(text: str, values: dict[str, str]) -> str:
    recipe = yaml.safe_load(text)
    current = recipe.get("variables") or {}
    missing = {key: value for key, value in values.items() if key not in current}
    if not missing:
        return text
    rendered = "".join(f"  {key}: {value}\n" for key, value in missing.items())
    if re.search(r"(?m)^variables:\s*$", text):
        return re.sub(
            r"(?m)^variables:\s*$",
            "variables:\n" + rendered.rstrip(),
            text,
            count=1,
        )
    anchor = re.search(r"(?m)^auto_update:\s*$", text)
    anchor = anchor or re.search(r"(?m)^build:\s*$", text)
    if anchor is None:
        raise ValueError("missing insertion point for variables")
    return (
        text[: anchor.start()]
        + "variables:\n"
        + rendered
        + "\n"
        + text[anchor.start() :]
    )


def replace_base(text: str, old: str, new: str) -> str:
    current = yaml.safe_load(text)["build"]["base-image"]
    if current == new:
        return text
    if current != old:
        raise ValueError(f"unexpected base image {current!r}; expected {old!r}")
    pattern = rf"(?m)^(\s*base-image:\s*){re.escape(old)}\s*$"
    updated, count = re.subn(pattern, rf"\g<1>{new}", text, count=1)
    if count != 1:
        raise ValueError(f"could not replace base image {old}")
    return updated


def ensure_file_sha(text: str, name: str, digest: str) -> str:
    recipe = yaml.safe_load(text)
    matches = [item for item in recipe.get("files", []) if item.get("name") == name]
    if len(matches) != 1:
        raise ValueError(f"expected one file named {name}")
    if matches[0].get("sha256") == digest:
        return text
    if "sha256" in matches[0]:
        raise ValueError(f"unexpected existing digest for {name}")
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if re.fullmatch(rf"\s*- name: {re.escape(name)}\s*\n", line):
            for target in range(index + 1, len(lines)):
                if re.match(r"\s*- name:", lines[target]):
                    break
                if re.match(r"\s+url:\s*", lines[target]):
                    indent = re.match(r"\s*", lines[target]).group()
                    lines.insert(target + 1, f"{indent}sha256: {digest}\n")
                    return "".join(lines)
    raise ValueError(f"could not insert digest for {name}")


def local_policy(
    base: tuple[str, str, str, str], fsl_bet2: bool, files: list[dict]
) -> str:
    _, image, tag, _ = base
    sources = [
        "    - id: base_image",
        "      method: oci_digest",
        f"      image: {image}",
        f'      tag: "{tag}"',
        "      target:",
        "        variable: base_image_digest",
    ]
    if fsl_bet2:
        sources += [
            "    - id: fsl_bet2",
            "      method: github_commit",
            "      repo: Bostrix/FSL-BET2",
            "      ref: HEAD",
            "      target:",
            "        variable: fsl_bet2_commit",
        ]
    for item in files:
        sources += [
            f"    - id: {item['id']}",
            "      method: http_digest",
            f"      url: {item['url']}",
            "      target:",
            f"        file: {item['name']}",
        ]
    prefix = "auto_update:\n  method: sources\n  local: []\n  sources:\n"
    return prefix + "\n".join(sources)


def migrate_local(name: str, base_key: str, fsl_bet2: bool) -> None:
    path = RECIPES / name / "build.yaml"
    text = path.read_text()
    base = BASES[base_key]
    variables = {"base_image_digest": base[3]}
    if fsl_bet2:
        variables["fsl_bet2_commit"] = "d5acd7fe09a34679aaac63f67f20409abf2ed3b9"
    text = ensure_variables(text, variables)
    text = replace_base(text, base[0], f"{base[0]}@{{{{ context.base_image_digest }}}}")
    checkout = "git -C FSL-BET2 checkout {{ context.fsl_bet2_commit }}"
    checkout_count = len(re.findall(r"(?m)^\s*- " + re.escape(checkout) + r"\s*$", text))
    if fsl_bet2 and checkout_count > 1:
        raise ValueError(f"{name}: duplicate FSL-BET2 checkout commands")
    if fsl_bet2 and checkout_count == 0:
        before = "              - git clone https://github.com/Bostrix/FSL-BET2\n"
        if text.count(before) != 1:
            raise ValueError(f"{name}: unexpected FSL-BET2 clone command")
        text = text.replace(
            before,
            before
            + "              - git -C FSL-BET2 checkout "
            + "{{ context.fsl_bet2_commit }}\n",
            1,
        )
    file_sources = []
    recipe = yaml.safe_load(text)
    by_name = {item["name"]: item for item in recipe.get("files", [])}
    for file_name, digest in REMOTE_FILES.get(name, {}).items():
        text = ensure_file_sha(text, file_name, digest)
        file_sources.append(
            {"id": file_name, "name": file_name, "url": by_name[file_name]["url"]}
        )
    policy = local_policy(base, fsl_bet2, file_sources)
    existing = recipe.get("auto_update") or {}
    if existing.get("method") == "sources":
        expected = yaml.safe_load(policy)["auto_update"]["sources"]
        actual = {source["id"]: source for source in existing.get("sources", [])}
        for source in expected:
            if actual.get(source["id"]) != source:
                raise ValueError(f"{name}: unexpected existing source {source['id']}")
    else:
        text = replace_top_level(text, "auto_update", policy)
    path.write_text(text)


def migrate_mutable(name: str, item: tuple[str, str, str, str]) -> None:
    old, image, tag, digest = item
    path = RECIPES / name / "build.yaml"
    text = ensure_variables(path.read_text(), {"base_image_digest": digest})
    display = old if ":" in old.rsplit("/", 1)[-1] else old + ":latest"
    text = replace_base(text, old, f"{display}@{{{{ context.base_image_digest }}}}")
    policy = f"""auto_update:
  method: sources
  sources:
    - id: base_image
      method: oci_digest
      image: {image}
      tag: {tag}
      target:
        variable: base_image_digest"""
    text = replace_top_level(text, "auto_update", policy)
    path.write_text(text)


def migrate_tagged(name: str, item: tuple[str, str, str, str, str]) -> None:
    old, source, tag, version_regex, method = item
    path = RECIPES / name / "build.yaml"
    text = ensure_variables(path.read_text(), {"base_image_tag": tag})
    registry, separator, repository = old.partition("/")
    if registry == "ghcr.io" and separator:
        image = f"{registry}/{repository.rsplit(':', 1)[0]}"
    else:
        image = old.split(":", 1)[0]
    text = replace_base(text, old, f"{image}:{{{{ context.base_image_tag }}}}")
    if method == "dockerhub":
        locator = f"      repo: {source}"
    else:
        locator = f"      url: {source}"
    prerelease = "\n      include_prereleases: true" if name == "cpac" else ""
    policy = f"""auto_update:
  method: sources
  sources:
    - id: base_image_tag
      method: {method}
{locator}
      version_regex: '{version_regex}'{prerelease}
      target:
        variable: base_image_tag
        value: tag"""
    text = replace_top_level(text, "auto_update", policy)
    path.write_text(text)


def add_missing_fulltests() -> None:
    suites = {
        "condaenvs": """name: condaenvs
version: 1.0.1

tests:
  - name: conda environment manager available
    description: Verify the environment manager installed by the shared launcher image.
    command: /opt/miniconda/bin/conda --version
    expected_output_contains: conda
""",
        "builder": """name: builder
version: "0.2"

tests:
  - name: sf-make launcher available
    description: Verify the packaged BuildKit and Apptainer build launcher is deployed.
    command: command -v sf-make
    expected_output_contains: sf-make
""",
        "neurocommand": """name: neurocommand
version: 1.0.0

tests:
  - name: apptainer available
    description: Verify the container launcher used by NeuroCommand is installed.
    command: apptainer --version
    expected_output_contains: apptainer version
  - name: cvmfs client available
    description: Verify the CVMFS client used for NeuroDesk modules is installed.
    command: command -v cvmfs_config
    expected_output_contains: cvmfs_config
""",
    }
    for name, contents in suites.items():
        path = RECIPES / name / "fulltest.yaml"
        if path.exists() and path.read_text() != contents:
            raise ValueError(f"refusing to overwrite existing {path}")
        path.write_text(contents)


def add_software_version_checks() -> None:
    for name, (version, before, after) in SOFTWARE_VERSIONS.items():
        build_path = RECIPES / name / "build.yaml"
        build_path.write_text(
            ensure_variables(build_path.read_text(), {"software_version": version})
        )
        suite_path = RECIPES / name / "fulltest.yaml"
        suite = suite_path.read_text()
        parsed = yaml.safe_load(suite)
        if "software_version" not in parsed:
            version_line = re.search(r"(?m)^version:.*$", suite)
            if version_line is None:
                raise ValueError(f"{name}: fulltest has no version")
            suite = (
                suite[: version_line.end()]
                + f"\nsoftware_version: {version}"
                + suite[version_line.end() :]
            )
        if after not in suite:
            if suite.count(before) != 1:
                raise ValueError(f"{name}: expected one software version assertion")
            suite = suite.replace(before, after, 1)
        suite_path.write_text(suite)


def main() -> None:
    for name, (base, fsl_bet2) in LOCAL.items():
        migrate_local(name, base, fsl_bet2)
    for name, item in MUTABLE_IMAGES.items():
        migrate_mutable(name, item)
    for name, item in TAGGED_IMAGES.items():
        migrate_tagged(name, item)
    add_missing_fulltests()
    add_software_version_checks()


if __name__ == "__main__":
    main()
