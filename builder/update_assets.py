"""Resolve declared downloads from the assets of a selected GitHub release."""

import json
import re
from urllib.parse import quote

import yaml


def rewrite_release_assets(text, config, repo, tag, new_version, session):
    patterns = config.get("assets", {})
    if not patterns:
        return text, []
    response = session.get(
        f"https://api.github.com/repos/{repo}/releases/tags/{quote(tag, safe='')}",
        timeout=30,
    )
    response.raise_for_status()
    assets = response.json()["assets"]
    document = yaml.compose(text)
    files_node = next(
        (value for key, value in document.value if key.value == "files"), None
    )
    if files_node is None:
        raise ValueError("auto_update.assets requires declared files")
    replacements = []
    changes = []
    for name, pattern in patterns.items():
        matches = [asset for asset in assets if re.fullmatch(pattern, asset["name"])]
        if len(matches) != 1:
            raise ValueError(
                f"release {tag}: expected one asset for {name}, found {len(matches)}"
            )
        asset = matches[0]
        url = asset["browser_download_url"]
        prefix = f"https://github.com/{repo}/releases/download/{quote(tag, safe='')}/"
        if not url.startswith(prefix):
            raise ValueError(f"unexpected download URL for {name}")
        # Keep recipe.version in the tag and, when present, the filename,
        # even when an upstream filename also contains a hash.
        tag_template = tag.replace(new_version, "{{ context.version }}")
        if "{{ context.version }}" not in tag_template:
            raise ValueError(f"tag {tag} does not contain recipe version {new_version}")
        filename = asset["name"].replace(new_version, "{{ context.version }}")
        template_url = (
            f"https://github.com/{repo}/releases/download/{tag_template}/{filename}"
        )
        declared = []
        for file_node in files_node.value:
            fields = {key.value: value for key, value in file_node.value}
            if fields.get("name") and fields["name"].value == name:
                declared.append(fields)
        if len(declared) != 1 or "url" not in declared[0]:
            raise ValueError(f"expected one declared URL file named {name}")
        fields = declared[0]
        if "sha256" in fields or "checksum" in fields:
            raise ValueError(
                f"{name} pins a checksum; update its checksum handling before using release assets"
            )
        node = fields["url"]
        replacements.append(
            (node.start_mark.index, node.end_mark.index, json.dumps(template_url))
        )
        changes.append(f"`{name}` download: `{asset['name']}`")
    for start, end, replacement in sorted(replacements, reverse=True):
        text = text[:start] + replacement + text[end:]
    return text, changes
