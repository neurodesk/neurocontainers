"""Resolve published applications whose scripts, models, or extensions move together."""

from __future__ import annotations

import hashlib
import re
import tempfile
import zipfile
from urllib.parse import quote

import requests
from packaging.version import InvalidVersion, Version

from .update_sources import configure_read_retries, latest_version, validate_update_config


GIRDER = "https://slicer-packages.kitware.com/api/v1"
ANNEX = "https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/repo/annex.git/annex/objects"
OBJECT_ID = re.compile(r"[0-9a-f]{24}")
MODEL_KEY = re.compile(r"SHA256E-s(?P<size>\d+)--(?P<sha256>[0-9a-f]{64})(?:\.[A-Za-z0-9.]+)?")
REPO_PATH = re.compile(r"[A-Za-z0-9_./-]+")
BUNDLE_METHODS = frozenset({
    "slicer_release", "freesurfer_release", "github_release_asset", "libreoffice_release",
})
LIBREOFFICE_DOWNLOAD = "https://download.documentfoundation.org/libreoffice/"
LIBREOFFICE_ARCHIVE = "https://downloadarchive.documentfoundation.org/libreoffice/old/"


def validate_bundle(config: dict) -> None:
    method = config.get("method")
    if method == "libreoffice_release":
        allowed = {"method"}
    elif method == "slicer_release":
        allowed = {"method", "app_id", "extension"}
        if not OBJECT_ID.fullmatch(str(config.get("app_id", ""))):
            raise ValueError("slicer_release.app_id must be a Girder object ID")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", str(config.get("extension", ""))):
            raise ValueError("slicer_release.extension must be an extension base name")
    elif method == "freesurfer_release":
        allowed = {"method", "script", "models"}
        _repo_path(config.get("script"))
        models = config.get("models")
        if not isinstance(models, dict) or not models:
            raise ValueError("freesurfer_release.models must map metadata prefixes to model paths")
        for prefix, path in models.items():
            if not isinstance(prefix, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", prefix):
                raise ValueError("model metadata prefixes must be lowercase identifiers")
            _repo_path(path)
    elif method == "github_release_asset":
        allowed = {"method", "repo", "asset", "version_regex", "version_scheme", "include_prereleases"}
        asset = config.get("asset")
        if (
            not isinstance(asset, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", asset)
            or asset in {"TODO", "TBD", "PLACEHOLDER"}
        ):
            raise ValueError("github_release_asset.asset must be an exact asset filename")
        validate_update_config({
            **{key: value for key, value in config.items() if key != "asset"},
            "method": "github_release",
        })
    else:
        raise ValueError(f"unsupported bundle method: {method!r}")
    if set(config) - allowed:
        raise ValueError(f"unsupported {method} fields: {sorted(set(config) - allowed)}")


def _repo_path(value: object) -> str:
    if (not isinstance(value, str) or not REPO_PATH.fullmatch(value)
            or value.startswith("/") or any(p in {"", ".", ".."} for p in value.split("/"))):
        raise ValueError("bundle paths must be relative repository file paths")
    return value


def _json_list(session: requests.Session, url: str, **params) -> list[dict]:
    response = session.get(url, params=params, timeout=60)
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("bundle API returned a malformed item list")
    return value


def _one(items: list[dict], description: str) -> dict:
    if len(items) != 1:
        raise ValueError(f"expected one {description}, found {len(items)}")
    item = items[0]
    if not OBJECT_ID.fullmatch(str(item.get("_id", ""))):
        raise ValueError(f"{description} has no valid item ID")
    return item


def _download(session: requests.Session, url: str, *, expected_sha256: str | None = None,
              expected_sha512: str | None = None, expected_size: int | None = None) -> tuple[str, int]:
    from .update_observations import _https_url

    with session.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        _https_url(response.url, "bundle download response URL")
        sha256, sha512 = hashlib.sha256(), hashlib.sha512()
        size = 0
        for chunk in response.iter_content(1024 * 1024):
            if chunk:
                sha256.update(chunk)
                sha512.update(chunk)
                size += len(chunk)
    digest = sha256.hexdigest()
    if expected_size is not None and size != expected_size:
        raise ValueError("bundle download size disagrees with published metadata")
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError("bundle download SHA256 disagrees with published digest")
    if expected_sha512 is not None and sha512.hexdigest() != expected_sha512:
        raise ValueError("bundle download SHA512 disagrees with published metadata")
    return digest, size


def _slicer(config: dict, session: requests.Session):
    from .update_observations import SourceObservation

    app_url = f"{GIRDER}/app/{config['app_id']}"
    releases = _json_list(session, app_url + "/release", limit=0)
    versions = []
    for release in releases:
        name = release.get("name")
        if not isinstance(name, str):
            continue
        try:
            version = Version(name)
        except InvalidVersion:
            continue
        if not version.is_prerelease and not version.is_devrelease:
            versions.append((version, name))
    if not versions:
        raise ValueError("Slicer has no published stable release")
    _, version = max(versions)
    package = _one(_json_list(session, app_url + "/package", os="linux", arch="amd64",
                             release_id_or_name=version, limit=0), "Slicer Linux package")
    meta = package.get("meta", {})
    revision = str(meta.get("revision", ""))
    if (meta.get("app_id") != config["app_id"] or meta.get("version") != version or meta.get("pre_release") is not False
            or meta.get("os") != "linux" or meta.get("arch") != "amd64" or not revision.isdecimal()):
        raise ValueError("Slicer package release or architecture metadata disagrees")
    extension = _one(_json_list(session, app_url + "/extension", os="linux", arch="amd64",
                               app_revision=revision, baseName=config["extension"], limit=0),
                     f"{config['extension']} extension for Slicer revision {revision}")
    ext_meta = extension.get("meta", {})
    if (ext_meta.get("app_id") != config["app_id"] or str(ext_meta.get("app_revision")) != revision or ext_meta.get("os") != "linux"
            or ext_meta.get("arch") != "amd64" or ext_meta.get("baseName") != config["extension"]):
        raise ValueError("Slicer extension ABI metadata disagrees with its application")
    main_url = f"https://download.slicer.org/download?os=linux&stability=release&version={version}"
    extension_url = f"{GIRDER}/item/{extension['_id']}/download"
    main_sha, size = _download(session, main_url, expected_sha512=meta.get("sha512"),
                               expected_size=package.get("size"))
    extension_sha, _ = _download(session, extension_url, expected_sha512=ext_meta.get("sha512"),
                                 expected_size=extension.get("size"))
    return SourceObservation(value=main_url, url=app_url, version=version, tag=version,
                             metadata={"version": version, "revision": revision,
                                       "abi": ".".join(version.split(".")[:2]),
                                       "extension_item": extension["_id"],
                                       "extension_sha256": extension_sha,
                                       "sha256": main_sha, "size": size})


def _model(session: requests.Session, raw_url: str) -> tuple[str, str]:
    # Raw GitHub responses contain either the actual model or an annex symlink.
    with session.get(raw_url, stream=True, timeout=60) as response:
        response.raise_for_status()
        first = next(response.iter_content(1024), b"")
    if not first.startswith(b"../.git/annex/objects/"):
        digest, _ = _download(session, raw_url)
        return raw_url, digest
    pointer = first.decode("ascii").strip()
    parts = pointer.split("/")
    key = parts[-1]
    match = MODEL_KEY.fullmatch(key)
    if match is None or len(parts) != 8 or parts[-2] != key:
        raise ValueError("FreeSurfer returned an invalid SHA256 git-annex pointer")
    directory_hash = hashlib.md5(key.encode(), usedforsecurity=False).hexdigest()
    url = f"{ANNEX}/{directory_hash[:3]}/{directory_hash[3:6]}/{key}/{key}"
    digest, _ = _download(session, url, expected_sha256=match['sha256'], expected_size=int(match['size']))
    return url, digest


def _freesurfer(config: dict, github_session: requests.Session, session: requests.Session):
    from .update_observations import SourceObservation

    release = latest_version({"method": "github_tags", "repo": "freesurfer/freesurfer",
                              "version_regex": r"v(?P<version>\d+(?:\.\d+)+)"}, github_session)
    if release is None:
        raise ValueError("FreeSurfer has no stable version tag")
    raw_base = f"https://raw.githubusercontent.com/freesurfer/freesurfer/{quote(release.tag, safe='')}"
    script_url = f"{raw_base}/{config['script']}"
    digest, size = _download(session, script_url)
    metadata = {"version": release.version, "sha256": digest, "size": size}
    for prefix, path in config["models"].items():
        url, model_sha = _model(session, f"{raw_base}/{path}")
        metadata[prefix + "_url"] = url
        metadata[prefix + "_sha256"] = model_sha
    return SourceObservation(value=script_url, url=release.url, version=release.version,
                             tag=release.tag, metadata=metadata)


def _github_release_asset(config: dict, github_session: requests.Session, session: requests.Session):
    from .update_observations import SourceObservation

    release = latest_version({
        **{key: value for key, value in config.items() if key != "asset"},
        "method": "github_release",
    }, github_session)
    if release is None:
        raise ValueError("GitHub repository has no suitable release")
    repo, tag = config["repo"], quote(release.tag, safe="")
    response = github_session.get(
        f"https://api.github.com/repos/{repo}/releases/tags/{tag}", timeout=30
    )
    response.raise_for_status()
    published = response.json()
    if (
        not isinstance(published, dict)
        or published.get("tag_name") != release.tag
        or published.get("draft")
        or (published.get("prerelease") and not config.get("include_prereleases", False))
    ):
        raise ValueError("GitHub selected release changed before asset resolution")
    assets = published.get("assets")
    if not isinstance(assets, list) or not all(isinstance(asset, dict) for asset in assets):
        raise ValueError("GitHub release assets must be an array of objects")
    matches = [asset for asset in assets if asset.get("name") == config["asset"]]
    if len(matches) != 1:
        raise ValueError(f"expected one release asset named {config['asset']}, found {len(matches)}")
    asset = matches[0]
    size, published_digest = asset.get("size"), asset.get("digest")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0 or asset.get("state") != "uploaded":
        raise ValueError("GitHub asset must be uploaded with a valid size")
    if published_digest is not None and (
        not isinstance(published_digest, str)
        or not re.fullmatch(r"sha256:[a-f0-9]{64}", published_digest)
    ):
        raise ValueError("GitHub asset has an invalid published SHA256 digest")
    url = f"https://github.com/{repo}/releases/download/{tag}/{quote(config['asset'], safe='')}"
    digest, size = _download(
        session, url, expected_size=size,
        expected_sha256=published_digest.removeprefix("sha256:") if published_digest else None,
    )
    return SourceObservation(value=url, url=release.url, version=release.version,
                             tag=release.tag, metadata={"sha256": digest, "size": size})


def _libreoffice(session: requests.Session):
    from .update_observations import SourceObservation, _https_url

    stable_url = LIBREOFFICE_DOWNLOAD + "stable/"
    release = latest_version({
        "method": "webpage",
        "url": stable_url,
        "version_regex": r'href="(?P<version>\d+\.\d+\.\d+)/"',
    }, session)
    if release is None:
        raise ValueError("LibreOffice supplied no stable release")

    source_url = LIBREOFFICE_DOWNLOAD + f"src/{release.version}/"
    response = session.get(source_url, timeout=30)
    response.raise_for_status()
    _https_url(response.url, "LibreOffice source listing response URL")
    pattern = (r"href=[\"']libreoffice-(" + re.escape(release.version)
               + r"\.\d+)\.tar\.xz[\"']")
    builds = set(re.findall(pattern, response.text))
    if len(builds) != 1:
        raise ValueError("LibreOffice stable source listing must identify one released build")
    build = builds.pop()

    def checksum(url: str) -> str:
        result = session.get(url + ".sha256", timeout=30)
        result.raise_for_status()
        _https_url(result.url, "LibreOffice checksum response URL")
        filename = url.rsplit("/", 1)[1]
        match = re.fullmatch(
            r"([a-fA-F0-9]{64})[ \t]+\*?" + re.escape(filename) + r"\s*",
            result.text,
        )
        if match is None:
            raise ValueError(f"LibreOffice supplied an invalid SHA256 for {filename}")
        return match[1].lower()

    metadata = {}
    for arch, filename_arch in (("x86_64", "x86-64"), ("aarch64", "aarch64")):
        stable = (stable_url + f"{release.version}/deb/{arch}/"
                  + f"LibreOffice_{release.version}_Linux_{filename_arch}_deb.tar.gz")
        archived = (LIBREOFFICE_ARCHIVE + f"{build}/deb/{arch}/"
                    + f"LibreOffice_{build}_Linux_{filename_arch}_deb.tar.gz")
        digest = checksum(stable)
        if checksum(archived) != digest:
            raise ValueError(f"LibreOffice {arch} archive differs from the stable release")
        metadata[f"{arch}_sha256"] = digest
    return SourceObservation(
        value=build, version=build, url=LIBREOFFICE_ARCHIVE + build + "/",
        metadata=metadata,
    )


def observe_bundle(config: dict, github_session: requests.Session, current: str | None = None):
    """Resolve every coupled input before returning one immutable observation."""
    validate_bundle(config)
    with requests.Session() as session:
        configure_read_retries(session)
        if config["method"] == "libreoffice_release":
            return _libreoffice(session)
        if config["method"] == "slicer_release":
            return _slicer(config, session)
        if config["method"] == "github_release_asset":
            return _github_release_asset(config, github_session, session)
        return _freesurfer(config, github_session, session)


def read_matlab_artifact(session: requests.Session, url: str, member: str) -> tuple[str, int, str, str]:
    """Read the compiler-generated runtime requirement from one exact ZIP member."""
    from .update_observations import _https_url

    digest = hashlib.sha256()
    size = 0
    with tempfile.TemporaryFile() as archive:
        with session.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            final_url = _https_url(response.url, "MATLAB artifact response URL")
            for chunk in response.iter_content(1024 * 1024):
                if chunk:
                    archive.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        archive.seek(0)
        with zipfile.ZipFile(archive) as package:
            entries = [entry for entry in package.infolist() if entry.filename == member]
            if len(entries) != 1 or entries[0].file_size > 65536:
                raise ValueError("MATLAB artifact requires one bounded runtime readme member")
            text = package.read(entries[0]).decode("utf-8")
        runtimes = set(re.findall(r"\bR(20\d{2}[ab])\b", text))
        if len(runtimes) != 1:
            raise ValueError("MATLAB artifact readme does not identify one runtime release")
    return digest.hexdigest(), size, final_url, runtimes.pop()
