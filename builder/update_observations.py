"""Resolve update sources into immutable, target-independent observations."""

from __future__ import annotations

import base64
import binascii
import gzip
import hashlib
import lzma
import re
import subprocess
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import quote, unquote, urljoin, urlsplit

import requests
from packaging.version import InvalidVersion, Version

from .image_fingerprint import (
    DEFAULT_REGISTRY,
    DOCKER_HUB_API,
    MANIFEST_ACCEPT,
    parse_image_reference,
)
from .update_bundles import BUNDLE_METHODS, observe_bundle, validate_bundle
from .update_sources import (
    METHOD_FIELDS,
    REPOSITORY,
    configure_read_retries,
    latest_version,
    validate_update_config,
)
from .update_records import observe_record, validate_record_source


@dataclass(frozen=True)
class SourceObservation:
    value: str
    url: str
    version: str | None = None
    tag: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


VERSION_METHODS = frozenset(METHOD_FIELDS) - {"manual"}
NEW_METHOD_FIELDS = {
    "github_commit": frozenset({"repo", "ref", "version_file"}),
    "git_commit": frozenset({"url", "ref"}),
    "oci_digest": frozenset({"image", "tag"}),
    "http_digest": frozenset({"url", "matlab_readme"}),
    "artifact_listing": frozenset(
        {
            "url",
            "version_regex",
            "download_base",
            "version_scheme",
            "listing_format",
            "rebase_root_relative_links",
            "matlab_readme",
        }
    ),
    "apt": frozenset({"package", "urls"}),
}
REQUIRED_FIELDS = {
    "github_commit": frozenset({"repo"}),
    "git_commit": frozenset({"url", "ref"}),
    "oci_digest": frozenset({"image", "tag"}),
    "http_digest": frozenset({"url"}),
    "artifact_listing": frozenset({"url", "version_regex", "download_base"}),
    "apt": frozenset({"package", "urls"}),
}
FULL_SHA = re.compile(r"[0-9a-fA-F]{40}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
OCI_TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]{0,127}")
APT_PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]*")
PLACEHOLDER = re.compile(r"\b(?:TODO|TBD|FIXME|CHANGEME|PLACEHOLDER)\b", re.I)


def _https_url(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a nonempty string")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{field_name} must be an HTTPS URL without credentials")
    return value


def _safe_ref(value: object, *, default: str | None = None) -> str:
    if value is None and default is not None:
        return default
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("ref must be a nonempty Git ref")
    if (
        len(value) > 255
        or value.startswith(("-", "/", "."))
        or value.endswith(("/", ".", ".lock"))
        or ".." in value
        or "//" in value
        or "@{" in value
        or any(character in value for character in " ~^:?*[\\\x00\n\r\t")
    ):
        raise ValueError("ref is not a safe exact Git ref")
    return value


def validate_source(config: dict) -> None:
    """Validate a source mapping before any external request is made."""
    if not isinstance(config, dict):
        raise ValueError("source must be a mapping")
    method = config.get("method")
    if method == "zenodo":
        validate_record_source(config)
        return
    if method in BUNDLE_METHODS:
        validate_bundle(config)
        return
    if method in VERSION_METHODS:
        validate_update_config(config)
        return
    if not isinstance(method, str) or method not in NEW_METHOD_FIELDS:
        raise ValueError(f"unsupported source method: {method!r}")
    unknown = set(config) - {"method"} - NEW_METHOD_FIELDS[method]
    if unknown:
        raise ValueError(f"unsupported {method} fields: {sorted(map(str, unknown))}")
    missing = REQUIRED_FIELDS[method] - set(config)
    if missing:
        raise ValueError(f"{method} requires fields: {sorted(missing)}")

    if method == "github_commit":
        repo = config.get("repo")
        if (
            not isinstance(repo, str)
            or not REPOSITORY.fullmatch(repo)
            or repo.endswith(".git")
            or PLACEHOLDER.search(repo)
        ):
            raise ValueError("github_commit.repo must be an owner/repository name")
        _safe_ref(config.get("ref"), default="HEAD")
        if "version_file" in config and (
            not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_./-]*", str(config["version_file"]))
            or ".." in config["version_file"].split("/")
        ):
            raise ValueError("version_file must be a repository-relative plain version file")
    elif method == "git_commit":
        url = _https_url(config.get("url"), "git_commit.url")
        if (urlsplit(url).hostname or "").lower() in {"github.com", "www.github.com"}:
            raise ValueError("GitHub repositories must use github_commit")
        _safe_ref(config.get("ref"))
    elif method == "oci_digest":
        image = config.get("image")
        if (
            not isinstance(image, str)
            or not image
            or image != image.strip()
            or "://" in image
            or "@" in image
        ):
            raise ValueError(
                "oci_digest.image must be an image repository without a tag"
            )
        parsed = parse_image_reference(image)
        if parsed.reference != "latest" or ":" in image.rsplit("/", 1)[-1]:
            raise ValueError("oci_digest.image must not contain a tag")
        if not OCI_TAG.fullmatch(str(config.get("tag", ""))):
            raise ValueError("oci_digest.tag is not a valid OCI tag")
    elif method == "http_digest":
        _https_url(config.get("url"), "http_digest.url")
    elif method == "artifact_listing":
        _https_url(config.get("url"), "artifact_listing.url")
        base = _https_url(config.get("download_base"), "artifact_listing.download_base")
        if not base.endswith("/"):
            raise ValueError("artifact_listing.download_base must end with /")
        if not isinstance(config.get("rebase_root_relative_links", False), bool):
            raise ValueError("artifact_listing.rebase_root_relative_links must be boolean")
        regex = config.get("version_regex")
        if not isinstance(regex, str) or not regex or PLACEHOLDER.search(regex):
            raise ValueError(
                "artifact_listing.version_regex must be a nonempty pattern"
            )
        try:
            pattern = re.compile(regex)
        except re.error as exc:
            raise ValueError("invalid artifact_listing.version_regex") from exc
        if "version" not in pattern.groupindex:
            raise ValueError(
                "artifact_listing.version_regex requires a named 'version' group"
            )
        if config.get("version_scheme", "numeric") not in {"numeric", "year_letter"}:
            raise ValueError(
                "artifact_listing.version_scheme must be numeric or year_letter"
            )
        if config.get("listing_format", "auto") not in {"auto", "girder"}:
            raise ValueError(
                "artifact_listing.listing_format must be auto or girder"
            )
    elif method == "apt":
        package = config.get("package")
        urls = config.get("urls")
        if not isinstance(package, str) or not APT_PACKAGE.fullmatch(package):
            raise ValueError("apt.package must be a Debian package name")
        if not isinstance(urls, list) or not urls:
            raise ValueError("apt.urls must be a nonempty list")
        for url in urls:
            _https_url(url, "apt.urls entry")
    if "matlab_readme" in config:
        member = config["matlab_readme"]
        if (
            not isinstance(member, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", member)
            or any(part in {".", ".."} for part in member.split("/"))
        ):
            raise ValueError("matlab_readme must be one exact relative ZIP member")


def _mapping(response: requests.Response, description: str) -> dict:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict):
        raise ValueError(f"{description} returned a non-object response")
    return value


def _github_commit(
    config: dict, session: requests.Session, current: str | None
) -> SourceObservation:
    repo = config["repo"]
    ref = config.get("ref", "HEAD")
    response = session.get(
        f"https://api.github.com/repos/{repo}/commits/{quote(ref, safe='')}",
        timeout=30,
    )
    payload = _mapping(response, "GitHub commit API")
    sha = payload.get("sha")
    if not isinstance(sha, str) or not FULL_SHA.fullmatch(sha):
        raise ValueError("GitHub commit API returned no full commit SHA")
    sha = sha.lower()
    metadata: dict[str, object] = {"ref": ref}
    if path := config.get("version_file"):
        result = session.get(
            f"https://api.github.com/repos/{repo}/contents/{path}",
            params={"ref": sha},
            timeout=30,
        )
        content = _mapping(result, "GitHub version file API")
        if content.get("encoding") != "base64":
            raise ValueError("GitHub version file is not base64 text")
        encoded = content.get("content")
        if not isinstance(encoded, str) or len(encoded) > 4096:
            raise ValueError("GitHub version file is not bounded base64 text")
        compact = "".join(encoded.split())
        try:
            decoded = base64.b64decode(compact, validate=True)
            value = decoded.decode("utf-8").strip()
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise ValueError("GitHub version file is not valid UTF-8 base64 text") from exc
        if len(decoded) > 256:
            raise ValueError("GitHub version file is too large")
        if not re.fullmatch(r"[0-9][A-Za-z0-9._+-]*", value):
            raise ValueError("GitHub version file must contain one plain version")
        metadata["version"] = value
    if current is not None:
        if not FULL_SHA.fullmatch(current):
            raise ValueError("current commit must be a full 40-character SHA")
        current = current.lower()
        if current == sha:
            metadata["ancestry"] = "identical"
            metadata["ahead_by"] = 0
        else:
            comparison = session.get(
                f"https://api.github.com/repos/{repo}/compare/{current}...{sha}",
                timeout=30,
            )
            compared = _mapping(comparison, "GitHub compare API")
            status = compared.get("status")
            if status not in {"ahead", "identical"}:
                raise ValueError(
                    f"observed commit {sha} is not a descendant of current {current}"
                )
            metadata["ancestry"] = status
            ahead_by = compared.get("ahead_by")
            if isinstance(ahead_by, int):
                metadata["ahead_by"] = ahead_by
    return SourceObservation(
        value=sha,
        url=f"https://github.com/{repo}/commit/{sha}",
        metadata=metadata,
    )


def _git_commit(config: dict) -> SourceObservation:
    url = config["url"]
    ref = config["ref"]
    try:
        result = subprocess.run(
            ["git", "ls-remote", "--exit-code", url, ref, f"{ref}^{{}}"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"git ls-remote failed for {url}: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit code {result.returncode}"
        raise ValueError(f"git ls-remote failed for {url}: {detail}")

    refs: list[tuple[str, str]] = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 2 or not FULL_SHA.fullmatch(fields[0]):
            raise ValueError("git ls-remote returned malformed output")
        refs.append((fields[0].lower(), fields[1]))
    peeled = [sha for sha, name in refs if name == f"{ref}^{{}}"]
    candidates = peeled or list(dict.fromkeys(sha for sha, _ in refs))
    if len(candidates) != 1:
        raise ValueError(f"git ref {ref!r} did not resolve to exactly one commit")
    return SourceObservation(
        value=candidates[0],
        url=f"{url}#{ref}",
        metadata={"ref": ref},
    )


def _header(headers: object, name: str) -> str:
    if not hasattr(headers, "items"):
        return ""
    for key, value in headers.items():
        if str(key).lower() == name.lower():
            return str(value)
    return ""


def _bearer_challenge(header: str) -> dict[str, str]:
    if not header.lower().startswith("bearer "):
        return {}
    return dict(re.findall(r'(\w+)="([^"]*)"', header))


def _oci_digest(config: dict, session: requests.Session) -> SourceObservation:
    image = config["image"]
    tag = config["tag"]
    parsed = parse_image_reference(image)
    api_host = (
        DOCKER_HUB_API if parsed.registry == DEFAULT_REGISTRY else parsed.registry
    )
    manifest_url = (
        f"https://{api_host}/v2/{parsed.repository}/manifests/"
        f"{quote(tag, safe='')}"
    )
    headers = {"Accept": MANIFEST_ACCEPT}
    response = session.get(manifest_url, headers=headers, timeout=30)
    if response.status_code == 401:
        challenge = _bearer_challenge(
            _header(response.headers, "WWW-Authenticate")
        )
        realm = _https_url(challenge.get("realm"), "OCI token realm")
        token_response = session.get(
            realm,
            params={
                key: challenge[key]
                for key in ("service", "scope")
                if challenge.get(key)
            },
            timeout=30,
        )
        token_payload = _mapping(token_response, "OCI token service")
        token = token_payload.get("token") or token_payload.get("access_token")
        if not isinstance(token, str) or not token:
            raise ValueError("OCI token service returned no token")
        headers = {**headers, "Authorization": f"Bearer {token}"}
        response = session.get(manifest_url, headers=headers, timeout=30)
    response.raise_for_status()
    _https_url(
        getattr(response, "url", manifest_url) or manifest_url,
        "OCI response URL",
    )
    supplied = _header(response.headers, "Docker-Content-Digest")
    if supplied:
        if not DIGEST.fullmatch(supplied):
            raise ValueError("registry returned an invalid Docker-Content-Digest")
        digest = supplied
    else:
        digest = "sha256:" + hashlib.sha256(response.content).hexdigest()
    return SourceObservation(
        value=digest,
        url=manifest_url,
        metadata={"image": image, "tag": tag},
    )


def _stream_sha256(
    session: requests.Session, url: str
) -> tuple[str, int, str]:
    from .update_http_cache import stream_sha256

    return stream_sha256(session, url)


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, "".join(self._text).strip()))
            self._href = None
            self._text = []


def _artifact_names(
    response: requests.Response, listing_format: str
) -> list[tuple[str, str]]:
    content_type = _header(response.headers, "Content-Type").lower()
    if listing_format == "girder":
        listing = response.json()
        if not isinstance(listing, list):
            raise ValueError("Girder artifact listing must be an array")
        names = []
        for entry in listing:
            name = entry.get("name") if isinstance(entry, dict) else None
            item_id = entry.get("_id") if isinstance(entry, dict) else None
            if (
                not isinstance(name, str)
                or not name
                or not isinstance(item_id, str)
                or not re.fullmatch(r"[0-9a-f]{24}", item_id)
            ):
                raise ValueError(
                    "Girder artifact entries require a name and 24-hex _id"
                )
            names.append((f"item/{item_id}/download", name))
        return names
    if "json" in content_type:
        listing = response.json()
        if not isinstance(listing, list):
            raise ValueError("artifact listing JSON must be an array")
        names: list[tuple[str, str]] = []
        for entry in listing:
            name = entry.get("name") if isinstance(entry, dict) else None
            if not isinstance(name, str) or not name:
                raise ValueError("artifact listing entries must contain a name")
            names.append((name, name))
        return names
    parser = _Links()
    try:
        parser.feed(response.content.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError("artifact listing is not UTF-8 HTML") from exc
    return parser.links


def _version_key(value: str, scheme: str) -> object:
    candidate = value.lstrip("vV").replace("_", ".")
    if scheme == "year_letter":
        match = re.fullmatch(r"(?:[rR])?(\d{2,4})([ab])", candidate)
        if not match:
            raise InvalidVersion(value)
        return int(match.group(1)), 0 if match.group(2) == "a" else 1
    parsed = Version(candidate)
    if parsed.is_prerelease or parsed.is_devrelease:
        raise InvalidVersion(value)
    return parsed


def _artifact_url(base: str, href: str, *, rebase_root_relative_links: bool = False) -> str:
    decoded = unquote(href)
    base_parts = urlsplit(base)
    if (
        rebase_root_relative_links
        and decoded.startswith("/")
        and not decoded.startswith("//")
        and not urlsplit(decoded).path.startswith(base_parts.path)
    ):
        decoded = decoded[1:]
    parsed_href = urlsplit(decoded)
    if parsed_href.scheme or parsed_href.netloc or decoded.startswith(("/", "../")):
        candidate = urljoin(base, decoded)
    else:
        candidate = urljoin(base, quote(decoded, safe="/._~+-%"))
    _https_url(candidate, "artifact download URL")
    parts = urlsplit(candidate)
    if parts.netloc != base_parts.netloc or not parts.path.startswith(base_parts.path):
        raise ValueError("artifact link escapes download_base")
    return candidate


def _artifact_listing(
    config: dict, session: requests.Session
) -> SourceObservation:
    listing_response = session.get(config["url"], timeout=30)
    listing_response.raise_for_status()
    _https_url(
        getattr(listing_response, "url", config["url"]) or config["url"],
        "artifact listing response URL",
    )
    pattern = re.compile(config["version_regex"])
    scheme = config.get("version_scheme", "numeric")
    candidates: list[tuple[object, str, str, dict[str, str], str]] = []
    for href, label in _artifact_names(
        listing_response, config.get("listing_format", "auto")
    ):
        matched_text = ""
        match = pattern.fullmatch(href)
        if match is not None:
            matched_text = href
        elif label:
            match = pattern.fullmatch(label)
            if match is not None:
                matched_text = label
        if match is None:
            continue
        groups = {
            key: value
            for key, value in match.groupdict().items()
            if value is not None
        }
        version = groups.get("version", "")
        try:
            key = _version_key(version, scheme)
        except InvalidVersion:
            continue
        candidates.append(
            (
                key,
                version,
                matched_text,
                groups,
                _artifact_url(
                    config["download_base"], href,
                    rebase_root_relative_links=config.get("rebase_root_relative_links", False),
                ),
            )
        )
    if not candidates:
        raise ValueError("artifact listing contained no matching stable version")
    best_key = max(candidate[0] for candidate in candidates)
    best = [candidate for candidate in candidates if candidate[0] == best_key]
    if len(best) != 1:
        raise ValueError(
            "artifact listing matched multiple files at the latest version"
        )
    _, version, tag, groups, artifact_url = best[0]
    runtime = None
    if member := config.get("matlab_readme"):
        from .update_bundles import read_matlab_artifact

        digest, size, _, runtime = read_matlab_artifact(
            session, artifact_url, member
        )
        final_url = artifact_url
    else:
        digest, size, final_url = _stream_sha256(session, artifact_url)
    if final_url != artifact_url:
        base = config["download_base"]
        base_parts = urlsplit(base)
        final_parts = urlsplit(final_url)
        if final_parts.netloc != base_parts.netloc or not final_parts.path.startswith(
            base_parts.path
        ):
            raise ValueError("artifact download redirected outside download_base")
    metadata: dict[str, object] = dict(groups)
    metadata.update(sha256=digest, size=size)
    if runtime is not None:
        metadata["runtime"] = runtime
    return SourceObservation(
        value=artifact_url,
        url=config["url"],
        version=version,
        tag=tag,
        metadata=metadata,
    )


def _package_versions(text: str, package: str) -> Iterable[str]:
    for paragraph in re.split(r"\n\s*\n", text):
        fields: dict[str, str] = {}
        for line in paragraph.splitlines():
            key, separator, value = line.partition(":")
            if separator and key and not line[:1].isspace():
                fields[key] = value.strip()
        if fields.get("Package") == package and fields.get("Version"):
            yield fields["Version"]


def _debian_newer(candidate: str, current: str) -> bool:
    result = subprocess.run(
        ["dpkg", "--compare-versions", candidate, "gt", current],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode not in {0, 1}:
        raise ValueError(
            f"dpkg could not compare versions {candidate!r} and {current!r}"
        )
    return result.returncode == 0


def debian_upstream_version(version: str) -> str:
    """Return the software's own version, without the epoch or Debian revision.

    A container labelled with the packaging revision would rename itself for a
    rebuild that ships identical software.
    """
    upstream = version.split(":", 1)[-1]
    return upstream.rsplit("-", 1)[0] if "-" in upstream else upstream


def _apt(config: dict, session: requests.Session) -> SourceObservation:
    package = config["package"]
    best: str | None = None
    best_url = ""
    for url in config["urls"]:
        response = session.get(url, timeout=60)
        response.raise_for_status()
        _https_url(getattr(response, "url", url) or url, "APT index response URL")
        try:
            if urlsplit(url).path.endswith(".gz"):
                data = gzip.decompress(response.content)
            elif urlsplit(url).path.endswith(".xz"):
                data = lzma.decompress(response.content)
            else:
                data = response.content
            text = data.decode("utf-8")
        except (OSError, EOFError, UnicodeDecodeError, lzma.LZMAError) as exc:
            raise ValueError(f"could not read APT index {url}: {exc}") from exc
        for candidate in _package_versions(text, package):
            if best is None or _debian_newer(candidate, best):
                best = candidate
                best_url = url
    if best is None:
        raise ValueError(f"package {package} was not found in configured APT indexes")
    return SourceObservation(
        value=best,
        url=best_url,
        version=debian_upstream_version(best),
        metadata={"package": package},
    )


def observe_source(
    config: dict,
    github_session: requests.Session,
    *,
    current: str | None = None,
) -> SourceObservation:
    """Resolve a validated source without leaking GitHub credentials."""
    validate_source(config)
    method = config["method"]
    if method == "zenodo":
        return observe_record(config)
    if method in BUNDLE_METHODS:
        return observe_bundle(config, github_session, current)
    if method in VERSION_METHODS:
        release = latest_version(config, github_session)
        if release is None:
            raise ValueError("no stable version found upstream")
        return SourceObservation(
            value=release.version,
            url=release.url,
            version=release.version,
            tag=release.tag,
        )
    if method == "github_commit":
        return _github_commit(config, github_session, current)
    if method == "git_commit":
        return _git_commit(config)

    with requests.Session() as public_session:
        configure_read_retries(public_session)
        if method == "oci_digest":
            return _oci_digest(config, public_session)
        if method == "http_digest":
            runtime = None
            if member := config.get("matlab_readme"):
                from .update_bundles import read_matlab_artifact

                digest, size, _, runtime = read_matlab_artifact(
                    public_session, config["url"], member
                )
                final_url = config["url"]
            else:
                digest, size, final_url = _stream_sha256(
                    public_session, config["url"]
                )
            metadata: dict[str, object] = {
                "sha256": digest,
                "size": size,
            }
            if runtime is not None:
                metadata["runtime"] = runtime
            return SourceObservation(
                value=digest,
                url=final_url,
                metadata=metadata,
            )
        if method == "artifact_listing":
            return _artifact_listing(config, public_session)
        return _apt(config, public_session)
