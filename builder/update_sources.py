"""Discover stable upstream versions from explicit recipe update sources."""

import re
from dataclasses import dataclass
from typing import Iterable, Iterator
from urllib.parse import quote, urljoin, urlsplit

import requests
from packaging.version import InvalidVersion, Version
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class UpstreamRelease:
    version: str
    tag: str
    url: str


SOURCE_FIELDS = {
    "version_regex",
    "version_scheme",
    "include_prereleases",
    "mode",
    "reason",
}
METHOD_FIELDS = {
    "github_release": {"repo", "assets", "tag_variable"} | SOURCE_FIELDS,
    "github_tags": {"repo", "tag_variable"} | SOURCE_FIELDS,
    "pypi": {"package"} | SOURCE_FIELDS,
    "npm": {"package"} | SOURCE_FIELDS,
    "dockerhub": {"repo"} | SOURCE_FIELDS,
    "oci": {"url"} | SOURCE_FIELDS,
    "webpage": {"url"} | SOURCE_FIELDS,
    "manual": {"reason", "url"},
}
PLACEHOLDER = re.compile(r"\b(?:TODO|TBD|FIXME|CHANGEME|PLACEHOLDER)\b", re.I)
SAFE_VERSION = re.compile(r"[0-9][A-Za-z0-9._+\-]*")
SAFE_SOURCE_REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+/-]*")
YEAR_LETTER_VERSION = re.compile(r"[0-9]{2,4}[ab]")
REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*")
PACKAGE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")
NPM_PACKAGE = re.compile(r"(?:@[a-z0-9][a-z0-9._-]*/)?[a-z0-9][a-z0-9._-]*")


def configure_read_retries(session: requests.Session) -> None:
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=0.5,
                allowed_methods=frozenset({"GET", "HEAD"}),
                status_forcelist=(500, 502, 503, 504),
            )
        ),
    )


def validate_update_config(config: dict) -> None:
    """Reject missing sources, typos, and unexplained manual update policies."""
    if not isinstance(config, dict):
        raise ValueError("auto_update must be a mapping")
    method = config.get("method")
    if method == "sources":
        from .update_plan import validate_sources_config

        validate_sources_config(config)
        return
    if not isinstance(method, str) or method not in METHOD_FIELDS:
        raise ValueError(f"unsupported auto_update method: {method!r}")
    unknown = set(config) - {"method"} - METHOD_FIELDS[method]
    if unknown:
        raise ValueError(f"unsupported auto_update fields: {sorted(map(str, unknown))}")
    for key, value in config.items():
        if key == "assets":
            if not isinstance(value, dict) or not value:
                raise ValueError(
                    "auto_update.assets must map declared file names to asset regexes"
                )
            for name, pattern in value.items():
                if not isinstance(name, str) or not isinstance(pattern, str):
                    raise ValueError(
                        "auto_update.assets names and patterns must be strings"
                    )
                if (
                    not name.strip()
                    or not pattern.strip()
                    or PLACEHOLDER.search(name)
                    or PLACEHOLDER.search(pattern)
                ):
                    raise ValueError(
                        "auto_update.assets requires nonempty names and patterns without placeholders"
                    )
                try:
                    re.compile(pattern)
                except re.error as exc:
                    raise ValueError(f"invalid asset pattern for {name}") from exc
            continue
        if key == "include_prereleases":
            if not isinstance(value, bool):
                raise ValueError("auto_update.include_prereleases must be a boolean")
            continue
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"auto_update.{key} must be a nonempty string")
        if value != value.strip() or PLACEHOLDER.search(value):
            raise ValueError(
                f"auto_update.{key} contains a placeholder or surrounding whitespace"
            )
    if method in {"github_release", "github_tags", "dockerhub"}:
        repo = config.get("repo", "")
        if not REPOSITORY.fullmatch(repo) or repo.endswith(".git"):
            raise ValueError("auto_update.repo must be an owner/repository name")
    if method == "pypi" and not PACKAGE.fullmatch(config.get("package", "")):
        raise ValueError("auto_update.package must be a PyPI project name")
    if method == "npm" and not NPM_PACKAGE.fullmatch(config.get("package", "")):
        raise ValueError("auto_update.package must be an npm package name")
    if "tag_variable" in config and (
        not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", config["tag_variable"])
        or config["tag_variable"] in {"version", "original_version"}
    ):
        raise ValueError("auto_update.tag_variable must name a recipe variable")
    mode = config.get("mode", "automatic")
    if mode not in {"automatic", "notify"}:
        raise ValueError("auto_update.mode must be automatic or notify")
    if config.get("version_scheme", "numeric") not in {"numeric", "year_letter"}:
        raise ValueError("auto_update.version_scheme must be numeric or year_letter")
    if method == "manual" or mode == "notify" or "reason" in config:
        if len(config.get("reason", "").strip()) < 20:
            raise ValueError(
                "auto_update.reason must explain the update constraint "
                "in at least 20 characters"
            )
    if method == "webpage" and not {"url", "version_regex"}.issubset(config):
        raise ValueError("webpage updates require url and version_regex")
    if method == "oci" and not re.fullmatch(
        r"/v2/[a-z0-9][a-z0-9._/-]*/tags/list", urlsplit(config.get("url", "")).path
    ):
        raise ValueError(
            "oci updates require a registry /v2/<repository>/tags/list URL"
        )
    if "url" in config:
        parsed = urlsplit(config["url"])
        if parsed.scheme != "https" or not parsed.hostname or parsed.username:
            raise ValueError("auto_update.url must be an HTTPS URL without credentials")
    if "version_regex" in config:
        try:
            pattern = re.compile(config["version_regex"])
        except re.error as exc:
            raise ValueError(f"invalid auto_update.version_regex: {exc}") from exc
        if "version" not in pattern.groupindex:
            raise ValueError(
                "auto_update.version_regex requires a named 'version' group"
            )


def _has_prerelease_suffix(suffix: str) -> bool:
    return bool(
        re.match(
            r"[-_.]?(?:alpha|beta|preview|pre|rc|dev|[abc])(?:[-_.]?\d+)?(?:$|[^A-Za-z0-9])",
            suffix,
            re.I,
        )
    )


def parse_release_tag(
    tag: str,
    url: str,
    pattern: re.Pattern[str] | None,
    version_scheme: str = "numeric",
    include_prereleases: bool = False,
) -> UpstreamRelease | None:
    try:
        upstream_version = Version(tag)
    except InvalidVersion:
        pass
    else:
        if (
            version_scheme == "numeric"
            and not include_prereleases
            and (upstream_version.is_prerelease or upstream_version.is_devrelease)
        ):
            return None
    if pattern is not None:
        match = pattern.fullmatch(tag)
        if match is None:
            return None
        candidate = match.group("version")
        if (
            version_scheme == "numeric"
            and not include_prereleases
            and _has_prerelease_suffix(tag[match.end("version") :])
        ):
            return None
    else:
        candidate = tag[1:] if tag[:1] in {"v", "V"} else tag
    if not candidate or not SAFE_VERSION.fullmatch(candidate):
        return None
    if version_scheme == "year_letter":
        if not YEAR_LETTER_VERSION.fullmatch(candidate):
            return None
        return UpstreamRelease(candidate, tag, url)
    try:
        parsed = Version(candidate)
    except InvalidVersion:
        return None
    if not include_prereleases and (parsed.is_prerelease or parsed.is_devrelease):
        return None
    return UpstreamRelease(candidate, tag, url)


def _best(releases: Iterable[UpstreamRelease]) -> UpstreamRelease | None:
    return max(releases, key=lambda release: Version(release.version), default=None)


def _page_url(url: str, hostname: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != hostname:
        raise ValueError(f"unexpected upstream pagination URL: {url!r}")
    return url


def _github_items(session: requests.Session, url: str) -> Iterator[dict]:
    visited = set()
    while url:
        _page_url(url, "api.github.com")
        if url in visited:
            raise ValueError("GitHub returned a repeated pagination URL")
        visited.add(url)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        items = response.json()
        if not isinstance(items, list) or not all(
            isinstance(item, dict) for item in items
        ):
            raise ValueError("GitHub returned an invalid release or tag list")
        yield from items
        url = response.links.get("next", {}).get("url")


def _github_latest(
    config: dict, session: requests.Session, pattern: re.Pattern[str] | None
) -> UpstreamRelease | None:
    repo = config["repo"]
    base = f"https://api.github.com/repos/{repo}"
    releases = []
    has_releases = False
    if config["method"] == "github_release":
        for item in _github_items(session, f"{base}/releases?per_page=100"):
            has_releases = True
            if item.get("draft") or (
                item.get("prerelease") and not config.get("include_prereleases", False)
            ):
                continue
            tag = item.get("tag_name")
            if not isinstance(tag, str):
                raise ValueError("GitHub release has no tag_name")
            url = f"https://github.com/{repo}/releases/tag/{quote(tag, safe='')}"
            release = parse_release_tag(
                tag,
                url,
                pattern,
                config.get("version_scheme", "numeric"),
                config.get("include_prereleases", False),
            )
            if release is not None:
                releases.append(release)
        if has_releases:
            return _best(releases)
    for item in _github_items(session, f"{base}/tags?per_page=100"):
        tag = item.get("name")
        if not isinstance(tag, str):
            raise ValueError("GitHub tag has no name")
        url = f"https://github.com/{repo}/tree/{quote(tag, safe='')}"
        release = parse_release_tag(
            tag,
            url,
            pattern,
            config.get("version_scheme", "numeric"),
            config.get("include_prereleases", False),
        )
        if release is not None:
            releases.append(release)
    return _best(releases)


def _pypi_latest(
    config: dict, session: requests.Session, pattern: re.Pattern[str] | None
) -> UpstreamRelease | None:
    package = config["package"]
    response = session.get(f"https://pypi.org/pypi/{package}/json", timeout=30)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not isinstance(data.get("releases"), dict):
        raise ValueError("PyPI returned an invalid release mapping")
    releases = []
    for tag, files in data["releases"].items():
        if not isinstance(files, list) or not all(
            isinstance(file, dict) for file in files
        ):
            raise ValueError("PyPI returned an invalid release file list")
        if not any(not file.get("yanked", False) for file in files):
            continue
        url = f"https://pypi.org/project/{package}/{quote(tag, safe='')}/"
        release = parse_release_tag(
            tag,
            url,
            pattern,
            config.get("version_scheme", "numeric"),
            config.get("include_prereleases", False),
        )
        if release is not None:
            releases.append(release)
    return _best(releases)


def _npm_latest(
    config: dict, session: requests.Session, pattern: re.Pattern[str] | None
) -> UpstreamRelease | None:
    package = config["package"]
    response = session.get(
        f"https://registry.npmjs.org/{quote(package, safe='@')}",
        headers={"Accept": "application/vnd.npm.install-v1+json"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not isinstance(data.get("versions"), dict):
        raise ValueError("npm returned an invalid version mapping")
    dist_tags = data.get("dist-tags", {})
    if not isinstance(dist_tags, dict):
        raise ValueError("npm returned an invalid dist-tags mapping")
    releases = []
    for tag in data["versions"]:
        url = f"https://www.npmjs.com/package/{package}/v/{quote(tag, safe='')}"
        release = parse_release_tag(
            tag,
            url,
            pattern,
            config.get("version_scheme", "numeric"),
            config.get("include_prereleases", False),
        )
        if release is not None:
            if tag == dist_tags.get("latest"):
                return release
            releases.append(release)
    return _best(releases)


def _webpage_latest(
    config: dict, session: requests.Session, pattern: re.Pattern[str]
) -> UpstreamRelease | None:
    url = config["url"]
    response = session.get(url, timeout=30)
    response.raise_for_status()
    releases = []
    for match in pattern.finditer(response.text):
        tag = match.group("version")
        if tag is None:
            continue
        if (
            config.get("version_scheme", "numeric") == "numeric"
            and not config.get("include_prereleases", False)
            and _has_prerelease_suffix(response.text[match.end("version") :])
        ):
            continue
        release = parse_release_tag(
            tag,
            url,
            None,
            config.get("version_scheme", "numeric"),
            config.get("include_prereleases", False),
        )
        if release is not None:
            releases.append(release)
    return _best(releases)


def _dockerhub_latest(
    config: dict, session: requests.Session, pattern: re.Pattern[str] | None
) -> UpstreamRelease | None:
    repo = config["repo"]
    namespace, repository = repo.split("/")
    url = (
        f"https://hub.docker.com/v2/namespaces/{namespace}"
        f"/repositories/{repository}/tags?page_size=100"
    )
    visited = set()
    releases = []
    while url:
        _page_url(url, "hub.docker.com")
        if url in visited:
            raise ValueError("Docker Hub returned a repeated pagination URL")
        visited.add(url)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("results"), list):
            raise ValueError("Docker Hub returned an invalid tag list")
        for item in data["results"]:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                raise ValueError("Docker Hub tag has no name")
            tag = item["name"]
            release_url = (
                f"https://hub.docker.com/r/{repo}/tags?name={quote(tag, safe='')}"
            )
            release = parse_release_tag(
                tag,
                release_url,
                pattern,
                config.get("version_scheme", "numeric"),
                config.get("include_prereleases", False),
            )
            if release is not None:
                releases.append(release)
        url = data.get("next")
        if url is not None and not isinstance(url, str):
            raise ValueError("Docker Hub returned an invalid pagination URL")
    return _best(releases)


def _oci_latest(
    config: dict, session: requests.Session, pattern: re.Pattern[str] | None
) -> UpstreamRelease | None:
    url = config["url"]
    hostname = urlsplit(url).netloc
    headers = {}
    visited = set()
    releases = []
    while url:
        _page_url(url, hostname)
        if url in visited:
            raise ValueError("OCI registry returned a repeated pagination URL")
        visited.add(url)
        response = session.get(url, headers=headers, timeout=30)
        if response.status_code == 401:
            challenge = response.headers.get("WWW-Authenticate", "")
            scheme, _, fields = challenge.partition(" ")
            if scheme.lower() != "bearer":
                raise ValueError(
                    "OCI registry does not support anonymous Bearer authentication"
                )
            fields = requests.utils.parse_dict_header(fields)
            realm = fields.get("realm", "")
            parsed = urlsplit(realm)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username:
                raise ValueError(
                    "OCI token service must use an HTTPS URL without credentials"
                )
            token_response = session.get(
                realm,
                params={
                    key: fields[key] for key in ("service", "scope") if key in fields
                },
                timeout=30,
            )
            token_response.raise_for_status()
            data = token_response.json()
            token = data.get("token") or data.get("access_token")
            if not isinstance(token, str) or not token:
                raise ValueError("OCI token service returned no token")
            headers = {"Authorization": f"Bearer {token}"}
            response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        tags = data.get("tags") if isinstance(data, dict) else None
        if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
            raise ValueError("OCI registry returned an invalid tag list")
        for tag in tags:
            release = parse_release_tag(
                tag,
                config["url"],
                pattern,
                config.get("version_scheme", "numeric"),
                config.get("include_prereleases", False),
            )
            if release is not None:
                releases.append(release)
        next_url = response.links.get("next", {}).get("url")
        url = urljoin(url, next_url) if next_url else None
    return _best(releases)


def latest_version(
    config: dict, github_session: requests.Session
) -> UpstreamRelease | None:
    """Return the latest stable upstream version; propagate lookup failures.

    GitHub credentials are confined to GitHub requests. Registry lookups use a
    separate public session, including when the caller supplies a bearer token.
    """
    validate_update_config(config)
    method = config["method"]
    if method == "manual":
        return None
    pattern = re.compile(config["version_regex"]) if "version_regex" in config else None
    if method in {"github_release", "github_tags"}:
        return _github_latest(config, github_session, pattern)
    with requests.Session() as public_session:
        configure_read_retries(public_session)
        if method == "pypi":
            return _pypi_latest(config, public_session, pattern)
        if method == "npm":
            return _npm_latest(config, public_session, pattern)
        if method == "oci":
            return _oci_latest(config, public_session, pattern)
        if method == "webpage":
            return _webpage_latest(
                config, public_session, re.compile(config["version_regex"])
            )
        return _dockerhub_latest(config, public_session, pattern)
