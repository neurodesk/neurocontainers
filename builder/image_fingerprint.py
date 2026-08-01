from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable


VOLATILE_LABELS = {
    "GITHUB_REPOSITORY",
    "GITHUB_SHA",
}

# Runtime configuration fields compared between a locally built image
# (`docker inspect`) and the image config blob served by a registry. Both
# sources spell these keys the same way, so a fingerprint taken remotely for
# the previous `:latest` is comparable with one taken locally for the new
# build. Docker bookkeeping fields (Hostname, Image, ArgsEscaped, ...) are
# deliberately excluded: they carry no runtime meaning and their presence
# varies between engine versions and between local and registry views.
CONFIG_FIELDS = (
    "User",
    "ExposedPorts",
    "Env",
    "Cmd",
    "Healthcheck",
    "Volumes",
    "WorkingDir",
    "Entrypoint",
    "OnBuild",
    "Labels",
    "StopSignal",
    "Shell",
)

ARCHITECTURE_ALIASES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}

INDEX_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)

MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)

MANIFEST_ACCEPT = ", ".join(INDEX_MEDIA_TYPES + MANIFEST_MEDIA_TYPES)

DEFAULT_REGISTRY = "docker.io"
DOCKER_HUB_API = "registry-1.docker.io"


class ImageNotFound(Exception):
    """The requested image reference does not exist in the registry."""


class RegistryError(Exception):
    """The registry could not be queried for image metadata."""


def _normalize_value(value: Any) -> Any:
    """Collapse "unset" and "empty" to the same value.

    Docker renders an unset `Volumes`/`Entrypoint` as `null` while a registry
    config blob usually omits the key entirely, and either side may use an
    empty map or list. None of those differences change runtime behaviour.
    """
    if value is None:
        return None
    if isinstance(value, (str, list, dict, tuple)) and len(value) == 0:
        return None
    return value


def _normalize(config: dict[str, Any], diff_ids: Iterable[str] | None) -> dict[str, Any]:
    labels = dict(config.get("Labels") or {})
    for label in VOLATILE_LABELS:
        labels.pop(label, None)

    normalized_config: dict[str, Any] = {}
    for field_name in CONFIG_FIELDS:
        raw = labels if field_name == "Labels" else config.get(field_name)
        normalized_config[field_name] = _normalize_value(raw)

    return {
        "Config": normalized_config,
        "RootFS": {
            "Type": "layers",
            "Layers": list(diff_ids or []),
        },
    }


def normalize_inspect_data(image: dict[str, Any]) -> dict[str, Any]:
    """Normalize a `docker inspect` image object."""
    rootfs = image.get("RootFS") or {}
    return _normalize(image.get("Config") or {}, rootfs.get("Layers"))


def normalize_remote_config(config_blob: dict[str, Any]) -> dict[str, Any]:
    """Normalize an OCI/Docker image config blob fetched from a registry."""
    config = config_blob.get("config")
    if config is None:
        config = config_blob.get("Config") or {}
    rootfs = config_blob.get("rootfs") or config_blob.get("RootFS") or {}
    diff_ids = rootfs.get("diff_ids")
    if diff_ids is None:
        diff_ids = rootfs.get("Layers")
    return _normalize(config, diff_ids)


def _digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def fingerprint_inspect_data(image: dict[str, Any]) -> str:
    return _digest(normalize_inspect_data(image))


def fingerprint_remote_config(config_blob: dict[str, Any]) -> str:
    return _digest(normalize_remote_config(config_blob))


def inspect_image(image_ref: str) -> dict[str, Any]:
    result = subprocess.run(
        ["docker", "inspect", image_ref],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    inspected = json.loads(result.stdout)
    if not inspected:
        raise ValueError(f"No docker inspect data found for {image_ref}")
    return inspected[0]


@dataclass
class HttpResponse:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    def json(self) -> Any:
        return json.loads(self.body.decode())

    def header(self, name: str) -> str:
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return ""


Transport = Callable[[str, str, dict[str, str], dict[str, str] | None], HttpResponse]


def _requests_transport(
    method: str,
    url: str,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
) -> HttpResponse:
    import requests

    response = requests.request(method, url, headers=headers, params=params, timeout=60)
    return HttpResponse(response.status_code, dict(response.headers), response.content)


@dataclass
class ImageReference:
    registry: str
    repository: str
    reference: str

    @property
    def api_host(self) -> str:
        if self.registry == DEFAULT_REGISTRY:
            return DOCKER_HUB_API
        return self.registry


def parse_image_reference(image_ref: str) -> ImageReference:
    remainder = image_ref
    registry = DEFAULT_REGISTRY

    head, _, tail = remainder.partition("/")
    if tail and ("." in head or ":" in head or head == "localhost"):
        registry = head
        remainder = tail

    if "@" in remainder:
        repository, _, reference = remainder.partition("@")
    elif ":" in remainder.rsplit("/", 1)[-1]:
        repository, _, reference = remainder.rpartition(":")
    else:
        repository, reference = remainder, "latest"

    if registry == DEFAULT_REGISTRY and "/" not in repository:
        repository = f"library/{repository}"

    if not repository:
        raise ValueError(f"Could not parse image reference: {image_ref}")

    return ImageReference(registry=registry, repository=repository, reference=reference)


def _decode_docker_auth(entry: dict[str, Any]) -> tuple[str, str] | None:
    encoded = entry.get("auth")
    if encoded:
        try:
            username, _, password = base64.b64decode(encoded).decode().partition(":")
        except (ValueError, UnicodeDecodeError):
            return None
        if password:
            return username, password
    username = entry.get("username")
    password = entry.get("password")
    if username and password:
        return str(username), str(password)
    return None


def _credential_helper_credentials(registry: str, helper: str) -> tuple[str, str] | None:
    """Ask `docker-credential-<helper>` for the stored registry credentials."""
    try:
        result = subprocess.run(
            [f"docker-credential-{helper}", "get"],
            input=registry,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    username = payload.get("Username")
    secret = payload.get("Secret")
    if username and secret and username != "<token>":
        return str(username), str(secret)
    return None


def docker_config_credentials(registry: str, config_dir: str | None = None) -> tuple[str, str] | None:
    """Read credentials written by `docker login` for `registry`."""
    base = config_dir or os.environ.get("DOCKER_CONFIG") or str(Path.home() / ".docker")
    config_path = Path(base) / "config.json"
    try:
        config = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None

    candidates = [registry, f"https://{registry}", f"https://{registry}/"]
    if registry == DEFAULT_REGISTRY:
        candidates.append("https://index.docker.io/v1/")

    auths = config.get("auths") or {}
    for candidate in candidates:
        entry = auths.get(candidate)
        if isinstance(entry, dict):
            credentials = _decode_docker_auth(entry)
            if credentials:
                return credentials

    cred_helpers = config.get("credHelpers") or {}
    for candidate in candidates:
        helper = cred_helpers.get(candidate)
        if helper:
            credentials = _credential_helper_credentials(registry, str(helper))
            if credentials:
                return credentials

    store = config.get("credsStore")
    if store and (registry in auths or any(c in auths for c in candidates)):
        return _credential_helper_credentials(registry, str(store))
    return None


def resolve_credentials(registry: str) -> tuple[str, str] | None:
    """Find credentials for `registry` from the environment or docker login."""
    username = os.environ.get("REGISTRY_USERNAME")
    password = os.environ.get("REGISTRY_PASSWORD")
    if username and password:
        return username, password

    credentials = docker_config_credentials(registry)
    if credentials:
        return credentials

    if registry == "ghcr.io":
        token = os.environ.get("GHCR_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if token:
            return os.environ.get("GITHUB_ACTOR") or "x-access-token", token
    return None


def _parse_www_authenticate(header: str) -> dict[str, str]:
    if not header.lower().startswith("bearer"):
        return {}
    return dict(re.findall(r'(\w+)="([^"]*)"', header))


def resolve_architecture(architecture: str | None) -> str | None:
    if not architecture:
        return None
    lowered = architecture.lower()
    return ARCHITECTURE_ALIASES.get(lowered, lowered)


def select_platform_manifest(
    manifests: list[dict[str, Any]],
    architecture: str | None,
) -> str:
    """Pick the child manifest digest matching `architecture`.

    Attestation manifests (platform `unknown/unknown`) are never selected.
    """
    candidates = []
    for entry in manifests:
        platform = entry.get("platform") or {}
        if platform.get("os") == "unknown" or platform.get("architecture") == "unknown":
            continue
        candidates.append(entry)

    if not candidates:
        raise RegistryError("Manifest list contains no runnable image manifests")

    if architecture:
        for entry in candidates:
            platform = entry.get("platform") or {}
            entry_os = platform.get("os") or "linux"
            if platform.get("architecture") == architecture and entry_os == "linux":
                return entry["digest"]
        if len(candidates) > 1:
            available = ", ".join(
                f"{(c.get('platform') or {}).get('os', '?')}/"
                f"{(c.get('platform') or {}).get('architecture', '?')}"
                for c in candidates
            )
            raise RegistryError(
                f"No manifest for architecture {architecture} (available: {available})"
            )
        print(
            f"Warning: manifest list has no {architecture} entry; "
            "falling back to its only image manifest",
            file=sys.stderr,
        )

    return candidates[0]["digest"]


class RegistryClient:
    """Minimal registry client that reads image metadata without pulling layers."""

    def __init__(
        self,
        registry: str,
        credentials: tuple[str, str] | None = None,
        transport: Transport | None = None,
    ) -> None:
        self.registry = registry
        self.credentials = credentials
        self._transport = transport or _requests_transport
        self._token: str | None = None

    def _url(self, path: str) -> str:
        host = DOCKER_HUB_API if self.registry == DEFAULT_REGISTRY else self.registry
        scheme = "http" if host.startswith("localhost") or host.startswith("127.0.0.1") else "https"
        return f"{scheme}://{host}{path}"

    def _authenticate(self, challenge: dict[str, str]) -> bool:
        realm = challenge.get("realm")
        if not realm:
            return False
        params = {key: challenge[key] for key in ("service", "scope") if challenge.get(key)}
        headers = {"Accept": "application/json"}
        if self.credentials:
            username, password = self.credentials
            basic = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {basic}"
        response = self._transport("GET", realm, headers, params)
        if response.status_code != 200:
            raise RegistryError(
                f"Registry authentication failed with HTTP {response.status_code}"
            )
        payload = response.json()
        token = payload.get("token") or payload.get("access_token")
        if not token:
            raise RegistryError("Registry authentication response contained no token")
        self._token = token
        return True

    def get(self, path: str, accept: str) -> HttpResponse:
        headers = {"Accept": accept}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        response = self._transport("GET", self._url(path), headers, None)
        if response.status_code == 401:
            challenge = _parse_www_authenticate(response.header("WWW-Authenticate"))
            if challenge and self._authenticate(challenge):
                headers["Authorization"] = f"Bearer {self._token}"
                response = self._transport("GET", self._url(path), headers, None)
        return response

    def get_manifest(self, repository: str, reference: str) -> dict[str, Any]:
        response = self.get(f"/v2/{repository}/manifests/{reference}", MANIFEST_ACCEPT)
        if response.status_code in (401, 403, 404):
            raise ImageNotFound(
                f"{self.registry}/{repository}:{reference} is not available "
                f"(HTTP {response.status_code})"
            )
        if response.status_code != 200:
            raise RegistryError(
                f"Failed to read manifest for {repository}:{reference} "
                f"(HTTP {response.status_code})"
            )
        return response.json()

    def get_config_blob(self, repository: str, digest: str) -> dict[str, Any]:
        response = self.get(f"/v2/{repository}/blobs/{digest}", "*/*")
        if response.status_code != 200:
            raise RegistryError(
                f"Failed to read config blob {digest} for {repository} "
                f"(HTTP {response.status_code})"
            )
        return response.json()

    def get_image_config(
        self,
        repository: str,
        reference: str,
        architecture: str | None = None,
    ) -> dict[str, Any]:
        manifest = self.get_manifest(repository, reference)
        if manifest.get("manifests") is not None:
            digest = select_platform_manifest(manifest["manifests"], architecture)
            manifest = self.get_manifest(repository, digest)
        config = manifest.get("config") or {}
        config_digest = config.get("digest")
        if not config_digest:
            media_type = manifest.get("mediaType", "unknown")
            raise RegistryError(
                f"Manifest for {repository}:{reference} has no config descriptor "
                f"(mediaType {media_type})"
            )
        return self.get_config_blob(repository, config_digest)


def fetch_remote_image_config(
    image_ref: str,
    architecture: str | None = None,
    transport: Transport | None = None,
) -> dict[str, Any]:
    parsed = parse_image_reference(image_ref)
    client = RegistryClient(
        parsed.registry,
        credentials=resolve_credentials(parsed.registry),
        transport=transport,
    )
    return client.get_image_config(
        parsed.repository,
        parsed.reference,
        resolve_architecture(architecture),
    )


def remote_fingerprint(
    image_ref: str,
    architecture: str | None = None,
    transport: Transport | None = None,
) -> str:
    """Fingerprint an image using registry metadata only (no layer download)."""
    return fingerprint_remote_config(
        fetch_remote_image_config(image_ref, architecture, transport)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print a stable Docker image fingerprint for release build comparisons."
    )
    parser.add_argument("image", help="Docker image reference to inspect")
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Read the image config from the registry instead of the local docker daemon",
    )
    parser.add_argument(
        "--architecture",
        default=None,
        help="Architecture to select from a multi-platform manifest (e.g. x86_64, aarch64)",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Print an empty fingerprint and succeed when the image does not exist",
    )
    args = parser.parse_args(argv)

    try:
        if args.remote:
            print(remote_fingerprint(args.image, args.architecture))
        else:
            print(fingerprint_inspect_data(inspect_image(args.image)))
    except ImageNotFound as exc:
        if args.allow_missing:
            print(f"{args.image} is not in the registry yet: {exc}", file=sys.stderr)
            print("")
            return 0
        print(f"Failed to fingerprint {args.image}: {exc}", file=sys.stderr)
        return 1
    except (
        subprocess.CalledProcessError,
        RegistryError,
        ValueError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        print(f"Failed to fingerprint {args.image}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
