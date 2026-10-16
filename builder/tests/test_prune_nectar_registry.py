from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from tools import prune_nectar_registry as prune

NOW = datetime(2026, 10, 15, tzinfo=timezone.utc)
OLD = NOW - timedelta(days=60)
APPS = {
    "napari": {
        "apps": {
            "napari 0.9.2": {"version": "20261013", "exec": ""},
            "napari GUI-napari 0.9.2": {"version": "20261013", "exec": "napari"},
        }
    },
    "napari_arm64": {
        "apps": {"napari_arm64 0.9.2": {"version": "20261013", "exec": ""}}
    },
}


def artifact(repository: str, *tags: str, pushed: datetime = OLD) -> prune.Artifact:
    return prune.Artifact(repository, f"sha256:{repository}{tags}", tags, 10, pushed)


def test_apps_json_maps_launchers_and_arm64_to_their_images() -> None:
    assert prune.published_images(APPS) == {
        ("napari_0.9.2", "20261013"),
        ("napari_arm64_0.9.2", "20261013"),
    }


def test_only_published_or_recent_builds_survive() -> None:
    published = prune.published_images(APPS)
    current = artifact("napari_0.9.2", "latest", "20261013")
    older_build = artifact("napari_0.9.2", "20261002")
    retired_version = artifact("napari_0.8.0", "latest", "20260814")
    untagged = artifact("napari_0.9.2")
    in_flight = artifact("napari_1.0.0", "candidate-abc", pushed=NOW)

    plan = prune.plan_deletions(
        [current, older_build, retired_version, untagged, in_flight],
        published,
        NOW - timedelta(days=14),
    )

    assert plan.keep == [current]
    assert plan.recent == [in_flight]
    assert plan.delete == [older_build, retired_version, untagged]
    assert plan.published_missing == [("napari_arm64_0.9.2", "20261013")]


def test_reports_published_builds_absent_from_the_registry() -> None:
    plan = prune.plan_deletions(
        [artifact("napari_arm64_0.9.2", "20261001")],
        prune.published_images(APPS),
        NOW,
    )
    assert plan.published_missing == [
        ("napari_0.9.2", "20261013"),
        ("napari_arm64_0.9.2", "20261013"),
    ]


def reviewed_hash(apps_path, capsys) -> str:
    assert prune.main(["--apps-json", str(apps_path)]) == 0
    line = next(
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("Plan hash: ")
    )
    return line.removeprefix("Plan hash: ")


class FakeHarbor:
    def __init__(self, artifacts: dict[str, list[prune.Artifact]]):
        self.store = artifacts

    def repositories(self) -> list[str]:
        return list(self.store)

    def artifacts(self, repository: str) -> list[prune.Artifact]:
        return list(self.store[repository])

    def artifact(self, repository: str, digest: str) -> prune.Artifact | None:
        return next((a for a in self.store[repository] if a.digest == digest), None)

    def delete_artifact(self, item: prune.Artifact) -> None:
        self.store[item.repository].remove(item)


@pytest.mark.parametrize("apply", [False, True])
def test_main_deletes_only_with_apply(
    tmp_path, monkeypatch, capsys, apply: bool
) -> None:
    apps_path = tmp_path / "apps.json"
    apps_path.write_text(json.dumps(APPS))
    current = artifact("napari_0.9.2", "latest", "20261013")
    retired = artifact("napari_0.8.0", "latest", "20260814")
    fake = FakeHarbor({"napari_0.9.2": [current], "napari_0.8.0": [retired]})
    monkeypatch.setattr(prune, "Harbor", lambda *args: fake)
    monkeypatch.setenv("NECTAR_REGISTRY_USERNAME", "robot")
    monkeypatch.setenv("NECTAR_REGISTRY_PASSWORD", "secret")

    argv = ["--apps-json", str(apps_path)]
    if apply:
        argv += ["--apply", "--expect-plan", reviewed_hash(apps_path, capsys)]
    assert prune.main(argv) == 0

    assert fake.store["napari_0.9.2"] == [current]
    assert fake.store["napari_0.8.0"] == ([] if apply else [retired])


def test_apply_requires_credentials(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("NECTAR_REGISTRY_USERNAME", raising=False)
    monkeypatch.delenv("NECTAR_REGISTRY_PASSWORD", raising=False)
    with pytest.raises(SystemExit):
        prune.main(
            [
                "--apps-json",
                str(tmp_path / "unused.json"),
                "--apply",
                "--expect-plan",
                "x",
            ]
        )


def test_apply_refuses_to_delete_most_of_the_registry(
    tmp_path, monkeypatch, capsys
) -> None:
    """An empty or truncated apps.json must not wipe the registry."""
    apps_path = tmp_path / "apps.json"
    apps_path.write_text("{}")
    images = [artifact("napari_0.9.2", "20261013"), artifact("napari_0.8.0", "latest")]
    fake = FakeHarbor({a.repository: [a] for a in images})
    monkeypatch.setattr(prune, "Harbor", lambda *args: fake)
    monkeypatch.setenv("NECTAR_REGISTRY_USERNAME", "robot")
    monkeypatch.setenv("NECTAR_REGISTRY_PASSWORD", "secret")

    argv = ["--apps-json", str(apps_path), "--apply"]
    assert prune.main(argv + ["--expect-plan", reviewed_hash(apps_path, capsys)]) == 1
    assert all(fake.store.values())


def test_negative_retention_window_is_rejected(tmp_path) -> None:
    with pytest.raises(SystemExit):
        prune.main(
            ["--apps-json", str(tmp_path / "unused.json"), "--min-age-days", "-1"]
        )


def test_a_fresh_tag_on_an_old_digest_counts_as_recent() -> None:
    item = {
        "digest": "sha256:abc",
        "push_time": "2026-05-18T00:00:00Z",
        "tags": [
            {"name": "20260518", "push_time": "2026-05-18T00:00:00Z"},
            {"name": "candidate-new", "push_time": "2026-10-14T00:00:00Z"},
        ],
    }
    parsed = prune.parse_artifact("openadscpu_1.0.0", item)
    plan = prune.plan_deletions([parsed], set(), NOW - timedelta(days=14))
    assert plan.recent == [parsed]


def test_apply_skips_an_artifact_tagged_after_planning(
    tmp_path, monkeypatch, capsys
) -> None:
    apps_path = tmp_path / "apps.json"
    apps_path.write_text(json.dumps(APPS))
    current = artifact("napari_0.9.2", "latest", "20261013")
    retired = artifact("napari_0.8.0", "20260814")
    fake = FakeHarbor({"napari_0.9.2": [current], "napari_0.8.0": [retired]})
    retagged = prune.Artifact(
        retired.repository, retired.digest, ("20260814", "latest"), 10, OLD
    )
    # A promotion tags the digest between planning and deletion.
    monkeypatch.setattr(fake, "artifact", lambda repository, digest: retagged)
    monkeypatch.setattr(prune, "Harbor", lambda *args: fake)
    monkeypatch.setenv("NECTAR_REGISTRY_USERNAME", "robot")
    monkeypatch.setenv("NECTAR_REGISTRY_PASSWORD", "secret")

    reviewed = reviewed_hash(apps_path, capsys)
    argv = ["--apps-json", str(apps_path), "--apply", "--expect-plan", reviewed]
    assert prune.main(argv) == 0
    assert fake.store["napari_0.8.0"] == [retired]


def test_apply_refuses_a_plan_that_differs_from_the_reviewed_one(
    tmp_path, monkeypatch
) -> None:
    apps_path = tmp_path / "apps.json"
    apps_path.write_text(json.dumps(APPS))
    current = artifact("napari_0.9.2", "latest", "20261013")
    retired = artifact("napari_0.8.0", "20260814")
    fake = FakeHarbor({"napari_0.9.2": [current], "napari_0.8.0": [retired]})
    monkeypatch.setattr(prune, "Harbor", lambda *args: fake)
    monkeypatch.setenv("NECTAR_REGISTRY_USERNAME", "robot")
    monkeypatch.setenv("NECTAR_REGISTRY_PASSWORD", "secret")

    argv = ["--apps-json", str(apps_path), "--apply", "--expect-plan", "stale"]
    assert prune.main(argv) == 1
    assert fake.store["napari_0.8.0"] == [retired]


def test_deletes_use_a_registry_token_and_reads_stay_anonymous(monkeypatch) -> None:
    # Under OIDC, Harbor's REST API rejects the CLI secret; only the registry's
    # token service accepts it.
    sent = []

    class Response:
        def __init__(self, body: bytes) -> None:
            self.body = body

        def read(self) -> bytes:
            return self.body

        def close(self) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc) -> None:
            pass

        headers: dict = {}

    def urlopen(request, timeout):
        sent.append(request)
        if "/service/token" in request.full_url:
            return Response(b'{"token": "registry-token"}')
        return Response(b"[]")

    monkeypatch.setattr(prune.urllib.request, "urlopen", urlopen)
    harbor = prune.Harbor(prune.HARBOR_API, "ssdsorg", "user", "cli-secret")
    harbor.repositories()
    harbor.delete_artifact(artifact("napari_0.8.0", "20260814"))

    read, token, delete = sent
    assert read.get_header("Authorization") is None
    assert token.full_url.startswith("https://registry.rc.nectar.org.au/service/token?")
    assert "repository%3Assdsorg%2Fnapari_0.8.0%3Adelete" in token.full_url
    assert token.get_header("Authorization") == "Basic dXNlcjpjbGktc2VjcmV0"
    assert delete.get_method() == "DELETE"
    assert delete.full_url == (
        "https://registry.rc.nectar.org.au/v2/ssdsorg/napari_0.8.0/manifests/"
        "sha256:napari_0.8.0('20260814',)"
    )
    assert delete.get_header("Authorization") == "Bearer registry-token"
