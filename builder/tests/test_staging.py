from __future__ import annotations

from pathlib import Path

from builder.cache import get_guest_filename
from builder.config import default_config, resolve_recipe
from builder.recipe import compile_recipe
from builder.staging import DeclaredFile, StagingPlan, materialize_plan


def test_url_guest_filename_uses_url_basename() -> None:
    assert (
        get_guest_filename("downloaded_file", "https://example.com/releases/tool.tar.gz")
        == "tool.tar.gz"
    )


def test_stage_dcm2niix_creates_placeholder_without_download(tmp_path: Path) -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    cache_dir = materialize_plan(
        compiled.staging_plan,
        compiled.recipe_dir,
        tmp_path / "build",
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    assert (cache_dir / "dcm2niix_lnx.zip").exists()
    mounted = list(cache_dir.glob("h*/dcm2niix_lnx.zip"))
    assert mounted, "declared files used via get_file() must be staged under their cache mount id"


def test_stage_literal_file(tmp_path: Path) -> None:
    plan = StagingPlan()
    plan.add_file(DeclaredFile(name="script.sh", contents="#!/bin/sh\necho ok", executable=True))
    cache_dir = materialize_plan(
        plan,
        tmp_path,
        tmp_path / "build",
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    target = cache_dir / "script.sh"
    assert target.read_text() == "#!/bin/sh\necho ok"
    assert target.stat().st_mode & 0o111


def test_fixture_stages_filename_and_contents(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "file_variants"
    compiled = compile_recipe(fixture, architecture="x86_64")
    cache_dir = materialize_plan(
        compiled.staging_plan,
        fixture,
        tmp_path / "build",
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    assert (cache_dir / "inline.txt").read_text() == "hello inline"
    assert (cache_dir / "copied.txt").read_text() == "hello copied\n"
