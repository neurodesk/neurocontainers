import argparse
import json
import os
import re
import subprocess
import traceback
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import quote

import requests
import yaml
from packaging import version

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from builder.update_assets import rewrite_release_assets
from builder.audit_updates import validate_update_policy
from builder.update_plan import plan_sources
from builder.update_sources import (
    SAFE_SOURCE_REF,
    UpstreamRelease,
    configure_read_retries,
    latest_version,
)

DEBUG = os.getenv("AUTO_UPDATE_DEBUG") == "1"


def dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)


TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPOSITORY")
session = requests.Session()
configure_read_retries(session)
session.headers.update({"Accept": "application/vnd.github+json"})
if TOKEN:
    session.headers.update({"Authorization": f"Bearer {TOKEN}"})


# return true if have newer version,false if is up to date and none if need manual check
def newer(current_version, upstream_version):
    def clean(ver_str: str) -> str:
        ver_str = (ver_str or "").strip()
        ver_str = ver_str.lstrip("vV")
        ver_str = ver_str.replace("_", ".")
        ver_str = ver_str.split("+", 1)[0]  # +meta
        return ver_str

    clean_current, clean_upstream = clean(current_version), clean(upstream_version)
    try:
        ver_current = version.parse(clean_current)
        ver_upstream = version.parse(clean_upstream)
    except Exception as e:
        dbg("version parse failed:", e)
        return None  # we can not compare strings

    result = ver_upstream > ver_current
    dbg(
        f"Version compare: current={ver_current} upstream={ver_upstream} -> upstream_is_newer={result}"
    )
    return result


def tag_to_recipe_version(tag, current_version=""):
    """Map an upstream tag to the value that belongs in the recipe's version field.

    Recipes template URLs off ``{{ context.version }}`` (sometimes prefixed with a
    literal ``v``), so the leading ``v`` is normally dropped. A recipe that already
    carries the prefix in its own version keeps it, so the module name a user loads
    does not change shape underneath them. Anything that does not parse cleanly
    afterwards is left for a human.
    """
    candidate = (tag or "").strip()
    if candidate[:1] in ("v", "V"):
        candidate = candidate[1:]
    if not candidate:
        return None
    if (current_version or "").strip()[:1] in ("v", "V"):
        candidate = f"v{candidate}"
    try:
        version.parse(candidate)
    except Exception as e:
        dbg("tag_to_recipe_version parse failed:", e)
        return None
    return candidate


def resolve_tag_commit(repo, tag):
    """Resolve a tag to the commit it points at, dereferencing annotated tags."""
    try:
        response = session.get(
            f"https://api.github.com/repos/{repo}/git/ref/tags/{quote(tag, safe='')}",
            timeout=20,
        )
        dbg("GET tag ref status:", response.status_code)
        if response.status_code != 200:
            return None
        obj = response.json().get("object") or {}
        if obj.get("type") != "tag":
            return obj.get("sha")
        response = session.get(
            f"https://api.github.com/repos/{repo}/git/tags/{obj['sha']}", timeout=20
        )
        dbg("GET annotated tag status:", response.status_code)
        if response.status_code != 200:
            return None
        return (response.json().get("object") or {}).get("sha")
    except Exception as e:
        dbg("Resolve tag commit error:", e)
        dbg(traceback.format_exc())
        return None


REVISION_LINE = re.compile(
    r"^(?P<indent>[ \t]*)revision:[ \t]*"
    r"(?P<quote>[\"']?)(?P<sha>[0-9a-fA-F]{7,40})(?P=quote)[ \t]*$",
    re.M,
)


def rewrite_top_level_string(text, key, new_value):
    """Replace a top-level YAML scalar so it reloads as the requested string."""
    line = re.compile(
        rf"^{re.escape(key)}:[ \t]*(?P<value>.*?)[ \t]*(?P<comment>#.*)?$",
        re.M,
    )
    match = line.search(text)
    if not match:
        return None
    raw = match.group("value")
    quote = raw[0] if raw[:1] in ("'", '"') else ""
    comment = match.group("comment")
    trailer = f"  {comment}" if comment else ""

    for candidate_quote in (quote, '"'):
        updated = (
            f"{text[:match.start()]}{key}: "
            f"{candidate_quote}{new_value}{candidate_quote}{trailer}"
            f"{text[match.end():]}"
        )
        try:
            reloaded = yaml.safe_load(updated)
        except Exception as e:
            dbg("rewrite_top_level_string reload failed:", e)
            continue
        if isinstance(reloaded, dict) and str(reloaded.get(key)) == new_value:
            return updated
    return None


def rewrite_version(text, new_version):
    """Replace the top-level version field so it reads back as the same string.

    Original quoting is preserved where it survives a round trip. It often does
    not: `version: 1.9` is a float, and bumping it to an unquoted 1.10 would
    reload as 1.1. Most recipes leave the version unquoted, so the fallback to
    an explicitly quoted scalar is the common path for any two-part version.
    """
    return rewrite_top_level_string(text, "version", new_version)


def rewrite_fulltest_version(text, new_version):
    """Update a fulltest version while preserving a simple variable indirection."""
    try:
        config = yaml.safe_load(text)
    except yaml.YAMLError:
        return None
    if not isinstance(config, dict):
        return None

    raw_version = str(config.get("version", "") or "")
    variable = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", raw_version)
    if variable and variable.group(1) in config:
        return rewrite_top_level_string(text, variable.group(1), new_version)
    return rewrite_version(text, new_version)


def prepare_fulltest_bump(recipe_path, new_version):
    """Prepare a sibling fulltest update for an automated recipe version bump."""
    fulltest_path = os.path.join(os.path.dirname(recipe_path), "fulltest.yaml")
    if not os.path.isfile(fulltest_path):
        return None

    with open(fulltest_path, encoding="utf-8") as f:
        original = f.read()
    try:
        config = yaml.safe_load(original)
    except yaml.YAMLError as e:
        raise ValueError(f"invalid YAML in {fulltest_path}: {e}") from e
    if not isinstance(config, dict):
        raise ValueError(f"{fulltest_path} must contain a YAML mapping")

    old_version = str(config.get("version", "") or "")
    updated = rewrite_fulltest_version(original, new_version)
    if updated is None:
        raise ValueError(f"cannot rewrite the version in {fulltest_path}")
    if updated == original:
        return None
    return fulltest_path, updated, old_version


def revisions_owned_by(text, repo):
    """Return the pinned shas that sit in a mapping also naming the upstream repo.

    Proximity in the raw text is not evidence of ownership: the auto_update.repo
    line is itself usually within a few hundred characters of the revision, so a
    character window matches almost anything. Ownership is a structural claim --
    the sha and the repo URL have to be siblings in the same YAML mapping -- so
    it is decided on the parsed document.
    """
    if not repo:
        return set()
    try:
        with_repo = set()

        def walk(node):
            if isinstance(node, dict):
                sha = node.get("revision")
                if isinstance(sha, str) and re.fullmatch(r"[0-9a-fA-F]{7,40}", sha):
                    siblings = " ".join(
                        str(value)
                        for key, value in node.items()
                        if key != "revision" and not isinstance(value, (dict, list))
                    )
                    if repo.lower() in siblings.lower():
                        with_repo.add(sha.lower())
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(yaml.safe_load(text))
        return with_repo
    except Exception as e:
        dbg("revisions_owned_by parse failed:", e)
        return set()


def rewrite_revision(text, repo, new_sha):
    """Repoint pinned commit revisions that belong to the upstream repo."""
    owned = revisions_owned_by(text, repo)
    changed = []

    def replace(match):
        old_sha = match.group("sha")
        if old_sha.lower() not in owned:
            return match.group(0)
        if new_sha.lower().startswith(old_sha.lower()):
            return match.group(0)
        changed.append((old_sha, new_sha))
        quote = match.group("quote")
        return f'{match.group("indent")}revision: {quote}{new_sha}{quote}'

    return REVISION_LINE.sub(replace, text), changed


def find_stale_update_issues(path):
    """Legacy 'may update to' issues for this recipe, so the PR can close them."""
    if not REPO:
        return []
    q = f'repo:{REPO} state:open label:auto-update in:body "- Recipe: {path}"'
    dbg("Search stale update issues:", q)
    try:
        response = session.get(
            "https://api.github.com/search/issues", params={"q": q}, timeout=20
        )
        if response.status_code != 200:
            dbg("Search stale issues unexpected:", response.text[:300])
            return []
        numbers = []
        for item in response.json().get("items", []):
            if "pull_request" in item:
                continue
            if "may update to" not in (item.get("title") or ""):
                continue
            numbers.append(item["number"])
        dbg("Stale update issues:", numbers)
        return numbers
    except Exception as e:
        dbg("Search stale issues error:", e)
        return []


def git(*cmd, check=True):
    print("+ git", " ".join(cmd))
    result = subprocess.run(["git", *cmd], check=False, capture_output=True, text=True)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    if check and result.returncode != 0:
        raise RuntimeError(f"git {' '.join(cmd)} failed with {result.returncode}")
    return result


def remote_branch_exists(branch):
    result = git("ls-remote", "--exit-code", "--heads", "origin", branch, check=False)
    return result.returncode == 0


def pull_request_state(branch):
    if not REPO:
        return None
    owner = REPO.split("/", 1)[0]
    response = session.get(
        f"https://api.github.com/repos/{REPO}/pulls",
        params={"state": "all", "head": f"{owner}:{branch}", "per_page": 100},
        timeout=20,
    )
    response.raise_for_status()
    pulls = response.json()
    if any(pr["state"] == "open" for pr in pulls):
        return "open"
    if any(pr.get("merged_at") for pr in pulls):
        return "merged"
    return "closed" if pulls else None


def open_update_branches():
    """Read open branches once so moving source heads cannot flood the PR queue."""
    branches = set()
    page = 1
    while True:
        response = session.get(
            f"https://api.github.com/repos/{REPO}/pulls",
            params={"state": "open", "per_page": 100, "page": page},
            timeout=20,
        )
        response.raise_for_status()
        pulls = response.json()
        for pull in pulls:
            head = pull.get("head") or {}
            if (head.get("repo") or {}).get("full_name") == REPO:
                branch = head.get("ref", "")
                if branch.startswith("auto-update/"):
                    branches.add(branch)
        if len(pulls) < 100:
            return branches
        page += 1


def open_pull_request(branch, base, title, body, labels=None):
    response = session.post(
        f"https://api.github.com/repos/{REPO}/pulls",
        json={"title": title, "body": body, "head": branch, "base": base},
        timeout=20,
    )
    response.raise_for_status()
    pr = response.json()
    print(f"Opened PR #{pr['number']}: {pr['html_url']}")
    if labels:
        try:
            session.post(
                f"https://api.github.com/repos/{REPO}/issues/{pr['number']}/labels",
                json={"labels": labels},
                timeout=20,
            ).raise_for_status()
        except Exception as e:
            dbg("Labelling PR failed:", e)
    return pr


def rewrite_upstream_tag(text, variable, tag):
    if not SAFE_SOURCE_REF.fullmatch(tag):
        raise ValueError("upstream tag is not safe for a source ref variable")
    document = yaml.compose(text)
    variables = next(
        (value for key, value in document.value if key.value == "variables"), None
    )
    if not isinstance(variables, yaml.MappingNode):
        raise ValueError("auto_update.tag_variable requires recipe variables")
    matches = [(key, value) for key, value in variables.value if key.value == variable]
    if len(matches) != 1:
        raise ValueError(f"expected one variables.{variable} source ref")
    key, value = matches[0]
    if (
        not isinstance(value, yaml.ScalarNode)
        or value.style not in {None, "'", '"'}
        or value.start_mark.index < key.end_mark.index
        or "&" in text[value.start_mark.index : value.end_mark.index]
    ):
        raise ValueError(
            f"variables.{variable} must be a plain or quoted scalar without an alias or anchor"
        )
    updated = (
        text[: value.start_mark.index] + json.dumps(tag) + text[value.end_mark.index :]
    )
    if yaml.safe_load(updated)["variables"][variable] != tag:
        raise ValueError(f"variables.{variable} did not retain the selected source ref")
    return updated


def prepare_bump(path, current_version, new_version, repo, tag):
    """Compute the updated recipe text. Returns (text, changelog) or (None, reason)."""
    with open(path, encoding="utf-8") as f:
        original = f.read()

    updated = rewrite_version(original, new_version)
    if updated is None:
        return None, "no top-level version field to rewrite as an equal string"

    changes = [f"`version`: `{current_version}` → `{new_version}`"]

    # Only revisions pinned alongside the upstream repo need to move. A recipe
    # may also pin a helper from some other repo, and that sha has nothing to do
    # with this release.
    if revisions_owned_by(updated, repo):
        commit = resolve_tag_commit(repo, tag)
        if not commit:
            return (
                None,
                f"recipe pins a commit revision but tag {tag} could not be resolved",
            )
        updated, revision_changes = rewrite_revision(updated, repo, commit)
        if not revision_changes:
            return None, (
                "recipe pins a commit revision that could not be matched to "
                f"{repo}; bump it by hand"
            )
        for old_sha, new_sha in revision_changes:
            changes.append(f"`revision`: `{old_sha}` → `{new_sha}`")

    config = yaml.safe_load(updated).get("auto_update", {})
    if tag_variable := config.get("tag_variable"):
        updated = rewrite_upstream_tag(updated, tag_variable, tag)
        changes.append(f"`variables.{tag_variable}`: `{tag}`")
    updated, asset_changes = rewrite_release_assets(
        updated, config, repo, tag, new_version, session
    )
    changes.extend(asset_changes)

    if updated == original:
        return None, "no textual change"
    return updated, changes


def submit_bump(
    path,
    name,
    current_version,
    new_version,
    repo,
    tag,
    base_branch,
    dry_run,
    release_url=None,
    plan=None,
):
    branch = plan.branch if plan else f"auto-update/{name}-{new_version}"

    orphan_branch = False
    if not dry_run:
        state = pull_request_state(branch)
        if state:
            print(f"{branch}: existing pull request is {state}.")
            return state
        if remote_branch_exists(branch):
            # Pushed, but the pull request call never succeeded. Skipping here
            # would strand the recipe: the branch keeps the bump from being
            # retried, and no pull request ever carries it.
            print(f"branch {branch} exists with no pull request; opening one for it.")
            orphan_branch = True

    if plan:
        updated, result = plan.patches[0].after, list(plan.changes)
    else:
        updated, result = prepare_bump(path, current_version, new_version, repo, tag)
    if updated is None:
        print(f"cannot auto-bump {path}: {result}")
        return None
    changes = result

    try:
        fulltest_bump = (
            (str(plan.patches[1].path), plan.patches[1].after, current_version)
            if plan else prepare_fulltest_bump(path, new_version)
        )
    except ValueError as e:
        print(f"cannot auto-bump {path}: {e}")
        return None
    if fulltest_bump:
        fulltest_path, fulltest_updated, old_fulltest_version = fulltest_bump
        changes.append(
            f"`fulltest.yaml` version: `{old_fulltest_version}` → `{new_version}`"
        )

    if dry_run:
        print(f"=== dry run: would open {branch} ===")
        for change in changes:
            print(" -", change)
        return "opened"

    closes = find_stale_update_issues(path)
    body_lines = [
        "Automated version bump generated by `builder/check_version.py`.",
        "",
        f"- Recipe: `{path}`",
        f"- Upstream release: {release_url or f'https://github.com/{repo}/releases/tag/{tag}'}",
        "",
        "### Changes",
        *[f"- {change}" for change in changes],
        "",
        "A maintainer should confirm the container still builds and that the "
        "recipe does not need further changes for this release.",
    ]
    if closes:
        body_lines += ["", *[f"Closes #{number}" for number in closes]]
    body = "\n".join(body_lines)

    title = f"Bump {name} from {current_version} to {new_version}"

    if orphan_branch:
        # The commit is already on the remote; only the pull request is missing.
        open_pull_request(branch, base_branch, title, body, labels=["auto-update"])
        return "opened"

    git("checkout", "-B", branch, base_branch)
    try:
        if plan:
            plan.apply()
        with open(path, "w", encoding="utf-8") as f:
            f.write(updated)
        changed_paths = [path]
        if fulltest_bump:
            with open(fulltest_path, "w", encoding="utf-8") as f:
                f.write(fulltest_updated)
            changed_paths.append(fulltest_path)
        subprocess.run([sys.executable, "builder/validation.py", path], check=True)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "builder",
                "generate",
                os.path.dirname(path),
                "--recreate",
            ],
            check=True,
        )
        git("add", "--", *changed_paths)
        git("commit", "-m", title)
        git("push", "origin", branch)
        open_pull_request(branch, base_branch, title, body, labels=["auto-update"])
    finally:
        git("checkout", "--force", base_branch, check=False)
        git("clean", "-fd", "--", os.path.dirname(path) or ".", check=False)
    return "opened"


def write_report(rows, report_path=None):
    if report_path:
        Path(report_path).write_text(json.dumps(rows, indent=2) + "\n")
    counts = Counter(row["status"] for row in rows)
    lines = [
        "## Recipe update results",
        "",
        f"Recipes checked: {len(rows)}",
        "",
        ", ".join(f"{key}: {count}" for key, count in sorted(counts.items())),
        "",
        "| Recipe | Current | Upstream | Result | Detail |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        cells = [
            row.get(key, "")
            for key in ("recipe", "current", "upstream", "status", "detail")
        ]
        lines.append(
            "| "
            + " | ".join(
                str(cell).replace("|", "\\|").replace("\n", " ") for cell in cells
            )
            + " |"
        )
    report = "\n".join(lines) + "\n"
    print(report)
    if summary_path := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write(report)


def main():
    parser = argparse.ArgumentParser(
        description="Check every recipe's update policy and propose available updates."
    )
    parser.add_argument("recipes", nargs="*", help="Optional recipe names to check")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.getenv("AUTO_UPDATE_DRY_RUN") == "1",
    )
    parser.add_argument(
        "--max-prs",
        type=int,
        default=int(os.getenv("AUTO_UPDATE_MAX_PRS") or 5),
        help="Maximum new PRs per run; 0 means unlimited. Existing PRs do not consume the limit.",
    )
    parser.add_argument("--json", help="Write per-recipe results to this file")
    args = parser.parse_args()
    if args.max_prs < 0:
        parser.error("--max-prs must be nonnegative")
    base_branch = os.getenv("GITHUB_BASE_BRANCH", "main")
    if not args.dry_run:
        if not REPO or not TOKEN:
            parser.error(
                "GITHUB_REPOSITORY and GITHUB_TOKEN are required; use --dry-run locally"
            )
        if git("status", "--porcelain").stdout.strip():
            parser.error(
                "the updater requires a clean checkout; use --dry-run to inspect local changes"
            )
        head = git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
        base_branch = head if head and head != "HEAD" else base_branch
    rows = []
    candidates = []
    cache = {}
    paths = sorted(Path("recipes").glob("*/build.y*ml"))
    if args.recipes:
        unknown = set(args.recipes) - {path.parent.name for path in paths}
        if unknown:
            parser.error(f"unknown recipes: {', '.join(sorted(unknown))}")
        paths = [path for path in paths if path.parent.name in args.recipes]
    for path in paths:
        row = {
            "recipe": path.parent.name,
            "current": "",
            "upstream": "",
            "status": "error",
            "detail": "",
        }
        rows.append(row)
        try:
            data = yaml.safe_load(path.read_text())
            if not isinstance(data, dict):
                raise ValueError("recipe must be a YAML mapping")
            row["current"] = str(data.get("version", ""))
            validate_update_policy(data, recipe_path=path)
            config = data["auto_update"]
            if config["method"] == "sources":
                plan = plan_sources(path, session)
                if plan is None:
                    row.update(status="current" if config.get("sources") else "repository", detail="Repository changes trigger candidate builds." if "local" in config else "")
                else:
                    release = UpstreamRelease(plan.next_version, plan.fingerprint, ", ".join(plan.upstream_urls))
                    row.update(status="available", upstream=plan.next_version, detail="; ".join(plan.changes))
                    candidates.append((path, data, release, plan.next_version, row, plan))
                print(f"{row['recipe']}: {row['status']} {row['detail']}", flush=True)
                continue
            if config["method"] == "manual":
                row.update(status="manual", detail=config["reason"])
                continue
            key = json.dumps(config, sort_keys=True)
            if key not in cache:
                try:
                    cache[key] = latest_version(config, session)
                except (requests.RequestException, ValueError) as exc:
                    cache[key] = exc
            release = cache[key]
            if isinstance(release, Exception):
                raise release
            if release is None:
                raise ValueError(
                    "no stable version found upstream; check source and version filter"
                )
            row["upstream"] = release.version
            row["tag"] = release.tag
            row["url"] = release.url
            if config.get("mode") == "notify":
                row.update(status="needs-recipe-change", detail=config["reason"])
                continue
            comparison = newer(row["current"], release.version)
            if comparison is None:
                raise ValueError(
                    "cannot compare the recipe version with the upstream version"
                )
            if not comparison:
                row["status"] = "current"
                continue
            new_version = tag_to_recipe_version(release.version, row["current"])
            if not new_version:
                raise ValueError("upstream version is not safe for a recipe bump")
            row["status"] = "available"
            candidates.append((path, data, release, new_version, row, None))
        except Exception as exc:
            row.update(status="error", detail=f"{type(exc).__name__}: {exc}")
        print(f"{row['recipe']}: {row['status']} {row['detail']}", flush=True)

    opened = 0
    open_branches = None
    for path, data, release, new_version, row, plan in candidates:
        try:
            branch = plan.branch if plan else f"auto-update/{path.parent.name}-{new_version}"
            if REPO:
                if plan:
                    if open_branches is None:
                        open_branches = open_update_branches()
                    prefix = f"auto-update/{path.parent.name}-{new_version}-"
                    existing = next((item for item in sorted(open_branches) if item.startswith(prefix)), None)
                    if existing:
                        row.update(status="pr-open", detail=existing)
                        continue
                state = pull_request_state(branch)
                if state:
                    row.update(
                        status=f"pr-{state}",
                        detail=(
                            "Closed without merging; reopen the PR to retry this version."
                            if state == "closed"
                            else branch
                        ),
                    )
                    continue
            if args.max_prs and opened >= args.max_prs:
                row.update(
                    status="deferred",
                    detail="New PR limit reached; retried on the next run.",
                )
                continue
            result = submit_bump(
                str(path),
                path.parent.name,
                row["current"],
                new_version,
                (
                    data["auto_update"]["repo"]
                    if data["auto_update"]["method"].startswith("github_")
                    else ""
                ),
                release.tag,
                base_branch,
                args.dry_run,
                release_url=release.url,
                **({"plan": plan} if plan else {}),
            )
            if result is None:
                raise ValueError(
                    "could not prepare the recipe/fulltest bump; inspect the updater log"
                )
            if result == "opened":
                opened += 1
                row["status"] = "would-open" if args.dry_run else "opened"
            else:
                row["status"] = f"pr-{result}"
        except Exception as exc:
            row.update(status="error", detail=f"{type(exc).__name__}: {exc}")
    write_report(rows, args.json)
    return int(not rows or any(row["status"] == "error" for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
