import argparse
import glob
import os
import re
import subprocess
import traceback

import requests
import yaml
from packaging import version

DEBUG = True  # change it to true if wanna see detailed process


def dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)


TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPOSITORY")
session = requests.Session()
session.headers.update({"Accept": "application/vnd.github+json"})
if TOKEN:
    session.headers.update({"Authorization": f"Bearer {TOKEN}"})
    print("GITHUB_TOKEN set status: YES")
else:
    print("GITHUB_TOKEN set status: NO")

print(f"GITHUB_REPOSITORY={REPO}")


def latest_stable(repo):
    dbg(f"Query releases for {repo}")
    try:
        response = session.get(
            f"https://api.github.com/repos/{repo}/releases", timeout=20
        )
        dbg("GET releases status:", response.status_code)
        if response.status_code == 200:
            for rel in response.json():
                dbg(
                    "  release:",
                    {
                        "tag": rel.get("tag_name"),
                        "draft": rel.get("draft"),
                        "pre": rel.get("prerelease"),
                    },
                )
                if not rel.get("draft") and not rel.get("prerelease"):
                    tag = rel.get("tag_name") or rel.get("name")
                    dbg("  picked stable release tag:", tag)
                    return tag
        elif response.status_code == 404:
            dbg("No releases endpoint (404), will fallback to tags.")
        else:
            dbg("Releases request unexpected:", response.text[:300])
    except Exception as e:
        dbg("Releases request error:", e)
        dbg(traceback.format_exc())

    # fallback: tags
    dbg(f"Fallback to tags for {repo}")
    try:
        response = session.get(f"https://api.github.com/repos/{repo}/tags", timeout=20)
        dbg("GET tags status:", response.status_code)
        if response.status_code == 200:
            data = response.json()
            if data:
                dbg("Top tag:", data[0].get("name"))
                return data[0]["name"]
            else:
                dbg("No tags found.")
        else:
            dbg("Tags request unexpected:", response.text[:300])
    except Exception as e:
        dbg("Tags request error:", e)
        dbg(traceback.format_exc())

    return None


# return true if have newer version,false if is up to date and none if need manual check
def newer(current_version, upstream_version):
    def clean(ver_str: str) -> str:
        ver_str = (ver_str or "").strip()
        ver_str = ver_str.lstrip("vV")
        ver_str = ver_str.replace("_", ".")
        ver_str = ver_str.split("+", 1)[0]  # +meta
        ver_str = ver_str.split("-", 1)[0]  # -suffix
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
            f"https://api.github.com/repos/{repo}/git/ref/tags/{tag}", timeout=20
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


VERSION_LINE = re.compile(
    r"^version:[ \t]*(?P<value>.*?)[ \t]*(?P<comment>#.*)?$", re.M
)
REVISION_LINE = re.compile(
    r"^(?P<indent>[ \t]*)revision:[ \t]*"
    r"(?P<quote>[\"']?)(?P<sha>[0-9a-fA-F]{7,40})(?P=quote)[ \t]*$",
    re.M,
)


def rewrite_version(text, new_version):
    """Replace the top-level version field so it reads back as the same string.

    Original quoting is preserved where it survives a round trip. It often does
    not: `version: 1.9` is a float, and bumping it to an unquoted 1.10 would
    reload as 1.1. Most recipes leave the version unquoted, so the fallback to
    an explicitly quoted scalar is the common path for any two-part version.
    """
    match = VERSION_LINE.search(text)
    if not match:
        return None
    raw = match.group("value")
    quote = raw[0] if raw[:1] in ("'", '"') else ""
    comment = match.group("comment")
    trailer = f"  {comment}" if comment else ""

    for candidate_quote in (quote, '"'):
        updated = (
            f"{text[:match.start()]}version: "
            f"{candidate_quote}{new_version}{candidate_quote}{trailer}"
            f"{text[match.end():]}"
        )
        try:
            reloaded = yaml.safe_load(updated)
        except Exception as e:
            dbg("rewrite_version reload failed:", e)
            continue
        if isinstance(reloaded, dict) and str(reloaded.get("version")) == new_version:
            return updated
    return None


def revisions_owned_by(text, repo):
    """Return the pinned shas that sit in a mapping also naming the upstream repo.

    Proximity in the raw text is not evidence of ownership: the auto_update.repo
    line is itself usually within a few hundred characters of the revision, so a
    character window matches almost anything. Ownership is a structural claim --
    the sha and the repo URL have to be siblings in the same YAML mapping -- so
    it is decided on the parsed document.
    """
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


def issue_exists(fp):
    if not REPO:
        dbg("Skip issue_exists: REPO is not set.")
        return False
    # The fingerprint is only ever written into the issue body, so searching
    # in:title never matches and every recurring failure opens a fresh issue.
    q = f'repo:{REPO} in:body "{fp}" state:open'
    dbg("Search issues query:", q)
    try:
        response = session.get(
            "https://api.github.com/search/issues", params={"q": q}, timeout=20
        )
        dbg("Search issues status:", response.status_code)
        if response.status_code == 200:
            count = response.json().get("total_count", 0)
            dbg("Open issues with fp count:", count)
            return count > 0
        else:
            dbg("Search issues unexpected:", response.text[:300])
            return False
    except Exception as e:
        dbg("Search issues error:", e)
        dbg(traceback.format_exc())
        return False


DRY_RUN = False


def open_issue(title, body, labels=None):
    if labels is None:
        labels = ["auto-update"]
    if DRY_RUN:
        print(f"=== dry run: would open issue === {title}")
        return
    if not REPO:
        print("GITHUB_REPOSITORY not set; skip creating issue.")
        return
    print("=== opening issue ===")
    print("Title:", title)
    print("Body:\n", body)
    print("Labels:", labels)
    print("========================")
    response = session.post(
        f"https://api.github.com/repos/{REPO}/issues",
        json={"title": title, "body": body, "labels": labels},
        timeout=20,
    )
    response.raise_for_status()


def open_invalid_recipe_issue(path, name, reason, extra=None, labels=None):
    if labels is None:
        labels = ["auto-update", "invalid-recipe"]
    extra = extra or {}

    fp = f"{path} :: {reason}"
    if issue_exists(fp):
        print(f"duplicate invalid-recipe issue already open for: {fp}")
        return

    title = f"[invalid] {name}: {reason}"
    body = (
        f"- Recipe: {path}\n"
        f"- Name: {name}\n"
        f"- Reason: {reason}\n"
        + "".join(f"- {k}: {v}\n" for k, v in extra.items())
        + f"\nFingerprint: {fp}"
    )
    try:
        open_issue(title, body, labels=labels)
    except Exception as e:
        print(f"Failed to open invalid-recipe issue for {path}: {e}")


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
    result = subprocess.run(
        ["git", *cmd], check=False, capture_output=True, text=True
    )
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


def pull_request_exists(branch):
    if not REPO:
        return False
    owner = REPO.split("/", 1)[0]
    try:
        response = session.get(
            f"https://api.github.com/repos/{REPO}/pulls",
            params={"state": "all", "head": f"{owner}:{branch}"},
            timeout=20,
        )
        if response.status_code != 200:
            dbg("List pulls unexpected:", response.text[:300])
            return False
        return bool(response.json())
    except Exception as e:
        dbg("List pulls error:", e)
        return False


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
            return None, f"recipe pins a commit revision but tag {tag} could not be resolved"
        updated, revision_changes = rewrite_revision(updated, repo, commit)
        if not revision_changes:
            return None, (
                "recipe pins a commit revision that could not be matched to "
                f"{repo}; bump it by hand"
            )
        for old_sha, new_sha in revision_changes:
            changes.append(f"`revision`: `{old_sha}` → `{new_sha}`")

    if updated == original:
        return None, "no textual change"
    return updated, changes


def submit_bump(path, name, current_version, new_version, repo, tag, base_branch, dry_run):
    branch = f"auto-update/{name}-{new_version}"

    orphan_branch = False
    if not dry_run:
        if pull_request_exists(branch):
            print(f"a pull request already exists for {branch}; skipping.")
            return "exists"
        if remote_branch_exists(branch):
            # Pushed, but the pull request call never succeeded. Skipping here
            # would strand the recipe: the branch keeps the bump from being
            # retried, and no pull request ever carries it.
            print(f"branch {branch} exists with no pull request; opening one for it.")
            orphan_branch = True

    updated, result = prepare_bump(path, current_version, new_version, repo, tag)
    if updated is None:
        print(f"cannot auto-bump {path}: {result}")
        return None
    changes = result

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
        f"- Upstream repo: https://github.com/{repo}",
        f"- Upstream release: https://github.com/{repo}/releases/tag/{tag}",
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
        with open(path, "w", encoding="utf-8") as f:
            f.write(updated)
        git("add", "--", path)
        git("commit", "-m", title)
        git("push", "origin", branch)
        open_pull_request(branch, base_branch, title, body, labels=["auto-update"])
    finally:
        git("checkout", "--force", base_branch, check=False)
        git("clean", "-fd", "--", os.path.dirname(path) or ".", check=False)
    return "opened"


def open_manual_issue(path, name, current_version, tag, repo, note):
    fp = f"{path} -> {tag} (manual-verify)"
    print("Fingerprint:", fp)
    if issue_exists(fp):
        print("duplicate issue already open for this fingerprint (manual verify).")
        return
    title = f"[manual] Verify upstream version for {name}: current={current_version}, upstream_tag={tag}"
    body = (
        f"- Recipe: {path}\n"
        f"- Current version: {current_version}\n"
        f"- Upstream tag: {tag}\n"
        f"- Repo: {repo}\n\n"
        f"{note}\n\n"
        f"Fingerprint: {fp}"
    )
    try:
        open_issue(title, body, labels=["auto-update", "manual-review"])
    except Exception as e:
        print(f"Failed to open manual-review issue for {path}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Check recipes with an auto_update block against upstream releases "
        "and open a version-bump pull request when one is available."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.getenv("AUTO_UPDATE_DRY_RUN") == "1",
        help="report what would change without touching git or the GitHub API",
    )
    parser.add_argument(
        "--max-prs",
        type=int,
        default=int(os.getenv("AUTO_UPDATE_MAX_PRS") or 5),
        help="stop after opening this many pull requests in one run (0 = no limit). "
        "Every bump PR triggers a full container build, so a backlog is worked "
        "through over several weekly runs rather than all at once.",
    )
    args = parser.parse_args()
    global DRY_RUN
    DRY_RUN = dry_run = args.dry_run
    opened = 0
    deferred = []

    base_branch = "main"
    if not dry_run:
        head = git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
        # A detached checkout reports "HEAD", which is not a branch the pull
        # request API will accept as a base.
        base_branch = head if head and head != "HEAD" else "main"
    print(f"Base branch: {base_branch} (dry_run={dry_run})")

    files = glob.glob("recipes/**/*.y*ml", recursive=True)
    print("Files matched:", len(files))
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except Exception:
            print("YAML load error")
            continue
        if not isinstance(data, dict):
            continue
        name = data.get("name", os.path.basename(path))
        au = data.get("auto_update")
        if not isinstance(au, dict):
            continue

        method = au.get("method")
        repo = au.get("repo")
        if method != "github_release":
            open_invalid_recipe_issue(
                path, name, "unsupported auto_update.method", {"method": repr(method)}
            )
            continue
        if not repo:
            open_invalid_recipe_issue(path, name, "auto_update.repo missing")
            continue

        cur = str(data.get("version", "")).strip()
        if not cur:
            open_invalid_recipe_issue(path, name, "version missing")
            continue
        print(f"Handling file: {path}")
        print(f"Check: name={name}, current_version={cur}, upstream_repo={repo}")
        up = latest_stable(repo)
        print("Upstream tag got:", up)
        if not up:
            print("no upstream tag/release")
            continue
        cmp = newer(cur, up)

        if cmp is None:
            open_manual_issue(
                path,
                name,
                cur,
                up,
                repo,
                "Packaging cannot parse one/both versions after cleaning. "
                "Please verify manually.",
            )
            continue

        if not cmp:
            print("current version is Up-to-date.")
            continue

        new_version = tag_to_recipe_version(up, cur)
        if not new_version:
            open_manual_issue(
                path,
                name,
                cur,
                up,
                repo,
                "The upstream tag does not map cleanly onto a recipe version, so no "
                "pull request was opened. Please bump the recipe manually.",
            )
            continue

        if args.max_prs and opened >= args.max_prs:
            deferred.append(f"{name} {cur} -> {new_version}")
            print(f"reached --max-prs={args.max_prs}; deferring {name} to a later run.")
            continue

        try:
            submitted = submit_bump(
                path, name, cur, new_version, repo, up, base_branch, dry_run
            )
        except Exception as e:
            print(f"Failed to open bump PR for {path}: {e}")
            print(traceback.format_exc())
            submitted = None

        if submitted == "opened":
            opened += 1

        if submitted is None and not dry_run:
            open_manual_issue(
                path,
                name,
                cur,
                up,
                repo,
                "An automatic version-bump pull request could not be created. "
                "Please bump the recipe manually.",
            )


    print(f"\nPull requests opened this run: {opened}")
    if deferred:
        print(f"Deferred to a later run ({len(deferred)}):")
        for item in deferred:
            print(" -", item)


if __name__ == "__main__":
    main()
