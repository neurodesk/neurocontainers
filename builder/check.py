#!/usr/bin/env python3
import os, glob, yaml, requests, traceback
from packaging import version as V

DEBUG = False  # change it to true if wanna see detailed process

def dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)

TOKEN = os.getenv("GITHUB_TOKEN")
REPO  = os.getenv("GITHUB_REPOSITORY")  
S = requests.Session()
S.headers.update({"Accept": "application/vnd.github+json"})
if TOKEN:
    S.headers.update({"Authorization": f"Bearer {TOKEN}"})
    print("GITHUB_TOKEN set status: YES" )
else:
    print("GITHUB_TOKEN set status: NO" )

print(f"GITHUB_REPOSITORY={REPO}")


def latest_stable(repo):
    dbg(f"Query releases for {repo}")
    try:
        r = S.get(f"https://api.github.com/repos/{repo}/releases", timeout=20)
        dbg("GET releases status:", r.status_code)
        if r.status_code == 200:
            for rel in r.json():
                dbg("  release:", {"tag": rel.get("tag_name"), "draft": rel.get("draft"), "pre": rel.get("prerelease")})
                if not rel.get("draft") and not rel.get("prerelease"):
                    tag = rel.get("tag_name") or rel.get("name")
                    dbg("  picked stable release tag:", tag)
                    return tag
        elif r.status_code == 404:
            dbg("No releases endpoint (404), will fallback to tags.")
        else:
            dbg("Releases request unexpected:", r.text[:300])
    except Exception as e:
        dbg("Releases request error:", e)
        dbg(traceback.format_exc())

    # fallback: tags
    dbg(f"Fallback to tags for {repo}")
    try:
        r = S.get(f"https://api.github.com/repos/{repo}/tags", timeout=20)
        dbg("GET tags status:", r.status_code)
        if r.status_code == 200:
            data = r.json()
            if data:
                dbg("Top tag:", data[0].get("name"))
                return data[0]["name"]
            else:
                dbg("No tags found.")
        else:
            dbg("Tags request unexpected:", r.text[:300])
    except Exception as e:
        dbg("Tags request error:", e)
        dbg(traceback.format_exc())

    return None

def newer(a, b):
    na, nb = (a or "").lstrip("v"), (b or "").lstrip("v")
    try:
        result = V.parse(nb) > V.parse(na)
        dbg(f"Version compare: current={na} upstream={nb} -> upstream_is_newer={result}")
        return result
    except Exception as e:
        dbg("packaging.version parse failed, fallback to string compare:", e)
        result = nb > na
        dbg(f"String compare: current={na} upstream={nb} -> upstream_is_newer={result}")
        return result

def issue_exists(fp):
    if not REPO:
        dbg("Skip issue_exists: REPO is not set.")
        return False
    q = f'repo:{REPO} in:title "{fp}" state:open'
    dbg("Search issues query:", q)
    try:
        r = S.get("https://api.github.com/search/issues", params={"q": q}, timeout=20)
        dbg("Search issues status:", r.status_code)
        if r.status_code == 200:
            count = r.json().get("total_count", 0)
            dbg("Open issues with fp count:", count)
            return count > 0
        else:
            dbg("Search issues unexpected:", r.text[:300])
            return False
    except Exception as e:
        dbg("Search issues error:", e)
        dbg(traceback.format_exc())
        return False

def open_issue(title, body):
    print("=== Would open issue ===")
    print("Title:", title)
    print("Body:\n", body)
    print("========================")
    r = S.post(f"https://api.github.com/repos/{REPO}/issues",
    json={"title": title, "body": body, "labels": ["auto-update"]}, timeout=20)
    r.raise_for_status()



if __name__ == "__main__":
    files = glob.glob("../recipes/**/*.y*ml", recursive=True)
    #print("Files matched:", files)
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except Exception as e:
            print("YAML load error")
            continue
        if not isinstance(data, dict):
            continue

        au = data.get("auto_update") or {}
        if not isinstance(au, dict):
            print("auto_update missing or not a dict.")
            continue

        method = au.get("method")
        repo   = au.get("repo")
        if method != "github_release":
            #the operation of other methods like pypi, conda, git_tag ...
            continue
        if not repo:
            print("auto_update.repo missing")
            continue

        name = data.get("name", os.path.basename(path))
        cur  = str(data.get("version", "")).strip()
        if not cur:
            print("version missing")
            continue
        print(f"Handling file: {path}")
        print(f"Check: name={name}, current_version={cur}, upstream_repo={repo}")
        up = latest_stable(repo)
        print("Upstream tag got:", up)
        if not up:
            print("no upstream tag/release")
            continue

        if not newer(cur, up):
            print(f"current version is Up-to-date ).")
            continue

        fp = f"{path} -> {up}"
        print("Fingerprint:", fp)
        if issue_exists(fp):
            print("duplicate issue already open for this fingerprint.")
            continue

        title = f"{name} {cur} may update to {up}"
        body  = f"-Recipe: {path} -Current version: {cur} -Upstream version: {up} -Repo:{repo}\n"
        open_issue(title, body)
