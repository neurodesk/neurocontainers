import os, glob, yaml, requests, traceback
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


def issue_exists(fp):
    if not REPO:
        dbg("Skip issue_exists: REPO is not set.")
        return False
    q = f'repo:{REPO} in:title "{fp}" state:open'
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


def open_issue(title, body, labels=None):
    if labels is None:
        labels = ["auto-update"]
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


if __name__ == "__main__":
    manual_review_summary = []
    files = glob.glob("recipes/**/*.y*ml", recursive=True)
    print("Files matched:", files)
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except Exception as e:
            print("YAML load error")
            continue
        if not isinstance(data, dict):
            continue
        name = data.get("name", os.path.basename(path))
        au = data.get("auto_update")
        if not isinstance(au, dict):
            print(path, name, "auto_update missing or not a dict")
            continue    
        method = au.get("method")
        ## method of github_release
        if method == "github_release":
         repo = au.get("repo")
         cur = str(data.get("version", "")).strip()
         if not repo:
                open_invalid_recipe_issue(path, name, "auto_update.repo missing")
                continue
         if not cur:
            open_invalid_recipe_issue(path, name, "version missing")
            continue
         print(f"Handling file: {path}")
         print(f"Check: name={name}, current_version={cur}, upstream_repo={repo}")
         up = latest_stable(repo)
         if not up:
            print("no upstream tag/release")
            continue
         else:
             print("Upstream tag got:", up)
             
         cmp = newer(cur, up)
         if cmp is None:
            # manual check if version strings cannot be compared
            fp = f"{path} -> {up} (manual-verify)"
            print("Fingerprint:", fp)
            if issue_exists(fp):
                print("duplicate issue already open for this fingerprint (manual verify).")
            else:
                title = f"[manual] Verify upstream version for {name}: current={cur}, upstream_tag={up}"
                body = (
                    f"- Recipe: {path}\n"
                    f"- Current version: {cur}\n"
                    f"- Upstream tag: {up}\n"
                    f"- Repo: {repo}\n\n"
                    "Packaging cannot parse one/both versions after cleaning. Please verify manually."
                )
                open_issue(title, body, labels=["auto-update", "manual-review"])

         elif cmp is False:
            print("current version is Up-to-date.")

         # detect newer version update
         else:
             fp = f"{path} -> {up}"
             print("Fingerprint:", fp)
             if issue_exists(fp):
                 print("duplicate issue already open for this fingerprint.")
             else:
                title = f"{name} {cur} may update to {up}"
                body = (
                    f"- Recipe: {path}\n"
                    f"- Current version: {cur}\n"
                    f"- Upstream version: {up}\n"
                    f"- Repo: {repo}\n"
                )
                open_issue(title, body, labels=["auto-update"])

        elif method == "manual_review":
           information = au.get("information")
           cur = str(data.get("version", "")).strip()
           manual_review_summary.append(
                    {
                        "Name": name,
                        "Current Version": cur,
                        "Information": information,
                    })
           if not information:
                open_invalid_recipe_issue(path, name, "auto_update.information missing for manual_review")
                continue

    # After the loop, if there's a manual review summary, raise an issue
    if manual_review_summary:
        # Calculate column widths for a pretty table
        col_widths = {
            "Name": len("Name"),
            "Current Version": len("Current Version"),
            "Information": len("Information"),
        }
        for item in manual_review_summary:
            col_widths["Name"] = max(col_widths["Name"], len(item.get("Name", "")))
            col_widths["Current Version"] = max(col_widths["Current Version"], len(item.get("Current Version", "")))
            col_widths["Information"] = max(col_widths["Information"], len(str(item.get("Information", ""))) + 2) # +2 for backticks

        summary_title = "[auto-update] Manual Review Summary"
        summary_body_lines = [
            "The following recipes require manual review:",
            "",
            f"| {'Name'.ljust(col_widths['Name'])} | {'Current Version'.ljust(col_widths['Current Version'])} | {'Information'.ljust(col_widths['Information'])} |",
            f"|{'-' * (col_widths['Name'] + 2)}|{'-' * (col_widths['Current Version'] + 2)}|{'-' * (col_widths['Information'] + 2)}|",
        ]
        for item in manual_review_summary:
            name = item.get("Name", "N/A")
            version = item.get("Current Version", "N/A")
            # Wrap info in backticks for better rendering and to avoid breaking the table
            info = f"`{item.get('Information', '')}`"

            summary_body_lines.append(
                f"| {name.ljust(col_widths['Name'])} | {version.ljust(col_widths['Current Version'])} | {info.ljust(col_widths['Information'])} |"
            )
        summary_body_lines.append("")
        summary_body_lines.append("Please review these items and update the recipes as needed.")
        summary_body = "\n".join(summary_body_lines)

        # Check if a similar summary issue already exists to avoid duplicates
        fp_summary = "[auto-update] Manual Review Summary"
        if not issue_exists(fp_summary):
            open_issue(summary_title, summary_body, labels=["auto-update", "manual-review-summary"])
        else:
            print("Duplicate manual review summary issue already open.")
           
    
            


        