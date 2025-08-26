import os
import yaml
import subprocess
import requests
import argparse
from copy import deepcopy

# Try pulling the Docker image. Return True if successful, False otherwise.
def is_image_valid(image):
    try:
        print(f" Checking Docker image: {image}")
        result = subprocess.run(["docker", "pull",image], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        if result.returncode == 0:
            print(" Docker image is valid.")
            return True
        else:
            print(" Docker image pull failed.")
            return False
    except Exception as e:
        print(f" Error during image pull: {e}")
        return False

#Guess GitHub repo URL from a Docker image name using GitHub Search API.
#Tries known orgs first (bids, bids-apps, etc), then falls back to global search.
def guess_github_url(image_name):
    if ":" in image_name:
        image_name = image_name.split(":")[0]

    parts = image_name.split("/")
    if len(parts) == 2:
        org, repo = parts
    else:
        repo = image_name
        org = None

    # trustable orgnizations
    trusted_orgs = ["bids", "bids-apps", "pennlinc", "neurodocker", "nipreps", "akhanf"]

    headers = {
        "Accept": "application/vnd.github+json",
        # "Authorization": "Bearer YOUR_GITHUB_TOKEN"
    }

    # Step 1: Try specific searches from trusted orgs
    for candidate_org in ([org] if org else []) + trusted_orgs:
        query = f"{repo}+in:name+user:{candidate_org}"
        try:
            response = requests.get(
                f"https://api.github.com/search/repositories?q={query}",
                headers=headers,
                timeout=10
            )
            if response.status_code == 200:
                results = response.json()
                if results.get("total_count", 0) > 0:
                    top = results["items"][0]
                    print(f" GitHub match in user:{candidate_org}: {top['full_name']}")
                    return top["html_url"]
            else:
                print(f" GitHub API error: {response.status_code}")
        except Exception as e:
            print(f" GitHub API request failed: {e}")

    # Step 2: Global fallback 
    query = f"{repo}+in:name"
    print(f" Fallback search GitHub for: {query}")
    try:
        response = requests.get(
            f"https://api.github.com/search/repositories?q={query}",
            headers=headers,
            timeout=10
        )
        if response.status_code == 200:
            results = response.json()
            if results.get("total_count", 0) > 0:
                for item in results["items"]:
                    if item["owner"]["login"].lower() in [o.lower() for o in trusted_orgs]:
                        print(f" GitHub global trusted match: {item['full_name']}")
                        return item["html_url"]
                top = results["items"][0]
                print(f" No trusted org match, using top result: {top['full_name']}")
                return top["html_url"]
            else:
                print(" No matching repos found globally.")
        else:
            print(f" GitHub API error: {response.status_code}")
    except Exception as e:
        print(f" GitHub API request failed: {e}")

    return None


def _merge_env(old_env, new_env):
    out = dict(old_env or {})
    out.update(new_env or {})
    return out

   # Use ubuntu LTS base; clone repo; ; merge env/directives safely.
def patch_yaml_to_use_github(data, github_url):
    y = deepcopy(data)
    y.setdefault("build", {})
    y["build"]["base-image"] = "ubuntu:24.04"
    y["build"]["pkg-manager"] = "apt"

    repo_name = github_url.rstrip("/").split("/")[-1]

    y["build"]["env"] = _merge_env(
        y["build"].get("env"),
        {
            "DEPLOY_PATH": f"/opt/{repo_name}/bin",
            "PATH": f"$PATH:/opt/{repo_name}/bin"
        }
    )

    directives = list(y["build"].get("directives", []))
    directives += [
        {"install": ["git", "curl", "ca-certificates", "python3", "python3-pip"]},
        {"run": ["useradd -m -u 1000 neuro || true"]},  
        {"workdir": "/opt"},
        {"run": [
            f"git clone --depth 1 {github_url}.git {repo_name}",
            f"mkdir -p /data /output"
        ]},
        {"run": [
            f"bash -lc \"printf '#!/usr/bin/env bash\\nset -euo pipefail\\n' > /usr/local/bin/smoke-{repo_name}.sh\"",
            f"bash -lc \"echo '{repo_name} --version || {repo_name} --help' >> /usr/local/bin/smoke-{repo_name}.sh\"",
            f"bash -lc \"echo '{repo_name} --help > /dev/null' >> /usr/local/bin/smoke-{repo_name}.sh\"",
            f"chmod +x /usr/local/bin/smoke-{repo_name}.sh"
        ]},
        {"entrypoint": "bash"},
        {"user": "neuro"},
    ]
    y["build"]["directives"] = directives

    deploy = y.get("deploy", {})
    paths = set(deploy.get("path", [])) | {f"/opt/{repo_name}", f"/opt/{repo_name}/bin"}
    y["deploy"] = {**deploy, "path": sorted(paths)}

    return y

def auto_rebuild_with_github(yaml_path="./build.yaml", output_path="build_patched.yaml"):
    if not os.path.exists(yaml_path):
        print(f" File not found: {yaml_path}")
        return

    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)

    base_image = data["build"]["base-image"]

    if is_image_valid(base_image):
        print(" No need to patch. Base image works.")
        return

    github_url = guess_github_url(base_image)
    if github_url:
        print(f" Guessed GitHub repo: {github_url}")
        patched_data = patch_yaml_to_use_github(data, github_url)

        with open(output_path, "w") as out:
            yaml.dump(patched_data, out, sort_keys=False)
        print(f" Patched YAML written to {output_path}")
    else:
        print(" Failed to guess GitHub repo. Please fix manually.")

#set the command "input"  and output
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Patch build.yaml to rebuild from GitHub when base image is invalid."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input YAML file (e.g., recipes/bidsappaa/build.yaml)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="path to output modified file "
    )
    args = parser.parse_args()

    auto_rebuild_with_github(yaml_path=args.input,output_path=args.output)
   
