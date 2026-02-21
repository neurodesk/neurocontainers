#!/usr/bin/env python3
import yaml
import subprocess
import os
import sys
import urllib.request
import argparse
import shutil
import jinja2
import platform
import hashlib
import typing
import json
import datetime
import re
import tempfile
from pathlib import Path

# add the parent directory to the path to import builder modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

GLOBAL_MOUNT_POINT_LIST = [
    "/afm01",
    "/afm02",
    "/cvmfs",
    "/90days",
    "/30days",
    "/QRISdata",
    "/RDS",
    "/data",
    "/short",
    "/proc_temp",
    "/TMPDIR",
    "/nvme",
    "/neurodesktop-storage",
    "/local",
    "/gpfs1",
    "/working",
    "/winmounts",
    "/state",
    "/tmp",
    "/autofs",
    "/cluster",
    "/local_mount",
    "/scratch",
    "/clusterdata",
    "/nvmescratch",
]

ARCHITECTURES = {
    "x86_64": "x86_64",
    "arm64": "aarch64",
    "aarch64": "aarch64",
    "AMD64": "x86_64",  # Windows x86_64
    "ARM64": "aarch64",  # Windows ARM64
}

CONTAINER_TESTER_IMAGE = "neurocontainers/container-tester:latest"
CONTAINER_TESTER_BINARY_NAME = "tester"
CONTAINER_TESTER_MOUNT_PATH = "/tmp/neurocontainers-container-tester"
NEURODOCKER_PIP_PACKAGE = "neurodocker"


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def ensure_neurodocker_renderer():
    """
    Ensure NeuroDocker is importable.

    Set NEURODOCKER_AUTO_UPGRADE=0 to skip automatic upgrades.
    """
    neurodocker_package = os.environ.get(
        "NEURODOCKER_PIP_PACKAGE", NEURODOCKER_PIP_PACKAGE
    )
    auto_upgrade = _env_truthy("NEURODOCKER_AUTO_UPGRADE", default=True)

    install_error: Exception | None = None
    if auto_upgrade:
        try:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--no-cache-dir",
                    "--upgrade",
                    neurodocker_package,
                ]
            )
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            install_error = e
            print(
                f"Warning: failed to upgrade neurodocker package {neurodocker_package}: {e}"
            )

    try:
        from neurodocker.reproenv.renderers import DockerRenderer
    except ImportError as e:
        if install_error is not None:
            raise ImportError(
                "neurodocker is not installed and automatic upgrade failed. "
                f"Install it manually with: {sys.executable} -m pip install --no-cache-dir --upgrade {neurodocker_package}"
            ) from e
        raise ImportError(
            "neurodocker is not installed. "
            f"Install it with: {sys.executable} -m pip install --no-cache-dir --upgrade {neurodocker_package}"
        ) from e

    return DockerRenderer


def get_repo_path() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def get_cache_dir() -> str:
    # Get the cache directory
    cache_dir = os.path.join(os.path.expanduser("~"), ".cache", "neurocontainers")
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir


def get_recipe_commit_date(recipe_path: str) -> str:
    """
    Get the commit date of the recipe file in YYYYMMDD format.
    
    This ensures consistent build dates based on when the recipe was last modified,
    rather than when the container was built. This fixes timezone issues where
    builds crossing midnight would have inconsistent dates.
    
    Args:
        recipe_path: Path to the recipe directory
        
    Returns:
        Date string in YYYYMMDD format
    """
    # First check if BUILDDATE environment variable is set (from GitHub Actions)
    builddate_env = os.environ.get("BUILDDATE")
    if builddate_env:
        return builddate_env
    
    try:
        # Get the commit date of the build.yaml file
        build_yaml_path = os.path.join(recipe_path, "build.yaml")
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ad", "--date=format:%Y%m%d", "--", build_yaml_path],
            capture_output=True,
            text=True,
            check=True,
            cwd=get_repo_path()
        )
        commit_date = result.stdout.strip()
        if commit_date:
            return commit_date
    except (subprocess.CalledProcessError, FileNotFoundError):
        # If git command fails or git is not available, fall back to current date
        pass
    
    # Fallback to current date if git is not available or command fails
    return datetime.datetime.now().strftime("%Y%m%d")


def load_description_file(recipe_dir: str) -> typing.Any:
    # Load the description file
    description_file = os.path.join(recipe_dir, "build.yaml")
    if not os.path.exists(description_file):
        raise ValueError(f"Description file {description_file} does not exist.")

    with open(description_file, "r") as f:
        recipe_dict = yaml.safe_load(f)

    # Convert name and version to strings to handle YAML numeric parsing
    # YAML safe_load interprets values like "1.1" as floats, but we need strings
    if "name" in recipe_dict and recipe_dict["name"] is not None:
        recipe_dict["name"] = str(recipe_dict["name"])
    if "version" in recipe_dict and recipe_dict["version"] is not None:
        recipe_dict["version"] = str(recipe_dict["version"])

    # Validate the recipe using attrs schema
    try:
        import builder.validation as validation

        validation.validate_recipe_dict(recipe_dict)
    except ImportError:
        # If validation module is not available, skip validation
        print("Warning: Recipe validation module not available, skipping validation")
    except Exception as e:
        print(f"Warning: Recipe validation failed: {e}")
        # For now, continue with build but print warning
        # In the future, this could be made into a hard error

    return recipe_dict


_jinja_env = jinja2.Environment(undefined=jinja2.StrictUndefined)


def generate_release_file(
    name: str,
    version: str,
    recipe: dict,
    recipe_path: str = None,
) -> None:
    """
    Generate a release JSON file for the built container.

    Args:
        name: Container name
        version: Container version
        recipe: Recipe dictionary from build.yaml
        recipe_path: Path to the recipe directory (optional, for getting commit date)
    """
    if recipe is None:
        build_info = {}

    # Extract categories from build.yaml
    categories = recipe.get("categories", ["other"])

    apptainer_args = recipe.get("apptainer_args", [])

    # Extract GUI applications from build.yaml
    gui_apps = recipe.get("gui_apps", [])

    # Get build date from git commit or environment variable
    if recipe_path:
        build_date = get_recipe_commit_date(recipe_path)
    else:
        # Fallback: check environment variable first, then current date
        build_date = os.environ.get("BUILDDATE", datetime.datetime.now().strftime("%Y%m%d"))
    
    cli_app_name = f"{name} {version}"

    # Create release data structure
    release_data = {
        "apps": {
            cli_app_name: {
                "version": build_date,
                "exec": "",
                "apptainer_args": apptainer_args,
            }
        },
        "categories": categories,
    }

    # Add GUI apps from build.yaml
    for gui_app in gui_apps:
        gui_app_name = f"{gui_app['name']}-{name} {version}"
        release_data["apps"][gui_app_name] = {
            "version": build_date,
            "exec": gui_app["exec"],
            "apptainer_args": apptainer_args,
        }

    # Convert to JSON string for potential GitHub Actions use
    release_json = json.dumps(release_data, indent=2)

    # Check if running in GitHub Actions
    if os.environ.get("GITHUB_ACTIONS") == "true":
        # In GitHub Actions, output the release data for workflow use
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            with open(github_output, "a") as f:
                f.write(f"container_name={name}\n")
                f.write(f"container_version={version}\n")
                # For multiline output, use heredoc format
                f.write(f"release_file_content<<EOF\n{release_json}\nEOF\n")
        print(f"Generated release data for {name} {version} (GitHub Actions mode)")
    else:
        # Local development mode - write file directly
        repo_path = get_repo_path()
        releases_dir = os.path.join(repo_path, "releases", name)
        os.makedirs(releases_dir, exist_ok=True)

        # Write release file
        release_file = os.path.join(releases_dir, f"{version}.json")
        with open(release_file, "w") as f:
            f.write(release_json)

        print(f"Generated release file: {release_file}")


def should_generate_release_file(generate_release_flag: bool = False) -> bool:
    """
    Determine if release file should be generated based on environment.

    Args:
        generate_release_flag: Command line flag to force release generation

    Returns True if running in CI, auto-build mode, or flag is set.
    """
    # Check command line flag first
    if generate_release_flag:
        return True

    # Check for common CI environment variables
    ci_vars = ["CI", "GITHUB_ACTIONS", "GITLAB_CI", "TRAVIS", "CIRCLECI", "JENKINS_URL"]

    for var in ci_vars:
        if os.environ.get(var):
            return True

    # Check for auto-build mode (set via command line)
    return os.environ.get("AUTO_BUILD", "false").lower() == "true"


class NeuroDockerBuilder:
    def __init__(
        self, base_image: str, pkg_manager: str = "apt", add_default: bool = True
    ):
        self.renderer_dict = {
            "pkg_manager": pkg_manager,
            "instructions": [],
        }

        self.add_directive("from_", base_image=base_image)

        # Always set the root user for the neurocontainer installation.
        self.set_user("root")

        if add_default:
            self.add_directive("_default")

    def add_directive(self, directive: typing.Any, **kwargs: typing.Any):
        """
        Low level function to add a directive to the renderer_dict.
        Can also be used to add templates.
        :param directive: The name of the directive.
        :param kwargs: The keyword arguments for the directive.
        """
        self.renderer_dict["instructions"].append({"name": directive, "kwds": kwargs})

    def install_packages(self, packages: typing.List[str]):
        """
        Install packages using the specified package manager.
        :param packages: List of packages to install.
        """
        self.add_directive("install", pkgs=packages, opts=None)

    def run_command(self, command: str):
        """
        Run a command in the container.
        :param args: The command to run.
        """
        self.add_directive("run", command=command)

    def set_user(self, user: str):
        """
        Set the user for the container.
        :param user: The user to set.
        """
        self.add_directive("user", user=user)

    def set_workdir(self, path: str):
        """
        Set the working directory for the container.
        :param path: The path to set as the working directory.
        """
        self.add_directive("workdir", path=path)

    def set_entrypoint(self, entrypoint: str):
        """
        Set the entrypoint for the container.
        :param entrypoint: The entrypoint to set.
        """
        self.add_directive("entrypoint", args=[entrypoint])

    def set_environment(self, key: str, value: str):
        """
        Set an environment variable for the container.
        :param key: The name of the environment variable.
        :param value: The value of the environment variable.
        """
        self.add_directive("env", **{key: value})

    def copy(self, *args: str):
        """
        Copy files into the container.
        :param args: The files to copy.
        """
        source, destination = list(args[:-1]), args[-1]
        self.add_directive("copy", source=source, destination=destination)

    def generate(self) -> str:
        """
        Generate the NeuroDocker Dockerfile.
        :return: The generated Dockerfile as a string.
        """
        DockerRenderer = ensure_neurodocker_renderer()

        if (
            len(
                [
                    i
                    for i in self.renderer_dict["instructions"]
                    if i["name"] == "entrypoint"
                ]
            )
            == 0
            and len(
                [
                    i
                    for i in self.renderer_dict["instructions"]
                    if i["name"] == "_default"
                ]
            )
            > 0
        ):
            self.set_entrypoint("/neurodocker/startup.sh")

        r = DockerRenderer.from_dict(self.renderer_dict)

        return str(r)


class LocalBuildContext(object):
    def __init__(self, context, cache_id):
        self.context = context
        self.run_args = []
        self.mounted_cache = False
        self.cache_id = cache_id
        self.local_mounts = {}

    def try_mount_cache(self):
        target = "/.neurocontainer-cache/" + self.cache_id

        if self.mounted_cache:
            return target

        cache_dir = self.context.get_context_cache_dir(self.cache_id)

        cache_relpath = os.path.relpath(cache_dir, self.context.build_directory)

        self.run_args.append(
            f"--mount=type=bind,source={cache_relpath},target={target},readonly"
        )
        self.mounted_cache = True

        return target

    def try_mount_local(self, key):
        if key in self.local_mounts:
            return self.local_mounts[key]

        target = f"/.neurocontainer-local/{key}"
        self.run_args.append(
            f"--mount=type=bind,from={key},source=/,target={target},readonly"
        )
        self.local_mounts[key] = target
        return target

    def ensure_context_cached(self, cache_filename, guest_filename):
        # Check if the file is already cached
        context_cache_dir = self.context.get_context_cache_dir(self.cache_id)

        cached_file = os.path.join(context_cache_dir, guest_filename)
        if os.path.exists(cached_file):
            return guest_filename

        # if not then link it from the cache (skip in Pyodide to avoid large file copying)
        try:
            import sys

            if "pyodide" in sys.modules:
                # In Pyodide, skip copying large files
                return guest_filename
            else:
                os.link(cache_filename, cached_file)
        except FileNotFoundError:
            return None
        except AttributeError:
            # Fallback if os.link is not available
            return guest_filename

        # return the filename
        return guest_filename

    def get_file(self, filename):
        file_info = self.context.files.get(filename)
        if file_info is None:
            raise ValueError(f"File {filename} not found.")

        if "cached_path" in file_info:
            cache_dir = self.try_mount_cache()
            cache_filename = self.ensure_context_cached(
                file_info["cached_path"],
                filename,
            )
            if cache_filename is None:
                return None
            return cache_dir + "/" + cache_filename
        else:
            raise ValueError("File has no cached path or context path.")

    def get_local(self, key):
        if not self.has_local(key):
            raise ValueError(f"Local file {key} not found.")

        return self.try_mount_local(key)

    def has_local(self, key):
        return self.context.has_local(key)

    def methods(self):
        return {
            "get_file": self.get_file,
            "has_local": self.has_local,
            "get_local": self.get_local,
        }


def hash_obj(obj):
    # Hash the object using SHA256
    if isinstance(obj, str):
        obj = obj.encode("utf-8")
    elif isinstance(obj, dict):
        obj = yaml.dump(obj).encode("utf-8")
    elif isinstance(obj, list):
        obj = yaml.dump(obj).encode("utf-8")
    else:
        raise ValueError("Object type not supported.")

    return hashlib.sha256(obj).hexdigest()


class BuildContext(object):
    build_directory: str | None = None
    readme: str | None = None
    tag: str | None = None
    build_info: typing.Any | None = None
    build_kind: str | None = None
    dockerfile_name: str | None = None
    gpu: bool = False

    def __init__(self, base_path, recipe_path, name, version, arch, check_only):
        self.base_path = base_path
        self.recipe_path = recipe_path
        self.name = name
        self.version = version
        self.original_version = version
        self.arch = arch
        self.max_parallel_jobs = os.cpu_count()
        self.options = {}
        self.option_info = {}
        self.files = {}
        self.lint_error = False
        self.deploy_bins = []
        self.deploy_path = []
        self.top_level_deploy_bins = []
        self.top_level_deploy_path = []
        self.local_context = {}
        self.check_only = check_only
        self.skip_file_population = False

    def lint_fail(self, message):
        if self.lint_error:
            raise ValueError("lint failed: " + message)
        print("lint failed: " + message)

    def add_option(self, key, description="", default=False, version_suffix=""):
        self.options[key] = default
        self.option_info[key] = {
            "description": description,
            "default": default,
            "version_suffix": version_suffix,
        }

    def add_local_context(self, key, local_path):
        self.local_context[key] = local_path

    def has_local(self, key):
        return key in self.local_context

    def set_option(self, key, value):
        if key not in self.options:
            raise ValueError(f"Option {key} not found.")

        if value == "true":
            self.options[key] = True
            self.calculate_version()
        elif value == "false":
            self.options[key] = False
        else:
            raise ValueError(f"Value {value} not supported.")

    def calculate_version(self):
        version = self.original_version
        for key, value in self.options.items():
            version_suffix = self.option_info[key]["version_suffix"]
            if value and version_suffix != "":
                version += version_suffix

        self.version = version

    def set_max_parallel_jobs(self, max_parallel_jobs):
        self.max_parallel_jobs = max_parallel_jobs

    def get_context_cache_dir(self, cache_id):
        if self.build_directory is None:
            raise ValueError("Build directory not set.")

        cache_dir = os.path.join(self.build_directory, "cache", cache_id)
        if not os.path.exists(cache_dir) and not self.skip_file_population:
            os.makedirs(cache_dir)

        return cache_dir

    def render_template(self, template, locals=None, methods=None):
        tpl = _jinja_env.from_string(template)
        return tpl.render(
            context=self,
            arch=self.arch,
            parallel_jobs=self.max_parallel_jobs,
            local=locals,
            **(methods or {}),
        )

    def execute_condition(self, condition, locals=None):
        result = self.render_template("{{" + condition + "}}", locals=locals)
        return result == "True"

    def execute_template(self, obj, locals, methods=None):
        if type(obj) == str:
            try:
                return self.render_template(obj, locals=locals, methods=methods)
            except jinja2.exceptions.TemplateSyntaxError as e:
                raise ValueError(f"Template syntax error: {e} in {obj}")
        elif type(obj) == list:
            return [
                self.execute_template(o, locals=locals, methods=methods) for o in obj
            ]
        elif type(obj) == dict:
            if "try" in obj:
                for value in obj["try"]:
                    if self.execute_condition(value["condition"], locals=locals):
                        return self.execute_template(value["value"], locals=locals)

                raise NotImplementedError("Try not implemented.")
            else:
                # Handle regular dictionaries by processing each key-value pair
                return {
                    self.execute_template(
                        k, locals=locals, methods=methods
                    ): self.execute_template(v, locals=locals, methods=methods)
                    for k, v in obj.items()
                }
        elif obj is None or type(obj) in (int, float, bool):
            # Handle primitive types - return as-is
            return obj
        else:
            raise ValueError(f"Template object not supported: {type(obj)} - {obj}")

    def execute_template_string(self, obj: str, locals, methods=None) -> str:
        try:
            return self.render_template(obj, locals=locals, methods=methods)
        except jinja2.exceptions.TemplateSyntaxError as e:
            raise ValueError(f"Template syntax error: {e} in {obj}")

    def add_file(self, file, recipe_path, locals, check_only=False, dry_run=False):
        dry_run = dry_run or self.skip_file_population

        if self.build_directory is None and not dry_run:
            raise ValueError("Build directory not set.")

        name = self.execute_template_string(file["name"], locals=locals)

        if name == "":
            raise ValueError("File name cannot be empty.")

        output_filename = (
            os.path.join(self.build_directory, name)
            if self.build_directory is not None
            else name
        )

        if "url" in file:
            # Check if running in Pyodide environment
            import sys

            url = self.execute_template(file["url"], locals=locals)

            if dry_run:
                self.files[name] = {
                    "cached_path": get_cached_download_path(url),
                    "url": url,
                }
                return

            if "pyodide" in sys.modules:
                # In Pyodide, create a dummy file entry for check_only mode
                print(f"Pyodide environment: skipping file download for {file['url']}")
                self.files[name] = {
                    "cached_path": output_filename,
                    "url": url,
                }
            else:
                # download and cache the file
                cached_file = download_with_cache(
                    url,
                    check_only=check_only,
                    insecure=file.get("insecure", False),
                    retry=file.get("retry", 1),
                    curl_options=file.get("curl_options", ""),
                )

                if "executable" in file and file["executable"]:
                    os.chmod(output_filename, 0o755)

                self.files[name] = {
                    "cached_path": cached_file,
                    "url": url,
                }
        else:
            if "contents" in file:
                contents = self.execute_template_string(file["contents"], locals=locals)
                if not dry_run:
                    with open(output_filename, "w") as f:
                        f.write(contents)
            elif "filename" in file:
                base = os.path.abspath(recipe_path)
                filename = os.path.join(base, file["filename"])
                if not dry_run:
                    with open(output_filename, "wb") as f:
                        with open(filename, "rb") as f2:
                            f.write(f2.read())
            else:
                raise ValueError("File contents not found.")

            if not dry_run and "executable" in file and file["executable"]:
                os.chmod(output_filename, 0o755)

            self.files[name] = {
                "cached_path": output_filename,
                "url": None,
            }

    def file_exists(self, filename: str) -> bool:
        # First check in recipe directory, then in build directory
        if os.path.exists(os.path.join(self.recipe_path, filename)):
            return True

        if self.build_directory is None:
            raise ValueError("Build directory not set.")

        return os.path.exists(os.path.join(self.build_directory, filename))

    def generate_cache_id(self, directive: str) -> str:
        return "h" + directive[:8]

    def load_include_file(self, filename: str) -> typing.Any:
        filename = os.path.join(self.base_path, filename)

        if not os.path.exists(filename):
            raise ValueError(f"Include file {filename} not found.")

        with open(filename, "r") as f:
            return yaml.safe_load(f)

    def check_docker_image(self, image: str) -> str:
        if image == "":
            raise ValueError("Docker image cannot be empty.")

        if ":" not in image:
            self.lint_fail("Docker image must have a tag. Use <image>:<tag> format.")
            return image + ":latest"

        name, tag = image.split(":", 1)

        if name == "ubuntu":
            if tag not in ["16.04", "18.04", "20.04", "22.04", "24.04", "26.04"]:
                self.lint_fail(
                    "Ubuntu version not supported. Use 16.04, 18.04, 20.04, 22.04, 24.04 or 26.04."
                )

        return image

    def build_neurodocker(self, build_directive, locals):
        base_raw = self.execute_template(
            build_directive.get("base-image") or "", locals=locals
        )
        if not isinstance(base_raw, str):
            raise ValueError("Base image must be a string.")

        base = self.check_docker_image(base_raw)

        pkg_manager = self.execute_template(
            build_directive.get("pkg-manager") or "", locals=locals
        )
        if not isinstance(pkg_manager, str):
            raise ValueError("Package manager must be a string.")

        if base == "" or pkg_manager == "":
            raise ValueError("Base image or package manager cannot be empty.")

        add_default_template = build_directive.get("add-default-template", True)
        builder = NeuroDockerBuilder(
            base, pkg_manager, add_default_template
        )

        # Add the ll command as a convenience alias for ls -la
        builder.run_command("printf '#!/bin/bash\\nls -la' > /usr/bin/ll")
        builder.run_command("chmod +x /usr/bin/ll")

        # Create the global mount points
        builder.run_command("mkdir -p " + " ".join(GLOBAL_MOUNT_POINT_LIST))

        # Automatically install tzdata on Debian systems and set timezone to UTC
        # By default, tzdata installation follows add-default-template setting
        # but can be overridden explicitly with add-tzdata
        if pkg_manager == "apt" and build_directive.get("add-tzdata", add_default_template):
            # Set non-interactive frontend to avoid prompts
            builder.set_environment("DEBIAN_FRONTEND", "noninteractive")
            # Set timezone to UTC
            builder.set_environment("TZ", "UTC")
            # Install tzdata package
            builder.install_packages(["tzdata"])
            # Configure timezone to UTC
            builder.run_command(
                "ln -snf /usr/share/zoneinfo/UTC /etc/localtime && echo UTC > /etc/timezone"
            )

        def add_directive(directive, locals):
            if "condition" in directive:
                if not self.execute_condition(directive["condition"], locals=locals):
                    return []

            if "install" in directive:
                if type(directive["install"]) == str:
                    pkg_list = self.execute_template(
                        [
                            f
                            for f in directive["install"].replace("\n", " ").split(" ")
                            if f != ""
                        ],
                        locals=locals,
                    )
                    if not isinstance(pkg_list, list):
                        raise ValueError(
                            "Install directive must be a list of packages."
                        )
                    builder.install_packages(pkg_list)  # type: ignore
                elif type(directive["install"]) == list:
                    pkg_list = self.execute_template(
                        directive["install"], locals=locals
                    )
                    if not isinstance(pkg_list, list):
                        raise ValueError(
                            "Install directive must be a list of packages."
                        )
                    builder.install_packages(pkg_list)  # type: ignore
                else:
                    raise ValueError("Install directive must be a string or list.")
            elif "run" in directive:
                local = LocalBuildContext(
                    self, self.generate_cache_id(hash_obj(directive))
                )
                args = self.execute_template(
                    directive["run"],
                    locals=locals,
                    methods=local.methods(),
                )
                if not isinstance(args, list):
                    raise ValueError("Run directive must be a list of commands.")
                args = [arg for arg in args if arg != None]
                builder.run_command(
                    " ".join(local.run_args) + " " + " \\\n && ".join(args)  # type: ignore
                )
            elif "workdir" in directive:
                workdir = self.execute_template(directive["workdir"], locals=locals)
                if not isinstance(workdir, str):
                    raise ValueError("Workdir must be a string.")

                builder.set_workdir(workdir)
            elif "user" in directive:
                user = self.execute_template(directive["user"], locals=locals)
                if not isinstance(user, str):
                    raise ValueError("User must be a string.")

                builder.set_user(user)
            elif "entrypoint" in directive:
                entrypoint = self.execute_template(
                    directive["entrypoint"], locals=locals
                )
                if not isinstance(entrypoint, str):
                    raise ValueError("Entrypoint must be a string.")

                builder.set_entrypoint(entrypoint)
            elif "environment" in directive:
                if directive["environment"] == None:
                    raise ValueError("Environment must be a map of keys and values.")

                for key, value in directive["environment"].items():
                    key = self.execute_template(key, locals=locals)
                    if not isinstance(key, str):
                        raise ValueError("Environment key must be a string.")

                    value = self.execute_template(value, locals=locals)
                    if not isinstance(value, str):
                        raise ValueError("Environment value must be a string.")

                    builder.set_environment(key, value)  # type: ignore
            elif "template" in directive:
                name = self.execute_template(
                    directive["template"].get("name") or "", locals=locals
                )
                if name == "":
                    raise ValueError("Template name cannot be empty.")

                builder.add_directive(
                    name,
                    **{
                        k: self.execute_template(v, locals=locals)
                        for k, v in directive["template"].items()
                        if k != "name"
                    },
                )
            elif "copy" in directive:
                args = []
                if type(directive["copy"]) == str:
                    args = self.execute_template(
                        directive["copy"].split(" "), locals=locals
                    )
                elif type(directive["copy"]) == list:
                    args = self.execute_template(directive["copy"], locals=locals)

                if not isinstance(args, list):
                    raise ValueError("Copy directive must be a list of files.")

                if len(args) == 2:
                    arg = args[0]
                    if not isinstance(arg, str):
                        raise ValueError("Copy directive must be a list of files.")

                    # check to make sure the first reference is a file and it exists.
                    if not self.file_exists(arg):
                        filename = args[0]
                        raise ValueError(f"File {filename} does not exist.")

                    # Copy file from recipe directory to build directory if it exists in recipe
                    # This ensures that files referenced in copy directives are available in the Docker build context
                    import os as os_module

                    recipe_file_path = os_module.path.join(self.recipe_path, arg)
                    build_file_path = os_module.path.join(self.build_directory, arg)

                    if (
                        not self.skip_file_population
                        and os_module.path.exists(recipe_file_path)
                        and not os_module.path.exists(build_file_path)
                    ):
                        shutil.copy2(recipe_file_path, build_file_path)

                builder.copy(*args)  # type: ignore
            elif "group" in directive:
                variables = {**locals}

                if "with" in directive:
                    for key, value in directive["with"].items():
                        variables[key] = self.execute_template(value, locals=variables)

                for item in directive["group"]:
                    add_directive(item, locals=variables)
            elif "include" in directive:
                filename = self.execute_template(
                    directive["include"] or "", locals=locals
                )

                if not isinstance(filename, str):
                    raise ValueError("Include filename must be a string.")

                include_file = self.load_include_file(filename)

                if include_file.get("builder") != "neurodocker":
                    raise ValueError("Include file must be a neurodocker file.")

                variables = {**locals}

                if "with" in directive:
                    for key, value in directive["with"].items():
                        variables[key] = self.execute_template(value, locals=variables)

                for directive in include_file["directives"]:
                    add_directive(directive, locals=variables)
            elif "file" in directive:
                self.add_file(
                    directive["file"],
                    self.recipe_path,
                    locals=locals,
                    check_only=self.check_only,
                )
            elif "variables" in directive:
                for key, value in directive["variables"].items():
                    locals[key] = self.execute_template(value, locals=locals)
            elif "test" in directive:
                # TODO: implement test directive
                pass
            elif "deploy" in directive:
                if "bins" in directive["deploy"]:
                    bins = self.execute_template(
                        directive["deploy"]["bins"], locals=locals
                    )
                    if not isinstance(bins, list):
                        raise ValueError("Deploy bins must be a list.")
                    self.deploy_bins.extend(bins)

                if "path" in directive["deploy"]:
                    path = self.execute_template(
                        directive["deploy"]["path"], locals=locals
                    )
                    if not isinstance(path, list):
                        raise ValueError("Deploy path must be a list.")
                    self.deploy_path.extend(path)
            elif "boutique" in directive:
                import json
                import os

                # TODO(joshua): Support template execution later.
                boutique_data = directive["boutique"]

                # Check if boutique_data is valid
                if boutique_data is None:
                    raise ValueError("Boutique directive data cannot be None")

                if not isinstance(boutique_data, dict):
                    raise ValueError("Boutique directive must be a dictionary")

                # Pretty print the JSON
                boutique_json = json.dumps(boutique_data, indent=2, sort_keys=True)

                # Get the tool name for the filename
                tool_name = boutique_data.get("name", "tool")
                filename = f"{tool_name}.json"

                # Write to a file in the context directory
                boutique_file_path = os.path.join(self.build_directory, filename)

                with open(boutique_file_path, "w") as f:
                    f.write(boutique_json)

                # Create the /boutique directory in the container
                builder.run_command("mkdir -p /boutique")

                # Copy the boutique file to the container
                builder.copy(filename, f"/boutique/{filename}")
            else:
                raise ValueError(f"Directive {directive} not supported.")

        for directive in build_directive["directives"]:
            add_directive(directive, locals=locals)

        # If no deploy paths/bins were found in directives, use the top-level deploy section values
        if len(self.deploy_path) == 0 and hasattr(self, 'top_level_deploy_path'):
            self.deploy_path = self.top_level_deploy_path
        
        if len(self.deploy_bins) == 0 and hasattr(self, 'top_level_deploy_bins'):
            self.deploy_bins = self.top_level_deploy_bins
        
        # Always set DEPLOY_PATH and DEPLOY_BINS environment variables, even if empty
        # This prevents the container tester from receiving undefined environment variables
        # which would be treated as empty strings that split into [""] instead of []
        path = self.execute_template(self.deploy_path, locals=locals) if len(self.deploy_path) > 0 else []
        if not isinstance(path, list):
            raise ValueError("Deploy path must be a list.")
        builder.set_environment("DEPLOY_PATH", ":".join(path) if path else "")  # type: ignore
        
        bins = self.execute_template(self.deploy_bins, locals=locals) if len(self.deploy_bins) > 0 else []
        if not isinstance(bins, list):
            raise ValueError("Deploy bins must be a list.")
        builder.set_environment("DEPLOY_BINS", ":".join(bins) if bins else "")  # type: ignore

        builder.copy("README.md", "/README.md")
        builder.copy("build.yaml", "/build.yaml")

        try:
            output = builder.generate()
        except ImportError as e:
            if "neurodocker" in str(e):
                # In environments where neurodocker is not available (like CI),
                # we can't generate the actual Dockerfile but we can still validate the recipe
                raise ImportError(
                    "neurodocker is not installed. This is required for Dockerfile generation. "
                    f"Install it with: {sys.executable} -m pip install --no-cache-dir --upgrade {NEURODOCKER_PIP_PACKAGE}"
                ) from e
            else:
                raise

        # Hack to remove the localedef installation since neurodocker adds it.
        if build_directive.get("fix-locale-def"):
            # go though the output looking for the first line containing localedef and remove it.
            lines = output.split("\n")
            for i, line in enumerate(lines):
                if "localedef" in line:
                    lines[i] = ""
                    break
            output = "\n".join(lines)

        return output


def http_get(url):
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8")


def build_tinyrange(
    tinyrange_path: str, description_file: str, output_dir: str, name: str, version: str
):
    tinyrange_config = None
    try:
        with open("tinyrange.yaml", "r") as f:
            tinyrange_config = yaml.safe_load(f)
    except FileNotFoundError:
        print("WARN: TinyRange configuration file not found.")
        tinyrange_config = {
            "cpu_cores": 4,
            "memory_size_gb": 8,
            "root_size_gb": 8,
            "docker_persist_size_gb": 16,
        }

    # ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    build_dir = subprocess.check_output([tinyrange_path, "env", "build-dir"]).decode(
        "utf-8"
    )

    # Remove the persist docker image each time.
    try:
        os.remove(os.path.join(build_dir, "persist", "docker_persist.img"))
    except:
        pass

    description_filename = os.path.basename(description_file)

    persist_size = str(tinyrange_config["docker_persist_size"] * 1024)

    login_file = {
        "version": 1,
        "builder": "alpine@3.21",
        "service_commands": [
            "dockerd",
        ],
        "commands": [
            "%verbose,exit_on_failure",
            "cd /root;python3 -m venv env;source env/bin/activate;pip install -r requirements.txt",
            f"cd /root;source env/bin/activate;python build.py --build {description_filename} build",
            "killall dockerd",
        ],
        "files": ["../build.py", "../requirements.txt", "../" + description_file],
        "packages": ["py3-pip", "docker"],
        "macros": ["//lib/alpine_kernel:kernel,3.21"],
        "volumes": [f"docker,{persist_size},/var/lib/docker,persist"],
        "min_spec": {
            "cpu": tinyrange_config["cpu_cores"],
            "memory": tinyrange_config["memory_size"] * 1024,
            "disk": tinyrange_config["root_size"] * 1024,
        },
    }

    with open(os.path.join(output_dir, f"{name}_{version}.yaml"), "w") as f:
        yaml.dump(login_file, f)

    subprocess.check_call(
        [
            tinyrange_path,
            "login",
            "--verbose",
            "-c",
            os.path.join(output_dir, f"{name}_{version}.yaml"),
        ]
    )


def get_recipe_directory(repo_path, name):
    return os.path.join(repo_path, "recipes", name)


def init_new_recipe(repo_path: str, name: str, version: str):
    if name == "" or version == "":
        raise ValueError("Name and version cannot be empty.")

    recipe_path = get_recipe_directory(repo_path, name)
    if not os.path.exists(recipe_path):
        os.makedirs(recipe_path)

    # Create description file
    description_file = os.path.join(recipe_path, "build.yaml")
    if os.path.exists(description_file):
        raise ValueError("Description file {} already exists.".format(description_file))

    with open(description_file, "w") as f:
        yaml.safe_dump(
            {
                "name": name,
                "version": version,
                "architectures": ["x86_64"],
                "copyright": [
                    {"license": "TODO", "url": "TODO"},
                ],
                "build": {
                    "kind": "neurodocker",
                    "base-image": "ubuntu:24.04",
                    "pkg-manager": "apt",
                    "directives": [
                        {
                            "file": {
                                "name": "hello.txt",  # Example file
                                "contents": "Hello, world!",  # Example content
                            }
                        },
                        {"run": ['cat {{ get_file("hello.txt") }}']},
                        {
                            "deploy": {
                                "bins": ["TODO"],
                            }
                        },
                        {
                            "test": {
                                "name": "Simple Deploy Bins/Path Test",
                                "builtin": "test_deploy.sh",
                            },
                        },
                    ],
                },
                "readme": "TODO",
            },
            f,
            sort_keys=False,
            default_flow_style=False,
            width=10000,
        )


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def get_cached_download_path(url: str) -> str:
    cache_dir = get_cache_dir()
    os.makedirs(cache_dir, exist_ok=True)
    filename = sha256(url.encode("utf-8"))
    return os.path.join(cache_dir, filename)


def download_with_cache(
    url, check_only=False, insecure=False, retry=1, curl_options=""
):
    # download with curl to a temporary file
    if shutil.which("curl") is None:
        raise ValueError("curl not found in PATH.")

    # Make the output filename and check if it exists
    output_filename = get_cached_download_path(url)
    temp_filename = output_filename + ".tmp"

    if os.path.exists(output_filename):
        # Validate cached file is not corrupted
        if os.path.getsize(output_filename) > 0:
            return output_filename
        else:
            print(
                f"Cached file {output_filename} is empty, removing and re-downloading"
            )
            try:
                os.remove(output_filename)
            except OSError as e:
                print(f"Warning: Failed to remove empty cached file: {e}")

    # Skip download if check_only is True
    if check_only:
        with open(output_filename, "w") as f:
            f.write("")
        print("Check only mode: skipping file download.")
        return output_filename

    # Ensure retry is at least 1
    retry = max(1, retry)

    # download the file with retry logic
    for attempt in range(retry):
        try:
            print(
                f"Downloading {url} to {output_filename} (attempt {attempt + 1}/{retry})"
            )

            # Use full argument names for curl for clarity
            curl_args = ["curl", "--location", "--output", temp_filename]

            # Add resumable download support
            resume_size = 0
            if os.path.exists(temp_filename):
                # Get file size and try to resume
                file_size = os.path.getsize(temp_filename)
                if file_size > 0:
                    # Validate partial file is not corrupted by checking if it's suspiciously small
                    # for a retry (likely an error page or corrupted)
                    if attempt > 0 and file_size < 1024:
                        print(
                            f"Partial file is suspiciously small ({file_size} bytes), removing and starting fresh"
                        )
                        try:
                            os.remove(temp_filename)
                        except OSError:
                            pass
                    else:
                        curl_args.extend(["--continue-at", str(file_size)])
                        resume_size = file_size
                        print(f"Resuming download from byte {file_size}")

            # Add robust curl options for HTTP/2 issues and retries
            curl_args.extend(
                [
                    "--http1.1",  # Force HTTP/1.1 to avoid HTTP/2 issues
                    "--retry",
                    "15",  # Increased from 10
                    "--retry-delay",
                    "10",  # Increased from 5
                    "--retry-all-errors",
                    "--retry-max-time",
                    "600",  # Increased from 300 (10 minutes)
                    "--connect-timeout",
                    "60",  # Increased from 30
                    "--max-time",
                    "3600",  # Increased from 1800 (1 hour max)
                    "--fail",
                    "--show-error",
                    "--silent",
                ]
            )

            if insecure:
                curl_args.append("--insecure")

            # Add custom curl options if provided
            if curl_options:
                # Split curl_options string and add to args
                # Handle quoted arguments properly
                import shlex

                additional_args = shlex.split(curl_options)
                curl_args.extend(additional_args)

            # Add the URL last
            curl_args.append(url)

            subprocess.check_call(
                curl_args,
                stdout=subprocess.DEVNULL,
            )

            # Validate downloaded file
            if os.path.exists(temp_filename):
                file_size = os.path.getsize(temp_filename)
                if file_size == 0:
                    raise ValueError("Downloaded file is empty")
                elif file_size < 1024 and resume_size == 0:
                    # Very small file on fresh download might be an error page
                    print(
                        f"Warning: Downloaded file is suspiciously small ({file_size} bytes)"
                    )

                # Move temp file to final location if download was successful
                shutil.move(temp_filename, output_filename)
                print(f"Successfully downloaded {url} ({file_size} bytes)")
                return output_filename
            else:
                raise ValueError("Download completed but file not found")

        except subprocess.CalledProcessError as e:
            print(
                f"Download attempt {attempt + 1}/{retry} failed with exit code {e.returncode}"
            )

            # Handle different curl error codes
            if e.returncode == 18:  # CURL_PARTIAL_FILE
                print(
                    "Partial file transfer detected (HTTP/2 stream issue or connection closed)"
                )
            elif e.returncode == 92:  # CURL_HTTP2_STREAM
                print("HTTP/2 stream error detected")
            elif e.returncode == 28:  # CURL_OPERATION_TIMEDOUT
                print("Operation timed out")

            # Clean up potentially corrupted temp file based on error type and size
            if os.path.exists(temp_filename):
                file_size = os.path.getsize(temp_filename)
                should_remove = False

                # Remove file if:
                # 1. It's very small (likely error page)
                # 2. It's the last attempt (clean up completely)
                # 3. Specific curl errors that indicate corruption
                if file_size < 1024:
                    should_remove = True
                    print(
                        f"Removing small/corrupted temp file: {temp_filename} ({file_size} bytes)"
                    )
                elif attempt == retry - 1:
                    should_remove = True
                    print(f"Last attempt failed, removing temp file: {temp_filename}")
                elif e.returncode in [92, 18]:  # HTTP/2 or partial file errors
                    should_remove = True
                    print(
                        f"HTTP/2 or partial file error, removing temp file for fresh retry"
                    )
                else:
                    print(
                        f"Keeping partial download ({file_size} bytes) for potential resume"
                    )

                if should_remove:
                    try:
                        os.remove(temp_filename)
                        print(f"Cleaned up temp file: {temp_filename}")
                    except OSError as cleanup_error:
                        print(
                            f"Warning: Failed to clean up temp file {temp_filename}: {cleanup_error}"
                        )

            # If this was the last attempt, re-raise the exception
            if attempt == retry - 1:
                print(f"All {retry} download attempts failed for {url}")
                raise ValueError(
                    f"Failed to download {url} after {retry} attempts. Last error: curl exit code {e.returncode}"
                )

            # Wait a bit before retrying (exponential backoff with jitter)
            import time
            import random

            base_wait = min(60, 2**attempt)  # Cap at 60 seconds
            jitter = random.uniform(0.5, 1.5)  # Add some randomness
            wait_time = int(base_wait * jitter)
            print(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

        except Exception as e:
            print(
                f"Unexpected error during download attempt {attempt + 1}/{retry}: {e}"
            )

            # Clean up temp file on unexpected error
            if os.path.exists(temp_filename):
                try:
                    os.remove(temp_filename)
                    print(
                        f"Cleaned up temp file after unexpected error: {temp_filename}"
                    )
                except OSError as cleanup_error:
                    print(
                        f"Warning: Failed to clean up temp file {temp_filename}: {cleanup_error}"
                    )

            # If this was the last attempt, re-raise the exception
            if attempt == retry - 1:
                print(f"All {retry} download attempts failed for {url}")
                raise ValueError(
                    f"Failed to download {url} after {retry} attempts. Last error: {e}"
                )

            # Wait before retrying
            import time

            wait_time = min(60, 2**attempt)
            print(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

    # This should never be reached due to the exception handling above
    raise ValueError(f"Unexpected error: download failed for {url}")


def cleanup_cached_file(url):
    """
    Clean up a cached file by URL. Useful for removing corrupted downloads.
    Also removes any associated temp files.

    Args:
        url (str): The URL that was used to cache the file

    Returns:
        bool: True if file was successfully removed, False otherwise
    """
    cache_dir = get_cache_dir()
    filename = sha256(url.encode("utf-8"))
    output_filename = os.path.join(cache_dir, filename)
    temp_filename = output_filename + ".tmp"

    success = True

    # Remove main cached file
    if os.path.exists(output_filename):
        try:
            os.remove(output_filename)
            print(f"Cleaned up cached file for {url}")
        except OSError as e:
            print(f"Failed to clean up cached file {output_filename}: {e}")
            success = False

    # Remove temp file if it exists
    if os.path.exists(temp_filename):
        try:
            os.remove(temp_filename)
            print(f"Cleaned up temp file for {url}")
        except OSError as e:
            print(f"Failed to clean up temp file {temp_filename}: {e}")
            success = False

    return success


def cleanup_temp_files():
    """
    Clean up all temporary download files in the cache directory.
    This is useful for cleanup after interrupted downloads.

    Returns:
        int: Number of temp files cleaned up
    """
    cache_dir = get_cache_dir()
    if not os.path.exists(cache_dir):
        return 0

    temp_files = []
    for filename in os.listdir(cache_dir):
        if filename.endswith(".tmp"):
            temp_files.append(os.path.join(cache_dir, filename))

    cleaned = 0
    for temp_file in temp_files:
        try:
            os.remove(temp_file)
            print(f"Cleaned up temp file: {temp_file}")
            cleaned += 1
        except OSError as e:
            print(f"Failed to clean up temp file {temp_file}: {e}")

    if cleaned > 0:
        print(f"Cleaned up {cleaned} temporary download files")

    return cleaned
    return False


def get_build_platform(arch: str) -> str:
    if arch == "x86_64":
        return "linux/amd64"
    elif arch == "aarch64":
        return "linux/arm64"
    else:
        raise ValueError(f"Architecture {arch} not supported.")


def load_spdx_licenses():
    # the JSON file is next to the script
    spdx_licenses_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "licenses.json"
    )

    if not os.path.exists(spdx_licenses_file):
        raise ValueError("SPDX licenses file not found.")

    with open(spdx_licenses_file, "r") as f:
        spdx_licenses = json.load(f)

        ret = {}

        for license in spdx_licenses["licenses"]:
            if "licenseId" in license:
                ret[license["licenseId"]] = license

        return ret


def validate_license(description_file):
    # don't try to validate if the license is not present
    if "copyright" not in description_file:
        return

    valid_licenses = load_spdx_licenses()

    copyright_list = description_file["copyright"]
    if not isinstance(copyright_list, list):
        raise ValueError("Copyright must be a list of dicts.")

    for copyright in copyright_list:
        if "license" in copyright:
            license = copyright["license"]
            if license not in valid_licenses:
                raise ValueError(f"License {license} not found in SPDX licenses.")
        elif "name" in copyright:
            # ignore custom licenses
            pass

        if "url" not in copyright:
            raise ValueError("License URL not found in copyright.")


def generate_from_description(
    repo_path: str,
    recipe_path: str,
    description_file: typing.Any,
    output_directory: str,
    architecture: str | None = None,
    ignore_architecture: bool | None = False,
    auto_build: bool = False,
    max_parallel_jobs: int | None = None,
    options: list[str] | None = None,
    recreate_output_dir: bool = False,
    check_only: bool = False,
    gpu: bool = False,
    local_context: str | None = None,
    skip_file_population: bool = False,
) -> BuildContext | None:
    if max_parallel_jobs is None:
        max_parallel_jobs = os.cpu_count()

    # Get basic information
    name = description_file.get("name") or ""
    version = description_file.get("version") or ""

    readme = description_file.get("readme") or ""

    draft = description_file.get("draft") or False
    if draft:
        print("WARN: This is a draft recipe.")
        if auto_build:
            print("WARN: Auto build is enabled. Skipping build.")
            return None

    arch = ARCHITECTURES[architecture or platform.machine()]

    allowed_architectures = description_file.get("architectures") or []
    if allowed_architectures == []:
        raise ValueError("No architectures specified in description file.")

    if arch not in allowed_architectures and not ignore_architecture:
        raise ValueError(f"Architecture {arch} not supported by this recipe.")

    validate_license(description_file)

    ctx = BuildContext(repo_path, recipe_path, name, version, arch, check_only)
    ctx.set_max_parallel_jobs(max_parallel_jobs)
    ctx.gpu = gpu
    if local_context:
        key, local_path = local_context.split("=", 1)
        ctx.add_local_context(key, local_path)

    locals = {}

    if "variables" in description_file:
        for key, value in description_file["variables"].items():
            ctx.__dict__[key] = ctx.execute_template(value, locals=locals)

    description_options = description_file.get("options") or {}
    for key, value in description_options.items():
        ctx.add_option(
            key,
            description=value.get("description") or "",
            default=value.get("default") or False,
            version_suffix=value.get("version_suffix") or "",
        )

    # Set options from command line
    if options is not None:
        for option in options:
            key, value = option.split("=")
            ctx.set_option(key, value)

    # Set options from description file
    ctx.calculate_version()

    if (readme == "") and ("readme_url" not in description_file):
        # If readme is not found, try to get it from a file
        readme_file = os.path.join(recipe_path, "README.md")
        if os.path.exists(readme_file):
            with open(readme_file, "r") as f:
                readme = f.read()
        else:
            raise ValueError("README.md not found and readme is empty")

    ctx.readme = ctx.execute_template_string(readme, locals=locals)

    # If readme is not found, try to get it from a URL
    # This is done after so we don't execute the template
    if "readme_url" in description_file:
        readme_url = ctx.execute_template(description_file["readme_url"], locals=locals)
        if readme_url != "":
            ctx.readme = http_get(readme_url)

    # Check if name, version, or readme is empty
    if ctx.name == "" or ctx.version == "" or ctx.readme == "":
        raise ValueError("Name, version, or readme cannot be empty.")

    # Get hardcoded deploy info
    # Store the deploy info as attributes for use during build
    ctx.top_level_deploy_bins = []
    ctx.top_level_deploy_path = []
    
    if "deploy" in description_file:
        if "bins" in description_file["deploy"]:
            ctx.top_level_deploy_bins = ctx.execute_template(description_file["deploy"]["bins"], locals=locals)  # type: ignore
        if "path" in description_file["deploy"]:
            ctx.top_level_deploy_path = ctx.execute_template(description_file["deploy"]["path"], locals=locals)  # type: ignore

    ctx.tag = f"{name}:{version}"

    # Get build information
    ctx.build_info = description_file.get("build") or None

    if ctx.build_info is None:
        raise ValueError("No build info found in description file.")

    ctx.build_kind = ctx.build_info.get("kind") or ""
    if ctx.build_kind == "":
        raise ValueError("Build kind cannot be empty.")

    # Create build directory
    ctx.build_directory = os.path.join(output_directory, name)
    ctx.dockerfile_name = "{}_{}.Dockerfile".format(
        ctx.name, ctx.version.replace(":", "_")
    )
    ctx.skip_file_population = skip_file_population

    if skip_file_population:
        for file in description_file.get("files", []):
            ctx.add_file(
                file,
                recipe_path,
                check_only=check_only,
                locals=locals,
                dry_run=True,
            )

        if ctx.build_kind == "neurodocker":
            ctx.build_neurodocker(ctx.build_info, locals=locals)
        else:
            raise ValueError("Build kind not supported.")

        return ctx

    if os.path.exists(ctx.build_directory):
        if recreate_output_dir:
            shutil.rmtree(ctx.build_directory)
        else:
            raise ValueError(
                "Build directory already exists. Pass --recreate to overwrite it."
            )

    os.makedirs(ctx.build_directory)

    # Write README.md
    with open(os.path.join(ctx.build_directory, "README.md"), "w") as f:
        if ctx.readme == None:
            raise ValueError("README.md is empty.")

        f.write(ctx.readme)
        # add empty line at the end so that promt in a container is on the new line:
        f.write("\n")

    # Write all files
    for file in description_file.get("files", []):
        ctx.add_file(file, recipe_path, check_only=check_only, locals=locals)

    # Copy build.yaml to build directory for inclusion in container
    build_yaml_source = os.path.join(recipe_path, "build.yaml")
    build_yaml_dest = os.path.join(ctx.build_directory, "build.yaml")
    if os.path.exists(build_yaml_source):
        with open(build_yaml_source, "r") as src, open(build_yaml_dest, "w") as dst:
            dst.write(src.read())

    # Write Dockerfile
    dockerfile_generated = False
    if ctx.build_kind == "neurodocker":
        try:
            dockerfile = ctx.build_neurodocker(ctx.build_info, locals=locals)
            with open(os.path.join(ctx.build_directory, ctx.dockerfile_name), "w") as f:
                f.write(dockerfile)
            dockerfile_generated = True
        except ImportError as e:
            if "neurodocker" in str(e) and check_only:
                # In check-only mode, we can skip Dockerfile generation if neurodocker is missing
                # This allows tests to run in CI environments without neurodocker installed
                print(
                    f"⚠️  neurodocker not available, skipping Dockerfile generation for {ctx.name}"
                )
                print("   Recipe validation was successful")
                dockerfile_generated = False
            else:
                # In normal mode or for other import errors, re-raise the error
                raise
    else:
        raise ValueError("Build kind not supported.")

    if check_only:
        if dockerfile_generated:
            print("Dockerfile generated successfully at", ctx.dockerfile_name)
        else:
            print("Recipe validation completed (Dockerfile generation skipped)")
        return ctx

    return ctx


def _get_tester_binary_cache_path() -> str:
    tester_cache_dir = os.path.join(get_cache_dir(), "tester")
    os.makedirs(tester_cache_dir, exist_ok=True)
    return os.path.join(tester_cache_dir, CONTAINER_TESTER_BINARY_NAME)


def _get_tester_binary_hash_cache_path() -> str:
    return _get_tester_binary_cache_path() + ".sha256"


def _get_container_tester_source_hash() -> str:
    tester_dir = os.path.join(get_repo_path(), "builder", "tester")
    source_files = ["Dockerfile", "go.mod", "go.sum", "main.go"]

    hasher = hashlib.sha256()
    for filename in source_files:
        source_path = os.path.join(tester_dir, filename)
        if not os.path.exists(source_path):
            continue

        hasher.update(filename.encode("utf-8"))
        with open(source_path, "rb") as f:
            hasher.update(f.read())

    return hasher.hexdigest()


def ensure_container_tester_image(container_cli: str, force_rebuild: bool = False) -> str:
    tester_dir = os.path.join(get_repo_path(), "builder", "tester")

    if force_rebuild:
        print(f"Rebuilding container tester image {CONTAINER_TESTER_IMAGE}...")
        subprocess.check_call(
            [container_cli, "build", "-t", CONTAINER_TESTER_IMAGE, "."],
            cwd=tester_dir,
        )
        return CONTAINER_TESTER_IMAGE

    try:
        subprocess.check_call(
            [container_cli, "image", "inspect", CONTAINER_TESTER_IMAGE],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        print(
            f"Container tester image {CONTAINER_TESTER_IMAGE} not found; building it now..."
        )
        subprocess.check_call(
            [container_cli, "build", "-t", CONTAINER_TESTER_IMAGE, "."],
            cwd=tester_dir,
        )
    return CONTAINER_TESTER_IMAGE


def ensure_container_tester_binary(container_cli: str) -> str:
    binary_path = _get_tester_binary_cache_path()
    source_hash = _get_container_tester_source_hash()
    hash_cache_path = _get_tester_binary_hash_cache_path()

    cached_hash = ""
    if os.path.exists(hash_cache_path):
        with open(hash_cache_path, "r") as f:
            cached_hash = f.read().strip()

    if os.path.exists(binary_path) and cached_hash == source_hash and cached_hash != "":
        return binary_path

    # If the tester source changed (or hash metadata is missing), rebuild the
    # tester image to avoid reusing a stale cached binary.
    ensure_container_tester_image(container_cli, force_rebuild=True)

    fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(binary_path),
        prefix="tester-",
    )
    os.close(fd)

    try:
        container_id = (
            subprocess.check_output(
                [container_cli, "create", CONTAINER_TESTER_IMAGE],
                text=True,
            )
            .strip()
        )
    except subprocess.CalledProcessError as exc:
        os.unlink(tmp_path)
        raise RuntimeError(
            f"unable to create container from tester image: {exc}"
        ) from exc

    try:
        try:
            subprocess.check_call(
                [container_cli, "cp", f"{container_id}:/tester", tmp_path]
            )
        except subprocess.CalledProcessError as exc:
            os.unlink(tmp_path)
            raise RuntimeError(f"unable to extract tester binary: {exc}") from exc
    finally:
        subprocess.check_call(
            [container_cli, "rm", container_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    os.chmod(tmp_path, 0o755)
    os.replace(tmp_path, binary_path)

    with open(hash_cache_path, "w") as f:
        f.write(source_hash)

    return binary_path


def _collect_dependency_failures(
    scope: str, dependency: dict[str, typing.Any], failures: list[str]
) -> None:
    # Recursively collect any dependency errors to make debugging easier.
    error = dependency.get("Error")
    name = dependency.get("FullPath") or dependency.get("ExecutableType") or "dependency"
    label = f"{scope} -> {name}"
    if error:
        failures.append(f"{label}: {error}")
    for child in dependency.get("Dependencies", []) or []:
        if isinstance(child, dict):
            _collect_dependency_failures(label, child, failures)


def _collect_tester_failures(results: dict[str, typing.Any]) -> list[str]:
    failures: list[str] = []
    executables = results.get("Executables") or {}
    if not isinstance(executables, dict):
        return failures

    for name, info in executables.items():
        if not isinstance(info, dict):
            continue
        error = info.get("Error")
        if error:
            failures.append(f"{name}: {error}")
        for dependency in info.get("Dependencies", []) or []:
            if isinstance(dependency, dict):
                _collect_dependency_failures(name, dependency, failures)
    return failures


def _compact_tester_summary(
    results: dict[str, typing.Any], failures: list[str]
) -> dict[str, typing.Any]:
    deploy_bins_raw = results.get("DeployBins")
    deploy_paths_raw = results.get("DeployPaths")
    executables = results.get("Executables") or {}

    deploy_bins = (
        [entry for entry in deploy_bins_raw if isinstance(entry, str) and entry != ""]
        if isinstance(deploy_bins_raw, list)
        else []
    )
    deploy_paths = (
        [entry for entry in deploy_paths_raw if isinstance(entry, str) and entry != ""]
        if isinstance(deploy_paths_raw, list)
        else []
    )

    executable_count = len(executables) if isinstance(executables, dict) else 0
    summary: dict[str, typing.Any] = {
        "DeployBins": deploy_bins,
        "DeployPaths": deploy_paths,
        "ExecutableCount": executable_count,
        "FailureCount": len(failures),
    }

    if failures:
        summary["FailurePreview"] = failures[:10]
        if len(failures) > 10:
            summary["FailurePreviewTruncated"] = len(failures) - 10

    return summary


def _format_tester_failures(failures: list[str], verbose: bool) -> str:
    if verbose or len(failures) <= 25:
        return "\n".join(f"- {failure}" for failure in failures)

    shown = failures[:25]
    remaining = len(failures) - len(shown)
    failure_text = "\n".join(f"- {failure}" for failure in shown)
    failure_text += (
        "\n"
        f"- ... {remaining} more failure(s) hidden. "
        "Set CONTAINER_TESTER_VERBOSE=1 for full details."
    )
    return failure_text


def _parse_tester_output(raw_output: str) -> dict[str, typing.Any] | None:
    text = raw_output.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            snippet = text[start : end + 1]
            try:
                return json.loads(snippet)
            except json.JSONDecodeError:
                pass
    print("Warning: Unable to parse container tester output as JSON; skipping failure inspection.")
    return None


def run_container_tester(tag: str, architecture: str, use_podman: bool = False) -> None:
    container_cli = "podman" if use_podman else "docker"
    tester_binary = ensure_container_tester_binary(container_cli)
    tester_binary = os.path.abspath(tester_binary)

    mount_suffix = ":ro,Z" if use_podman else ":ro"

    run_cmd = [container_cli, "run", "--rm"]
    if not use_podman:
        run_cmd.extend(["--platform", get_build_platform(architecture)])

    run_cmd.extend(
        ["-v", f"{tester_binary}:{CONTAINER_TESTER_MOUNT_PATH}{mount_suffix}"]
    )
    run_cmd.extend(["--entrypoint", CONTAINER_TESTER_MOUNT_PATH, tag])

    print(f"Running container tester for image {tag}...")
    process = subprocess.Popen(
        run_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    assert process.stdout is not None
    captured_output: list[str] = []
    try:
        for line in process.stdout:
            captured_output.append(line)
    finally:
        exit_code = process.wait()

    raw_output = "".join(captured_output)
    verbose_tester_output = _env_truthy("CONTAINER_TESTER_VERBOSE", default=False)

    if exit_code != 0:
        if raw_output:
            print(raw_output, end="")
        raise subprocess.CalledProcessError(exit_code, run_cmd)

    parsed_output = _parse_tester_output(raw_output)
    failures: list[str] = []
    if parsed_output is not None:
        failures = _collect_tester_failures(parsed_output)
        if verbose_tester_output:
            print("Container tester results:")
            print(json.dumps(parsed_output, indent=2, sort_keys=True))
        else:
            print("Container tester summary:")
            print(
                json.dumps(
                    _compact_tester_summary(parsed_output, failures),
                    indent=2,
                    sort_keys=True,
                )
            )
    else:
        print(raw_output, end="")

    if parsed_output is None:
        return

    if failures:
        failure_text = _format_tester_failures(failures, verbose=verbose_tester_output)
        raise RuntimeError(
            "Container tester reported failures:\n"
            f"{failure_text}"
        )


def build_and_run_container(
    dockerfile_name: str,
    name: str,
    version: str,
    tag: str,
    architecture: str,
    recipe_path: str,
    build_directory: str,
    login=False,
    build_sif=False,
    generate_release=False,
    gpu=False,
    local_context=None,
    mount: str | None = None,
    use_buildkit: bool = False,
    use_podman: bool = False,
    load_into_docker: bool = False,
):
    if use_buildkit:
        # Build using buildkitd + buildctl (no host Docker daemon required)
        if not shutil.which("buildkitd"):
            raise ValueError("buildkitd not found in PATH.")
        if not shutil.which("buildctl"):
            raise ValueError("buildctl not found in PATH.")

        # Runtime/setup paths (can be overridden by env)
        xdg_runtime_dir = os.environ.get("XDG_RUNTIME_DIR", "/tmp/buildkit")
        root_dir = os.environ.get("ROOTDIR", "/tmp/buildkit-root")
        sock = os.path.join(xdg_runtime_dir, "buildkitd.sock")

        os.makedirs(xdg_runtime_dir, exist_ok=True)
        os.makedirs(root_dir, exist_ok=True)

        # Start buildkitd
        bk_flags = [
            f"--addr=unix://{sock}",
            f"--root={root_dir}",
        ]

        print(f"Starting buildkitd (XDG_RUNTIME_DIR={xdg_runtime_dir})…")
        bk_proc = subprocess.Popen(["buildkitd", *bk_flags])

        try:
            # Wait until the daemon is ready
            import time

            for _ in range(40):
                if os.path.exists(sock):
                    try:
                        subprocess.check_call(
                            [
                                "buildctl",
                                "--addr",
                                f"unix://{sock}",
                                "debug",
                                "workers",
                            ],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                        break
                    except subprocess.CalledProcessError:
                        pass
                time.sleep(0.25)

            if not os.path.exists(sock):
                raise RuntimeError("buildkitd did not become ready")

            # Build with buildctl (Dockerfile frontend)
            platform = get_build_platform(architecture)

            image_tar = os.path.join(build_directory, f"{name}_{version}.docker.tar")

            buildctl_cmd = [
                "buildctl",
                "--addr",
                f"unix://{sock}",
                "build",
                "--frontend=dockerfile.v0",
                "--local",
                "context=.",
                "--local",
                "dockerfile=.",
                "--opt",
                f"filename={dockerfile_name}",
                "--opt",
                f"platform={platform}",
                "--output",
                f"type=docker,name={tag}",
            ]

            # Support additional named local build contexts (for RUN --mount=from=<key>)
            if local_context is not None:
                key, value = local_context.split("=", 1)
                value = os.path.abspath(value)
                buildctl_cmd.extend(["--local", f"{key}={value}"])

            print(
                f"Building Dockerfile via buildctl in {build_directory} → {os.path.basename(image_tar)}"
            )
            with open(image_tar, "wb") as out_f:
                subprocess.check_call(buildctl_cmd, cwd=build_directory, stdout=out_f)

            print(f"Image archive created: {image_tar}")

            # Optionally load into host docker if available and requested
            if load_into_docker and shutil.which("docker"):
                print(f"Loading image into Docker daemon: {tag}")
                with open(image_tar, "rb") as f:
                    subprocess.check_call(["docker", "load"], stdin=f)
                print("Docker image loaded successfully")
                run_container_tester(tag, architecture, use_podman=False)
            elif load_into_docker:
                print(
                    "Skipping container tester run: Docker CLI not available to load/run the image."
                )
            else:
                print(
                    "Skipping container tester run: image not loaded into Docker (--load-into-docker not set)."
                )

            if login:
                print(
                    "Login shell is not supported with BuildKit mode; skipping interactive run."
                )

            if build_sif:
                print("Building Singularity image from docker-archive…")
                sif_cli = shutil.which("singularity") or shutil.which("apptainer")
                if not sif_cli:
                    raise ValueError(
                        "Neither 'singularity' nor 'apptainer' found in PATH."
                    )
                output_filename = os.path.join("sifs", f"{name}_{version}.sif")
                if not os.path.exists("sifs"):
                    os.makedirs("sifs")
                subprocess.check_call(
                    [
                        sif_cli,
                        "build",
                        "--force",
                        output_filename,
                        "docker-archive://" + image_tar,
                    ],
                )
                print("Singularity image built successfully as", output_filename)

            return

        finally:
            try:
                bk_proc.terminate()
            except Exception:
                pass
            try:
                bk_proc.wait(timeout=5)
            except Exception:
                try:
                    bk_proc.kill()
                except Exception:
                    pass

    # Default: use host Docker CLI
    if use_podman:
        if not shutil.which("podman"):
            raise ValueError("Podman not found in PATH.")
        print("[WARNING] Using Podman for building the container.")
    else:
        if not shutil.which("docker"):
            raise ValueError("Docker not found in PATH.")

    docker_args = [
        "docker" if not use_podman else "podman",
        "build",
        "--platform",
        get_build_platform(architecture),
        "-f",
        dockerfile_name,
        "-t",
        tag,
    ]

    if local_context is not None:
        key, value = local_context.split("=", 1)
        value = os.path.abspath(value)
        docker_args += ["--build-context", key + "=" + value]

    # Shell out to Docker
    # docker-py does not support using BuildKit
    subprocess.check_call(
        docker_args + ["."],
        cwd=build_directory,
    )
    print("Docker image built successfully at", tag)
    run_container_tester(tag, architecture, use_podman=use_podman)

    # Generate release file if in CI or auto-build mode
    if should_generate_release_file(generate_release):
        generate_release_file(name, version, load_description_file(recipe_path), recipe_path)

    if login:
        abs_path = os.path.abspath(recipe_path)

        docker_run_cmd = [
            "docker" if not use_podman else "podman",
            "run",
            "--platform",
            get_build_platform(architecture),
            "--rm",
            "-it",
            "-v",
            abs_path + ":/buildhostdirectory",
        ]

        # Expose webapp ports if defined in recipe
        recipe = load_description_file(recipe_path)
        deploy = recipe.get("deploy") or {}
        webapp = deploy.get("webapp") or {}
        if webapp.get("port"):
            main_port = webapp["port"]
            docker_run_cmd.extend(["-p", f"{main_port}:{main_port}"])
            print(f"Exposing webapp port {main_port}")
        for proxy in webapp.get("additional_proxies") or []:
            if proxy.get("port"):
                docker_run_cmd.extend(["-p", f"{proxy['port']}:{proxy['port']}"])
                print(f"Exposing additional proxy port {proxy['port']}")

        if mount:
            # Handle Windows paths with drive letters (e.g., C:\Users\...:container)
            # Pattern: drive letter (X:) followed by path separator, then path, then : separator
            windows_path_match = re.match(r"^([A-Za-z]:[/\\].+?):(.+)$", mount)
            if windows_path_match:
                host = windows_path_match.group(1)
                container = windows_path_match.group(2)
            else:
                # Unix-style path or relative path - split on first colon
                host, container = mount.split(":", 1)

            host = os.path.abspath(host)
            docker_run_cmd.extend(["-v", f"{host}:{container}"])

        if gpu:
            docker_run_cmd.extend(["--gpus", "all"])

        docker_run_cmd.append(tag)

        subprocess.check_call(
            docker_run_cmd,
            cwd=build_directory,
        )
        return

    if build_sif:
        print("Building Singularity image...")

        if use_podman:
            raise ValueError("Singularity build is not supported with Podman.")

        sif_cli = shutil.which("singularity") or shutil.which("apptainer")
        if not sif_cli:
            raise ValueError("Neither 'singularity' nor 'apptainer' found in PATH.")

        output_filename = os.path.join("sifs", f"{name}_{version}.sif")
        if not os.path.exists("sifs"):
            os.makedirs("sifs")

        subprocess.check_call(
            [
                sif_cli,
                "build",
                "--force",
                output_filename,
                "docker-daemon://" + tag,
            ],
        )

        print("Singularity image built successfully as", tag + ".sif")


def run_docker_prep(prep, volume_name):
    name = prep.get("name")
    image = prep.get("image")
    script = prep.get("script")
    if name is None or image is None or script is None:
        raise ValueError("Prep step must have a name, image and script")

    # Docker run the script in the container mounting the volume as /test
    subprocess.check_call(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{volume_name}:/test",
            image,
            "bash",
            "-c",
            f"""set -ex
                cd /test
                {script}""",
        ],
    )


def _parse_builtin_test_json(raw_output: str) -> dict[str, typing.Any] | None:
    text = raw_output.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            snippet = text[start : end + 1]
            try:
                payload = json.loads(snippet)
                return payload if isinstance(payload, dict) else None
            except json.JSONDecodeError:
                return None
    return None


def _summarise_builtin_test_payload(
    payload: dict[str, typing.Any],
) -> tuple[dict[str, typing.Any], list[dict[str, typing.Any]]]:
    tests = payload.get("tests")
    failed_entries: list[dict[str, typing.Any]] = []
    if isinstance(tests, list):
        failed_entries = [
            entry
            for entry in tests
            if isinstance(entry, dict) and entry.get("status") == "failed"
        ]

    summary = {
        "total": payload.get("total", 0),
        "passed": payload.get("passed", 0),
        "failed": payload.get("failed", len(failed_entries)),
        "skipped": payload.get("skipped", 0),
    }
    return summary, failed_entries


def run_builtin_test(tag, test, gpu=False):
    # Locate builtin tests in either the legacy builder directory or the
    # relocated workflows directory.
    search_dirs = [
        os.path.dirname(__file__),
        os.path.join(get_repo_path(), "workflows"),
    ]

    builtin_test = None
    for directory in search_dirs:
        candidate = os.path.join(directory, test)
        if os.path.exists(candidate):
            builtin_test = candidate
            break

    if builtin_test is None:
        raise ValueError(f"Builtin test {test} does not exist")

    test_content = open(builtin_test).read()

    # Docker run the test script in the container mounting the volume as /test
    docker_run_cmd = [
        "docker",
        "run",
        "--rm",
    ]

    if gpu:
        docker_run_cmd.extend(["--gpus", "all"])

    docker_run_cmd.extend([tag, "bash", "-c", test_content])

    process = subprocess.run(
        docker_run_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    raw_output = process.stdout or ""
    verbose_builtin_output = _env_truthy("CONTAINER_TESTER_VERBOSE", default=False)

    # test_deploy.sh emits JSON. Show a compact summary by default and print
    # per-entry details only when failures occur (or when verbose mode is enabled).
    parsed_output = _parse_builtin_test_json(raw_output)
    if parsed_output is not None and {"total", "passed", "failed", "skipped"} <= set(parsed_output):
        summary, failed_entries = _summarise_builtin_test_payload(parsed_output)
        print("Builtin deploy test summary:")
        print(json.dumps(summary, indent=2, sort_keys=True))

        if verbose_builtin_output:
            print("Builtin deploy test details:")
            print(json.dumps(parsed_output, indent=2, sort_keys=True))
        elif failed_entries:
            print("Builtin deploy test failures:")
            print(json.dumps(failed_entries, indent=2, sort_keys=True))
    elif raw_output:
        print(raw_output, end="")

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, docker_run_cmd)


def run_docker_test(tag, test, gpu=False):
    if test.get("builtin") in {"test_deploy.sh"}:
        return run_builtin_test(tag, test.get("builtin"), gpu=gpu)

    script = test.get("script")
    if script is None:
        raise ValueError("Test step must have a script")

    # Create a docker volume for the test, if it exists remove it first
    cleaned_tag = tag.replace(":", "-")
    volume_name = f"neurocontainer-test-{cleaned_tag}"
    try:
        subprocess.check_call(
            ["docker", "volume", "rm", volume_name],
            stdout=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as e:
        # check to make sure the volume is not in use
        if "is in use" in str(e):
            raise ValueError(
                f"Volume {volume_name} is in use, please remove it manually"
            )

        # If the volume does not exist, ignore the error
        pass
    subprocess.check_call(
        ["docker", "volume", "create", volume_name],
        stdout=subprocess.DEVNULL,
    )

    # For each prep step in the test, run it in a docker container
    if "prep" in test:
        for prep in test["prep"]:
            run_docker_prep(prep, volume_name)

    # Docker run the test script in the container mounting the volume as /test
    docker_run_cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{volume_name}:/test",
    ]

    if gpu:
        docker_run_cmd.extend(["--gpus", "all"])

    if test.get("executable") is not None:
        executable = test.get("executable")
        docker_run_cmd.extend(
            [
                tag,
                executable,
                "-c",
                script,
            ]
        )
    else:
        docker_run_cmd.extend(
            [
                tag,
                "bash",
                "-c",
                f"""set -ex
                cd /test
                {script}""",
            ]
        )

    subprocess.check_call(docker_run_cmd)


def run_test(tag, test, gpu=False):
    test_name = test["name"]
    print(f"Running test {test_name} on image {tag}")
    return run_docker_test(tag, test, gpu=gpu)


def check_docker(tag):
    # use docker image inspect
    subprocess.check_call(
        ["docker", "image", "inspect", tag],
        stdout=subprocess.DEVNULL,
    )


def get_directives(description_file: dict) -> list[dict]:
    # Get directives from the description file
    if "build" not in description_file:
        raise ValueError("Description file must have a build key")

    if "directives" not in description_file["build"]:
        raise ValueError("Description file must have a build.directives key")

    return description_file["build"]["directives"]


def get_all_tests(description_file: typing.Any, recipe_path: str) -> list[dict]:
    # tests can come from two locations. Either in the description file or in a separate test.yaml file.

    tests = []

    if os.path.exists(os.path.join(recipe_path, "test.yaml")):
        with open(os.path.join(recipe_path, "test.yaml"), "r") as f:
            test_file = yaml.safe_load(f)
            if "tests" not in test_file:
                raise ValueError("Test file must have a tests key")
            tests.extend(test_file["tests"])

    directives = get_directives(description_file)

    def walk_directives(directives):
        for directive in directives:
            if "group" in directive:
                walk_directives(directive["group"])
            elif "test" in directive:
                tests.append(directive["test"])

    walk_directives(directives)

    # Ensure builtin tests are always present unless explicitly disabled
    def ensure_builtin(default_test, position=0):
        if not any(
            isinstance(t, dict) and t.get("builtin") == default_test["builtin"]
            for t in tests
        ):
            tests.insert(position, dict(default_test))

    ensure_builtin(
        {"name": "Simple Deploy Bins/Path Test", "builtin": "test_deploy.sh"}
    )

    return tests


def get_tag_from_description_file(description_file: dict) -> str:
    # Get the tag from the description file
    if "name" not in description_file:
        raise ValueError("Description file must have a name key")

    if "version" not in description_file:
        raise ValueError("Description file must have a version key")

    name = description_file["name"]
    version = description_file["version"]

    return f"{name}:{version}"


def run_tests(recipe_path: str, gpu=False):
    description_file = load_description_file(recipe_path)

    tag = get_tag_from_description_file(description_file)

    for test in get_all_tests(description_file, recipe_path):
        run_test(tag, test, gpu=gpu)


def autodetect_recipe_path(repo_path: str, path: str) -> str | None:
    # look for build.yaml in path and keep going up until we find it or reach the repo path

    # if path is not a descendant of the repo path, raise an error
    if not os.path.commonpath([repo_path, path]) == repo_path:
        raise ValueError("Path is not a descendant of the repo path.")

    while path != repo_path:
        if os.path.exists(os.path.join(path, "build.yaml")):
            return path

        path = os.path.dirname(path)

    return None


def generate_dockerfile(
    repo_path,
    recipe_path,
    architecture=None,
    ignore_architecture=False,
    gpu=False,
    local_context=None,
):
    build_directory = os.path.join(repo_path, "build")

    print(f"Generate Dockerfile from {recipe_path}...")

    return generate_from_description(
        repo_path,
        recipe_path,
        load_description_file(recipe_path),
        build_directory,
        architecture=architecture or platform.machine(),
        ignore_architecture=ignore_architecture,
        recreate_output_dir=True,
        gpu=gpu,
        local_context=local_context,
    )


def cache_main():
    root = argparse.ArgumentParser(
        description="NeuroContainer Builder - Manage cached recipe files",
    )
    root.add_argument("recipe", help="Name of the recipe to inspect")
    root.add_argument(
        "filename",
        help="Name of the recipe file entry to populate in the cache",
        nargs="?",
    )
    root.add_argument(
        "local_filename",
        help="Path to the local file that should be copied into the cache",
        nargs="?",
    )
    root.add_argument(
        "--architecture",
        help="Architecture to evaluate templates with (defaults to host or first recipe architecture)",
    )

    args = root.parse_args()

    repo_path = get_repo_path()
    recipe_path = get_recipe_directory(repo_path, args.recipe)
    if not os.path.exists(recipe_path):
        print(f"Recipe {args.recipe} not found at {recipe_path}")
        sys.exit(1)

    description_file = load_description_file(recipe_path)

    available_architectures = description_file.get("architectures") or []
    architecture = args.architecture
    if architecture is None:
        host_arch = platform.machine()
        if host_arch in available_architectures:
            architecture = host_arch
        elif available_architectures:
            architecture = available_architectures[0]
        else:
            architecture = host_arch

    if architecture not in ARCHITECTURES:
        print(f"Unsupported architecture {architecture}")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as tmpdir:
        ctx = generate_from_description(
            repo_path,
            recipe_path,
            description_file,
            tmpdir,
            architecture=architecture,
            ignore_architecture=True,
            recreate_output_dir=True,
            check_only=True,
            skip_file_population=True,
        )

    if ctx is None:
        print(f"Recipe {args.recipe} is marked as draft and cannot be processed.")
        sys.exit(1)

    def print_status():
        print("Cache status:")
        cached = [
            (name, info) for name, info in sorted(ctx.files.items()) if info.get("url")
        ]
        if not cached:
            print("No cached downloads defined in this recipe.")
        else:
            for name, info in cached:
                cached_path = info["cached_path"]
                if os.path.exists(cached_path):
                    status = "present"
                    if os.path.getsize(cached_path) == 0:
                        status = "empty"
                else:
                    status = "missing"
                print(f"- {name}: {status} ({cached_path})")

    filename = args.filename
    local_filename = args.local_filename

    if (filename and not local_filename) or (local_filename and not filename):
        print(
            "Both a recipe filename and a local filename must be provided to populate the cache."
        )
        print_status()
        sys.exit(1)

    load_error: str | None = None
    if filename:
        file_entry = ctx.files.get(filename)
        if file_entry is None:
            available = ", ".join(sorted(ctx.files.keys())) or "none"
            print(f"File {filename} not found in recipe. Available files: {available}")
            print_status()
            sys.exit(1)

        if file_entry.get("url"):
            source_path = os.path.abspath(local_filename or "")
            if not os.path.exists(source_path):
                load_error = f"Local file {source_path} not found."
            elif not os.path.isfile(source_path):
                load_error = f"Local path {source_path} is not a file."
            else:
                target_path = file_entry["cached_path"]
                try:
                    shutil.copy2(source_path, target_path)
                    print(f"Loaded {source_path} into cache at {target_path}")
                except OSError as exc:
                    load_error = f"Failed to copy {source_path} to {target_path}: {exc}"
        else:
            load_error = (
                f"File {filename} is not backed by a cached download; nothing to load."
            )

        if load_error:
            print(load_error, file=sys.stderr)
            print_status()
            sys.exit(1)

    print_status()


def generate_main():
    root = argparse.ArgumentParser(
        description="NeuroContainer Builder - Generate Docker images from description files",
    )

    # add a optional name positional argument
    root.add_argument(
        "name",
        help="Name of the recipe to generate",
        type=str,
        nargs="?",
    )

    args = root.parse_args()

    repo_path = get_repo_path()

    recipe_path = ""
    if args.name == None:
        recipe_path = autodetect_recipe_path(repo_path, os.getcwd())
        if recipe_path is None:
            print("No recipe found in current directory.")
            sys.exit(1)
    else:
        recipe_path = get_recipe_directory(repo_path, args.name)

    generate_dockerfile(repo_path, recipe_path)


def generate_and_build(
    repo_path,
    recipe_path,
    login=False,
    architecture=None,
    ignore_architecture=False,
    generate_release=False,
    gpu=False,
    local_context=None,
    mount: str | None = None,
    use_buildkit: bool = False,
    use_podman: bool = False,
    load_into_docker: bool = False,
):
    ctx = generate_dockerfile(
        repo_path,
        recipe_path,
        architecture=architecture,
        ignore_architecture=ignore_architecture,
        gpu=gpu,
        local_context=local_context,
    )
    if ctx is None:
        print("Recipe generation failed.")
        sys.exit(1)

    if ctx.dockerfile_name is None:
        raise ValueError("Dockerfile name not set.")
    if ctx.build_directory is None:
        raise ValueError("Build directory not set.")
    if ctx.tag is None:
        raise ValueError("Tag not set.")

    tag = ctx.tag

    if login:
        print(f"Building and Running Docker image {tag}...")
    else:
        print(f"Building Docker image {tag}...")

    build_and_run_container(
        ctx.dockerfile_name,
        ctx.name,
        ctx.version,
        ctx.tag,
        ctx.arch,
        recipe_path,
        ctx.build_directory,
        login=login,
        generate_release=generate_release,
        gpu=gpu,
        local_context=local_context,
        mount=mount,
        use_buildkit=use_buildkit,
        use_podman=use_podman,
        load_into_docker=load_into_docker,
    )


def build_main(login=False):
    root = argparse.ArgumentParser(
        description="NeuroContainer Builder - Build Docker images from description files",
    )

    # add a optional name positional argument
    root.add_argument(
        "name",
        help="Name of the recipe to generate",
        type=str,
        nargs="?",
    )
    root.add_argument(
        "--architecture",
        help="Architecture to build for",
        default=platform.machine(),
    )
    root.add_argument(
        "--ignore-architectures", action="store_true", help="Ignore architecture checks"
    )
    root.add_argument(
        "--generate-release",
        action="store_true",
        help="Generate release files after successful build",
    )
    root.add_argument(
        "--gpu",
        action="store_true",
        help="Enable GPU support by adding --gpus all to Docker run commands",
    )
    root.add_argument(
        "--local",
        help="Add local directories into the build context",
    )
    root.add_argument(
        "--mount",
        help="Mount a host directory into the container (host:container)",
    )
    root.add_argument(
        "--use-buildkit",
        action="store_true",
        help="Use buildkitd/buildctl instead of Docker CLI",
    )
    root.add_argument(
        "--use-podman",
        action="store_true",
        help="Use Podman instead of Docker",
    )
    root.add_argument(
        "--load-into-docker",
        action="store_true",
        help="After BuildKit build, docker load the resulting image tar if Docker is available",
    )

    args = root.parse_args()

    repo_path = get_repo_path()

    recipe_path = ""
    if args.name == None:
        # if build.yaml exists in the current directory then use it.
        if os.path.exists("build.yaml"):
            recipe_path = os.getcwd()
        else:
            recipe_path = autodetect_recipe_path(repo_path, os.getcwd())
            if recipe_path is None:
                print("No recipe found in current directory.")
                sys.exit(1)
    else:
        recipe_path = get_recipe_directory(repo_path, args.name)

    generate_and_build(
        repo_path,
        recipe_path,
        login=login,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        generate_release=args.generate_release,
        gpu=args.gpu,
        local_context=args.local,
        mount=args.mount,
        use_buildkit=args.use_buildkit,
        use_podman=args.use_podman,
        load_into_docker=args.load_into_docker,
    )


def login_main():
    build_main(login=True)


def sf_make_main():
    root = argparse.ArgumentParser(
        description="Build a recipe directory into a SIF using BuildKit (no Docker required)",
    )
    # add a optional name positional argument
    root.add_argument(
        "name",
        help="Name of the recipe to generate",
        type=str,
        nargs="?",
    )
    root.add_argument(
        "--architecture",
        help="Architecture to build for",
        default=platform.machine(),
    )
    root.add_argument(
        "--ignore-architectures", action="store_true", help="Ignore architecture checks"
    )
    root.add_argument(
        "--local",
        help="Add local directories into the build context (key=path)",
    )
    root.add_argument(
        "--mount",
        help="Mount a host directory into the container (host:container)",
    )
    root.add_argument(
        "--use-docker",
        action="store_true",
        help="Use Docker for building instead of BuildKit",
    )

    args = root.parse_args()

    repo_path = get_repo_path()

    recipe_path = ""
    if args.name == None:
        # if build.yaml exists in the current directory then use it.
        if os.path.exists("build.yaml"):
            recipe_path = os.getcwd()
        else:
            recipe_path = autodetect_recipe_path(repo_path, os.getcwd())
            if recipe_path is None:
                print("No recipe found in current directory.")
                sys.exit(1)
    else:
        recipe_path = get_recipe_directory(repo_path, args.name)

    # Generate Dockerfile and build context from the provided recipe directory
    ctx = generate_dockerfile(
        repo_path,
        recipe_path,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        gpu=False,
        local_context=args.local,
    )
    if ctx is None:
        print("Recipe generation failed.")
        sys.exit(1)

    if ctx.dockerfile_name is None or ctx.build_directory is None or ctx.tag is None:
        raise ValueError(
            "Context not fully generated (missing Dockerfile, build dir, or tag)"
        )

    print(f"Making SIF for {ctx.name}:{ctx.version} from {recipe_path}")
    build_and_run_container(
        ctx.dockerfile_name,
        ctx.name,
        ctx.version,
        ctx.tag,
        ctx.arch,
        recipe_path,
        ctx.build_directory,
        login=False,
        build_sif=True,
        generate_release=False,
        gpu=False,
        local_context=args.local,
        mount=args.mount,
        use_buildkit=args.use_docker == False,
    )


def test_main():
    root = argparse.ArgumentParser(
        description="NeuroContainer Builder - Run tests on Docker images",
    )

    # add a optional name positional argument
    root.add_argument(
        "name",
        help="Name of the recipe to generate",
        type=str,
        nargs="?",
    )
    root.add_argument(
        "--architecture",
        help="Architecture to build for",
        default=platform.machine(),
    )
    root.add_argument(
        "--ignore-architectures", action="store_true", help="Ignore architecture checks"
    )
    root.add_argument(
        "--gpu",
        action="store_true",
        help="Enable GPU support by adding --gpus all to Docker run commands",
    )

    args = root.parse_args()

    repo_path = get_repo_path()

    recipe_path = ""
    if args.name == None:
        recipe_path = autodetect_recipe_path(repo_path, os.getcwd())
        if recipe_path is None:
            print("No recipe found in current directory.")
            sys.exit(1)
    else:
        recipe_path = get_recipe_directory(repo_path, args.name)

    generate_and_build(
        repo_path,
        recipe_path,
        login=False,
        architecture=args.architecture,
        ignore_architecture=args.ignore_architectures,
        gpu=args.gpu,
    )

    run_tests(recipe_path, gpu=args.gpu)


def test_remote_main():
    """Run release container tests for a single recipe using shared tooling."""

    root = argparse.ArgumentParser(
        description="Run release container tests for a single Neurocontainer",
    )

    root.add_argument("recipe", help="Name of the recipe to test")
    root.add_argument(
        "--version",
        help="Container version to test; defaults to the latest release metadata",
    )
    root.add_argument(
        "--release-file",
        help="Path to a specific release JSON file (overrides automatic lookup)",
    )
    root.add_argument(
        "--runtime",
        choices=["docker", "apptainer", "singularity"],
        help="Preferred container runtime (defaults to auto-detection)",
    )
    root.add_argument(
        "--location",
        choices=["auto", "cvmfs", "local", "release", "docker"],
        default="auto",
        help="Where to source the container from",
    )
    root.add_argument(
        "--test-config",
        help="Override the test configuration file (defaults to recipe test.yaml/build.yaml)",
    )
    root.add_argument(
        "-o",
        "--output",
        help="Path to write JSON test results (defaults to builder/test-results-<recipe>.json)",
    )
    root.add_argument(
        "--gpu", action="store_true", help="Enable GPU support when running tests"
    )
    root.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove downloaded container after testing",
    )
    root.add_argument(
        "--auto-cleanup",
        action="store_true",
        help="Automatically remove downloaded container even if tests fail",
    )
    root.add_argument(
        "--cleanup-all",
        action="store_true",
        help="Remove all cached containers and exit",
    )
    root.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    args = root.parse_args()

    from workflows.test_runner import ContainerTestRunner, TestRequest

    runner = ContainerTestRunner()

    if args.cleanup_all:
        count = runner.cleanup_all(verbose=args.verbose)
        print(f"Cleaned up {count} cached container file(s)")
        return

    repo_path = Path(get_repo_path())
    recipe_dir = repo_path / "recipes" / args.recipe
    if not recipe_dir.is_dir():
        print(f"Error: Recipe directory not found: {recipe_dir}", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output).resolve() if args.output else None
    output_dir = output_path.parent if output_path else None

    request = TestRequest(
        recipe=args.recipe,
        version=args.version,
        release_file=args.release_file,
        test_config=args.test_config,
        runtime=args.runtime,
        location=args.location,
        gpu=args.gpu,
        cleanup=args.cleanup,
        auto_cleanup=args.auto_cleanup,
        verbose=args.verbose,
        allow_missing_tests=False,
        output_dir=output_dir,
        results_path=output_path,
    )

    try:
        outcome = runner.run(request)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    results = outcome.results
    print(f"\nTest results written to {outcome.results_path}")
    if outcome.comment_path:
        print(f"Comment markdown: {outcome.comment_path}")
    if outcome.report_path:
        print(f"Detailed report: {outcome.report_path}")

    container_ref = results.get("container", f"{args.recipe}:{outcome.version}")
    print(f"Container: {container_ref}")
    print(f"  Total: {results.get('total_tests', results.get('total', 0))}")
    print(f"  Passed: {results.get('passed', 0)}")
    print(f"  Failed: {results.get('failed', 0)}")
    print(f"  Skipped: {results.get('skipped', 0)}")

    exit_status = 0 if outcome.status == "passed" else 1
    sys.exit(exit_status)


def init_main():
    root = argparse.ArgumentParser(
        description="NeuroContainer Builder - Initialize a new recipe",
    )

    root.add_argument("name", help="Name of the recipe to create")
    root.add_argument("version", help="Version of the recipe to create")

    args = root.parse_args()

    repo_path = get_repo_path()

    init_new_recipe(
        repo_path,
        args.name,
        args.version,
    )


def main(args):
    root = argparse.ArgumentParser(
        description="NeuroContainer Builder",
    )

    command = root.add_subparsers(dest="command")

    build_parser = command.add_parser(
        "generate",
        help="Generate a Docker image from a description file",
    )
    build_parser.add_argument("name", help="Name of the recipe to generate")
    build_parser.add_argument(
        "--output-directory",
        help="Output directory for the build",
        default=os.path.join(os.getcwd(), "build"),
    )
    build_parser.add_argument(
        "--recreate", action="store_true", help="Recreate the build directory"
    )
    build_parser.add_argument(
        "--build", action="store_true", help="Build the Docker image after creating it"
    )
    build_parser.add_argument(
        "--build-sif",
        action="store_true",
        help="Build a Singularity image after building the Docker image",
    )
    build_parser.add_argument(
        "--build-tinyrange",
        action="store_true",
        help="Build the Docker image after creating it using TinyRange",
    )
    build_parser.add_argument(
        "--tinyrange-path",
        help="Path to the TinyRange binary",
        default="tinyrange",
    )
    build_parser.add_argument(
        "--max-parallel-jobs",
        type=int,
        help="Maximum number of parallel jobs to run during the build",
        default=os.cpu_count(),
    )
    build_parser.add_argument(
        "--test", action="store_true", help="Run tests after building"
    )
    build_parser.add_argument(
        "--architecture",
        help="Architecture to build for",
        default=platform.machine(),
    )
    build_parser.add_argument(
        "--ignore-architectures", action="store_true", help="Ignore architecture checks"
    )
    build_parser.add_argument(
        "--option",
        action="append",
        help="Set an option in the description file. Use --option key=value",
    )
    build_parser.add_argument(
        "--login",
        action="store_true",
        help="Run a interactive docker container with the generated image",
    )
    build_parser.add_argument(
        "--check-only",
        action="store_true",
        help="Check the recipe and exit without building",
    )
    build_parser.add_argument(
        "--auto-build",
        action="store_true",
        help="Set if the recipe is being built in CI",
    )
    build_parser.add_argument(
        "--generate-release",
        action="store_true",
        help="Generate release files after successful build",
    )
    build_parser.add_argument(
        "--gpu",
        action="store_true",
        help="Enable GPU support by adding --gpus all to Docker run commands",
    )
    build_parser.add_argument(
        "--use-buildkit",
        action="store_true",
        help="Use buildkitd/buildctl instead of Docker CLI",
    )
    build_parser.add_argument(
        "--use-podman",
        action="store_true",
        help="Use Podman instead of Docker",
    )
    build_parser.add_argument(
        "--load-into-docker",
        action="store_true",
        help="After BuildKit build, docker load the resulting image tar if Docker is available",
    )

    init_parser = command.add_parser(
        "init",
        help="Initialize a new recipe",
    )
    init_parser.add_argument("name", help="Name of the recipe to create")
    init_parser.add_argument("version", help="Version of the recipe to create")

    cleanup_parser = command.add_parser(
        "cleanup",
        help="Clean up cached files and temporary downloads",
    )
    cleanup_parser.add_argument("--url", help="URL of specific cached file to clean up")
    cleanup_parser.add_argument(
        "--temp-files",
        action="store_true",
        help="Clean up all temporary download files",
    )
    cleanup_parser.add_argument(
        "--all", action="store_true", help="Clean up all cached files and temp files"
    )

    args = root.parse_args()

    repo_path = get_repo_path()

    if args.command == "cleanup":
        if args.url:
            # Clean up specific URL
            success = cleanup_cached_file(args.url)
            if success:
                print(f"Successfully cleaned up cached file for: {args.url}")
            else:
                print(f"Failed to clean up cached file for: {args.url}")
                sys.exit(1)
        elif args.temp_files:
            # Clean up temp files
            count = cleanup_temp_files()
            print(f"Cleaned up {count} temporary files")
        elif args.all:
            # Clean up everything
            cache_dir = get_cache_dir()
            if os.path.exists(cache_dir):
                try:
                    shutil.rmtree(cache_dir)
                    print(f"Cleaned up entire cache directory: {cache_dir}")
                except OSError as e:
                    print(f"Failed to clean up cache directory: {e}")
                    sys.exit(1)
            else:
                print("Cache directory does not exist")
        else:
            # Default: just clean up temp files
            count = cleanup_temp_files()
            print(f"Cleaned up {count} temporary files")
    elif args.command == "init":
        init_new_recipe(
            repo_path,
            args.name,
            args.version,
        )
    elif args.command == "generate":
        recipe_path = get_recipe_directory(repo_path, args.name)

        if args.build_tinyrange:
            build_tinyrange(
                args.tinyrange_path,
                os.path.join(recipe_path, "build.yaml"),
                args.output_directory,
                args.name,
                args.version,
            )
            return

        recipe = load_description_file(recipe_path)

        ctx = generate_from_description(
            repo_path,
            recipe_path,
            recipe,
            args.output_directory,
            architecture=args.architecture,
            ignore_architecture=args.ignore_architectures,
            auto_build=args.auto_build,
            max_parallel_jobs=args.max_parallel_jobs,
            options=args.option,
            recreate_output_dir=args.recreate,
            check_only=args.check_only,
            gpu=args.gpu,
        )

        # Generate release file if requested (even without building)
        if (
            ctx
            and args.generate_release
            and should_generate_release_file(args.generate_release)
        ):
            generate_release_file(
                ctx.name,
                ctx.version,
                recipe,
                recipe_path,
            )

        if args.build:
            if ctx is None:
                print("Recipe generation failed.")
                sys.exit(1)
            if ctx.dockerfile_name is None:
                raise ValueError("Dockerfile name not set.")
            if ctx.build_directory is None:
                raise ValueError("Build directory not set.")
            if ctx.tag is None:
                raise ValueError("Tag not set.")

            build_and_run_container(
                ctx.dockerfile_name,
                ctx.name,
                ctx.version,
                ctx.tag,
                ctx.arch,
                recipe_path,
                ctx.build_directory,
                login=args.login,
                build_sif=args.build_sif,
                generate_release=args.generate_release,
                gpu=args.gpu,
                use_buildkit=args.use_buildkit,
                use_podman=args.use_podman,
                load_into_docker=args.load_into_docker,
            )
    else:
        root.print_help()
        sys.exit(1)


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
