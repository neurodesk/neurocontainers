#!/usr/bin/env python3
"""
Portable Container Testing Tool for NeuroContainers

This tool provides a unified interface for testing containers across different
container runtimes (Docker, Apptainer/Singularity) and storage systems (CVMFS).

Features:
- Multi-runtime support (Docker, Apptainer, Singularity)
- CVMFS integration for distributed container access
- Test definition extraction from embedded YAML files
- Automatic test execution for release PRs
- Portable test execution environment
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import yaml
from typing import Dict, List, Optional, Any
import shutil
import urllib.request


class ContainerRuntime:
    """Base class for container runtime implementations"""

    def __init__(self):
        self.name = self.__class__.__name__.lower()

    def is_available(self) -> bool:
        """Check if the runtime is available on the system"""
        raise NotImplementedError

    def run_test(
        self,
        container_ref: str,
        test_script: str,
        volumes: List[Dict[str, str]] = None,
        gpu: bool = False,
        working_dir: str = "/test",
    ) -> subprocess.CompletedProcess:
        """Run a test script in the container"""
        raise NotImplementedError

    def extract_file(
        self, container_ref: str, file_path: str, output_path: str
    ) -> bool:
        """Extract a file from the container"""
        raise NotImplementedError


class DockerRuntime(ContainerRuntime):
    """Docker container runtime implementation"""

    def __init__(self):
        super().__init__()
        self.name = "docker"

    def is_available(self) -> bool:
        return shutil.which("docker") is not None

    def run_test(
        self,
        container_ref: str,
        test_script: str,
        volumes: List[Dict[str, str]] = None,
        gpu: bool = False,
        working_dir: str = "/test",
    ) -> subprocess.CompletedProcess:
        cmd = ["docker", "run", "--rm"]

        # Add volumes
        if volumes:
            for vol in volumes:
                cmd.extend(["-v", f"{vol['host']}:{vol['container']}"])

        # Add GPU support
        if gpu:
            cmd.extend(["--gpus", "all"])

        # Set working directory
        cmd.extend(["-w", working_dir])

        # Add container and command
        cmd.extend([container_ref, "bash", "-c", test_script])

        return subprocess.run(cmd, capture_output=True, text=True)

    def extract_file(
        self, container_ref: str, file_path: str, output_path: str
    ) -> bool:
        try:
            # Create a temporary container to extract the file
            result = subprocess.run(
                ["docker", "create", container_ref], capture_output=True, text=True
            )

            if result.returncode != 0:
                return False

            container_id = result.stdout.strip()

            try:
                # Copy file from container
                subprocess.run(
                    ["docker", "cp", f"{container_id}:{file_path}", output_path],
                    check=True,
                )
                return True
            finally:
                # Clean up temporary container
                subprocess.run(["docker", "rm", container_id], capture_output=True)
        except subprocess.CalledProcessError:
            return False


class ApptainerRuntime(ContainerRuntime):
    """Apptainer/Singularity container runtime implementation"""

    def __init__(self):
        super().__init__()
        self.name = "apptainer"

    def is_available(self) -> bool:
        return (
            shutil.which("apptainer") is not None
            or shutil.which("singularity") is not None
        )

    def _get_command(self) -> str:
        """Get the appropriate command (apptainer or singularity)"""
        if shutil.which("apptainer"):
            return "apptainer"
        elif shutil.which("singularity"):
            return "singularity"
        else:
            raise RuntimeError("Neither apptainer nor singularity found")

    def run_test(
        self,
        container_ref: str,
        test_script: str,
        volumes: List[Dict[str, str]] = None,
        gpu: bool = False,
        working_dir: str = "/test",
    ) -> subprocess.CompletedProcess:
        cmd = [self._get_command(), "exec"]

        # Add volumes (bind mounts)
        if volumes:
            for vol in volumes:
                cmd.extend(["-B", f"{vol['host']}:{vol['container']}"])

        # Add GPU support
        if gpu:
            cmd.append("--nv")

        # For Apptainer, only set working directory if it exists or we have volumes mounted
        if volumes:
            cmd.extend(["--pwd", working_dir])

        # Add container and command - modify script to handle working directory if needed
        if not volumes and working_dir != "/":
            # If no volumes mounted, don't try to cd to /test, just run in root
            final_script = test_script
        else:
            final_script = test_script

        cmd.extend([container_ref, "bash", "-c", final_script])

        return subprocess.run(cmd, capture_output=True, text=True)

    def extract_file(
        self, container_ref: str, file_path: str, output_path: str
    ) -> bool:
        try:
            cmd = [self._get_command(), "exec", container_ref, "cat", file_path]
            result = subprocess.run(cmd, capture_output=True)

            if result.returncode == 0:
                with open(output_path, "wb") as f:
                    f.write(result.stdout)
                return True
        except subprocess.CalledProcessError:
            pass
        return False


class CVMFSContainerLocator:
    """Locate containers in CVMFS"""

    def __init__(self, cvmfs_base: str = "/cvmfs/neurodesk.ardc.edu.au"):
        self.cvmfs_base = cvmfs_base

    def is_available(self) -> bool:
        """Check if CVMFS is mounted and accessible"""
        return os.path.exists(self.cvmfs_base) and os.path.isdir(self.cvmfs_base)

    def find_container(self, name: str, version: str) -> Optional[str]:
        """Find a container in CVMFS by name and version"""
        # Common CVMFS container paths
        potential_paths = [
            f"{self.cvmfs_base}/containers/{name}_{version}.sif",
            f"{self.cvmfs_base}/containers/{name}/{version}.sif",
            f"{self.cvmfs_base}/singularity/{name}_{version}.sif",
            f"{self.cvmfs_base}/singularity/{name}/{version}.sif",
        ]

        for path in potential_paths:
            if os.path.exists(path):
                return path

        return None

    def list_containers(self, name_filter: str = None) -> List[Dict[str, str]]:
        """List available containers in CVMFS"""
        containers = []

        container_dirs = [
            f"{self.cvmfs_base}/containers",
            f"{self.cvmfs_base}/singularity",
        ]

        for container_dir in container_dirs:
            if not os.path.exists(container_dir):
                continue

            for item in os.listdir(container_dir):
                if item.endswith(".sif"):
                    # Parse name_version.sif format
                    name_version = item[:-4]
                    if "_" in name_version:
                        name, version = name_version.rsplit("_", 1)
                        if not name_filter or name_filter in name:
                            containers.append(
                                {
                                    "name": name,
                                    "version": version,
                                    "path": os.path.join(container_dir, item),
                                }
                            )

        return containers


class TestDefinitionExtractor:
    """Extract test definitions from containers and YAML files"""

    def __init__(self, runtime: ContainerRuntime):
        self.runtime = runtime

    def _ensure_builtin_tests(self, tests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Ensure core builtin checks are present in the test list"""
        default_test = {
            "name": "Simple Deploy Bins/Path Test",
            "builtin": "test_deploy.sh",
        }

        if not any(
            isinstance(test, dict) and test.get("builtin") == default_test["builtin"]
            for test in tests
        ):
            tests.insert(0, dict(default_test))

        return tests

    def extract_from_container(self, container_ref: str) -> Optional[Dict[str, Any]]:
        """Extract test definitions from embedded YAML in container"""
        # Try to extract build.yaml from container
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".yaml", delete=False
        ) as temp_file:
            if self.runtime.extract_file(container_ref, "/build.yaml", temp_file.name):
                try:
                    with open(temp_file.name, "r") as f:
                        build_config = yaml.safe_load(f)
                    os.unlink(temp_file.name)
                    return self._extract_tests_from_config(build_config)
                finally:
                    if os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)

        return None

    def extract_from_file(self, config_path: str) -> Optional[Dict[str, Any]]:
        """Extract test definitions from a YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            return self._extract_tests_from_config(config)
        except (FileNotFoundError, yaml.YAMLError):
            return None

    def _extract_tests_from_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract test definitions from build configuration"""
        tests = []

        # Extract tests from build directives
        if "build" in config and "directives" in config["build"]:
            tests.extend(self._walk_directives(config["build"]["directives"]))

        # Look for separate test definitions
        if "tests" in config:
            tests.extend(config["tests"])

        self._ensure_builtin_tests(tests)

        return {
            "name": config.get("name", "unknown"),
            "version": config.get("version", "unknown"),
            "tests": tests,
        }

    def default_test_config(self, name: str = "unknown", version: str = "unknown") -> Dict[str, Any]:
        """Return a minimal test configuration when none is defined"""
        tests: List[Dict[str, Any]] = []
        self._ensure_builtin_tests(tests)
        return {
            "name": name,
            "version": version,
            "tests": tests,
        }

    def _walk_directives(
        self, directives: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Walk through build directives to find test definitions"""
        tests = []

        for directive in directives:
            if "test" in directive:
                tests.append(directive["test"])
            elif "group" in directive:
                tests.extend(self._walk_directives(directive["group"]))

        return tests


class ReleaseContainerDownloader:
    """Download containers from release PR URLs"""

    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or os.path.join(
            os.path.expanduser("~"), ".cache", "neurocontainers"
        )
        os.makedirs(self.cache_dir, exist_ok=True)

        # Base URLs for NeuroContainers
        self.base_urls = [
            "https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk",
        ]

    def download_from_release(
        self, name: str, version: str, build_date: str
    ) -> Optional[str]:
        """Download container using release build information"""
        # The primary format for release containers includes the build date
        # URL format: https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk/{name}_{version}_{build_date}.simg
        filenames = [
            f"{name}_{version}_{build_date}.simg",  # Primary format for releases
        ]

        for filename in filenames:
            cache_path = os.path.join(self.cache_dir, filename)

            # Check cache first
            if os.path.exists(cache_path):
                print(f"Using cached container: {cache_path}")
                return cache_path

            # Try downloading from each base URL
            for base_url in self.base_urls:
                url = f"{base_url}/{filename}"
                print(f"Attempting to download: {url}")
                print(f"Note: Container will be cached in {self.cache_dir}")
                print(
                    "Use --cleanup to remove after testing or --cleanup-all to remove all cached containers"
                )

                try:
                    urllib.request.urlretrieve(url, cache_path)
                    print(f"Successfully downloaded: {cache_path}")
                    return cache_path
                except Exception as e:
                    print(f"Failed to download from {url}: {e}")
                    continue

        return None

    def extract_build_date_from_release(self, release_file: str) -> Optional[str]:
        """Extract build date from release JSON file"""
        try:
            with open(release_file, "r") as f:
                release_data = json.load(f)

            # Get the first app's version (build date)
            apps = release_data.get("apps", {})
            if apps:
                first_app = list(apps.values())[0]
                return first_app.get("version")
        except Exception:
            pass
        return None

    def cleanup_downloaded_container(
        self, container_path: str, verbose: bool = False
    ) -> bool:
        """Remove a downloaded container file from cache"""
        if not container_path or not os.path.exists(container_path):
            return False

        # Only remove files from our cache directory to avoid accidents
        cache_path = os.path.abspath(self.cache_dir)
        container_abs_path = os.path.abspath(container_path)

        if not container_abs_path.startswith(cache_path):
            if verbose:
                print(f"Skipping cleanup: {container_path} is not in cache directory")
            return False

        try:
            os.remove(container_path)
            if verbose:
                print(f"Cleaned up downloaded container: {container_path}")
            return True
        except Exception as e:
            if verbose:
                print(f"Failed to cleanup container {container_path}: {e}")
            return False

    def cleanup_all_cache(self, verbose: bool = False) -> int:
        """Remove all downloaded containers from cache directory"""
        if not os.path.exists(self.cache_dir):
            if verbose:
                print("Cache directory does not exist")
            return 0

        removed_count = 0
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith((".sif", ".simg")):
                    file_path = os.path.join(self.cache_dir, filename)
                    try:
                        os.remove(file_path)
                        removed_count += 1
                        if verbose:
                            print(f"Removed cached container: {filename}")
                    except Exception as e:
                        if verbose:
                            print(f"Failed to remove {filename}: {e}")
        except Exception as e:
            if verbose:
                print(f"Failed to list cache directory: {e}")

        if verbose:
            print(f"Cleaned up {removed_count} cached container(s)")
        return removed_count


class ContainerTester:
    """Main container testing orchestrator"""

    def __init__(self):
        self.runtimes = [DockerRuntime(), ApptainerRuntime()]
        self.cvmfs = CVMFSContainerLocator()
        self.release_downloader = ReleaseContainerDownloader()
        self.test_extractor = None
        self.selected_runtime = None
        self.downloaded_container_path = None  # Track downloaded containers for cleanup

    def select_runtime(self, preferred: str = None) -> ContainerRuntime:
        """Select the best available container runtime"""
        if preferred:
            for runtime in self.runtimes:
                # Handle both "apptainer" and "singularity" for ApptainerRuntime
                runtime_names = [runtime.name]
                if runtime.name == "apptainer":
                    runtime_names.append("singularity")

                if preferred.lower() in runtime_names and runtime.is_available():
                    self.selected_runtime = runtime
                    self.test_extractor = TestDefinitionExtractor(runtime)
                    return runtime

        # Auto-select first available runtime
        for runtime in self.runtimes:
            if runtime.is_available():
                self.selected_runtime = runtime
                self.test_extractor = TestDefinitionExtractor(runtime)
                return runtime

        raise RuntimeError("No container runtime available")

    def find_container(
        self, name: str, version: str, location: str = "auto", release_file: str = None
    ) -> Optional[str]:
        """Find a container across different locations"""
        if location == "auto" or location == "cvmfs":
            if self.cvmfs.is_available():
                cvmfs_path = self.cvmfs.find_container(name, version)
                if cvmfs_path:
                    return cvmfs_path

        if location == "auto" or location == "local":
            # Check for local .sif/.simg files
            local_paths = [
                f"{name}_{version}.sif",
                f"{name}_{version}.simg",
                f"sifs/{name}_{version}.sif",
                f"sifs/{name}_{version}.simg",
                f"./{name}_{version}.sif",
                f"./{name}_{version}.simg",
            ]

            for path in local_paths:
                if os.path.exists(path):
                    return os.path.abspath(path)

        if location == "auto" or location == "release":
            # Try to download using release information
            build_date = None
            if release_file and os.path.exists(release_file):
                build_date = self.release_downloader.extract_build_date_from_release(
                    release_file
                )

            downloaded_path = self.release_downloader.download_from_release(
                name, version, build_date
            )
            if downloaded_path:
                self.downloaded_container_path = downloaded_path  # Track for cleanup
                return downloaded_path

        if location == "auto" or location == "docker":
            # For Docker, the container reference is the tag
            return f"{name}:{version}"

        return None

    def run_test_suite(
        self,
        container_ref: str,
        test_config: Dict[str, Any],
        gpu: bool = False,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Run a complete test suite on a container"""
        results = {
            "container": container_ref,
            "runtime": self.selected_runtime.name,
            "total_tests": len(test_config.get("tests", [])),
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "test_results": [],
        }

        for test in test_config.get("tests", []):
            test_result = self._run_single_test(container_ref, test, gpu, verbose)
            results["test_results"].append(test_result)

            if test_result["status"] == "passed":
                results["passed"] += 1
            elif test_result["status"] == "failed":
                results["failed"] += 1
            else:
                results["skipped"] += 1

        return results

    def _run_single_test(
        self,
        container_ref: str,
        test: Dict[str, Any],
        gpu: bool = False,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Run a single test on a container"""
        test_name = test.get("name", "Unnamed Test")

        if verbose:
            print(f"Running test: {test_name}")

        result = {
            "name": test_name,
            "status": "skipped",
            "stdout": "",
            "stderr": "",
            "return_code": -1,
        }

        # Handle manual tests
        if test.get("manual", False):
            result["status"] = "skipped"
            result["stderr"] = "Manual test - skipped in automated run"
            return result

        # Handle builtin tests
        if "builtin" in test:
            return self._run_builtin_test(container_ref, test, gpu, verbose)

        # Handle script tests
        if "script" in test:
            script = test["script"]
            if isinstance(script, list):
                script = " && ".join(script)

            # Create test volume and handle prep steps if needed
            volumes = []
            volume_name = None

            # Only create volumes for Docker runtime if prep steps exist
            if "prep" in test and self.selected_runtime.name == "docker":
                volume_name = self._create_test_volume(container_ref)
                volumes = [{"host": volume_name, "container": "/test"}]

                # Run prep steps
                for prep in test["prep"]:
                    self._run_prep_step(prep, volume_name, verbose)

            try:
                proc_result = self.selected_runtime.run_test(
                    container_ref, script, volumes, gpu
                )

                result["stdout"] = proc_result.stdout
                result["stderr"] = proc_result.stderr
                result["return_code"] = proc_result.returncode
                result["status"] = "passed" if proc_result.returncode == 0 else "failed"

            except Exception as e:
                result["stderr"] = str(e)
                result["status"] = "failed"
            finally:
                # Clean up test volume
                if volume_name:
                    self._cleanup_test_volume(volume_name)

        return result

    def _create_test_volume(self, container_ref: str) -> str:
        """Create a Docker test volume"""
        if self.selected_runtime.name != "docker":
            return None

        # Generate volume name from container reference
        cleaned_ref = container_ref.replace(":", "-").replace("/", "-")
        volume_name = f"neurocontainer-test-{cleaned_ref}"

        # Remove existing volume if it exists
        try:
            subprocess.run(
                ["docker", "volume", "rm", volume_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass

        # Create new volume
        subprocess.run(
            ["docker", "volume", "create", volume_name],
            stdout=subprocess.DEVNULL,
            check=True,
        )

        return volume_name

    def _cleanup_test_volume(self, volume_name: str):
        """Clean up a Docker test volume"""
        if self.selected_runtime.name != "docker" or not volume_name:
            return

        try:
            subprocess.run(
                ["docker", "volume", "rm", volume_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass

    def _run_prep_step(
        self, prep: Dict[str, Any], volume_name: str, verbose: bool = False
    ):
        """Run a test preparation step"""
        if self.selected_runtime.name != "docker":
            return

        name = prep.get("name")
        image = prep.get("image")
        script = prep.get("script")

        if not name or not image or not script:
            raise ValueError("Prep step must have name, image, and script")

        if verbose:
            print(f"Running prep step: {name}")

        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{volume_name}:/test",
            image,
            "bash",
            "-c",
            f"set -ex\ncd /test\n{script}",
        ]

        subprocess.run(cmd, check=True)

    def _run_builtin_test(
        self,
        container_ref: str,
        test: Dict[str, Any],
        gpu: bool = False,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Run a builtin test (like test_deploy.sh or test_builder.sh)"""
        builtin_name = test["builtin"]

        if verbose:
            print(f"Running builtin test: {builtin_name}")

        # Find builtin test script
        script_path = os.path.join(os.path.dirname(__file__), builtin_name)
        if not os.path.exists(script_path):
            return {
                "name": test.get("name", builtin_name),
                "status": "failed",
                "stdout": "",
                "stderr": f"Builtin test {builtin_name} not found",
                "return_code": -1,
            }

        # Read the builtin test script
        with open(script_path, "r") as f:
            script_content = f.read()

        try:
            proc_result = self.selected_runtime.run_test(
                container_ref, script_content, [], gpu
            )

            return {
                "name": test.get("name", builtin_name),
                "status": "passed" if proc_result.returncode == 0 else "failed",
                "stdout": proc_result.stdout,
                "stderr": proc_result.stderr,
                "return_code": proc_result.returncode,
            }
        except Exception as e:
            return {
                "name": test.get("name", builtin_name),
                "status": "failed",
                "stdout": "",
                "stderr": str(e),
                "return_code": -1,
            }

    def cleanup_downloaded_containers(self, verbose: bool = False) -> bool:
        """Clean up any containers downloaded during testing"""
        if not self.downloaded_container_path:
            if verbose:
                print("No downloaded containers to clean up")
            return True

        success = self.release_downloader.cleanup_downloaded_container(
            self.downloaded_container_path, verbose
        )

        # Reset the tracked path after cleanup
        if success:
            self.downloaded_container_path = None

        return success

    def cleanup_all_cached_containers(self, verbose: bool = False) -> int:
        """Clean up all cached containers"""
        return self.release_downloader.cleanup_all_cache(verbose)

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup"""
        self.cleanup_downloaded_containers(verbose=False)


def main():
    parser = argparse.ArgumentParser(
        description="Portable Container Testing Tool for NeuroContainers",
        epilog="""
Examples:
  %(prog)s mycontainer:1.0 --cleanup                 # Test and cleanup downloaded container
  %(prog)s mycontainer:1.0 --auto-cleanup            # Test with automatic cleanup on exit
  %(prog)s --cleanup-all                             # Remove all cached containers
  %(prog)s --list-containers                         # List available containers in CVMFS
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "container", nargs="?", help="Container name:version or path to container file"
    )
    parser.add_argument(
        "--runtime",
        choices=["docker", "apptainer", "singularity"],
        help="Preferred container runtime",
    )
    parser.add_argument(
        "--location",
        choices=["auto", "cvmfs", "docker", "local"],
        default="auto",
        help="Where to find the container",
    )
    parser.add_argument("--test-config", help="Path to test configuration file (YAML)")
    parser.add_argument(
        "--gpu", action="store_true", help="Enable GPU support for tests"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--output", "-o", help="Output file for test results (JSON)")
    parser.add_argument(
        "--list-containers",
        action="store_true",
        help="List available containers in CVMFS",
    )
    parser.add_argument(
        "--release-file", help="Path to release JSON file for build date extraction"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove downloaded container files after testing",
    )
    parser.add_argument(
        "--cleanup-all",
        action="store_true",
        help="Remove all cached container files and exit",
    )
    parser.add_argument(
        "--auto-cleanup",
        action="store_true",
        help="Automatically cleanup downloaded containers on exit (even if tests fail)",
    )

    args = parser.parse_args()

    tester = ContainerTester()

    # Handle cleanup-all option and exit early
    if args.cleanup_all:
        count = tester.cleanup_all_cached_containers(args.verbose)
        print(f"Cleaned up {count} cached container file(s)")
        return

    # Use context manager for automatic cleanup if requested
    if args.auto_cleanup:
        with tester:
            return run_tests(args, tester)
    else:
        return run_tests(args, tester)


def run_tests(args, tester):
    """Run the actual tests - separated to work with context manager"""


def run_tests(args, tester):
    """Run the actual tests - separated to work with context manager"""
    # List containers if requested
    if args.list_containers:
        if tester.cvmfs.is_available():
            containers = tester.cvmfs.list_containers()
            for container in containers:
                print(
                    f"{container['name']}:{container['version']} -> {container['path']}"
                )
        else:
            print("CVMFS not available")
        return

    # Select runtime
    try:
        runtime = tester.select_runtime(args.runtime)
        if args.verbose:
            print(f"Using container runtime: {runtime.name}")
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Parse container reference if provided
    if args.container:
        # Check if it's a file path
        if (
            os.path.exists(args.container)
            or args.container.startswith("/")
            or args.container.startswith("./")
        ):
            # It's a file path, use it directly
            container_ref = args.container
            name = (
                os.path.basename(args.container).split("_")[0]
                if "_" in os.path.basename(args.container)
                else "unknown"
            )
            version = "latest"
        elif ":" in args.container and not args.container.endswith((".sif", ".simg")):
            # It's a name:version format
            name, version = args.container.split(":", 1)
            container_ref = None  # Will be found later
        else:
            # Assume it's a name without version
            name = args.container
            version = "latest"
            container_ref = None  # Will be found later
    else:
        if not args.list_containers:
            print(
                "Error: Container argument required unless using --list-containers",
                file=sys.stderr,
            )
            sys.exit(1)
        name = version = None
        container_ref = None

    # Find container (skip if just listing and if not already found)
    if not args.list_containers:
        if container_ref is None:
            container_ref = tester.find_container(
                name, version, args.location, args.release_file
            )
        if not container_ref:
            print(f"Error: Container {name}:{version} not found", file=sys.stderr)
            sys.exit(1)

    if not args.list_containers:
        if args.verbose:
            print(f"Found container: {container_ref}")

        # Load test configuration
        test_config = None
        if args.test_config:
            test_config = tester.test_extractor.extract_from_file(args.test_config)
        else:
            # Try to extract from container
            test_config = tester.test_extractor.extract_from_container(container_ref)

        if not test_config or not test_config.get("tests"):
            if args.verbose:
                print(
                    "No test configuration found; falling back to builtin test suite"
                )

            inferred_name = name or "unknown"
            inferred_version = version or "unknown"

            # Derive a name from the container reference if possible
            if (inferred_name == "unknown" or not inferred_name) and container_ref:
                base_name = os.path.basename(container_ref)
                inferred_name = os.path.splitext(base_name)[0] or "unknown"

            test_config = tester.test_extractor.default_test_config(
                inferred_name, inferred_version
            )

        if args.verbose:
            print(f"Found {len(test_config['tests'])} tests")

        # Run tests
        try:
            results = tester.run_test_suite(
                container_ref, test_config, args.gpu, args.verbose
            )
        finally:
            # Clean up downloaded containers if requested (even if tests fail)
            if args.cleanup or args.auto_cleanup:
                tester.cleanup_downloaded_containers(args.verbose)

    if not args.list_containers:
        # Output results
        if args.output:
            with open(args.output, "w") as f:
                json.dump(results, f, indent=2)

        # Print summary
        print(f"\nTest Results for {container_ref}:")
        print(f"  Total: {results['total_tests']}")
        print(f"  Passed: {results['passed']}")
        print(f"  Failed: {results['failed']}")
        print(f"  Skipped: {results['skipped']}")

        if args.verbose:
            print("\nDetailed Results:")
            for test_result in results["test_results"]:
                status_icon = (
                    "✓"
                    if test_result["status"] == "passed"
                    else "✗" if test_result["status"] == "failed" else "⊝"
                )
                print(f"  {status_icon} {test_result['name']}: {test_result['status']}")
                if test_result["status"] == "failed" and test_result["stderr"]:
                    print(f"    Error: {test_result['stderr']}")

        # Exit with error code if any tests failed
        sys.exit(1 if results["failed"] > 0 else 0)


if __name__ == "__main__":
    main()
