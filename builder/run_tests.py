#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyyaml>=6.0",
#     "rich>=13.0",
#     "nibabel>=5.0",
# ]
# ///
"""
Neurocontainer Test Runner

Runs YAML-based tests for neuroimaging containers with parallel execution support.

Usage:
    ./run_tests.py                          # Run all tests
    ./run_tests.py niimath.yaml             # Run specific test file
    ./run_tests.py *.yaml -j 4              # Run with 4 parallel workers
    ./run_tests.py -l                       # List available test files
    ./run_tests.py niimath.yaml -f "smooth" # Filter tests by name pattern
    ./run_tests.py --retry results/test_results_20260210.jsonl  # Re-run failed tests

Test files are loaded from tests/ directory. Tests run in work/ directory.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.panel import Panel
from rich import box

console = Console()


@dataclass
class TestResult:
    """Result of a single test execution."""
    name: str
    passed: bool
    duration: float
    start_time: str = ""
    message: str = ""
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0


@dataclass
class TestSuiteResult:
    """Result of a test suite (YAML file) execution."""
    name: str
    container: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    duration: float = 0.0
    results: list[TestResult] = field(default_factory=list)


def find_container(container_pattern: str, containers_dir: Path) -> Path | None:
    """Find container file matching pattern."""
    if containers_dir.exists():
        # Try exact match first
        exact = containers_dir / container_pattern
        if exact.exists():
            return exact

        # Try glob pattern
        base_name = container_pattern.replace(".simg", "").split("_")[0]
        matches = list(containers_dir.glob(f"{base_name}_*.simg"))
        if matches:
            return sorted(matches)[-1]  # Return newest version

    return None


def substitute_variables(text: str, variables: dict[str, str]) -> str:
    """Substitute ${var} placeholders with values."""
    if not text:
        return text

    result = text
    for key, value in variables.items():
        result = result.replace(f"${{{key}}}", str(value))
        result = result.replace(f"${key}", str(value))

    return result


def check_file_exists(path: str) -> bool:
    """Check if file exists."""
    return Path(path).exists()


def check_same_dimensions(path1: str, path2: str) -> tuple[bool, str]:
    """Check if two NIfTI files have same dimensions."""
    try:
        import nibabel as nib

        img1 = nib.load(path1)
        img2 = nib.load(path2)

        shape1 = img1.shape
        shape2 = img2.shape

        if shape1 == shape2:
            return True, f"Dimensions match: {shape1}"
        else:
            return False, f"Dimension mismatch: {shape1} vs {shape2}"
    except Exception as e:
        return False, f"Error comparing dimensions: {e}"


def prepare_required_files(
    required_files: list[dict],
    suite_name: str,
    work_dir: Path,
) -> Path:
    """Prepare required data files for a test suite.

    Uses a datalad-managed cache in work/.data_cache/ and creates hardlinks
    into a per-suite directory (work/<suite_name>/) so each suite gets isolated
    copies that don't affect the cache if deleted by tests.

    Returns the per-suite work directory.
    """
    cache_dir = work_dir / ".data_cache"
    suite_work_dir = work_dir / suite_name

    for entry in required_files:
        dataset = entry["dataset"]
        files = entry.get("files", [])

        dataset_cache = cache_dir / dataset

        # Ensure cache clone exists
        if not dataset_cache.exists():
            console.print(f"  [dim]Cloning {dataset} into cache...[/]")
            cache_dir.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(
                [
                    "datalad", "install", "-s",
                    f"https://github.com/OpenNeuroDatasets/{dataset}.git",
                    str(dataset_cache),
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to clone {dataset}: {result.stderr[:500]}"
                )

        # Ensure each required file is fetched in the cache
        for file_path in files:
            cached_file = dataset_cache / file_path

            # Check if the symlink resolves to an actual file
            if not cached_file.exists():
                console.print(f"  [dim]Fetching {file_path}...[/]")
                result = subprocess.run(
                    ["datalad", "get", file_path],
                    capture_output=True,
                    text=True,
                    timeout=600,
                    cwd=dataset_cache,
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"Failed to fetch {file_path}: {result.stderr[:500]}"
                    )

            # Create copy in per-suite directory.
            # We use copies (not hardlinks) because some tools like SPM
            # decompress .nii.gz in-place and delete the original. Copies
            # let tools freely modify/delete files without affecting the cache.
            suite_file = suite_work_dir / dataset / file_path
            suite_file.parent.mkdir(parents=True, exist_ok=True)

            # Ensure parent dir is writable (may be read-only from prior run)
            if not os.access(suite_file.parent, os.W_OK):
                os.chmod(suite_file.parent, 0o755)

            # Remove existing file/link if present (stale from previous run)
            if suite_file.exists() or suite_file.is_symlink():
                suite_file.unlink()

            real_path = Path(os.path.realpath(cached_file))
            shutil.copy2(real_path, suite_file)

    return suite_work_dir


def run_single_test(
    test: dict,
    container_path: Path,
    variables: dict[str, str],
    work_dir: Path,
    global_env_setup: str | None = None,
    default_timeout: int = 120,
    script_runner: str | None = None,
    script_ext: str = ".sh",
) -> TestResult:
    """Run a single test and return result."""
    from datetime import datetime

    name = test.get("name", "Unnamed test")
    start_timestamp = datetime.now().isoformat()
    start_time = time.time()

    try:
        # Get command or script content
        command = test.get("command", "")
        test_script = test.get("script", "")

        if not command and not test_script:
            return TestResult(
                name=name,
                passed=False,
                duration=0,
                start_time=start_timestamp,
                message="No command or script specified",
            )

        # Handle script: directive — save script to temp file and build command
        extra_script_path = None
        if test_script and not command:
            test_script = substitute_variables(test_script, variables)
            ts = int(time.time() * 1e6)
            extra_script_path = work_dir / f".test_script_{os.getpid()}_{ts}{script_ext}"
            try:
                with open(extra_script_path, 'w') as f:
                    f.write(test_script)
                os.chmod(extra_script_path, 0o755)
            except OSError as e:
                return TestResult(
                    name=name, passed=False, duration=time.time() - start_time,
                    start_time=start_timestamp, message=f"Failed to create test script: {e}",
                )

            if script_runner:
                command = f"{script_runner} {extra_script_path}"
            else:
                # Default: run as bash script
                command = str(extra_script_path)
        else:
            command = substitute_variables(command, variables)

        # Build environment setup
        env_setup = test.get("env_setup", global_env_setup) or ""
        if env_setup:
            env_setup = substitute_variables(env_setup, variables)

        # Write command to temporary script file (avoids all shell quoting issues)
        script_path = work_dir / f".test_{os.getpid()}_{int(time.time()*1e6)}.sh"
        try:
            with open(script_path, 'w') as f:
                f.write("#!/usr/bin/env bash\n")
                if env_setup:
                    f.write(f"{env_setup}\n")
                f.write(f"{command}\n")
            os.chmod(script_path, 0o755)
        except OSError as e:
            return TestResult(
                name=name, passed=False, duration=time.time() - start_time,
                start_time=start_timestamp, message=f"Failed to create test script: {e}",
            )

        try:
            if container_path:
                binds = set()
                binds.add(f"{work_dir}:{work_dir}")
                # Only bind host paths that need to be accessible inside the container.
                # Skip paths under standard container directories (e.g. /opt, /usr)
                # to avoid overlaying the container's own filesystem.
                container_dirs = {"/opt", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc", "/var"}
                for key, value in variables.items():
                    if key not in ["output_dir"] and "/" in str(value):
                        parent = Path(value).parent
                        if parent.exists():
                            # Don't bind if the path is under a standard container directory
                            abs_parent = str(parent.resolve())
                            if not any(abs_parent == d or abs_parent.startswith(d + "/") for d in container_dirs):
                                binds.add(f"{parent}:{parent}")

                cmd_list = ["apptainer", "exec", "--writable-tmpfs"]
                for b in binds:
                    cmd_list.extend(["-B", b])
                cmd_list.extend([str(container_path), "bash", str(script_path)])
            else:
                cmd_list = ["bash", str(script_path)]

            timeout = test.get("timeout", default_timeout)

            result = subprocess.run(
                cmd_list,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=work_dir,
            )

            duration = time.time() - start_time
            stdout = result.stdout
            stderr = result.stderr
            exit_code = result.returncode
        finally:
            try:
                script_path.unlink(missing_ok=True)
            except OSError:
                pass
            if extra_script_path:
                try:
                    extra_script_path.unlink(missing_ok=True)
                except OSError:
                    pass

        # Check expected exit code (default: expect success)
        ignore_exit_code = test.get("ignore_exit_code", False)
        expected_exit_code = test.get("expected_exit_code", 0)
        expected_exit_code_not = test.get("expected_exit_code_not")

        if ignore_exit_code:
            pass  # Skip exit code validation entirely
        elif expected_exit_code_not is not None:
            # expected_exit_code_not takes precedence when explicitly set
            if exit_code == expected_exit_code_not:
                return TestResult(
                    name=name,
                    passed=False,
                    duration=duration,
                    start_time=start_timestamp,
                    message=f"Exit code should not be {expected_exit_code_not}",
                    stdout=stdout,
                    stderr=stderr,
                    exit_code=exit_code,
                )
        elif exit_code != expected_exit_code:
            return TestResult(
                name=name,
                passed=False,
                duration=duration,
                start_time=start_timestamp,
                message=f"Expected exit code {expected_exit_code}, got {exit_code}",
                stdout=stdout,
                stderr=stderr,
                exit_code=exit_code,
            )

        # Check expected output
        expected_output = test.get("expected_output_contains")
        if expected_output:
            combined_output = stdout + stderr

            if isinstance(expected_output, str):
                expected_list = [expected_output]
            else:
                expected_list = expected_output

            for expected in expected_list:
                if expected and expected not in combined_output:
                    return TestResult(
                        name=name,
                        passed=False,
                        duration=duration,
                        start_time=start_timestamp,
                        message=f"Expected output not found: '{expected[:50]}...'",
                        stdout=stdout,
                        stderr=stderr,
                        exit_code=exit_code,
                    )

        # Run validations
        validations = test.get("validate", [])
        for validation in validations:
            if isinstance(validation, dict):
                for val_type, val_arg in validation.items():
                    if val_type == "output_exists":
                        path = substitute_variables(str(val_arg), variables)
                        if not check_file_exists(path):
                            return TestResult(
                                name=name,
                                passed=False,
                                duration=duration,
                                start_time=start_timestamp,
                                message=f"Output file not found: {path}",
                                stdout=stdout,
                                stderr=stderr,
                                exit_code=exit_code,
                            )

                    elif val_type == "same_dimensions":
                        if isinstance(val_arg, list) and len(val_arg) == 2:
                            path1 = substitute_variables(str(val_arg[0]), variables)
                            path2 = substitute_variables(str(val_arg[1]), variables)
                            ok, msg = check_same_dimensions(path1, path2)
                            if not ok:
                                return TestResult(
                                    name=name,
                                    passed=False,
                                    duration=duration,
                                    start_time=start_timestamp,
                                    message=msg,
                                    stdout=stdout,
                                    stderr=stderr,
                                    exit_code=exit_code,
                                )

        return TestResult(
            name=name,
            passed=True,
            duration=duration,
            start_time=start_timestamp,
            message="OK",
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
        )

    except subprocess.TimeoutExpired:
        return TestResult(
            name=name,
            passed=False,
            duration=time.time() - start_time,
            start_time=start_timestamp,
            message=f"Timeout after {test.get('timeout', default_timeout)}s",
        )
    except Exception as e:
        return TestResult(
            name=name,
            passed=False,
            duration=time.time() - start_time,
            start_time=start_timestamp,
            message=f"Error: {e}",
        )


def _run_container_health_check(
    container_path: Path, work_dir: Path, variables: dict[str, str]
) -> TestResult:
    """Quick check that the container can execute a basic command."""
    from datetime import datetime

    start = time.time()

    binds = set()
    binds.add(f"{work_dir}:{work_dir}")

    cmd_list = ["apptainer", "exec", "--writable-tmpfs"]
    for b in binds:
        cmd_list.extend(["-B", b])
    cmd_list.extend([str(container_path), "true"])

    try:
        result = subprocess.run(
            cmd_list, capture_output=True, text=True, timeout=30, cwd=work_dir
        )
        if result.returncode == 0:
            return TestResult(
                name="Container health check",
                passed=True,
                duration=time.time() - start,
                start_time=datetime.now().isoformat(),
                message="OK",
                exit_code=0,
            )
        else:
            return TestResult(
                name="Container health check",
                passed=False,
                duration=time.time() - start,
                start_time=datetime.now().isoformat(),
                message=f"Container cannot execute commands (exit {result.returncode}): {result.stderr[:500]}",
                exit_code=result.returncode,
                stderr=result.stderr,
            )
    except Exception as e:
        return TestResult(
            name="Container health check",
            passed=False,
            duration=time.time() - start,
            start_time=datetime.now().isoformat(),
            message=f"Container health check error: {e}",
        )


def _run_setup_in_container(
    setup_script: str,
    container_path: Path,
    work_dir: Path,
    variables: dict[str, str],
) -> str | None:
    """Run setup script inside the container. Returns error message or None on success."""
    script_path = work_dir / f".setup_{os.getpid()}_{int(time.time()*1e6)}.sh"
    try:
        with open(script_path, 'w') as f:
            f.write(setup_script)
        os.chmod(script_path, 0o755)

        binds = set()
        binds.add(f"{work_dir}:{work_dir}")
        for key, value in variables.items():
            if key not in ["output_dir"] and "/" in str(value):
                parent = Path(value).parent
                if parent.exists():
                    binds.add(f"{parent}:{parent}")

        cmd_list = ["apptainer", "exec", "--writable-tmpfs"]
        for b in binds:
            cmd_list.extend(["-B", b])
        cmd_list.extend([str(container_path), "bash", str(script_path)])

        result = subprocess.run(
            cmd_list,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=work_dir,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            return f"Setup failed (exit {result.returncode}): {stderr[:500]}"
        return None
    except subprocess.TimeoutExpired:
        return "Setup failed: timed out after 120s"
    except Exception as e:
        return f"Setup failed: {e}"
    finally:
        try:
            script_path.unlink(missing_ok=True)
        except OSError:
            pass


def run_test_suite(
    yaml_path: Path,
    containers_dir: Path,
    work_dir: Path,
    test_filter: str | None = None,
    verbose: bool = False,
    on_test_complete: Any = None,
    result_queue: Any = None,
    running_tests: Any = None,
    test_names: set[str] | None = None,
) -> TestSuiteResult:
    """Run all tests in a YAML file."""
    start_time = time.time()

    # Load YAML
    with open(yaml_path) as f:
        config = yaml.safe_load(f)

    suite_name = config.get("name", yaml_path.stem)
    container_name = config.get("container", "")
    default_timeout = config.get("default_timeout", 120)  # Default 2 minutes

    # Find container
    container_path = find_container(container_name, containers_dir)
    if not container_path:
        return TestSuiteResult(
            name=suite_name,
            container=container_name,
            total=0,
            failed=1,
            results=[TestResult(
                name="Container lookup",
                passed=False,
                duration=0,
                message=f"Container not found: {container_name}",
            )],
        )

    # Prepare required data files (datalad cache + hardlinks)
    required_files = config.get("required_files", [])
    if required_files:
        try:
            suite_data_dir = prepare_required_files(required_files, suite_name, work_dir)
        except RuntimeError as e:
            return TestSuiteResult(
                name=suite_name,
                container=container_name,
                total=0,
                failed=1,
                results=[TestResult(
                    name="Data preparation",
                    passed=False,
                    duration=0,
                    message=str(e),
                )],
            )
    else:
        suite_data_dir = work_dir

    # Build variables dict
    variables = {}
    test_data = config.get("test_data", {})
    for key, value in test_data.items():
        if key == "output_dir":
            # Make output dir absolute under work_dir (not suite_data_dir)
            variables[key] = str(work_dir / value)
        else:
            # Make paths absolute — resolve against suite_data_dir so
            # hardlinked required_files are found
            path = Path(value)
            if not path.is_absolute():
                path = suite_data_dir / value
            variables[key] = str(path)

    # Extract top-level simple values as variables (e.g. mitk_path)
    reserved_keys = {"name", "version", "container", "test_data", "setup", "cleanup",
                     "tests", "env_setup", "default_timeout", "matlab_runtime",
                     "script_runner", "script_ext", "required_files"}
    for key, value in config.items():
        if key not in reserved_keys and isinstance(value, (str, int, float)):
            if key not in variables:
                variables[key] = str(value)

    # Extract script runner config for script: directive support
    script_runner = config.get("script_runner")
    script_ext = config.get("script_ext", ".sh")
    if not script_runner:
        matlab_rt = config.get("matlab_runtime")
        if matlab_rt:
            runner = matlab_rt.get("runner", "")
            rt_path = matlab_rt.get("path", "")
            if runner:
                script_runner = f"{runner} {rt_path}".strip()
                script_ext = ".m"

    # Get global env setup
    global_env_setup = config.get("env_setup")

    # Support env: dict format (convert to export statements)
    env_dict = config.get("env")
    if isinstance(env_dict, dict):
        env_exports = "\n".join(f'export {k}="{v}"' for k, v in env_dict.items())
        if global_env_setup:
            global_env_setup = env_exports + "\n" + global_env_setup
        else:
            global_env_setup = env_exports

    # In parallel mode, namespace output_dir per suite to avoid races
    if "output_dir" in variables and result_queue is not None:
        variables["output_dir"] = str(Path(variables["output_dir"]) / suite_name)

    # Clean output directory before running suite
    if "output_dir" in variables:
        output_dir = Path(variables["output_dir"])
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    # Run host setup script (runs on host before container health check)
    setup = config.get("setup", {})
    host_script = setup.get("host_script", "")
    if host_script:
        host_script = substitute_variables(host_script, variables)
        try:
            subprocess.run(
                host_script,
                shell=True,
                check=True,
                cwd=work_dir,
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            return TestSuiteResult(
                name=suite_name,
                container=container_name,
                total=0,
                failed=1,
                results=[TestResult(
                    name="Setup (host)",
                    passed=False,
                    duration=0,
                    message=f"Host setup failed: {e.stderr.decode() if e.stderr else str(e)}",
                )],
            )

    # Container health check
    health_result = _run_container_health_check(container_path, work_dir, variables)
    if not health_result.passed:
        # Get and filter tests to know how many to skip
        tests = config.get("tests", [])
        if test_filter:
            pattern = re.compile(test_filter, re.IGNORECASE)
            tests = [t for t in tests if pattern.search(t.get("name", ""))]
        if test_names is not None:
            tests = [t for t in tests if t.get("name", "") in test_names]

        skip_results = [health_result]
        for test in tests:
            skip_result = TestResult(
                name=test.get("name", "Unnamed test"),
                passed=False,
                duration=0,
                start_time=health_result.start_time,
                message="Skipped: container health check failed",
            )
            skip_results.append(skip_result)

        # Report results via callback/queue so they appear in JSONL output
        for r in skip_results:
            if on_test_complete is not None:
                on_test_complete(suite_name, container_name, r)
            if result_queue is not None:
                result_queue.put({
                    "suite": suite_name,
                    "container": container_name,
                    "test": r.name,
                    "passed": r.passed,
                    "start_time": r.start_time,
                    "duration": r.duration,
                    "message": r.message,
                    "exit_code": r.exit_code,
                    "stdout": r.stdout,
                    "stderr": r.stderr,
                })

        return TestSuiteResult(
            name=suite_name,
            container=container_name,
            total=len(skip_results),
            failed=len(skip_results),
            results=skip_results,
        )

    # Run container setup script (runs inside the container)
    setup_script = setup.get("script", "")
    if setup_script:
        setup_script = substitute_variables(setup_script, variables)
        setup_error = _run_setup_in_container(
            setup_script, container_path, work_dir, variables
        )
        if setup_error:
            return TestSuiteResult(
                name=suite_name,
                container=container_name,
                total=0,
                failed=1,
                results=[TestResult(
                    name="Setup",
                    passed=False,
                    duration=0,
                    message=setup_error,
                )],
            )

    # Get and filter tests
    tests = config.get("tests", [])
    if test_filter:
        pattern = re.compile(test_filter, re.IGNORECASE)
        tests = [t for t in tests if pattern.search(t.get("name", ""))]
    if test_names is not None:
        tests = [t for t in tests if t.get("name", "") in test_names]

    # Run tests
    results = []
    failed_tests: set[str] = set()
    for test in tests:
        test_name = test.get("name", "Unnamed test")
        test_key = f"{suite_name}: {test_name}"

        # Check depends_on — skip if any dependency failed
        depends_on = test.get("depends_on", [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        failed_deps = [dep for dep in depends_on if dep in failed_tests]
        if failed_deps:
            result = TestResult(
                name=test_name,
                passed=False,
                duration=0,
                message=f"Skipped: depends on failed test(s): {', '.join(failed_deps)}",
            )
            results.append(result)
            failed_tests.add(test_name)
        else:
            # Track running test
            if running_tests is not None:
                running_tests[test_key] = True

            result = run_single_test(
                test=test,
                container_path=container_path,
                variables=variables,
                work_dir=work_dir,
                global_env_setup=global_env_setup,
                default_timeout=default_timeout,
                script_runner=script_runner,
                script_ext=script_ext,
            )
            results.append(result)
            if not result.passed:
                failed_tests.add(test_name)

            # Remove from running tests
            if running_tests is not None:
                running_tests.pop(test_key, None)

        # Call callback immediately after each test (for sequential mode)
        if on_test_complete is not None:
            on_test_complete(suite_name, container_name, result)

        # Put result on queue (for parallel mode)
        if result_queue is not None:
            result_queue.put({
                "suite": suite_name,
                "container": container_name,
                "test": result.name,
                "passed": result.passed,
                "start_time": result.start_time,
                "duration": result.duration,
                "message": result.message,
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "stderr": result.stderr,
            })

        if verbose:
            status = "[green]PASS[/]" if result.passed else "[red]FAIL[/]"
            console.print(f"  {status} {result.name} ({result.duration:.2f}s)")
            if not result.passed:
                console.print(f"    [dim]{result.message}[/]")

    # Run cleanup script
    cleanup = config.get("cleanup", {})
    cleanup_script = cleanup.get("script", "")
    if cleanup_script:
        cleanup_script = substitute_variables(cleanup_script, variables)
        try:
            subprocess.run(
                cleanup_script,
                shell=True,
                cwd=work_dir,
                capture_output=True,
            )
        except Exception:
            pass  # Ignore cleanup errors

    duration = time.time() - start_time
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    return TestSuiteResult(
        name=suite_name,
        container=container_name,
        total=len(results),
        passed=passed,
        failed=failed,
        duration=duration,
        results=results,
    )


def run_test_suite_wrapper(args: tuple) -> TestSuiteResult:
    """Wrapper for parallel execution."""
    yaml_path, containers_dir, work_dir, test_filter, verbose, result_queue, running_tests, test_names = args
    return run_test_suite(
        yaml_path, containers_dir, work_dir, test_filter, verbose,
        on_test_complete=None, result_queue=result_queue, running_tests=running_tests,
        test_names=test_names,
    )


def _yaml_suite_name(yaml_path: Path) -> str:
    """Extract the suite name from a YAML test file."""
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
    return config.get("name", yaml_path.stem)


def main():
    parser = argparse.ArgumentParser(
        description="Run neurocontainer tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "yaml_files",
        nargs="*",
        help="YAML test files to run (default: all *.yaml files)",
    )
    parser.add_argument(
        "-j", "--jobs",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)",
    )
    parser.add_argument(
        "-c", "--containers-dir",
        type=Path,
        default=Path("containers"),
        help="Directory containing container files (default: containers)",
    )
    parser.add_argument(
        "-f", "--filter",
        type=str,
        help="Filter tests by name pattern (regex)",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Hide individual test results (only show summary)",
    )
    parser.add_argument(
        "-l", "--list",
        action="store_true",
        help="List available test files",
    )
    parser.add_argument(
        "--failed-only",
        action="store_true",
        help="Only show failed tests in output",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Write results to JSON file",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("test_results.log"),
        help="Write detailed test log (default: test_results.log)",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Disable log file output",
    )
    parser.add_argument(
        "--jsonl",
        type=Path,
        help="Write streaming results to JSONL file (default: results/test_results_<timestamp>.jsonl)",
    )
    parser.add_argument(
        "--no-jsonl",
        action="store_true",
        help="Disable JSONL streaming output",
    )
    parser.add_argument(
        "--retry",
        type=Path,
        metavar="JSONL_FILE",
        help="Re-run only the failed tests from a previous JSONL results file",
    )

    args = parser.parse_args()

    # Default JSONL output path with timestamp
    if args.jsonl is None and not args.no_jsonl:
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_dir = Path.cwd() / "results"
        results_dir.mkdir(exist_ok=True)
        args.jsonl = results_dir / f"test_results_{ts}.jsonl"

    base_dir = Path.cwd()
    tests_dir = base_dir / "tests"
    work_dir = base_dir / "work"
    containers_dir = args.containers_dir.resolve()

    # Ensure work directory exists
    work_dir.mkdir(exist_ok=True)

    # Parse --retry JSONL to build map of suite -> failed test names
    retry_map: dict[str, set[str]] | None = None
    if args.retry:
        if not args.retry.exists():
            console.print(f"[red]Retry file not found: {args.retry}[/]")
            return 1
        retry_map = {}
        with open(args.retry) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if not record.get("passed", True):
                    suite = record["suite"]
                    test_name = record["test"]
                    retry_map.setdefault(suite, set()).add(test_name)
        if not retry_map:
            console.print("[green]No failed tests found in retry file — nothing to re-run.[/]")
            return 0
        total_retry = sum(len(v) for v in retry_map.values())
        console.print(f"[bold]Retrying {total_retry} failed test(s) across {len(retry_map)} suite(s)[/]")

    # Find YAML files in tests/ directory
    if args.yaml_files:
        yaml_files = []
        for pattern in args.yaml_files:
            # Check tests/ directory first
            yaml_files.extend(tests_dir.glob(pattern))
            # Also check if absolute/relative path was given
            if Path(pattern).exists():
                yaml_files.append(Path(pattern))
    else:
        yaml_files = list(tests_dir.glob("*.yaml"))

    yaml_files = sorted(set(yaml_files))

    # When retrying, filter to only suites that had failures
    if retry_map is not None:
        yaml_files = [
            yf for yf in yaml_files
            if _yaml_suite_name(yf) in retry_map
        ]

    if args.list:
        console.print(Panel(f"[bold]Available Test Files[/] (in tests/)", box=box.ROUNDED))
        for f in yaml_files:
            console.print(f"  {f.name}")
        console.print(f"\n[dim]Total: {len(yaml_files)} files[/]")
        return 0

    if not yaml_files:
        console.print(f"[red]No YAML test files found in {tests_dir}[/]")
        return 1

    mode = f"Retry: {args.retry.name}" if retry_map else f"Filter: {args.filter or 'none'}"
    console.print(Panel(
        f"[bold]Neurocontainer Test Runner[/]\n"
        f"Files: {len(yaml_files)} | Workers: {args.jobs} | {mode}\n"
        f"Tests dir: {tests_dir} | Work dir: {work_dir}",
        box=box.ROUNDED,
    ))

    all_results: list[TestSuiteResult] = []
    start_time = time.time()

    # Open JSONL file for streaming results
    jsonl_file = None
    if not args.no_jsonl:
        jsonl_file = open(args.jsonl, "w")

    # Lock for thread-safe JSONL writing
    jsonl_lock = threading.Lock()

    def write_jsonl_record(record: dict):
        """Write a single record to JSONL file (thread-safe)."""
        if jsonl_file is None:
            return
        with jsonl_lock:
            jsonl_file.write(json.dumps(record) + "\n")
            jsonl_file.flush()

    def write_test_result_callback(suite_name: str, container: str, test: TestResult):
        """Callback for sequential mode to write results immediately."""
        write_jsonl_record({
            "suite": suite_name,
            "container": container,
            "test": test.name,
            "passed": test.passed,
            "start_time": test.start_time,
            "duration": test.duration,
            "message": test.message,
            "exit_code": test.exit_code,
            "stdout": test.stdout,
            "stderr": test.stderr,
        })

    if args.jobs > 1:
        # Parallel execution at suite level (tests within a suite run sequentially
        # to preserve intra-suite dependencies on shared output files)
        import queue

        console.print(f"[dim]Running {len(yaml_files)} suites with {args.jobs} parallel workers[/]")

        # Count total tests across all suites for progress bar
        total_tests = 0
        for yaml_path in yaml_files:
            try:
                with open(yaml_path) as f:
                    config = yaml.safe_load(f)
                suite = config.get("name", yaml_path.stem)
                tests = config.get("tests", [])
                if args.filter:
                    pattern = re.compile(args.filter, re.IGNORECASE)
                    tests = [t for t in tests if pattern.search(t.get("name", ""))]
                if retry_map is not None and suite in retry_map:
                    tests = [t for t in tests if t.get("name", "") in retry_map[suite]]
                total_tests += len(tests)
            except Exception:
                pass

        running_tests: dict[str, bool] = {}
        running_tests_lock = threading.Lock()
        test_counts = {"passed": 0, "failed": 0, "completed": 0}
        result_queue: queue.Queue = queue.Queue()

        # Background thread to update progress with running tests
        progress_stop_event = threading.Event()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[green]{task.fields[passed]}[/] passed | [red]{task.fields[failed]}[/] failed | {task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
            refresh_per_second=4,
        ) as progress:
            task = progress.add_task("Running tests...", total=total_tests, passed=0, failed=0)

            def drain_result_queue():
                """Drain result queue, writing JSONL and updating progress."""
                while True:
                    try:
                        record = result_queue.get_nowait()
                    except queue.Empty:
                        break
                    write_jsonl_record(record)
                    test_counts["completed"] += 1
                    if record["passed"]:
                        test_counts["passed"] += 1
                    else:
                        test_counts["failed"] += 1
                    if not args.quiet:
                        test_status = "[green]PASS[/]" if record["passed"] else "[red]FAIL[/]"
                        progress.console.print(f"  {test_status} {record['suite']}: {record['test']} ({record['duration']:.2f}s)")
                        if not record["passed"]:
                            progress.console.print(f"    [dim]{record['message']}[/]")

            def update_running_description():
                """Update progress description with currently running tests."""
                while not progress_stop_event.is_set():
                    try:
                        drain_result_queue()
                        progress.update(task, completed=test_counts["completed"],
                                        passed=test_counts["passed"], failed=test_counts["failed"])

                        with running_tests_lock:
                            running = list(running_tests.keys())
                        if running:
                            display = running[:3]
                            if len(running) > 3:
                                desc = f"Running: {', '.join(display)} (+{len(running)-3} more)"
                            else:
                                desc = f"Running: {', '.join(display)}"
                        else:
                            desc = "Running tests..."
                        progress.update(task, description=desc)
                    except Exception:
                        pass
                    time.sleep(0.25)

            desc_thread = threading.Thread(target=update_running_description, daemon=True)
            desc_thread.start()

            with ThreadPoolExecutor(max_workers=args.jobs) as executor:
                futures = {
                    executor.submit(
                        run_test_suite_wrapper,
                        (yaml_path, containers_dir, work_dir, args.filter, False, result_queue, running_tests,
                         retry_map.get(_yaml_suite_name(yaml_path)) if retry_map else None),
                    ): yaml_path
                    for yaml_path in yaml_files
                }

                for future in as_completed(futures):
                    suite_result = future.result()
                    all_results.append(suite_result)

            # Stop background thread and drain remaining results
            progress_stop_event.set()
            desc_thread.join(timeout=1.0)
            drain_result_queue()
            progress.update(task, completed=test_counts["completed"],
                            passed=test_counts["passed"], failed=test_counts["failed"])
    else:
        # Sequential execution
        for yaml_path in yaml_files:
            suite_test_names = retry_map.get(_yaml_suite_name(yaml_path)) if retry_map else None
            console.print(f"\n[bold cyan]Running: {yaml_path.name}[/]")
            result = run_test_suite(
                yaml_path,
                containers_dir,
                work_dir,
                args.filter,
                verbose=not args.quiet,
                on_test_complete=write_test_result_callback,
                test_names=suite_test_names,
            )
            all_results.append(result)

            status = "[green]PASS[/]" if result.failed == 0 else "[red]FAIL[/]"
            console.print(f"  {status} {result.passed}/{result.total} tests passed ({result.duration:.1f}s)")

    # Close JSONL file
    if jsonl_file is not None:
        jsonl_file.close()
        console.print(f"[dim]Streaming results written to {args.jsonl}[/]")

    total_duration = time.time() - start_time

    # Summary
    console.print("\n")

    total_tests = sum(r.total for r in all_results)
    total_passed = sum(r.passed for r in all_results)
    total_failed = sum(r.failed for r in all_results)
    suites_passed = sum(1 for r in all_results if r.failed == 0)
    suites_failed = sum(1 for r in all_results if r.failed > 0)

    # Results table
    table = Table(title="Test Results Summary", box=box.ROUNDED)
    table.add_column("Suite", style="cyan")
    table.add_column("Passed", style="green", justify="right")
    table.add_column("Failed", style="red", justify="right")
    table.add_column("Total", justify="right")
    table.add_column("Time", justify="right")
    table.add_column("Status")

    for result in sorted(all_results, key=lambda r: (-r.failed, r.name)):
        if args.failed_only and result.failed == 0:
            continue

        status = "[green]PASS[/]" if result.failed == 0 else "[red]FAIL[/]"
        table.add_row(
            result.name,
            str(result.passed),
            str(result.failed),
            str(result.total),
            f"{result.duration:.1f}s",
            status,
        )

    console.print(table)

    # Show failed tests details
    if total_failed > 0:
        console.print("\n[bold red]Failed Tests:[/]")
        for result in all_results:
            for test in result.results:
                if not test.passed:
                    console.print(f"  [red]✗[/] {result.name} > {test.name}")
                    console.print(f"    [dim]{test.message}[/]")

    # Final summary
    console.print(Panel(
        f"[bold]Final Summary[/]\n\n"
        f"Suites: [green]{suites_passed} passed[/], [red]{suites_failed} failed[/] "
        f"({len(all_results)} total)\n"
        f"Tests:  [green]{total_passed} passed[/], [red]{total_failed} failed[/] "
        f"({total_tests} total)\n"
        f"Time:   {total_duration:.1f}s",
        box=box.ROUNDED,
    ))

    # Write JSON output if requested
    if args.output:
        from datetime import datetime

        output_data = {
            "summary": {
                "total_suites": len(all_results),
                "suites_passed": suites_passed,
                "suites_failed": suites_failed,
                "total_tests": total_tests,
                "tests_passed": total_passed,
                "tests_failed": total_failed,
                "duration": total_duration,
                "run_timestamp": datetime.now().isoformat(),
            },
            "suites": [
                {
                    "name": r.name,
                    "container": r.container,
                    "total": r.total,
                    "passed": r.passed,
                    "failed": r.failed,
                    "duration": r.duration,
                    "tests": [
                        {
                            "name": t.name,
                            "passed": t.passed,
                            "start_time": t.start_time,
                            "duration": t.duration,
                            "message": t.message,
                        }
                        for t in r.results
                    ],
                }
                for r in all_results
            ],
        }

        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)

        console.print(f"\n[dim]Results written to {args.output}[/]")

    # Write log file
    if not args.no_log:
        from datetime import datetime

        with open(args.log, "w") as f:
            f.write(f"# Neurocontainer Test Results\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# Total Duration: {total_duration:.2f}s\n")
            f.write(f"#\n")
            f.write(f"# Format: STATE | START_TIME | DURATION | SUITE | TEST_NAME | MESSAGE\n")
            f.write(f"#\n\n")

            for suite_result in sorted(all_results, key=lambda r: r.name):
                for test in suite_result.results:
                    state = "PASS" if test.passed else "FAIL"
                    f.write(
                        f"{state} | {test.start_time} | {test.duration:.3f}s | "
                        f"{suite_result.name} | {test.name} | {test.message}\n"
                    )

        console.print(f"[dim]Log written to {args.log}[/]")

    return 1 if total_failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
