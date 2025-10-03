#!/usr/bin/env python3
"""
Automatic Test Runner for Release Pull Requests

This script identifies modified files in a release PR and automatically
runs the appropriate test scripts for each modified container recipe.

Features:
- Detects modified build.yaml files in PR
- Downloads corresponding .sif containers
- Extracts and runs embedded test definitions
- Supports multiple container locations (CVMFS, local, remote)
- Generates comprehensive test reports
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Set
import re
import urllib.request
import urllib.parse
from container_tester import ContainerTester


class GitChangeDetector:
    """Detect changes in git repository"""
    
    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
    
    def get_modified_files(self, base_ref: str = "origin/master", 
                          head_ref: str = "HEAD") -> List[str]:
        """Get list of modified files between two git references"""
        try:
            result = subprocess.run([
                "git", "diff", "--name-only", f"{base_ref}...{head_ref}"
            ], cwd=self.repo_path, capture_output=True, text=True, check=True)
            
            return [line.strip() for line in result.stdout.split('\n') if line.strip()]
        except subprocess.CalledProcessError as e:
            print(f"Error getting git diff: {e}", file=sys.stderr)
            return []
    
    def get_modified_recipes(self, base_ref: str = "origin/master",
                           head_ref: str = "HEAD") -> List[Dict[str, str]]:
        """Get list of modified recipe directories with their build.yaml files"""
        modified_files = self.get_modified_files(base_ref, head_ref)
        modified_recipes = []
        
        for file_path in modified_files:
            # Check if it's a build.yaml file in a recipe directory
            if file_path.endswith('build.yaml') and '/recipes/' in file_path:
                recipe_dir = os.path.dirname(file_path)
                recipe_name = os.path.basename(recipe_dir)
                
                # Load the build.yaml to get version info
                full_path = os.path.join(self.repo_path, file_path)
                if os.path.exists(full_path):
                    try:
                        with open(full_path, 'r') as f:
                            build_config = yaml.safe_load(f)
                        
                        modified_recipes.append({
                            'name': build_config.get('name', recipe_name),
                            'version': build_config.get('version', 'latest'),
                            'recipe_dir': recipe_dir,
                            'build_file': file_path
                        })
                    except yaml.YAMLError as e:
                        print(f"Warning: Could not parse {file_path}: {e}")
        
        return modified_recipes


class ContainerDownloader:
    """Download containers from various sources"""
    
    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or os.path.join(
            os.path.expanduser("~"), ".cache", "neurocontainers"
        )
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def download_sif(self, name: str, version: str, 
                     base_url: str = None) -> Optional[str]:
        """Download a .sif container file with robust error handling and retry logic"""
        # Default base URLs to try
        base_urls = [
            base_url,
            "https://swift.rc.nectar.org.au/v1/AUTH_d6165cc7b52841659ce8644df1884d5e/singularityimages",
            "https://objectstorage.ap-sydney-1.oraclecloud.com/n/sd63lb5j6a/b/neurodesk/o",
        ]
        
        base_urls = [url for url in base_urls if url]  # Remove None values
        
        filename = f"{name}_{version}.sif"
        cache_path = os.path.join(self.cache_dir, filename)
        temp_path = cache_path + ".tmp"
        
        # Check if already cached and complete
        if os.path.exists(cache_path):
            print(f"Using cached container: {cache_path}")
            return cache_path
        
        # Try downloading from each base URL
        for base_url in base_urls:
            url = f"{base_url}/{filename}"
            print(f"Attempting to download: {url}")
            
            # Try up to 3 times per URL
            for attempt in range(3):
                try:
                    # Use curl for more robust downloading with resumable downloads
                    if shutil.which("curl") is not None:
                        success = self._download_with_curl(url, temp_path, cache_path, attempt)
                        if success:
                            return cache_path
                    else:
                        # Fallback to urllib if curl is not available
                        success = self._download_with_urllib(url, temp_path, cache_path, attempt)
                        if success:
                            return cache_path
                            
                except Exception as e:
                    print(f"Download attempt {attempt + 1}/3 failed from {url}: {e}")
                    # Clean up partial download if it's small (likely error page)
                    if os.path.exists(temp_path):
                        file_size = os.path.getsize(temp_path)
                        if file_size < 1024:  # Less than 1KB
                            try:
                                os.remove(temp_path)
                                print(f"Cleaned up small/corrupted temp file")
                            except OSError:
                                pass
                    
                    if attempt < 2:  # Not the last attempt
                        import time
                        wait_time = 2 ** attempt  # 1s, 2s exponential backoff
                        print(f"Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    continue
            
            # Clean up temp file after all attempts for this URL failed
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                    print(f"Cleaned up temp file after all attempts failed for {url}")
                except OSError:
                    pass
        
        return None
    
    def _download_with_curl(self, url: str, temp_path: str, final_path: str, attempt: int) -> bool:
        """Download using curl with resumable downloads"""
        import subprocess
        
        print(f"Downloading with curl: {url} (attempt {attempt + 1}/3)")
        
        curl_args = [
            "curl",
            "--location",
            "--output", temp_path,
            "--http1.1",  # Force HTTP/1.1 to avoid HTTP/2 issues
            "--retry", "5",
            "--retry-delay", "10",
            "--retry-all-errors",
            "--retry-max-time", "600",  # 10 minutes
            "--connect-timeout", "30",
            "--max-time", "3600",  # 1 hour max for large files
            "--fail",
            "--show-error",
            "--silent"
        ]
        
        # Add resume support if temp file exists
        if os.path.exists(temp_path):
            file_size = os.path.getsize(temp_path)
            if file_size > 0:
                curl_args.extend(["--continue-at", str(file_size)])
                print(f"Resuming download from byte {file_size}")
        
        curl_args.append(url)
        
        try:
            subprocess.check_call(curl_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # Move temp file to final location if download was successful
            if os.path.exists(temp_path):
                shutil.move(temp_path, final_path)
                print(f"Successfully downloaded: {final_path}")
                return True
            else:
                print("Download completed but file not found")
                return False
                
        except subprocess.CalledProcessError as e:
            print(f"Curl download failed with exit code {e.returncode}")
            return False
    
    def _download_with_urllib(self, url: str, temp_path: str, final_path: str, attempt: int) -> bool:
        """Fallback download using urllib"""
        print(f"Downloading with urllib: {url} (attempt {attempt + 1}/3)")
        
        try:
            urllib.request.urlretrieve(url, temp_path)
            
            # Move temp file to final location if download was successful
            if os.path.exists(temp_path):
                shutil.move(temp_path, final_path)
                print(f"Successfully downloaded: {final_path}")
                return True
            else:
                print("Download completed but file not found")
                return False
                
        except Exception as e:
            print(f"Urllib download failed: {e}")
            return False
    
    def find_local_sif(self, name: str, version: str, 
                      search_dirs: List[str] = None) -> Optional[str]:
        """Find a local .sif file"""
        search_dirs = search_dirs or ["./sifs", "./build", "."]
        filename = f"{name}_{version}.sif"
        
        for search_dir in search_dirs:
            full_path = os.path.join(search_dir, filename)
            if os.path.exists(full_path):
                return os.path.abspath(full_path)
        
        return None

    def cleanup_downloaded_container(self, container_path: str, verbose: bool = False) -> bool:
        """Remove a downloaded container file from cache, including temp files"""
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
            # Remove the main file
            os.remove(container_path)
            if verbose:
                print(f"Cleaned up downloaded container: {container_path}")
            
            # Also remove temp file if it exists
            temp_path = container_path + ".tmp"
            if os.path.exists(temp_path):
                os.remove(temp_path)
                if verbose:
                    print(f"Cleaned up temp file: {temp_path}")
            
            return True
        except Exception as e:
            if verbose:
                print(f"Failed to cleanup container {container_path}: {e}")
            return False


class PRTestRunner:
    """Main PR test runner orchestrator"""
    
    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self.git_detector = GitChangeDetector(repo_path)
        self.downloader = ContainerDownloader()
        self.tester = ContainerTester()
        self.downloaded_containers = []  # Track downloaded containers for cleanup
        
        # Select the best runtime
        try:
            self.tester.select_runtime()
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    def run_pr_tests(self, base_ref: str = "origin/master", 
                    head_ref: str = "HEAD",
                    download_containers: bool = True,
                    output_file: str = None,
                    verbose: bool = False) -> Dict[str, any]:
        """Run tests for all modified recipes in a PR"""
        
        # Get modified recipes
        modified_recipes = self.git_detector.get_modified_recipes(base_ref, head_ref)
        
        if not modified_recipes:
            print("No modified recipes found in PR")
            return {"recipes": [], "summary": {"total": 0, "passed": 0, "failed": 0}}
        
        if verbose:
            print(f"Found {len(modified_recipes)} modified recipes:")
            for recipe in modified_recipes:
                print(f"  - {recipe['name']}:{recipe['version']}")
        
        # Run tests for each modified recipe
        results = {
            "recipes": [],
            "summary": {"total": 0, "passed": 0, "failed": 0}
        }
        
        for recipe in modified_recipes:
            recipe_result = self._test_recipe(
                recipe, download_containers, verbose
            )
            results["recipes"].append(recipe_result)
            
            # Update summary
            results["summary"]["total"] += 1
            if recipe_result["status"] == "passed":
                results["summary"]["passed"] += 1
            else:
                results["summary"]["failed"] += 1
        
        # Save results if requested
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
        
        return results
    
    def _test_recipe(self, recipe: Dict[str, str], 
                    download_containers: bool = True,
                    verbose: bool = False) -> Dict[str, any]:
        """Test a single recipe"""
        name = recipe["name"]
        version = recipe["version"]
        recipe_dir = recipe["recipe_dir"]
        
        if verbose:
            print(f"\nTesting recipe: {name}:{version}")
        
        result = {
            "name": name,
            "version": version,
            "recipe_dir": recipe_dir,
            "status": "failed",
            "container_path": None,
            "test_results": None,
            "error": None
        }
        
        try:
            # Find or download container
            container_path = self._find_container(name, version, download_containers, verbose)
            
            if not container_path:
                result["error"] = "Container not found"
                return result
            
            result["container_path"] = container_path
            
            # Load test configuration
            build_file_path = os.path.join(self.repo_path, recipe["build_file"])
            test_config = self.tester.test_extractor.extract_from_file(build_file_path)
            
            if not test_config or not test_config.get("tests"):
                # Try to extract from container
                test_config = self.tester.test_extractor.extract_from_container(container_path)
            
            if not test_config or not test_config.get("tests"):
                if verbose:
                    print("  No test configuration found; using builtin defaults")
                test_config = self.tester.test_extractor.default_test_config(
                    name, version
                )

            # Run tests
            test_results = self.tester.run_test_suite(
                container_path, test_config, gpu=False, verbose=verbose
            )
            
            result["test_results"] = test_results
            result["status"] = "passed" if test_results["failed"] == 0 else "failed"
            
        except Exception as e:
            result["error"] = str(e)
            if verbose:
                import traceback
                traceback.print_exc()
        
        return result
    
    def _find_container(self, name: str, version: str, 
                       download_containers: bool = True,
                       verbose: bool = False) -> Optional[str]:
        """Find a container using multiple strategies"""
        
        # Strategy 1: Check CVMFS
        if self.tester.cvmfs.is_available():
            cvmfs_path = self.tester.cvmfs.find_container(name, version)
            if cvmfs_path:
                if verbose:
                    print(f"  Found in CVMFS: {cvmfs_path}")
                return cvmfs_path
        
        # Strategy 2: Check local files
        local_path = self.downloader.find_local_sif(name, version)
        if local_path:
            if verbose:
                print(f"  Found locally: {local_path}")
            return local_path
        
        # Strategy 3: Download if allowed
        if download_containers:
            downloaded_path = self.downloader.download_sif(name, version)
            if downloaded_path:
                if verbose:
                    print(f"  Downloaded: {downloaded_path}")
                return downloaded_path
        
        # Strategy 4: Try Docker (if available)
        if self.tester.selected_runtime.name == "docker":
            docker_tag = f"{name}:{version}"
            try:
                # Check if Docker image exists
                result = subprocess.run([
                    "docker", "image", "inspect", docker_tag
                ], capture_output=True)
                
                if result.returncode == 0:
                    if verbose:
                        print(f"  Found Docker image: {docker_tag}")
                    return docker_tag
            except Exception:
                pass
        
        return None
    
    def generate_report(self, results: Dict[str, any], 
                       output_format: str = "markdown") -> str:
        """Generate a human-readable test report"""
        
        if output_format == "markdown":
            return self._generate_markdown_report(results)
        elif output_format == "html":
            return self._generate_html_report(results)
        else:
            return json.dumps(results, indent=2)
    
    def _generate_markdown_report(self, results: Dict[str, any]) -> str:
        """Generate a markdown test report"""
        lines = []
        lines.append("# NeuroContainers PR Test Report")
        lines.append("")
        
        summary = results["summary"]
        lines.append(f"**Summary:** {summary['passed']}/{summary['total']} recipes passed")
        lines.append("")
        
        if summary["failed"] > 0:
            lines.append("## ❌ Failed Tests")
            lines.append("")
            
            for recipe in results["recipes"]:
                if recipe["status"] == "failed":
                    lines.append(f"### {recipe['name']}:{recipe['version']}")
                    
                    if recipe["error"]:
                        lines.append(f"**Error:** {recipe['error']}")
                    elif recipe["test_results"]:
                        tr = recipe["test_results"]
                        lines.append(f"**Tests:** {tr['failed']}/{tr['total_tests']} failed")
                        
                        # Show failed test details
                        for test in tr["test_results"]:
                            if test["status"] == "failed":
                                lines.append(f"- ❌ {test['name']}")
                                if test["stderr"]:
                                    lines.append(f"  ```")
                                    lines.append(f"  {test['stderr']}")
                                    lines.append(f"  ```")
                    lines.append("")
        
        if summary["passed"] > 0:
            lines.append("## ✅ Passed Tests")
            lines.append("")
            
            for recipe in results["recipes"]:
                if recipe["status"] == "passed":
                    lines.append(f"- ✅ {recipe['name']}:{recipe['version']}")
                    if recipe["test_results"]:
                        tr = recipe["test_results"]
                        lines.append(f"  - {tr['passed']}/{tr['total_tests']} tests passed")
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_html_report(self, results: Dict[str, any]) -> str:
        """Generate an HTML test report"""
        # Basic HTML report - could be enhanced with CSS styling
        html = """<!DOCTYPE html>
<html>
<head>
    <title>NeuroContainers PR Test Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .passed { color: green; }
        .failed { color: red; }
        .summary { background: #f0f0f0; padding: 10px; border-radius: 5px; }
        .recipe { margin: 10px 0; padding: 10px; border: 1px solid #ddd; }
        pre { background: #f8f8f8; padding: 10px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>NeuroContainers PR Test Report</h1>
"""
        
        summary = results["summary"]
        html += f"""
    <div class="summary">
        <h2>Summary</h2>
        <p><strong>{summary['passed']}/{summary['total']} recipes passed</strong></p>
    </div>
"""
        
        for recipe in results["recipes"]:
            status_class = "passed" if recipe["status"] == "passed" else "failed"
            status_icon = "✅" if recipe["status"] == "passed" else "❌"
            
            html += f"""
    <div class="recipe {status_class}">
        <h3>{status_icon} {recipe['name']}:{recipe['version']}</h3>
"""
            
            if recipe["error"]:
                html += f"<p><strong>Error:</strong> {recipe['error']}</p>"
            elif recipe["test_results"]:
                tr = recipe["test_results"]
                html += f"<p><strong>Tests:</strong> {tr['passed']}/{tr['total_tests']} passed"
                if tr["failed"] > 0:
                    html += f", {tr['failed']} failed"
                if tr["skipped"] > 0:
                    html += f", {tr['skipped']} skipped"
                html += "</p>"
                
                # Show test details
                for test in tr["test_results"]:
                    test_icon = "✅" if test["status"] == "passed" else "❌" if test["status"] == "failed" else "⊝"
                    html += f"<p>{test_icon} {test['name']}: {test['status']}</p>"
                    
                    if test["status"] == "failed" and test["stderr"]:
                        html += f"<pre>{test['stderr']}</pre>"
            
            html += "</div>"
        
        html += """
</body>
</html>"""
        
        return html


def main():
    parser = argparse.ArgumentParser(
        description="Run tests for modified containers in a PR"
    )
    
    parser.add_argument("--base-ref", default="origin/master",
                       help="Base git reference for comparison")
    parser.add_argument("--head-ref", default="HEAD",
                       help="Head git reference for comparison")
    parser.add_argument("--no-download", action="store_true",
                       help="Don't download containers, only use local/CVMFS")
    parser.add_argument("--output", "-o",
                       help="Output file for test results (JSON)")
    parser.add_argument("--report", 
                       choices=["markdown", "html", "json"],
                       help="Generate human-readable report")
    parser.add_argument("--report-file",
                       help="File to save the report to")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Verbose output")
    parser.add_argument("--repo-path", default=".",
                       help="Path to repository root")
    
    args = parser.parse_args()
    
    # Create PR test runner
    runner = PRTestRunner(args.repo_path)
    
    # Run tests
    results = runner.run_pr_tests(
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        download_containers=not args.no_download,
        output_file=args.output,
        verbose=args.verbose
    )
    
    # Generate report if requested
    if args.report:
        report = runner.generate_report(results, args.report)
        
        if args.report_file:
            with open(args.report_file, 'w') as f:
                f.write(report)
            print(f"Report saved to {args.report_file}")
        else:
            print(report)
    
    # Print summary
    summary = results["summary"]
    print(f"\nTest Summary: {summary['passed']}/{summary['total']} recipes passed")
    
    # Exit with error code if any tests failed
    sys.exit(1 if summary['failed'] > 0 else 0)


if __name__ == "__main__":
    main()
