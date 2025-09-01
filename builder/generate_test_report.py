#!/usr/bin/env python3
"""
Generate markdown test report from JSON test results.
"""
import argparse
import json


def generate_report(test_results_file: str, container_name: str, container_version: str) -> str:
    """Generate a markdown report from test results JSON."""
    try:
        with open(test_results_file, 'r') as f:
            results = json.load(f)
        
        report = f'## Test Results for {container_name}:{container_version}\n\n'
        
        # Status line
        status = "✅ PASSED" if results["failed"] == 0 else "❌ FAILED"
        report += f'**Status:** {status}\n'
        report += f'**Summary:** {results["passed"]}/{results["total_tests"]} tests passed\n\n'
        
        # Failed tests section
        if results['failed'] > 0:
            report += '### Failed Tests:\n'
            for test in results['test_results']:
                if test['status'] == 'failed':
                    report += f'- ❌ {test["name"]}\n'
                    if test.get('stderr'):
                        report += f'  ```\n  {test["stderr"]}\n  ```\n'
        
        # Passed tests section
        if results['passed'] > 0:
            report += '### Passed Tests:\n'
            for test in results['test_results']:
                if test['status'] == 'passed':
                    report += f'- ✅ {test["name"]}\n'
        
        return report
        
    except Exception as e:
        error_report = f'## Test Results for {container_name}:{container_version}\n\n'
        error_report += f'❌ **ERROR**: Could not generate test report: {str(e)}\n'
        return error_report


def main():
    parser = argparse.ArgumentParser(description='Generate markdown test report from JSON results')
    parser.add_argument('test_results_file', help='Path to JSON test results file')
    parser.add_argument('container_name', help='Container name')
    parser.add_argument('container_version', help='Container version')
    parser.add_argument('--output', '-o', help='Output markdown file path')
    
    args = parser.parse_args()
    
    # Generate the report
    report = generate_report(args.test_results_file, args.container_name, args.container_version)
    
    # Write to output file or stdout
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Test report written to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()