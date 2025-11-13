#!/usr/bin/env python3
"""
Quality Gates for instruments-service

Runs test coverage and ensures 75%+ coverage with all tests passing.

Usage:
    python scripts/run_quality_gates.py [--coverage-threshold 75]
"""

import os
import sys
import subprocess
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_tests_with_coverage(coverage_threshold: int = 75) -> dict:
    """Run tests with coverage and check threshold."""
    print("=" * 70)
    print("INSTRUMENTS-SERVICE QUALITY GATES")
    print("=" * 70)
    print(f"Coverage Threshold: {coverage_threshold}%")
    print("=" * 70)

    # Run pytest with coverage
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/",
        "--cov=instruments_service",
        "--cov-report=term-missing",
        "--cov-report=json:coverage.json",
        "-v",
    ]

    print(f"\nRunning: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, cwd=project_root, capture_output=True, text=True)

    # Check test results
    test_passed = result.returncode == 0
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    # Parse coverage
    coverage_file = project_root / "coverage.json"
    coverage_percent = 0.0

    if coverage_file.exists():
        with open(coverage_file, "r") as f:
            coverage_data = json.load(f)
            coverage_percent = coverage_data.get("totals", {}).get("percent_covered", 0.0)

    # Check if coverage meets threshold
    coverage_meets_threshold = coverage_percent >= coverage_threshold

    print("\n" + "=" * 70)
    print("QUALITY GATES RESULTS")
    print("=" * 70)
    print(f"Tests: {'✅ PASSED' if test_passed else '❌ FAILED'}")
    print(
        f"Coverage: {coverage_percent:.2f}% {'✅' if coverage_meets_threshold else '❌'} (threshold: {coverage_threshold}%)"
    )
    print("=" * 70)

    overall_status = test_passed and coverage_meets_threshold

    if overall_status:
        print("\n✅ ALL QUALITY GATES PASSED")
    else:
        print("\n❌ QUALITY GATES FAILED")
        if not test_passed:
            print("  - Tests are failing")
        if not coverage_meets_threshold:
            print(f"  - Coverage {coverage_percent:.2f}% is below threshold {coverage_threshold}%")

    return {
        "tests_passed": test_passed,
        "coverage_percent": coverage_percent,
        "coverage_meets_threshold": coverage_meets_threshold,
        "overall_status": overall_status,
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Quality Gates for instruments-service")
    parser.add_argument(
        "--coverage-threshold",
        type=int,
        default=75,
        help="Minimum coverage percentage (default: 75)",
    )

    args = parser.parse_args()

    results = run_tests_with_coverage(args.coverage_threshold)

    sys.exit(0 if results["overall_status"] else 1)


if __name__ == "__main__":
    main()
