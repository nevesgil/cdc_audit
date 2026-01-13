#!/usr/bin/env python3
"""
Test runner script for CDC Audit System.

Provides convenient ways to run different types of tests
with proper setup and configuration.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd: list, cwd: Path = None, env: dict = None) -> int:
    """Run a command and return exit code."""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or Path.cwd(),
            env=env or os.environ.copy(),
            check=True
        )
        return result.returncode
    except subprocess.CalledProcessError as e:
        return e.returncode


def setup_test_environment():
    """Set up test environment variables."""
    test_env = os.environ.copy()

    # Test-specific environment variables
    test_env.update({
        "ENVIRONMENT": "testing",
        "DEBUG": "true",
        "LOG_LEVEL": "WARNING",
        "LOG_STRUCTURED": "false",
        "MONITORING_ENABLED": "false",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",
        "SINK_DB_HOST": "localhost",
        "SINK_DB_PORT": "5433",
        "SINK_DB_USERNAME": "test_admin",
        "SINK_DB_PASSWORD": "test_admin",
        "SINK_DB_NAME": "test_sink_db",
    })

    return test_env


def run_unit_tests(args):
    """Run unit tests."""
    print("Running unit tests...")

    cmd = [
        sys.executable, "-m", "pytest",
        "tests/unit/",
        "-v",
        "--tb=short",
        f"--maxfail={args.maxfail}",
    ]

    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html",
            "--cov-report=term-missing",
        ])

    if args.parallel:
        cmd.extend(["-n", str(args.parallel)])

    return run_command(cmd, env=setup_test_environment())


def run_integration_tests(args):
    """Run integration tests."""
    print("Running integration tests...")

    cmd = [
        sys.executable, "-m", "pytest",
        "tests/integration/",
        "-v",
        "--tb=short",
        f"--maxfail={args.maxfail}",
        "-m", "integration",
    ]

    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html",
            "--cov-report=term-missing",
        ])

    return run_command(cmd, env=setup_test_environment())


def run_all_tests(args):
    """Run all tests."""
    print("Running all tests...")

    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "--tb=short",
        f"--maxfail={args.maxfail}",
    ]

    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html",
            "--cov-report=term-missing",
        ])

    if args.parallel:
        cmd.extend(["-n", str(args.parallel)])

    return run_command(cmd, env=setup_test_environment())


def run_specific_test(args):
    """Run a specific test file or class."""
    print(f"Running specific test: {args.test_path}")

    cmd = [
        sys.executable, "-m", "pytest",
        args.test_path,
        "-v",
        "--tb=short",
    ]

    if args.coverage:
        cmd.extend([
            "--cov=src",
            "--cov-report=html",
            "--cov-report=term-missing",
        ])

    return run_command(cmd, env=setup_test_environment())


def check_code_quality():
    """Run code quality checks."""
    print("Running code quality checks...")

    # Run flake8
    print("Running flake8...")
    flake8_cmd = [sys.executable, "-m", "flake8", "src/"]
    if run_command(flake8_cmd) != 0:
        print("❌ Flake8 checks failed")
        return 1

    # Run black check
    print("Running black check...")
    black_cmd = [sys.executable, "-m", "black", "--check", "src/"]
    if run_command(black_cmd) != 0:
        print("❌ Black formatting check failed")
        return 1

    print("✅ All code quality checks passed")
    return 0


def generate_coverage_report():
    """Generate coverage report."""
    print("Generating coverage report...")

    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "--cov=src",
        "--cov-report=html",
        "--cov-report=term-missing",
        "--cov-fail-under=80",
    ]

    return run_command(cmd, env=setup_test_environment())


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="CDC Audit System Test Runner")

    # Test type subcommands
    subparsers = parser.add_subparsers(dest="command", help="Test commands")

    # Unit tests
    unit_parser = subparsers.add_parser("unit", help="Run unit tests")
    unit_parser.add_argument("--coverage", action="store_true", help="Generate coverage report")
    unit_parser.add_argument("--parallel", type=int, help="Number of parallel workers")
    unit_parser.add_argument("--maxfail", type=int, default=5, help="Maximum number of failures")

    # Integration tests
    integration_parser = subparsers.add_parser("integration", help="Run integration tests")
    integration_parser.add_argument("--coverage", action="store_true", help="Generate coverage report")
    integration_parser.add_argument("--maxfail", type=int, default=5, help="Maximum number of failures")

    # All tests
    all_parser = subparsers.add_parser("all", help="Run all tests")
    all_parser.add_argument("--coverage", action="store_true", help="Generate coverage report")
    all_parser.add_argument("--parallel", type=int, help="Number of parallel workers")
    all_parser.add_argument("--maxfail", type=int, default=5, help="Maximum number of failures")

    # Specific test
    specific_parser = subparsers.add_parser("specific", help="Run specific test")
    specific_parser.add_argument("test_path", help="Path to test file or directory")
    specific_parser.add_argument("--coverage", action="store_true", help="Generate coverage report")

    # Code quality
    subparsers.add_parser("quality", help="Run code quality checks")

    # Coverage
    subparsers.add_parser("coverage", help="Generate coverage report")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Change to project root
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    # Run the appropriate command
    if args.command == "unit":
        return run_unit_tests(args)
    elif args.command == "integration":
        return run_integration_tests(args)
    elif args.command == "all":
        return run_all_tests(args)
    elif args.command == "specific":
        return run_specific_test(args)
    elif args.command == "quality":
        return check_code_quality()
    elif args.command == "coverage":
        return generate_coverage_report()

    return 0


if __name__ == "__main__":
    sys.exit(main())