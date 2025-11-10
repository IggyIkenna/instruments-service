"""
Setup script for instruments-service package.

IMPORTANT: This package requires unified-cloud-services to be installed first.
Install unified-cloud-services from the sibling directory:
    pip install -e ../unified-cloud-services

Then install this package:
    pip install -e .
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="instruments-service",
    version="0.1.0",
    description="Service for generating canonical instrument definitions from exchange APIs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Unified Trading System",
    packages=find_packages(exclude=["tests", "tests.*", "examples", "examples.*"]),
    python_requires=">=3.9",
    install_requires=[
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "pandas>=2.0.0",
        "python-dateutil>=2.8.0",
        "numpy>=1.24.0",
        "requests>=2.28.0",
        "ccxt>=4.1.77",
        # Note: unified-cloud-services must be installed separately from local repo:
        # pip install -e ../unified-cloud-services
        # This is a local dependency that cannot be specified in install_requires
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "instruments-service=instruments_service.cli.main:run_cli",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
