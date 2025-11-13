"""
CLI Module for Instruments Service

Provides command-line interface for instrument generation and querying.
"""

from instruments_service.cli.main import main
from instruments_service.cli.parser import parse_arguments

__all__ = ["main", "parse_arguments"]
