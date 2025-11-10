"""
CLI Module for Instruments Service

Provides command-line interface for instrument generation and querying.
"""

from .main import main
from .parser import parse_arguments

__all__ = ["main", "parse_arguments"]
