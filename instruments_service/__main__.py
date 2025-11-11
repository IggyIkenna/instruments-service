"""
Instruments Service CLI Entry Point

Enables execution via: python -m instruments_service
"""

from .cli.main import run_cli

if __name__ == "__main__":
    run_cli()

