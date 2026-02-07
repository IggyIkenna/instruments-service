"""
Instruments Service CLI Entry Point

Enables execution via: python -m instruments_service
"""

import sys

from instruments_service.cli.main import run_cli

if __name__ == "__main__":
    result = run_cli()
    # STRICT: Only exit 0 when status is explicitly "success"
    # All other states (error, partial, warning, unknown) exit non-zero
    # This prevents silent failures in VM/Cloud Run deployments
    exit_code = 0 if result.get("status") == "success" else 1
    sys.exit(exit_code)
