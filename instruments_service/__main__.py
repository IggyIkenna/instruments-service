"""
Instruments Service CLI Entry Point

Enables execution via: python -m instruments_service
ServiceBootstrap handles exit codes internally.
"""

from instruments_service.cli.main import main_service_cli

if __name__ == "__main__":
    main_service_cli()
