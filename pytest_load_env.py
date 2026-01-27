"""
Pytest plugin to load .env file before any test imports.

This plugin ensures environment variables are available when skipif decorators
are evaluated during test module imports.
"""

import os
from pathlib import Path


def pytest_load_initial_conftests(early_config, parser, args):
    """
    Pytest hook that runs VERY early, before any conftest.py files are loaded.

    This ensures .env is loaded before test modules are imported, so skipif
    decorators can access environment variables.
    """
    try:
        from dotenv import load_dotenv

        # Find project root (instruments-service directory)
        # This file should be in the project root
        project_root = Path(__file__).parent
        env_path = project_root / ".env"

        if env_path.exists():
            # Use override=True to ensure .env values take precedence
            # This prevents stale shell environment from breaking tests
            load_dotenv(dotenv_path=env_path, override=True)

            # Resolve relative paths in .env file to absolute paths
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path:
                creds_path_obj = Path(creds_path)
                if not creds_path_obj.is_absolute():
                    # Resolve relative to project root
                    abs_creds_path = (project_root / creds_path).resolve()
                    if abs_creds_path.exists():
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(abs_creds_path)
                    else:
                        # Try parent directories (unified-cloud-services, etc.)
                        parent_creds = project_root.parent / creds_path
                        if parent_creds.exists():
                            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(parent_creds.resolve())
                        else:
                            print(f"⚠️  Credentials file not found: {creds_path}")
                            print(f"   Checked: {abs_creds_path}")
                            print(f"   Checked: {parent_creds}")
                elif not creds_path_obj.exists():
                    print(f"⚠️  Credentials file not found at absolute path: {creds_path}")

            # Ensure GCP_PROJECT_ID is set (required for many tests)
            if not os.getenv("GCP_PROJECT_ID"):
                os.environ["GCP_PROJECT_ID"] = "central-element-323112"

            # Ensure ENVIRONMENT is set for test detection
            if not os.getenv("ENVIRONMENT"):
                os.environ["ENVIRONMENT"] = "development"

            # Debug output for troubleshooting
            final_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            creds_exists = Path(final_creds).exists() if final_creds else False
            print(f"✅ Loaded .env from {env_path}")
            print(f"   GOOGLE_APPLICATION_CREDENTIALS={final_creds}")
            print(f"   Credentials file exists: {creds_exists}")
        else:
            print(f"⚠️  .env file not found at {env_path}")
    except ImportError:
        print("⚠️  python-dotenv not available, skipping .env file loading")
    except Exception as e:
        print(f"⚠️  Error loading .env file: {e}")

