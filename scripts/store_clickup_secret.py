#!/usr/bin/env python3
"""
Helper script to store ClickUp API key in Google Secret Manager

Usage:
    python scripts/store_clickup_secret.py --api-key YOUR_API_KEY [--project-id PROJECT_ID]

This script stores the ClickUp API key in Secret Manager so it can be accessed
via unified-cloud-services without hardcoding it in .env files.
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from unified_cloud_services import create_secret_if_not_exists
except ImportError:
    print(
        "❌ Error: unified-cloud-services not found. Install with: pip install -e ../unified-cloud-services"
    )
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Store ClickUp API key in Secret Manager")
    parser.add_argument(
        "--api-key",
        required=True,
        help="ClickUp API token (starts with pk_...)",
    )
    parser.add_argument(
        "--project-id",
        default="central-element-323112",
        help="GCP project ID (default: central-element-323112)",
    )
    parser.add_argument(
        "--secret-name",
        default="clickup-api-key",
        help="Secret name in Secret Manager (default: clickup-api-key)",
    )

    args = parser.parse_args()

    api_key = args.api_key.strip()
    if not api_key.startswith("pk_"):
        print("⚠️  Warning: ClickUp API keys typically start with 'pk_'")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != "y":
            print("Aborted.")
            return 1

    print(f"📦 Storing ClickUp API key in Secret Manager...")
    print(f"   Project: {args.project_id}")
    print(f"   Secret name: {args.secret_name}")
    print(f"   API key: {api_key[:20]}...")

    try:
        success = create_secret_if_not_exists(
            project_id=args.project_id,
            secret_name=args.secret_name,
            secret_value=api_key,
        )

        if success:
            print(f"\n✅ Successfully stored ClickUp API key in Secret Manager!")
            print(f"\n📝 Next steps:")
            print(f"   1. The scripts will now automatically use this secret")
            print(f"   2. You can remove clickup_api_token from .env.clickup if desired")
            print(f"   3. Test with: python scripts/get_clickup_user_ids.py")
            return 0
        else:
            print(f"\n❌ Failed to store secret. Check your GCP permissions.")
            return 1

    except Exception as e:
        print(f"\n❌ Error storing secret: {e}")
        print(f"\n💡 Make sure you have:")
        print(f"   1. GCP credentials configured (GOOGLE_APPLICATION_CREDENTIALS)")
        print(f"   2. Secret Manager Admin permissions")
        print(f"   3. Correct project ID: {args.project_id}")
        return 1


if __name__ == "__main__":
    exit(main())
