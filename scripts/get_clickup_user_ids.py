#!/usr/bin/env python3
"""
Helper script to get ClickUp user IDs for assignees

This script queries the ClickUp API to find user IDs for team members.
Run this once to get the user IDs, then add them to .env file.

Usage:
    python scripts/get_clickup_user_ids.py
"""

import sys
from pathlib import Path

import requests
from unified_cloud_services import get_secret_with_fallback

from instruments_service.config import instruments_config

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_clickup_user_ids():
    """Get user IDs from ClickUp API"""
    # Get API token from Secret Manager, env var, or .env.clickup
    service_env_file = Path(__file__).parent.parent / ".env.clickup"
    root_env_file = Path(__file__).parent.parent.parent / ".env"

    api_token = None

    # Try environment variable first
    api_token = instruments_config.clieckup_api_token

    if not api_token:
        # Try Secret Manager via unified-cloud-services
        try:
            project_id = instruments_config.gcp_project_id
            secret_name = instruments_config.clickup_secret_name
            api_token = get_secret_with_fallback(
                secret_name=secret_name,
                project_id=project_id,
                fallback_env_var="CLICKUP_API_TOKEN",
            )
            if api_token:
                api_token = api_token.strip()
                print(f"✅ Retrieved ClickUp API key from Secret Manager (secret: {secret_name})")
        except ImportError:
            pass  # unified-cloud-services not available, continue to .env files
        except Exception as e:
            print(f"⚠️  Secret Manager lookup failed: {e}")

    if not api_token:
        # Check service-specific .env.clickup first, then root .env (for backwards compatibility)
        for env_file in [service_env_file, root_env_file]:
            if env_file.exists():
                for line in env_file.read_text().splitlines():
                    if line.startswith("clickup_api_token="):
                        api_token = line.split("=", 1)[1].strip()
                        break
                if api_token:
                    break

    if not api_token:
        print("❌ API token not found. Set CLICKUP_API_TOKEN env var or add clickup_api_token=... to .env.clickup")
        print("   Checked: Secret Manager (clickup-api-key)")
        print(f"   Checked: {service_env_file}")
        print(f"   Checked: {root_env_file}")
        print("\n💡 To store API key in Secret Manager, run:")
        print(
            "   cd ../unified-cloud-services && python scripts/store_secret.py --secret-name clickup-api-key --secret-value YOUR_TOKEN"
        )
        return 1

    print(f"✅ Using API token: {api_token[:20]}...")
    print()

    # Get teams (workspaces)
    headers = {"Authorization": api_token, "Content-Type": "application/json"}

    try:
        response = requests.get("https://api.clickup.com/api/v2/team", headers=headers)
        response.raise_for_status()
        teams_data = response.json()

        print("👥 Found teams/workspaces:")
        print()

        all_users = {}

        for team in teams_data.get("teams", []):
            team_id = team.get("id")
            team_name = team.get("name", "Unknown")

            print(f"📋 Team: {team_name} (ID: {team_id})")

            # Get team members from team object directly
            members = team.get("members", [])

            if members:
                for member in members:
                    user = member.get("user", {})
                    user_id = user.get("id")
                    username = user.get("username", "Unknown")
                    email = user.get("email", "N/A")
                    full_name = user.get("name", "N/A")  # Full name field

                    # Use user_id as key to avoid duplicates
                    if user_id:
                        all_users[str(user_id)] = {
                            "id": user_id,
                            "username": username,
                            "email": email,
                            "name": full_name,
                        }

                        print(f"   👤 {username} ({full_name}) - ID: {user_id}, Email: {email}")

            print()

        # Find Ikenna and Harsh
        print("=" * 60)
        print("🎯 User IDs for .env.clickup file:")
        print("=" * 60)
        print()

        ikenna_id = None
        harsh_id = None

        for user_id_key, user_info in all_users.items():
            username = (user_info.get("username") or "").lower()
            email = (user_info.get("email") or "").lower()
            full_name = (user_info.get("name") or "").lower()

            # Check username, email, and full name for "ikenna" or "igboaka"
            if (
                "ikenna" in username
                or "igboaka" in username
                or "ikenna" in email
                or "igboaka" in email
                or "ikenna" in full_name
                or "igboaka" in full_name
            ):
                ikenna_id = user_info["id"]
                print("✅ Found Ikenna:")
                print(f"   Username: {user_info.get('username', 'N/A')}")
                if full_name and full_name != "n/a":
                    print(f"   Full Name: {user_info.get('name', 'N/A')}")
                print(f"   Email: {user_info['email']}")
                print(f"   User ID: {ikenna_id}")
                print()
            elif "harsh" in username or "harsh" in email:
                harsh_id = user_info["id"]
                print("✅ Found Harsh:")
                print(f"   Username: {user_info.get('username', 'N/A')}")
                if full_name and full_name != "n/a":
                    print(f"   Full Name: {user_info.get('name', 'N/A')}")
                print(f"   Email: {user_info['email']}")
                print(f"   User ID: {harsh_id}")
                print()

        if not ikenna_id or not harsh_id:
            print("=" * 60)
            print("📋 All Users Found (for reference):")
            print("=" * 60)
            print()
            print("If Ikenna or Harsh weren't found above, check the list below:")
            print("Look for your email address or name in the list.")
            print()
            for username_lower, user_info in sorted(all_users.items(), key=lambda x: x[1].get("username", "") or ""):
                print(f"   👤 Username: '{user_info.get('username', 'N/A')}'")
                if user_info.get("name") and user_info["name"] != "N/A":
                    print(f"      Full Name: '{user_info['name']}'")
                print(f"      Email: '{user_info['email']}'")
                print(f"      User ID: {user_info['id']}")
                print()

        print("=" * 60)
        print("📝 Add these to your instruments-service/.env.clickup file:")
        print("=" * 60)
        print()

        if ikenna_id:
            print(f"clickup_user_id_ikenna={ikenna_id}")
        else:
            print("⚠️  Ikenna not found - check username spelling (looking for 'ikenna' or 'igboaka')")
            print("clickup_user_id_ikenna=")

        if harsh_id:
            print(f"clickup_user_id_harsh={harsh_id}")
        else:
            print("⚠️  Harsh not found - check username spelling")
            print("clickup_user_id_harsh=")

        print()
        print("💡 Copy the lines above and add them to instruments-service/.env.clickup file")

        return 0

    except requests.exceptions.RequestException as e:
        print(f"❌ Error calling ClickUp API: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"   Response: {e.response.text}")
        return 1


if __name__ == "__main__":
    exit(get_clickup_user_ids())
