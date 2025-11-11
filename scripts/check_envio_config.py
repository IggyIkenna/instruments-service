#!/usr/bin/env python3
"""
Helper script to check Envio Dashboard or test local deployment.

This script helps you:
1. Test if Envio endpoint URL is configured
2. Verify API token retrieval from Secret Manager
3. Test a simple GraphQL query (if endpoint is set)
"""

import os
import sys
import requests
from google.cloud import secretmanager


def get_envio_secret():
    """Get Envio API token from Secret Manager."""
    project_id = "central-element-323112"
    secret_id = "envio-api-key"

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        print(f"❌ Error retrieving secret: {e}")
        return None


def test_envio_endpoint(api_url: str, api_token: str):
    """Test Envio GraphQL endpoint with a simple query."""
    query = """
    {
      __schema {
        queryType {
          name
        }
      }
    }
    """

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    }

    try:
        response = requests.post(
            api_url, json={"query": query}, headers=headers, timeout=10
        )
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            print(f"⚠️  GraphQL errors: {data['errors']}")
            return False

        print(f"✅ Endpoint is accessible and responding!")
        print(
            f"   Query type: {data.get('data', {}).get('__schema', {}).get('queryType', {}).get('name', 'Unknown')}"
        )
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to endpoint: {e}")
        return False


def main():
    print("=" * 60)
    print("Envio Configuration Checker")
    print("=" * 60)

    # Check API token
    print("\n1. Checking Envio API token from Secret Manager...")
    api_token = get_envio_secret()
    if api_token:
        print(f"✅ API token retrieved: {api_token[:20]}...{api_token[-10:]}")
    else:
        print("❌ Failed to retrieve API token")
        return 1

    # Check endpoint URL
    print("\n2. Checking ENVIO_API_URL environment variable...")
    api_url = os.getenv("ENVIO_API_URL")
    if api_url:
        print(f"✅ Endpoint URL configured: {api_url}")

        # Test endpoint
        print("\n3. Testing endpoint connectivity...")
        if test_envio_endpoint(api_url, api_token):
            print("\n✅ All checks passed! Envio is configured correctly.")
            return 0
        else:
            print("\n⚠️  Endpoint configured but not accessible. Check the URL.")
            return 1
    else:
        print("⚠️  ENVIO_API_URL not set")
        print("\n📋 Next Steps for Local Development:")
        print("   1. Clone the Uniswap V4 Indexer:")
        print("      git clone https://github.com/enviodev/uniswap-v4-indexer.git")
        print("      cd uniswap-v4-indexer")
        print("\n   2. Install dependencies:")
        print("      pnpm install")
        print("\n   3. Configure environment:")
        print("      - Create .env file in uniswap-v4-indexer directory")
        print("      - Add: ENVIO_API_TOKEN=<token-from-secret-manager>")
        print("      - Optional: Add custom RPC endpoints")
        print("\n   4. Start the indexer:")
        print("      pnpm envio dev")
        print("      (This starts Docker containers and begins indexing)")
        print("\n   5. Set the endpoint URL in instruments-service/.env:")
        print("      ENVIO_API_URL=http://localhost:8080/v1/graphql")
        print("\n   6. Wait for initial sync (10-30 minutes)")
        print("   7. Test connection: python3 scripts/check_envio_config.py")
        print("\n   See docs/ENVIO_DEPLOYMENT_GUIDE.md for detailed instructions")
        return 1


if __name__ == "__main__":
    sys.exit(main())
