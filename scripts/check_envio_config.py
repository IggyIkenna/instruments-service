#!/usr/bin/env python3
"""
Helper script to check Envio Dashboard or test local deployment.

This script helps you:
1. Test if Envio endpoint URL is configured
2. Verify API token retrieval from Secret Manager (GCP or AWS)
3. Test a simple GraphQL query (if endpoint is set)

Supports multi-cloud via CLOUD_PROVIDER environment variable.
"""

import logging
import sys

import requests
from unified_trading_library import get_secret_client

from instruments_service.config import instruments_config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_envio_secret() -> str | None:
    """Get Envio API token from Secret Manager (GCP or AWS)."""
    project_id = instruments_config.gcp_project_id
    if not project_id:
        logger.error("PROJECT_ID not found. Set GCP_PROJECT_ID environment variable.")
        return None
    secret_id = "envio-api-key"

    secret_client = get_secret_client()
    token = secret_client.get_secret(secret_id)
    return token.strip() if token else None


def test_envio_endpoint(api_url: str, api_token: str) -> bool:
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
        response = requests.post(api_url, json={"query": query}, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            logger.warning("GraphQL errors: %s", data["errors"])
            return False

        logger.info("Endpoint is accessible and responding!")
        logger.info(
            "   Query type: %s",
            data.get("data", {}).get("__schema", {}).get("queryType", {}).get("name", "Unknown"),
        )
        return True

    except requests.exceptions.RequestException as e:
        logger.error("Failed to connect to endpoint: %s", e)
        return False


def main() -> int:
    logger.info("=" * 60)
    logger.info("Envio Configuration Checker")
    logger.info("=" * 60)

    # Check API token
    logger.info("1. Checking Envio API token from Secret Manager...")
    api_token = get_envio_secret()
    if api_token:
        logger.info("API token retrieved: %s...%s", api_token[:20], api_token[-10:])
    else:
        logger.error("Failed to retrieve API token")
        return 1

    # Check endpoint URL
    logger.info("2. Checking ENVIO_API_URL environment variable...")
    api_url = instruments_config.envio_api_url
    if api_url:
        logger.info("Endpoint URL configured: %s", api_url)

        # Test endpoint
        logger.info("3. Testing endpoint connectivity...")
        if test_envio_endpoint(api_url, api_token):
            logger.info("All checks passed! Envio is configured correctly.")
            return 0
        else:
            logger.warning("Endpoint configured but not accessible. Check the URL.")
            return 1
    else:
        logger.warning("ENVIO_API_URL not set")
        logger.info("Next Steps for Local Development:")
        logger.info("   1. Clone the Uniswap V4 Indexer:")
        logger.info("      git clone https://github.com/enviodev/uniswap-v4-indexer.git")
        logger.info("      cd uniswap-v4-indexer")
        logger.info("   2. Install dependencies: pnpm install")
        logger.info("   3. Configure environment:")
        logger.info("      - Create .env file in uniswap-v4-indexer directory")
        logger.info("      - Add: ENVIO_API_TOKEN=<token-from-secret-manager>")
        logger.info("   4. Start the indexer: pnpm envio dev")
        logger.info("   5. Set ENVIO_API_URL=http://localhost:8080/v1/graphql in .env")
        logger.info("   6. Wait for initial sync (10-30 minutes)")
        logger.info("   7. Test connection: python3 scripts/check_envio_config.py")
        return 1


if __name__ == "__main__":
    sys.exit(main())
