#!/usr/bin/env python3
"""
Script to find The Graph subgraph IDs for Ethereum subgraphs.

This script queries The Graph Explorer API to find subgraph IDs for popular DeFi protocols.
"""

import logging
from uuid import uuid4

import requests
from unified_trading_library import UnifiedCloudConfig
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext
from unified_trading_library import get_secret_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Subgraph names we need IDs for (Ethereum only)
SUBDGRAPH_NAMES = {
    "uniswap/uniswap-v3": "Uniswap V3 Ethereum",
    "curvefi/curve-ethereum": "Curve Ethereum",
    "balancer-labs/balancer-v2": "Balancer V2 Ethereum",
    "aave/aave-v3-ethereum": "AAVE V3 Ethereum",
    "lido/lido": "Lido Ethereum",
}

# Known subgraph IDs from The Graph Explorer
# These can be verified at https://thegraph.com/explorer/subgraphs
KNOWN_SUBGRAPH_IDS = {
    "uniswap/uniswap-v3": "9N2fFbE1sPvD1kHvksanLqGX8hNDXq7iLsjXLHSfKzBa",
    "lido/lido": "HXfMc1jPHfFQoccWd7VMv66km75FoxVHDMvsJj5vG5vf",
    # Try to find these by querying the subgraph registry
    "curvefi/curve-ethereum": None,
    "balancer-labs/balancer-v2": None,
    "aave/aave-v3-ethereum": None,
}


def verify_subgraph_id(subgraph_name: str, subgraph_id: str, api_key: str) -> bool:
    """
    Verify a subgraph ID works by querying it.

    Args:
        subgraph_name: Subgraph name
        subgraph_id: Subgraph ID to verify
        api_key: The Graph API key

    Returns:
        True if subgraph ID is valid
    """
    test_url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    # Simple query to test if subgraph exists
    query = "{ _meta { block { number } } }"

    try:
        response = requests.post(
            test_url,
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            if "errors" not in data:
                return True
    except (ConnectionError, TimeoutError, OSError, ValueError) as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.LOW,
            recovery_strategy=ErrorRecoveryStrategy.SKIP,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
    return False


def query_subgraph_registry(subgraph_name: str) -> str | None:
    """
    Query The Graph's subgraph registry to find subgraph ID.

    Args:
        subgraph_name: Subgraph name (e.g., 'uniswap/uniswap-v3')

    Returns:
        Subgraph ID or None
    """
    # The Graph Network subgraph registry
    registry_url = "https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-mainnet"

    # Extract org and name
    parts = subgraph_name.split("/")
    if len(parts) != 2:
        return None

    org, name = parts

    # Query for subgraphs matching the name
    query = f"""
    {{
        subgraphs(
            where: {{
                displayName_contains: "{name}"
            }}
            first: 20
        ) {{
            id
            displayName
            currentVersion {{
                subgraphDeployment {{
                    ipfsHash
                }}
            }}
        }}
    }}
    """

    try:
        response = requests.post(
            registry_url,
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        if "data" in data and "subgraphs" in data["data"]:
            subgraphs = data["data"]["subgraphs"]
            # Look for exact match first
            for subgraph in subgraphs:
                display_name = (subgraph.get("displayName") or "").lower()
                if name.lower() in display_name and org.lower() in display_name:
                    subgraph_id = subgraph.get("id")
                    if subgraph_id:
                        return subgraph_id

            # Fallback: return first match
            if subgraphs:
                subgraph_id = subgraphs[0].get("id")
                if subgraph_id:
                    return subgraph_id
    except (ConnectionError, TimeoutError, OSError, ValueError) as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("Error querying registry: %s", e)
    return None


def main() -> None:
    """Main function to find all subgraph IDs."""
    logger.info("Finding Ethereum subgraph IDs...")
    logger.info("=" * 60)

    # API key via get_secret_client (Secret Manager first, env fallback) per instruments-and-api-keys-standard
    config = UnifiedCloudConfig()
    project_id = config.gcp_project_id
    api_key = (
        get_secret_client(
            secret_name="thegraph-api-key",
            project_id=project_id or "",
        )
        if project_id
        else None
    )
    api_key = api_key or "test-key"  # Placeholder for dry-run when no key available

    subgraph_ids: dict[str, str | None] = {}

    for subgraph_name, display_name in SUBDGRAPH_NAMES.items():
        logger.info("Looking up: %s (%s)", display_name, subgraph_name)

        # Try known IDs first
        subgraph_id = KNOWN_SUBGRAPH_IDS.get(subgraph_name)

        if not subgraph_id:
            # Try querying registry
            logger.info("   Querying subgraph registry...")
            subgraph_id = query_subgraph_registry(subgraph_name)

        if subgraph_id:
            subgraph_ids[subgraph_name] = subgraph_id
            logger.info("   Found ID: %s", subgraph_id)

            # Verify if we have a real API key
            if api_key != "test-key":
                if verify_subgraph_id(subgraph_name, subgraph_id, api_key):
                    logger.info("   Verified: ID works with API key")
                else:
                    logger.warning("   Warning: ID may not be valid")
        else:
            logger.warning("   Could not find ID (will use name-based URL)")
            subgraph_ids[subgraph_name] = None

    logger.info("=" * 60)
    logger.info("Subgraph ID Mapping:")
    logger.info("=" * 60)
    logger.info("SUBGRAPH_IDS = {")
    for subgraph_name, subgraph_id in subgraph_ids.items():
        if subgraph_id:
            logger.info("    '%s': '%s',", subgraph_name, subgraph_id)
        else:
            logger.info("    # '%s': None,  # Need to look up", subgraph_name)
    logger.info("}")

    # Also log as Python dict (only with IDs)
    logger.info("=" * 60)
    logger.info("Copy this to the_graph_client.py:")
    logger.info("=" * 60)
    logger.info("SUBGRAPH_IDS = {")
    for subgraph_name, subgraph_id in subgraph_ids.items():
        if subgraph_id:
            logger.info("    '%s': '%s',", subgraph_name, subgraph_id)
    logger.info("}")


if __name__ == "__main__":
    main()
