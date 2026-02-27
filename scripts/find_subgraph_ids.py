#!/usr/bin/env python3
"""
Script to find The Graph subgraph IDs for Ethereum subgraphs.

This script queries The Graph Explorer API to find subgraph IDs for popular DeFi protocols.
"""

import os
from uuid import uuid4

import requests
from unified_cloud_services import get_secret_with_fallback
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext

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
    except Exception as e:
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
    except Exception as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        print(f"   ⚠️  Error querying registry: {e}")
    return None


def main():
    """Main function to find all subgraph IDs."""
    print("🔍 Finding Ethereum subgraph IDs...")
    print("=" * 60)

    # API key via get_secret_with_fallback (Secret Manager first, env fallback) per instruments-and-api-keys-standard
    project_id = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    api_key = (
        get_secret_with_fallback(
            secret_name="thegraph-api-key",
            project_id=project_id or "",
            fallback_env_var="THEGRAPH_API_KEY",
        )
        if project_id
        else None
    )
    api_key = api_key or "test-key"  # Placeholder for dry-run when no key available

    subgraph_ids = {}

    for subgraph_name, display_name in SUBDGRAPH_NAMES.items():
        print(f"\n📊 Looking up: {display_name} ({subgraph_name})")

        # Try known IDs first
        subgraph_id = KNOWN_SUBGRAPH_IDS.get(subgraph_name)

        if not subgraph_id:
            # Try querying registry
            print("   🔍 Querying subgraph registry...")
            subgraph_id = query_subgraph_registry(subgraph_name)

        if subgraph_id:
            subgraph_ids[subgraph_name] = subgraph_id
            print(f"   ✅ Found ID: {subgraph_id}")

            # Verify if we have a real API key
            if api_key != "test-key":
                if verify_subgraph_id(subgraph_name, subgraph_id, api_key):
                    print("   ✅ Verified: ID works with API key")
                else:
                    print("   ⚠️  Warning: ID may not be valid")
        else:
            print("   ⚠️  Could not find ID (will use name-based URL)")
            subgraph_ids[subgraph_name] = None

    print("\n" + "=" * 60)
    print("📋 Subgraph ID Mapping:")
    print("=" * 60)
    print("\nSUBGRAPH_IDS = {")
    for subgraph_name, subgraph_id in subgraph_ids.items():
        if subgraph_id:
            print(f"    '{subgraph_name}': '{subgraph_id}',")
        else:
            print(f"    # '{subgraph_name}': None,  # Need to look up")
    print("}")

    # Also print as Python dict (only with IDs)
    print("\n" + "=" * 60)
    print("📝 Copy this to the_graph_client.py:")
    print("=" * 60)
    print("\nSUBGRAPH_IDS = {")
    for subgraph_name, subgraph_id in subgraph_ids.items():
        if subgraph_id:
            print(f"    '{subgraph_name}': '{subgraph_id}',")
    print("}")


if __name__ == "__main__":
    main()
