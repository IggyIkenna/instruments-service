"""
AAVE protocol adapters package.

- AaveV3Adapter: AAVE V3 markets (The Graph + AaveScan)
- shared: Constants and pure helpers (STATIC_RISK_PARAMS, token mappings)
"""

from instruments_service.app.venues.defi.aave.v3_adapter import AaveV3Adapter

__all__ = ["AaveV3Adapter"]
