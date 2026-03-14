"""Unit tests for api-contracts schema usage in instruments-service.

Validates CCXT/The Graph response shapes with api-contracts where
raw API responses are parsed. VCR cassettes live under
api-contracts/ccxt/mocks/ and api-contracts/thegraph/mocks/ when used.
"""

from __future__ import annotations


def test_ccxt_order_validates_with_unified_api_contracts() -> None:
    """Validate a CCXT-like response with api-contracts schema."""
    from unified_api_contracts.external.ccxt.schemas import CcxtOrder

    raw = {
        "id": "order-123",
        "symbol": "BTC/USDT",
        "side": "buy",
        "type": "limit",
        "amount": 0.01,
        "status": "open",
    }
    order = CcxtOrder.model_validate(raw)
    assert order.id == "order-123"


def test_thegraph_response_validates_with_unified_api_contracts() -> None:
    """Validate a GraphQL response wrapper with api-contracts schema."""
    from unified_api_contracts.external.thegraph.schemas import (
        TheGraphResponse,
    )

    raw = {"data": {"pools": []}, "errors": None}
    resp = TheGraphResponse.model_validate(raw)
    assert resp.data is not None
    assert resp.errors is None
