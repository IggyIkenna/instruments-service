"""
Manual CCXT Fallback Mappings

Stable tick_size, min_size, contract_size when CCXT lookup fails.
Extracted from InstrumentProcessingService for file-size compliance (COD-SIZE).
"""


def get_manual_ccxt_fallback(venue: str, base_asset: str) -> dict[str, str | float | None]:
    """
    Manual fallback mappings for tick_size and min_size when CCXT lookup fails.

    Based on actual CCXT values observed. Format: {base_asset: {tick_size, min_size, contract_size}}
    """
    if venue == "HYPERLIQUID":
        HYPERLIQUID_MANUAL_MAPPINGS: dict[str, dict[str, str | float | None]] = {
            "BTC": {"tick_size": "1.0", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "ETH": {"tick_size": "0.1", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "SOL": {"tick_size": "0.01", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "AVAX": {"tick_size": "0.01", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "ATOM": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "DOGE": {"tick_size": "1e-05", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "NEAR": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "BNB": {"tick_size": "0.01", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "LTC": {"tick_size": "0.01", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "TRX": {"tick_size": "1e-05", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "ADA": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "ALGO": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "APT": {"tick_size": "0.01", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "CAKE": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "SUSHI": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "XLM": {"tick_size": "1e-05", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "MATIC": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "LINK": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
            "UNI": {"tick_size": "0.0001", "min_size": "cost_min:10.0", "contract_size": 1.0},
        }
        return HYPERLIQUID_MANUAL_MAPPINGS.get(base_asset, {})

    if venue == "ASTER":
        ASTER_MANUAL_MAPPINGS: dict[str, dict[str, str | float | None]] = {
            "BTC": {"tick_size": "0.1", "min_size": "0.001", "contract_size": None},
            "ETH": {"tick_size": "0.01", "min_size": "0.001", "contract_size": None},
            "SOL": {"tick_size": "0.01", "min_size": "0.01", "contract_size": None},
        }
        return ASTER_MANUAL_MAPPINGS.get(base_asset, {})

    return {}
