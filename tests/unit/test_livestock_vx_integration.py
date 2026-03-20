"""Integration tests for Livestock and VX instrument definitions.

Verifies end-to-end consistency: the Python definitions, the JSON data file,
and the exchange mapping module all agree on VX, LE, and HE definitions.
"""

from __future__ import annotations

import json
from pathlib import Path

_DATA_DIR = Path(__file__).parent.parent.parent / "instruments_service" / "data"


class TestPythonJsonConsistency:
    """Python definitions and JSON data file must agree on VX/LE/HE."""

    def test_livestock_vx_codes_in_both_sources(self) -> None:
        """LE, HE, VX must exist in both Python TRADFI_VENUE_MAPPINGS and JSON."""
        from unified_api_contracts import (
            TRADFI_VENUE_MAPPINGS as TRADFI_PY_MAPPINGS,
        )

        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            json_data = json.load(f)

        py_codes = {e.get("code") for e in TRADFI_PY_MAPPINGS if e.get("type") == "FUTURE"}
        json_codes = {e.get("code") for e in json_data["instruments"] if e.get("type") == "FUTURE"}

        for code in ("LE", "HE", "VX"):
            assert code in py_codes, f"{code} missing from Python TRADFI_VENUE_MAPPINGS"
            assert code in json_codes, f"{code} missing from JSON instruments"

    def test_vx_dataset_consistent(self) -> None:
        """VX dataset must be XCBF.MDP3 in both Python and JSON."""
        from unified_api_contracts import (
            TRADFI_VENUE_MAPPINGS as TRADFI_PY_MAPPINGS,
        )

        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            json_data = json.load(f)

        py_vx = next(e for e in TRADFI_PY_MAPPINGS if e.get("code") == "VX")
        json_vx = next(e for e in json_data["instruments"] if e.get("code") == "VX")

        assert py_vx["dataset"] == "XCBF.MDP3"
        assert json_vx["dataset"] == "XCBF.MDP3"

    def test_livestock_venues_consistent(self) -> None:
        """LE and HE must be on CME in both Python and JSON."""
        from unified_api_contracts import (
            TRADFI_VENUE_MAPPINGS as TRADFI_PY_MAPPINGS,
        )

        with open(_DATA_DIR / "tradfi_instruments.json") as f:
            json_data = json.load(f)

        for code in ("LE", "HE"):
            py_entry = next(e for e in TRADFI_PY_MAPPINGS if e.get("code") == code)
            json_entry = next(e for e in json_data["instruments"] if e.get("code") == code)
            assert py_entry["venue"] == "CME", f"{code} venue mismatch in Python"
            assert json_entry["venue"] == "CME", f"{code} venue mismatch in JSON"


class TestExchangeMappingIntegration:
    """Exchange mappings provide bidirectional lookup for VX/LE/HE."""

    def test_databento_parent_roundtrip_le(self) -> None:
        """LE code -> Databento symbol -> back to LIVECATTLE."""
        from unified_api_contracts import (
            DATABENTO_VALID_PARENT_SYMBOLS,
            EXCHANGE_CODE_TO_NAME,
        )

        symbol, dataset = DATABENTO_VALID_PARENT_SYMBOLS["LE"]
        assert symbol == "LE.FUT"
        assert dataset == "GLBX.MDP3"
        assert EXCHANGE_CODE_TO_NAME["LE"] == "LIVECATTLE"

    def test_databento_parent_roundtrip_he(self) -> None:
        """HE code -> Databento symbol -> back to LEANHOGS."""
        from unified_api_contracts import (
            DATABENTO_VALID_PARENT_SYMBOLS,
            EXCHANGE_CODE_TO_NAME,
        )

        symbol, dataset = DATABENTO_VALID_PARENT_SYMBOLS["HE"]
        assert symbol == "HE.FUT"
        assert dataset == "GLBX.MDP3"
        assert EXCHANGE_CODE_TO_NAME["HE"] == "LEANHOGS"

    def test_databento_parent_roundtrip_vx(self) -> None:
        """VX code -> Databento symbol -> back to VIX."""
        from unified_api_contracts import (
            DATABENTO_VALID_PARENT_SYMBOLS,
            EXCHANGE_CODE_TO_NAME,
        )

        symbol, dataset = DATABENTO_VALID_PARENT_SYMBOLS["VX"]
        assert symbol == "VX.FUT"
        assert dataset == "XCBF.MDP3"
        assert EXCHANGE_CODE_TO_NAME["VX"] == "VIX"
