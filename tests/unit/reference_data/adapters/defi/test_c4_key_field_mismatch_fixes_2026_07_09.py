"""Regression guards — C4 instrument_key-vs-instrument_type mismatch fixes (2026-07-09).

Consolidated cross-adapter coverage for
`instruments_docs_audit_outstanding_items_2026_07_08.md` finding C4: several DeFi
adapters' `instrument_key` TYPE segment (the middle `:X:` token) disagreed with the
`instrument_type` field stamped on the same `InstrumentRecord`. Each mismatch was
fixed by aligning whichever side used a non-canonical shorthand (`PERP`, `SPOT`,
`GOVERNANCE_TOKEN`) to the side that already carried the real `InstrumentType` enum
value (`PERPETUAL`, `SPOT_PAIR`) — the same convention documented in `lido.py`'s and
`karak.py`'s module docstrings for the earlier LST-vs-VAULT mismatch class.

Per-adapter identity assertions already live in each adapter's own dedicated test
file (`test_sanctum_metadata.py`, `test_solblaze_metadata.py`,
`test_jito_restaking_metadata.py`, `test_eigenlayer_metadata.py`,
`test_flash_trade_metadata.py`, `test_jupiter_metadata.py`) — this module adds the
2 fixes (Drift's PERP/SPOT dual mismatch) that don't have a dedicated per-adapter
metadata test file, kept standalone (not folded into the shared
`test_defi_adapters_comprehensive.py`) to avoid entangling this narrow C4 regression
guard with that file's much larger, independently-owned Morpho/Fluid/etc. lending
A_TOKEN/DEBT_TOKEN test coverage landing the same day.
"""

from __future__ import annotations

from unified_api_contracts.internal import InstrumentType

from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter


class TestDriftKeyFieldConsistency:
    """Drift perp/spot `instrument_key` TYPE segment must match `instrument_type`."""

    def test_perp_record_key_uses_real_perpetual_type(self) -> None:
        adapter = DriftReferenceDataAdapter()
        record = adapter._build_perp_record({"symbol": "SOL-PERP", "baseAsset": "SOL"})
        assert record is not None
        assert record.instrument_type == InstrumentType.PERPETUAL
        # Before 2026-07-09: "DRIFT-SOLANA:PERP:SOL-PERP" (non-canonical `PERP`
        # shorthand — not a real InstrumentType member).
        assert record.instrument_key == "DRIFT-SOLANA:PERPETUAL:SOL-PERP"
        assert ":PERPETUAL:" in record.instrument_key
        assert ":PERP:" not in record.instrument_key

    def test_spot_record_key_uses_real_spot_pair_type(self) -> None:
        adapter = DriftReferenceDataAdapter()
        record = adapter._build_spot_record({"symbol": "SOL", "baseAsset": "SOL"})
        assert record is not None
        assert record.instrument_type == InstrumentType.SPOT_PAIR
        # Before 2026-07-09: "DRIFT-SOLANA:SPOT:SOL" (non-canonical `SPOT`
        # shorthand — not a real InstrumentType member).
        assert record.instrument_key == "DRIFT-SOLANA:SPOT_PAIR:SOL"
        assert ":SPOT_PAIR:" in record.instrument_key
        assert ":SPOT:" not in record.instrument_key
