"""Guards the --tardis-only scope of instruments-service scripts/pipeline_e2e_check.py.

--tardis-only keeps only venues sourced via the Tardis ADAPTER
(VENUE_TO_ADAPTER_KEY == 'tardis'), NOT venues Tardis merely catalogs. The native-REST venues
(HYPERLIQUID / ASTER / LIGHTER-ZKSYNC / PACIFICA-SOLANA / EXTENDED-STARKNET) are catalogued by
Tardis but fetched via their own adapters, so they must be excluded.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(_SCRIPTS))

from smoke_matrix import enumerate_cells
from unified_api_contracts import VENUE_TO_ADAPTER_KEY

_NATIVE_REST = {"HYPERLIQUID", "ASTER", "LIGHTER-ZKSYNC", "PACIFICA-SOLANA", "EXTENDED-STARKNET"}
_TARDIS = {"BINANCE-FUTURES", "BYBIT", "OKX-SPOT", "DERIBIT", "KRAKEN-SPOT", "UPBIT"}


def _tardis_only(cells):
    return [c for c in cells if VENUE_TO_ADAPTER_KEY.get(c.venue) == "tardis"]


def test_tardis_only_filter_excludes_native_rest() -> None:
    cells = enumerate_cells(asset_group_filter="CEFI")
    venues = {c.venue for c in _tardis_only(cells)}
    assert venues, "tardis-only CEFI enumeration must not be empty"
    assert not (venues & _NATIVE_REST), f"native-REST leaked: {venues & _NATIVE_REST}"
    assert venues >= _TARDIS, f"expected Tardis venues missing: {_TARDIS - venues}"


def test_parser_exposes_tardis_only_flag() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location("is_pe2e", _SCRIPTS / "pipeline_e2e_check.py")
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    sys.modules["is_pe2e"] = m
    spec.loader.exec_module(m)
    ns = m._build_parser().parse_args(["--day", "2026-02-02", "--tardis-only"])
    assert ns.tardis_only is True


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
