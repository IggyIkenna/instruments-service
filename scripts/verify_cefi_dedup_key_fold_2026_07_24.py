#!/usr/bin/env python
# Epic: cefi_master
# Lifecycle: oneoff (re-runnable regression check)
# Delete-when: complete_cefi_manifest_canonical_dedup_2026_07_17.py's `_effective_dedup_key` /
#              `_dedup_blob` row_count-desc tie-break is covered by a proper pytest suite instead —
#              until then this is the only regression check for that logic.
"""Fast, local, no-GCS sanity check of the 2026-07-24 manifest-dedup key-fold fix
(`_effective_dedup_key` / `_dedup_blob` row_count-desc tie-break in
`complete_cefi_manifest_canonical_dedup_2026_07_17.py`, reused by v2's `_chain_merge_safety`).

WHY THIS EXISTS: a full-corpus dry-run/`--apply` run of the dedup script takes ~13-20 minutes
against the live 10.6M-row cefi manifest and, on this shared multi-agent host, is exposed to
resource-contention kills (see `investigate_cefi_dedup_residual_lossy_2026_07_24.py`'s docstring for
the earlyoom/timeout traps hit chasing that) — too slow and too flaky to be the ONLY way to verify a
logic change to this script. This test instead exercises the exact three real-data shapes found
2026-07-24 (`plans/active/issues/cefi_chain_drop_root_cause_and_heavy_io_vm_rule_2026_07_24.md`
Finding 5) against a tiny synthetic DataFrame built from real extracted rows, in well under a
second:

  1. DERIBIT-style chain-BUNDLE (synthetic, mirrors the FUTURES_CHAIN population): 2 different
     underlyings sharing one PIN_ATOM (blank instrument_id) — the `underlying` fold must split them
     into distinct keys so BOTH survive `_dedup_blob`.
  2. ASTER (real rows, `ASTER:PERPETUAL:BCH-USDT@LIN` 2024-01-01): identical PIN_ATOM but
     `chain=None` vs. `chain="ASTER"`, differing real `row_count` — the `chain` fold must split
     these too so BOTH survive.
  3. BYBIT-SPOT (real rows, `book_snapshot_5` 2024-01-01): blank instrument_id + blank underlying +
     blank chain on BOTH rows — no available column can split them, so this MUST land in the small
     explicit `_CHAIN_LOSSY_TOLERANCE_MAX` tolerance band (v2 script), and the row_count-desc
     tie-break in `_dedup_blob` must keep the LARGER (92,448,219-row) capture over the smaller
     (76,978,052-row) one rather than an arbitrary original-order pick.

Usage::

    cd instruments-service
    .venv/bin/python scripts/verify_cefi_dedup_key_fold_2026_07_24.py
"""

from __future__ import annotations

import importlib.util
import os

import pandas as pd

_BASE = os.path.dirname(os.path.abspath(__file__))


def _load_module(name: str, path: str) -> object:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - import guard
        raise RuntimeError(f"cannot load module at {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


v1 = _load_module("v1_dedup", os.path.join(_BASE, "complete_cefi_manifest_canonical_dedup_2026_07_17.py"))
v2 = _load_module("v2_dedup", os.path.join(_BASE, "complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py"))


def _build_rows() -> list[dict]:
    return [
        # group 1: synthetic DERIBIT-style bundle, 2 underlyings, blank id, same PIN_ATOM.
        {
            "date": "2026-01-01",
            "venue": "DERIBIT",
            "data_type": "trades",
            "instrument_type": "FUTURE",
            "instrument_id": "",
            "underlying": "BTC",
            "pipeline_mode": "batch_tardis",
            "chain": None,
            "capture_status": "captured",
            "row_count": 4989,
            "instrument_count": 4989,
        },
        {
            "date": "2026-01-01",
            "venue": "DERIBIT",
            "data_type": "trades",
            "instrument_type": "FUTURE",
            "instrument_id": "",
            "underlying": "ETH",
            "pipeline_mode": "batch_tardis",
            "chain": None,
            "capture_status": "captured",
            "row_count": 6034,
            "instrument_count": 6034,
        },
        # group 2: REAL ASTER rows.
        {
            "date": "2024-01-01",
            "venue": "ASTER",
            "data_type": "trades",
            "instrument_type": "PERPETUAL",
            "instrument_id": "ASTER:PERPETUAL:BCH-USDT@LIN",
            "underlying": "BCHUSDT",
            "pipeline_mode": "batch_aster",
            "chain": None,
            "capture_status": "captured",
            "row_count": 3909,
            "instrument_count": 3909,
        },
        {
            "date": "2024-01-01",
            "venue": "ASTER",
            "data_type": "trades",
            "instrument_type": "PERPETUAL",
            "instrument_id": "ASTER:PERPETUAL:BCH-USDT@LIN",
            "underlying": "BCHUSDT",
            "pipeline_mode": "batch_aster",
            "chain": "ASTER",
            "capture_status": "captured",
            "row_count": 1000,
            "instrument_count": 1000,
        },
        # group 3: REAL BYBIT-SPOT rows.
        {
            "date": "2024-01-01",
            "venue": "BYBIT-SPOT",
            "data_type": "book_snapshot_5",
            "instrument_type": "SPOT_PAIR",
            "instrument_id": "",
            "underlying": "",
            "pipeline_mode": "batch_tardis",
            "chain": None,
            "capture_status": "captured",
            "row_count": 92448219,
            "instrument_count": 92448219,
        },
        {
            "date": "2024-01-01",
            "venue": "BYBIT-SPOT",
            "data_type": "book_snapshot_5",
            "instrument_type": "SPOT_PAIR",
            "instrument_id": "",
            "underlying": "",
            "pipeline_mode": "batch_tardis",
            "chain": None,
            "capture_status": "captured",
            "row_count": 76978052,
            "instrument_count": 76978052,
        },
    ]


def main() -> int:
    df = pd.DataFrame(_build_rows())
    key = v1._effective_dedup_key(df)

    assert key[0] != key[1], "FAIL: DERIBIT BTC/ETH bundle rows still collide (underlying-fold broken)"
    assert key[2] != key[3], "FAIL: ASTER chain=None vs chain=ASTER rows still collide (chain-fold broken)"
    assert key[4] == key[5], "UNEXPECTED: BYBIT-SPOT rows now have distinct keys (tolerance-band logic changed)"
    print(
        "PASS: effective_dedup_key splits DERIBIT bundle + ASTER chain-tag pairs, collapses the irreducible BYBIT-SPOT pair"
    )

    kept, collapsed, breakdown = v1._dedup_blob(df)
    assert len(kept) == 5, f"expected 5 surviving rows (6 in, 1 collapsed), got {len(kept)}"
    print(f"PASS: _dedup_blob kept 5/6 rows (collapsed={collapsed}, breakdown={breakdown})")

    bybit_kept = kept[kept["venue"] == "BYBIT-SPOT"]
    assert len(bybit_kept) == 1, f"expected exactly 1 surviving BYBIT-SPOT row, got {len(bybit_kept)}"
    assert int(bybit_kept.iloc[0]["row_count"]) == 92448219, (
        "FAIL: row_count-desc tie-break did not keep the larger capture"
    )
    print("PASS: row_count-desc tie-break kept the LARGER BYBIT-SPOT capture (92,448,219 over 76,978,052)")

    assert len(kept[kept["venue"] == "DERIBIT"]) == 2, "FAIL: expected both DERIBIT underlyings to survive"
    assert len(kept[kept["venue"] == "ASTER"]) == 2, "FAIL: expected both ASTER chain-variant rows to survive"
    print("PASS: both DERIBIT underlyings and both ASTER chain-variant rows survive intact")

    n_multichain, n_lossy = v2._chain_merge_safety(df)
    assert n_lossy == 1, f"expected exactly 1 residual lossy group (BYBIT-SPOT), got {n_lossy}"
    assert n_lossy <= v2._CHAIN_LOSSY_TOLERANCE_MAX, "FAIL: residual exceeds the tolerated band"
    print(
        f"PASS: n_multichain={n_multichain} n_lossy={n_lossy} <= _CHAIN_LOSSY_TOLERANCE_MAX={v2._CHAIN_LOSSY_TOLERANCE_MAX}"
    )

    detail = v2._chain_merge_safety_detail(df)
    assert len(detail) == 2 and set(detail["venue"]) == {"BYBIT-SPOT"}, (
        "FAIL: detail rows are not the expected BYBIT-SPOT pair"
    )
    print("PASS: _chain_merge_safety_detail correctly identifies the tolerated BYBIT-SPOT pair")

    print("\nALL CHECKS PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
