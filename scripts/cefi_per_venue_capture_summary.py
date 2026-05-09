"""MVP CeFi coverage probe — what's missing for the scoped universe.

MVP scope (per unified-trading-pm/codex/11-project-management/mvp-universe.yaml):
  - 8 venues: BINANCE-SPOT, BINANCE-FUTURES, OKX-SPOT, OKX-SWAP, UPBIT, COINBASE-SPOT,
              HYPERLIQUID, ASTER
  - 2 instruments: BTC, ETH (per-venue ticker conventions)
  - data types per scope_key in SCOPE_DATA_TYPES (HL/ASTER drop liquidations,
    Coinbase uses tbbo not book_snapshot_5, etc.)

The expected-day denominator is sourced from UAC
``get_venue_data_type_start_date(venue, data_type)`` so pre-source-archive days
do not inflate the "missing" count. A 100% coverage cell means "every day the
source can supply, we have," which is the honest baseline post-2026-05-04.

For each (venue, btc_or_eth, data_type), report:
  - expected_days = (today - per_data_type_start).days + 1
  - captured = manifest rows where capture_status='captured'
  - empty = manifest rows where capture_status='empty_confirmed'
  - failed = manifest rows where capture_status='attempted_failed'
  - covered_pct = (captured + empty) / expected_days * 100
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

# UAC SSOT for per-(venue, data_type) start dates. Falls back to the
# venue-level entry passed via MVP_SPEC if UAC has no per-dt entry.
from unified_api_contracts.registry import get_venue_data_type_start_date

MANIFEST = "gs://market-data-tick-cefi-central-element-323112/_index/availability_index.parquet"

# Per-venue MVP scope. Each entry: (manifest_venue_label, [(coin, symbol), ...],
# start_date, scope_key). The scope_key indexes into SCOPE_DATA_TYPES below to
# pick the data types we expect for that venue. spot_simple = trades + book_5;
# perp_full = perp_simple + liquidations + derivative_ticker; perp_no_liq =
# perp without liquidations (e.g. HYPERLIQUID — no liquidations published).
MVP_SPEC: list[tuple[str, list[tuple[str, str]], str, str]] = [
    # ── Original MVP ─────────────────────────────────────────────────────
    ("BINANCE-SPOT", [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")], "2019-03-30", "spot_simple"),
    ("BINANCE-FUTURES", [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")], "2019-11-17", "perp_full"),
    ("OKX-SPOT", [("BTC", "BTC-USDT"), ("ETH", "ETH-USDT")], "2019-03-30", "spot_simple"),
    ("OKX-SWAP", [("BTC", "BTC-USDT-SWAP"), ("ETH", "ETH-USDT-SWAP")], "2019-03-30", "perp_full"),
    ("UPBIT", [("BTC", "KRW-BTC"), ("ETH", "KRW-ETH")], "2021-09-01", "spot_simple"),
    ("COINBASE-SPOT", [("BTC", "BTC-USD"), ("ETH", "ETH-USD")], "2019-03-30", "spot_tbbo"),
    ("HYPERLIQUID", [("BTC", "BTC"), ("ETH", "ETH")], "2023-05-01", "perp_no_liq"),
    ("ASTER", [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")], "2021-08-30", "perp_aster"),
    # ── Tier-3 (added 2026-05-04 for full CeFi visibility) ───────────────
    # Bitfinex spot uses the legacy BTCUSD/ETHUSD ticker shape (no slash).
    ("BITFINEX-SPOT", [("BTC", "BTCUSD"), ("ETH", "ETHUSD")], "2020-01-01", "spot_simple"),
    # Bitfinex perps use the colon-separated F0 marker (BTCF0:USTF0).
    ("BITFINEX-FUTURES", [("BTC", "BTCF0:USTF0"), ("ETH", "ETHF0:USTF0")], "2020-01-01", "perp_full"),
    # Bitget didn't appear on Tardis until 2024-11-08 — structural ceiling.
    ("BITGET-SPOT", [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")], "2024-11-08", "spot_simple"),
    ("BITGET-FUTURES", [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")], "2024-11-08", "perp_full"),
    # Kraken spot uses XBT for Bitcoin (legacy convention) and slash-quoted pairs.
    ("KRAKEN-SPOT", [("BTC", "XBT/USD"), ("ETH", "ETH/USD")], "2020-01-01", "spot_simple"),
    # Kraken Futures uses cryptofacilities's PI_ prefix.
    ("KRAKEN-FUTURES", [("BTC", "PI_XBTUSD"), ("ETH", "PI_ETHUSD")], "2020-01-01", "perp_full"),
]

# Data type sets per scope key.
SCOPE_DATA_TYPES = {
    "spot_simple": ["trades", "book_snapshot_5"],
    # COINBASE-SPOT: top-of-book only — Tardis Coinbase L2 archive isn't usable
    # at depth-5 (verified 2026-05-04). Spot-premium features only need TBBO.
    "spot_tbbo": ["trades", "tbbo"],
    "perp_full": ["trades", "book_snapshot_5", "liquidations", "derivative_ticker"],
    "perp_no_liq": ["trades", "book_snapshot_5", "derivative_ticker"],
    # ASTER: liquidations + book_snapshot_5 both out of scope (no wired fetch).
    # We get derivative_ticker via fundingRate REST and trades via aggTrades
    # (limited depth ~30 days, Binance-compatible API).
    "perp_aster": ["trades", "derivative_ticker"],
}

TODAY = date.today()


def main() -> None:
    print(f"Loading {MANIFEST} ...")
    df = pd.read_parquet(MANIFEST)
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    print(f"Total manifest rows: {len(df):,}")

    sym_col = "symbol" if "symbol" in df.columns else "instrument_id"

    rows = []
    for venue, sym_pairs, venue_start_str, scope_key in MVP_SPEC:
        venue_start = datetime.strptime(venue_start_str, "%Y-%m-%d").date()
        data_types = SCOPE_DATA_TYPES[scope_key]

        for coin, sym in sym_pairs:
            for dt in data_types:
                # Per-(venue, data_type) start from UAC. Falls back to the
                # MVP_SPEC venue-level start when UAC has no per-dt entry,
                # which keeps unsupported (yet expected) data types honest.
                uac_start_str = get_venue_data_type_start_date(venue, dt)
                if uac_start_str:
                    start = max(
                        venue_start,
                        datetime.strptime(uac_start_str, "%Y-%m-%d").date(),
                    )
                else:
                    start = venue_start
                expected_days = (TODAY - start).days + 1
                m = (
                    (df["venue"] == venue)
                    & (df["data_type"] == dt)
                    & (df[sym_col].astype(str) == sym)
                    & (df["date_dt"] >= start)
                    & (df["date_dt"] <= TODAY)
                )
                sub = df[m]
                cap = (sub["capture_status"] == "captured").sum()
                emp = (sub["capture_status"] == "empty_confirmed").sum()
                fail = (sub["capture_status"] == "attempted_failed").sum()
                missing = max(0, expected_days - cap - emp - fail)
                covered = cap + emp
                pct = (covered / expected_days * 100) if expected_days else 0
                rows.append(
                    {
                        "venue": venue,
                        "coin": coin,
                        "sym": sym,
                        "data_type": dt,
                        "start": str(start),
                        "expected": expected_days,
                        "captured": cap,
                        "empty": emp,
                        "failed": fail,
                        "missing": missing,
                        "covered_pct": round(pct, 1),
                    }
                )

    rep = pd.DataFrame(rows)
    print(f"\n=== MVP CeFi coverage — {len(rep)} (venue, coin, data_type) cells ===")
    print(rep.to_string(index=False))

    print("\n=== summary by venue ===")
    summary = rep.groupby("venue").agg(
        cells=("expected", "size"),
        total_expected=("expected", "sum"),
        total_captured=("captured", "sum"),
        total_empty=("empty", "sum"),
        total_failed=("failed", "sum"),
        total_missing=("missing", "sum"),
    )
    summary["covered_pct"] = (
        (summary["total_captured"] + summary["total_empty"]) / summary["total_expected"] * 100
    ).round(1)
    print(summary.to_string())

    print("\n=== HIGHEST-PRIORITY GAPS (covered_pct < 80%) ===")
    gaps = rep[rep["covered_pct"] < 80].sort_values("covered_pct")
    if len(gaps):
        print(gaps.to_string(index=False))
    else:
        print("None — all (venue, coin, data_type) cells > 80% covered.")


if __name__ == "__main__":
    main()
