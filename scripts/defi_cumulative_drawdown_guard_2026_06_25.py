#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: campaign
# Delete-when: superseded by a CLI subcommand once the cumulative-drawdown guard is wired into the catalogue build / QG (recurring need)
"""DeFi cumulative-drawdown (monotonic) health guard — plan §1.2.

Per defi venue (and venue+chain), build the daily active instrument_count series
from the IS _index captured instrument-catalog cells, and flag any NEGATIVE
day-over-day delta (a drop in active instruments). A cumulative-ever-seen series
(running max) is also reported — it MUST be non-decreasing; the active series may
drop only for a typed reason (delisting). Read-only report.

DoD: 0 unreconciled day-over-day drops per venue (drops net to delistings).
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import get_storage_client

PID = "central-element-323112"
IS_PRD = f"instruments-store-defi-prd-{PID}"


def main() -> int:
    only = sys.argv[1] if len(sys.argv) > 1 else None
    st = get_storage_client(project_id=PID)
    df = pd.read_parquet(io.BytesIO(st.download_bytes(IS_PRD, "_index/availability_index.parquet")))
    # defi instrument-catalog captured cells carry per-(date,venue,chain) instrument_count.
    # "instruments" is canonical (operator decision 2026-07-16, P9 Q2); "instrument-catalog" kept
    # for legacy pre-migration rows.
    m = (df["asset_group"].astype(str) == "defi") & (df["capture_status"].astype(str) == "captured")
    if "data_type" in df:
        m &= df["data_type"].astype(str).isin(["instruments", "instrument-catalog", ""])
    d = df[m].copy()
    d["date"] = d["date"].astype(str)
    d["venue"] = d["venue"].astype(str)
    d["chain"] = d["chain"].astype(str) if "chain" in d else ""
    d["instrument_count"] = pd.to_numeric(d["instrument_count"], errors="coerce").fillna(0).astype(int)

    print(f"defi captured catalog cells: {len(d):,}; venues: {d['venue'].nunique()}")
    total_drops = 0
    worst = []
    for venue, g in d.groupby("venue"):
        if only and only.upper() not in venue.upper():
            continue
        # aggregate per date across chains (venue-level active count)
        s = g.groupby("date")["instrument_count"].sum().sort_index()
        delta = s.diff()
        drops = delta[delta < 0]
        cum = s.cummax()
        cum.diff()[cum.diff() < 0]  # must be empty by construction of cummax
        ndrops = int((delta < 0).sum())
        total_drops += ndrops
        if ndrops:
            worst.append(
                (venue, ndrops, int(drops.min()), s.index.min(), s.index.max(), int(s.iloc[0]), int(s.iloc[-1]))
            )
    worst.sort(key=lambda x: -x[1])
    print(f"\nTOTAL venue day-over-day DROP-days: {total_drops}")
    print(f"{'venue':<26}{'#drop-days':>11}{'max-drop':>10}{'first':>12}{'last':>12}{'cnt0':>7}{'cntN':>7}")
    for v, n, mx, fst, lst, c0, cn in worst[:40]:
        print(f"{v:<26}{n:>11}{mx:>10}{fst:>12}{lst:>12}{c0:>7}{cn:>7}")
    if not worst:
        print("  ✅ no day-over-day drops in scope")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
