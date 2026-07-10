#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: campaign
# Delete-when: superseded by a CLI subcommand once the cumulative-drawdown guard is wired into the catalogue build / QG (recurring need)
"""CeFi capture-stability / cumulative-drawdown health guard — plan §1.2 (G1.2).

Per cefi venue, build the daily active ``instrument_count`` series from the IS
cefi ``_index`` captured cells and flag:

  1. **DAY-OVER-DAY active drops** — any negative delta in the per-day active count
     (the per-day active count MAY drop, but only for a typed delisting/expiry; an
     unreconciled drop is a capture-stability defect — §1.2).
  2. **THIN-DAY collapses** — a day whose count is < ``--thin-frac`` (default 0.5)
     of the venue's trailing median: the BINANCE-FUTURES 678→47 (≈7%) class — a
     partial capture that, written as ``captured``, mass-false-delists the catalogue
     (the G1.1 root trigger). These are the cells G1.2 must route to
     ``attempted_failed`` at capture time, not silently overwrite a full prior day.

Cumulative-ever-seen (running max) is reported too — it MUST be non-decreasing.
Read-only report. DoD: the 678→47 collapse is surfaced; 0 unreconciled drops.
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import get_storage_client

PID = "central-element-323112"
IS_PRD = f"instruments-store-cefi-prd-{PID}"
THIN_FRAC = 0.5
RECENT_WINDOW = 14


def main() -> int:
    only = next((a for a in sys.argv[1:] if not a.startswith("--")), None)
    thin_frac = THIN_FRAC
    for a in sys.argv[1:]:
        if a.startswith("--thin-frac="):
            thin_frac = float(a.split("=", 1)[1])
    st = get_storage_client(project_id=PID)
    df = pd.read_parquet(io.BytesIO(st.download_bytes(IS_PRD, "_index/availability_index.parquet")))
    m = (df["asset_group"].astype(str) == "cefi") & (df["capture_status"].astype(str) == "captured")
    d = df[m].copy()
    d["date"] = d["date"].astype(str)
    d["venue"] = d["venue"].astype(str)
    d["instrument_count"] = pd.to_numeric(d["instrument_count"], errors="coerce").fillna(0).astype(int)

    print(f"cefi captured cells: {len(d):,}; venues: {d['venue'].nunique()}")
    total_drops = 0
    total_thin = 0
    drop_rows = []
    thin_rows = []
    for venue, g in d.groupby("venue"):
        if only and only.upper() not in venue.upper():
            continue
        s = g.groupby("date")["instrument_count"].sum().sort_index()
        if len(s) < 2:
            continue
        delta = s.diff()
        ndrops = int((delta < 0).sum())
        total_drops += ndrops
        if ndrops:
            drop_rows.append((venue, ndrops, int(delta.min()), int(s.iloc[0]), int(s.iloc[-1])))
        # thin-day: count < thin_frac * trailing median (RECENT_WINDOW)
        med = s.rolling(RECENT_WINDOW, min_periods=2).median()
        thin = s[(s < thin_frac * med) & (med > 0)]
        if len(thin):
            total_thin += len(thin)
            for dt, cnt in thin.items():
                thin_rows.append((venue, str(dt), int(cnt), int(med.loc[dt])))

    drop_rows.sort(key=lambda x: -x[1])
    print(f"\nTOTAL venue day-over-day DROP-days: {total_drops}")
    print(f"{'venue':<22}{'#drop-days':>11}{'max-drop':>10}{'cnt0':>8}{'cntN':>8}")
    for v, n, mx, c0, cn in drop_rows:
        print(f"{v:<22}{n:>11}{mx:>10}{c0:>8}{cn:>8}")

    # total_thin was computed but never surfaced — the catalogue-wide collapse count is
    # invisible unless re-derived by hand (2026-07-07 audit finding). Print it up front,
    # unconditionally, before the (still truncated for readability) per-row detail.
    print(f"\nTOTAL catalogue-wide THIN-DAY collapses (< {thin_frac:.0%} of trailing median): {total_thin}")
    print(f"THIN-DAY collapses (< {thin_frac:.0%} of trailing median) — route these to attempted_failed:")
    print(f"{'venue':<22}{'date':>12}{'count':>8}{'median':>8}")
    for v, dt, cnt, med_v in sorted(thin_rows, key=lambda x: x[2] / max(x[3], 1)):
        print(f"{v:<22}{dt:>12}{cnt:>8}{med_v:>8}")
    if not thin_rows:
        print("  ✅ no thin-day collapses in scope")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
