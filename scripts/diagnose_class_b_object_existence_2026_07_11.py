#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after prediction _index final residual cleanup 2026-07-11 lands + is verified
"""Read-only: for a sample of Class-B phantom rows, list GCS under the canonical
prefix templates AND do a broader same-date/venue recursive listing, to distinguish
genuine object-absence from a phantom-prober path-shape gap.

NO writes.
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_api_contracts import canonical_path_templates
from unified_trading_library import get_storage_client, resolve_bucket_name

INDEX_BLOB = "_index/availability_index.parquet"
SAMPLE_PER_GROUP = 4


def main() -> int:
    bucket = resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
    storage = get_storage_client()
    raw = storage.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))

    err = df["error_reason"].fillna("").astype(str)
    phantom_mask = err.str.startswith("phantom_captured_no_parquet_at_canonical")
    df_b = df[phantom_mask].copy()
    df_b["venue"] = df_b["venue"].fillna("").astype(str)
    df_b["data_type"] = df_b["data_type"].fillna("").astype(str)

    groups = df_b.groupby(["venue", "data_type"]).size().sort_values(ascending=False)
    print("Phantom (venue, data_type) groups:\n", groups.to_string())

    tpls = canonical_path_templates("prediction")
    print(f"\ncanonical_path_templates('prediction') = {tpls}\n")

    client_native = storage  # StorageClient wrapper — has list_blobs per reconciler usage

    for (venue, data_type), _cnt in groups.items():
        sub = df_b[(df_b["venue"] == venue) & (df_b["data_type"] == data_type)]
        sample = sub.sample(n=min(SAMPLE_PER_GROUP, len(sub)), random_state=42)
        print(f"\n=== group venue={venue!r} data_type={data_type!r} (n={_cnt}) — sampling {len(sample)} ===")
        for _, row in sample.iterrows():
            date = str(row["date"])
            print(f"  -- date={date} venue={venue} data_type={data_type} instrument_id={row.get('instrument_id')!r}")
            any_found = False
            for t in tpls:
                stripped = t.split("instrument_type=")[0]
                try:
                    prefix = stripped.format(date=date, venue=venue, chain="")
                except (KeyError, IndexError):
                    continue
                blobs = [m.name for m in client_native.list_blobs(bucket, prefix=prefix) if m.name.endswith(".parquet")]
                if blobs:
                    any_found = True
                    dt_needle = f"data_type={data_type}/" if data_type else None
                    matching = [b for b in blobs if not dt_needle or dt_needle in b]
                    print(f"     prefix={prefix!r} -> {len(blobs)} parquet(s), {len(matching)} match data_type needle")
                    for b in matching[:3]:
                        print(f"       {b}")
                else:
                    print(f"     prefix={prefix!r} -> 0 objects")
            # Broader: any object anywhere for this date at all (day-level), regardless of venue-prefix guess
            broad_prefix = f"raw_tick_data/by_date/day={date}/"
            broad_blobs = [
                m.name for m in client_native.list_blobs(bucket, prefix=broad_prefix) if m.name.endswith(".parquet")
            ]
            print(f"     BROAD day-prefix {broad_prefix!r} -> {len(broad_blobs)} total parquet objects that day")
            venue_hits = [b for b in broad_blobs if f"/{venue}/" in b or f"={venue}/" in b or venue in b]
            print(f"       of those, {len(venue_hits)} mention venue={venue!r} anywhere in path")
            for b in venue_hits[:5]:
                print(f"         {b}")
            if not any_found and not venue_hits:
                print("     => CONFIRMED genuinely absent (no object anywhere for this date+venue)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
