"""One-shot migration: rewrite legacy DeFi manifest rows into v5 canonical form.

Problem:
    The orchestrator's batched `_write_venue` path was passing the hyphenated
    DeFi venue tag (``venue=AAVEV3-ETHEREUM``) to ``ManifestWriter.add`` and
    leaving ``chain=''`` — instead of splitting into ``venue=AAVEV3``,
    ``chain=ETHEREUM``. Per the workspace shard-key matrix the DeFi axis is
    ``chain``, so coverage-summary's legacy-row filter (correctly) drops these
    rows from "latest day" calculations, hiding ~24 days of recent DeFi
    captures from the data-status panel.

    The orchestrator fix at ``instruments_service/engine/orchestrator.py``
    line 2675 stops emitting new legacy rows. This script migrates the
    historical rows already on disk.

Manifest path:
    ``gs://instruments-store-defi-{project}/_index/availability_index.parquet``

What this does:
    1. Read the canonical manifest.
    2. For every row where venue contains a ``-`` and chain is empty (or NaN),
       call ``parse_defi_venue(venue)`` from UAC.
    3. If the parsed chain is in ``KNOWN_CHAINS``, rewrite the row:
       venue → uppercase protocol slug, chain → parsed chain.
    4. Leave non-matching rows untouched (e.g. legitimate hyphenated venue
       names that aren't DeFi protocol-chain pairs).
    5. Write back to the canonical manifest path.

Usage:
    python -m instruments_service.scripts.migrate_defi_legacy_venue_chain \\
        --project central-element-323112 \\
        --dry-run

    # Then drop --dry-run when the report looks right.

Idempotent: re-running on already-migrated rows is a no-op (the legacy filter
is "venue contains '-' AND chain empty"; canonical rows have chain populated).
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from typing import cast

import pandas as pd
import pyarrow.parquet as pq
from unified_api_contracts.registry.capability_declarations._defi import (
    KNOWN_CHAINS,
    parse_defi_venue,
)
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_MANIFEST_PATH = "_index/availability_index.parquet"


def _bucket_name(project_id: str) -> str:
    return f"instruments-store-defi-{project_id}"


def _read_manifest(project_id: str) -> pd.DataFrame:
    client = get_storage_client(project_id=project_id)
    blob = client.download_bytes(  # pyright: ignore[reportAttributeAccessIssue]
        bucket=_bucket_name(project_id),
        blob_path=_MANIFEST_PATH,
    )
    table = pq.read_table(io.BytesIO(blob))
    df = cast(pd.DataFrame, table.to_pandas())  # pyright: ignore[reportUnknownMemberType]
    logger.info("read manifest: %d rows", len(df))
    return df


def _is_legacy_row(venue: object, chain: object) -> bool:
    """Row needs migration iff venue is hyphenated AND chain is empty/NaN."""
    if not isinstance(venue, str):
        return False
    if "-" not in venue:
        return False
    if isinstance(chain, str) and chain.strip():
        return False
    if chain is not None and not isinstance(chain, str):
        try:
            return bool(pd.isna(chain))  # pyright: ignore[reportUnknownMemberType]
        except (TypeError, ValueError):
            return False
    return True


def _split(venue: str) -> tuple[str, str] | None:
    try:
        protocol, chain = parse_defi_venue(venue)
    except ValueError:
        return None
    if chain not in KNOWN_CHAINS:
        return None
    return protocol.upper(), chain


def _rewrite(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Return (rewritten_df, n_rewritten, n_unmatched_legacy)."""
    if "venue" not in df.columns or "chain" not in df.columns:
        return df, 0, 0
    out = df.copy()
    n_rewritten = 0
    n_unmatched = 0
    for idx, row in out.iterrows():
        if not _is_legacy_row(row["venue"], row["chain"]):
            continue
        split = _split(str(row["venue"]))
        if split is None:
            n_unmatched += 1
            continue
        protocol, chain = split
        out.at[idx, "venue"] = protocol
        out.at[idx, "chain"] = chain
        n_rewritten += 1
    return out, n_rewritten, n_unmatched


def _write_manifest(project_id: str, df: pd.DataFrame) -> None:
    import pyarrow as pa

    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    client = get_storage_client(project_id=project_id)
    client.upload_bytes(  # pyright: ignore[reportAttributeAccessIssue]
        bucket=_bucket_name(project_id),
        blob_path=_MANIFEST_PATH,
        data=buf.getvalue(),
        content_type="application/octet-stream",
    )
    logger.info("wrote manifest: %d rows", len(df))


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate legacy DeFi manifest rows to canonical (venue, chain) shape.")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the rewrite count; do not write to GCS.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    df = _read_manifest(args.project)
    rewritten, n_rewritten, n_unmatched = _rewrite(df)

    print(f"rows_total:           {len(df)}")
    print(f"rows_rewritten:       {n_rewritten}")
    print(f"rows_unmatched_legacy: {n_unmatched}  (hyphenated venue, chain not in KNOWN_CHAINS)")
    print(f"rows_unchanged:       {len(df) - n_rewritten}")

    if n_rewritten == 0:
        print("nothing to migrate — exiting clean.")
        return 0

    if args.dry_run:
        print("DRY RUN: not writing.")
        sample_legacy = df[df.apply(lambda r: _is_legacy_row(r["venue"], r["chain"]), axis=1)].head(5)
        if not sample_legacy.empty:
            print("\nSample rows that would be rewritten:")
            for _, r in sample_legacy.iterrows():
                split = _split(str(r["venue"]))
                if split:
                    proto, chain = split
                    print(
                        f"  {r['venue']!r}, chain={r['chain']!r}, date={r['date']!r}"
                        f"  →  venue={proto!r}, chain={chain!r}"
                    )
        return 0

    _write_manifest(args.project, rewritten)
    print(f"DONE: wrote {len(rewritten)} rows to gs://{_bucket_name(args.project)}/{_MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
