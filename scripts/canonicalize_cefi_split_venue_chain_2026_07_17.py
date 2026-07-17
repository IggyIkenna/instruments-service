#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   data_status_page_ux_and_canonicalisation_2026_07_16.md P7 follow-up)
"""Collapse the CeFi availability index's split ``venue={PROTOCOL}, chain={CHAIN}``
rows onto the canonical ``venue={PROTOCOL}-{CHAIN}, chain=""`` shard atom.

**What is wrong (measured on real prod GCS 2026-07-17, not inferred).** The CeFi
manifest shard atom is ``(date, venue)`` — cefi has NO chain axis (UAC
``SHARD_AXIS_MATRIX``: cefi = ``("venue",)``; only DeFi adds ``chain``). But the
live index carries 4,617 rows keyed ``venue=LIGHTER, chain=ZKSYNC`` (2,413) and
``venue=PACIFICA, chain=SOLANA`` (2,204), alongside the canonical combined-form
rows ``venue=LIGHTER-ZKSYNC`` (1,424) / ``venue=PACIFICA-SOLANA`` (1,426) for the
same real venues. Two consequences:

1. The venue axis renders each venue TWICE (``PACIFICA`` and ``PACIFICA-SOLANA``).
2. **The split rows are phantom pointers.** The by_date object for these venues is
   written under the GLUED venue
   (``…/day=<D>/pipeline_mode=<M>/asset_group=cefi/venue=LIGHTER-ZKSYNC/instruments.parquet``).
   Verified against real GCS: of the 1,078 ``captured`` split rows, the object
   exists at the canonical venue **1,078/1,078** (the run's own pre-write check
   below), and a 60-row sample found it at the split venue **0/60**. So every split
   row claims a capture at a path that does not exist, while the real object's own
   canonical row is in 493 cases absent entirely — this migration REPAIRS the
   manifest↔object correspondence, it does not merely retitle a label.

**Writer root cause — already fixed upstream, both venues, verified by reading
the code + registry (this migration is history-only, it cannot recur):**

* ``LIGHTER-ZKSYNC`` / ``EXTENDED-STARKNET``: ``writers._canonical_manifest_venue_chain``
  now short-circuits ``if VENUE_TO_ASSET_GROUP.get(venue_str) == "cefi": return
  venue_str, ""`` — confirmed live: the registry resolves ``LIGHTER-ZKSYNC`` ->
  ``'cefi'``, so the DeFi ``PROTOCOL-CHAIN`` split no longer fires for it.
* ``PACIFICA-SOLANA``: removed from the venue registry entirely (operator ruling
  2026-07-16 — all Solana perp DEXes dropped except Jupiter). Confirmed live:
  ``VENUE_TO_ASSET_GROUP.get("PACIFICA-SOLANA") is None``, so nothing enumerates
  or captures it and the writer path is dead for it.

**Collapse semantics.** For each split row: ``venue -> f"{venue}-{chain}"``,
``chain -> ""``. ``chain`` is itself a ``_ROW_KEY_COLUMNS`` member, so this
changes the row key — collisions with the already-canonical rows are therefore
expected and are de-duplicated on the manifest's REAL composite row identity
(``unified_trading_library.manifest_writer._ROW_KEY_COLUMNS``), preferring a
``captured`` row over a non-captured one and then the most recent ``attempted_at``
(same winner rule as ``canonicalize_defi_data_type_instrument_catalog_2026_07_16``,
whose dry-run caught that naive recency alone can drop a captured row in favour of
a newer empty one). Measured collision structure: 672 of the 4,617 collapse onto an
existing key and **every collision is status-identical** (captured->captured 585,
empty_confirmed->empty_confirmed 87 — zero status conflicts, so no information is
lost by dropping either side); the other 3,945 land on keys with no existing row,
of which 493 are ``captured`` rows that fill a genuine manifest gap.

**Honest-absence guard (fail loud, never fabricate).** Before writing, every
``captured`` row being collapsed is checked against real GCS for its object at the
CANONICAL path. If any is missing, the script ABORTS rather than promoting a
capture claim onto a path with no object (that would be exactly the phantom row
this migration exists to remove). ``--allow-missing-objects`` proceeds with the
rest and leaves those specific rows entirely UNTOUCHED (reported, re-appended
verbatim, still split) rather than rewriting them onto an empty path.

Scope: the CeFi availability index ONLY. ``prod/catalog.parquet`` is not touched
(it is keyed on instrument identity, not the manifest shard atom) — verified
separately. instrument_type/data_type are untouched.

Usage:
  .venv/bin/python scripts/canonicalize_cefi_split_venue_chain_2026_07_17.py --dry-run
  .venv/bin/python scripts/canonicalize_cefi_split_venue_chain_2026_07_17.py --apply --confirm
"""

from __future__ import annotations

import argparse
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name
from unified_trading_library.manifest_writer import _ROW_KEY_COLUMNS

logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
BACKUP_PREFIX = "_index/snapshots/pre_cefi_split_venue_chain_canon_2026_07_17_"

#: EXPLICIT allowlist of the (split_venue, chain) pairs to collapse, rather than a
#: generic "any cefi row with a chain" rule. Deliberate: these are the only two
#: pairs the live index actually carries (verified — the ONLY distinct cefi chains
#: are SOLANA + ZKSYNC), and an explicit list cannot silently rewrite an
#: unexpected row if the data shifts under us.
_SPLIT_PAIRS: dict[tuple[str, str], str] = {
    ("LIGHTER", "ZKSYNC"): "LIGHTER-ZKSYNC",
    ("PACIFICA", "SOLANA"): "PACIFICA-SOLANA",
}

#: capture_status ranked by how much evidence it carries. A collision winner is
#: picked on this FIRST, recency second — a newer empty row must never evict an
#: older captured one (the footgun the defi data_type migration's dry-run caught).
_STATUS_PRIORITY: dict[str, int] = {
    "captured": 3,
    "empty_confirmed": 2,
    "attempted_failed": 1,
    "expected_unattempted": 0,
}


def _norm(series: pd.Series) -> pd.Series:  # type: ignore[type-arg]
    """Manifest string columns mix ``None``/``NaN``/"" — flatten to "" so the
    row-key join and the emptiness tests agree."""
    return series.astype(str).fillna("").replace({"nan": "", "None": "", "<NA>": ""})


def _object_path(row: pd.Series, venue: str) -> str:  # type: ignore[type-arg]
    """The by_date object path a manifest row points at, for ``venue``."""
    return (
        f"instrument_availability/by_date/day={row['date']}"
        f"/pipeline_mode={row['pipeline_mode']}"
        f"/asset_group={row['asset_group']}"
        f"/venue={venue}/instruments.parquet"
    )


def _row_key(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    """The manifest's REAL composite row identity, joined into one comparable key."""
    cols = [c for c in _ROW_KEY_COLUMNS if c in df.columns]
    key = df[cols].copy()
    for col in cols:
        key[col] = _norm(key[col])
    return key.agg("|".join, axis=1)


def _split_mask(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    venue = _norm(df["venue"])
    chain = _norm(df["chain"])
    mask = pd.Series(data=False, index=df.index)
    for (split_venue, split_chain), _canonical in _SPLIT_PAIRS.items():
        mask |= (venue == split_venue) & (chain == split_chain)
    return mask


def verify_captured_objects(df: pd.DataFrame, bucket: str, *, max_workers: int = 16) -> list[int]:
    """Indices of ``captured`` split rows whose object is ABSENT at the canonical
    venue. Empty list = every capture claim survives the collapse honestly."""
    storage = get_storage_client(project_id=None)
    captured = df[df["capture_status"].astype(str) == "captured"]
    if captured.empty:
        return []

    def _check(item: tuple[int, pd.Series]) -> tuple[int, bool]:  # type: ignore[type-arg]
        idx, row = item
        venue = _norm(pd.Series([row["venue"]])).iloc[0]
        chain = _norm(pd.Series([row["chain"]])).iloc[0]
        canonical = _SPLIT_PAIRS[(venue, chain)]
        try:
            return idx, bool(storage.blob_exists(bucket, _object_path(row, canonical)))
        except Exception as exc:  # a lookup failure must not read as "absent"
            logger.warning("object check failed for idx=%s (%s) — treating as PRESENT-unknown", idx, exc)
            return idx, True

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_check, captured.iterrows()))
    return [idx for idx, ok in results if not ok]


def canonicalize_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Collapse + de-duplicate. Pure — no I/O, so it is unit-testable."""
    mask = _split_mask(df)
    split = df[mask].copy()
    rest = df[~mask].copy()

    stats: dict[str, int] = {
        "rows_in": len(df),
        "split_rows": int(mask.sum()),
        "collisions_dropped": 0,
    }
    if split.empty:
        stats["rows_out"] = len(df)
        return df, stats

    venue = _norm(split["venue"])
    chain = _norm(split["chain"])
    split["venue"] = [_SPLIT_PAIRS[(v, c)] for v, c in zip(venue, chain, strict=True)]
    split["chain"] = ""
    # The rest of the frame must use the SAME "" normalisation or the row keys
    # would compare a real "" against a NaN and never match.
    rest["chain"] = _norm(rest["chain"])

    out = pd.concat([rest, split], ignore_index=True)
    out["_row_key"] = _row_key(out)

    # Winner per key: strongest capture_status first, then most recent attempt.
    out["_status_rank"] = out["capture_status"].astype(str).map(_STATUS_PRIORITY).fillna(-1)
    out["_attempted"] = _norm(out["attempted_at"]) if "attempted_at" in out.columns else ""
    out = out.sort_values(["_status_rank", "_attempted"], kind="stable")
    before = len(out)
    out = out.drop_duplicates(subset=["_row_key"], keep="last")
    stats["collisions_dropped"] = before - len(out)

    out = out.drop(columns=["_row_key", "_status_rank", "_attempted"])
    stats["rows_out"] = len(out)
    return out, stats


def _summarize(df: pd.DataFrame, label: str) -> None:
    venue = _norm(df["venue"])
    chain = _norm(df["chain"])
    split_rows = int(_split_mask(df).sum())
    logger.info(
        "%s: rows=%d  split_rows=%d  chain_nonempty=%d  LIGHTER=%d  LIGHTER-ZKSYNC=%d  PACIFICA=%d  PACIFICA-SOLANA=%d",
        label,
        len(df),
        split_rows,
        int((chain != "").sum()),
        int((venue == "LIGHTER").sum()),
        int((venue == "LIGHTER-ZKSYNC").sum()),
        int((venue == "PACIFICA").sum()),
        int((venue == "PACIFICA-SOLANA").sum()),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Report only (default).")
    parser.add_argument("--apply", action="store_true", help="Write the migrated index.")
    parser.add_argument("--confirm", action="store_true", help="Required alongside --apply.")
    parser.add_argument(
        "--allow-missing-objects",
        action="store_true",
        help="Leave (do NOT collapse) captured rows whose canonical object is absent, instead of aborting.",
    )
    args = parser.parse_args()

    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    storage = get_storage_client(project_id=None)
    logger.info("Mode=%s bucket=%s blob=%s", "APPLY" if apply else "DRY-RUN", bucket, INDEX_BLOB)

    raw = storage.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    _summarize(df, "BEFORE")

    split = df[_split_mask(df)]
    if split.empty:
        logger.info("Idempotent no-op — 0 split venue/chain rows found.")
        return 0

    logger.info(
        "Verifying %d captured split rows have a real object at the CANONICAL venue…",
        int((split["capture_status"].astype(str) == "captured").sum()),
    )
    missing = verify_captured_objects(split, bucket)
    if missing:
        logger.error(
            "%d captured split rows have NO object at their canonical venue — collapsing them would "
            "create phantom capture claims.",
            len(missing),
        )
        for idx in missing[:10]:
            row = df.loc[idx]
            logger.error("  MISSING: venue=%s chain=%s date=%s", row["venue"], row["chain"], row["date"])
        if not args.allow_missing_objects:
            logger.error(
                "ABORTING (honest-absence). Re-run with --allow-missing-objects to leave those rows untouched."
            )
            return 2
        logger.warning("--allow-missing-objects: leaving those %d rows UNTOUCHED (not collapsed).", len(missing))
    else:
        logger.info("✅ every captured split row has a real object at its canonical venue")

    # Rows whose canonical object is absent are held OUT of the collapse and
    # re-appended verbatim, so they keep their current (honest) shape rather than
    # being promoted onto a path with nothing behind it.
    held_back = df.loc[missing] if missing else None
    work = df.drop(index=missing) if missing else df
    out, stats = canonicalize_frame(work)
    if held_back is not None:
        out = pd.concat([out, held_back], ignore_index=True)

    logger.info(
        "plan: rows_in=%d split_rows=%d collisions_dropped=%d rows_out=%d (delta=%d)",
        stats["rows_in"],
        stats["split_rows"],
        stats["collisions_dropped"],
        len(out),
        stats["rows_in"] - len(out),
    )
    _summarize(out, "AFTER (planned)")

    if not apply:
        logger.info("DRY-RUN (no writes). Re-run with --apply --confirm to migrate the live index.")
        return 0

    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_key = f"{BACKUP_PREFIX}{timestamp}.bak.parquet"
    storage.upload_bytes(bucket, backup_key, raw)
    logger.info("Backup written: gs://%s/%s (%d bytes)", bucket, backup_key, len(raw))

    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage.upload_bytes(bucket, INDEX_BLOB, buf.getvalue())
    logger.info("APPLIED — gs://%s/%s rewritten (%d -> %d rows)", bucket, INDEX_BLOB, stats["rows_in"], len(out))

    verify_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, INDEX_BLOB)))
    _summarize(verify_df, "AFTER (re-read from GCS)")
    residual = int(_split_mask(verify_df).sum())
    expected_residual = len(missing) if held_back is not None else 0
    if residual != expected_residual:
        logger.error("POST-VERIFY FAILED — %d split rows remain (expected %d)", residual, expected_residual)
        return 3
    logger.info(
        "✅ POST-VERIFY: %d split venue/chain rows remain (expected %d); rollback: gs://%s/%s",
        residual,
        expected_residual,
        bucket,
        backup_key,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
