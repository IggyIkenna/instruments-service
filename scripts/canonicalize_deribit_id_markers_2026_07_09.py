#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the full DERIBIT PERPETUAL/FUTURE/OPTION @LIN/@INV backfill
#   (prod/catalog.parquet + instrument_availability/by_date per-day snapshots) is
#   applied and re-verified, AND the go-forward capture paths (tardis/adapter.py,
#   ccxt_adapter.py) are wired to the same marker convention.
"""Canonicalize DERIBIT PERPETUAL/FUTURE/OPTION instrument_id to the operator-decided
``@LIN``/``@INV`` marker + ``YYYYMMDD`` target format.

Background (instrument_id_format_canonicalization_2026_07_08.md finding 1, PERPETUAL
scope-expanded 2026-07-09): Deribit's real current instrument_id embeds the native
``DDMMMYY`` expiry and carries no margin-type marker —
``DERIBIT:OPTION:BTC-10JUL26-48000-C`` — even though Deribit genuinely runs BOTH
inverse (USD-quoted, BTC/ETH-settled) and linear (USDC-quoted/settled) instruments
side by side and the id alone cannot disambiguate them. Target:
``VENUE:TYPE:BASE[-QUOTE]@LIN|INV-YYYYMMDD[-STRIKE-C|P]`` — PERPETUAL keeps the quote
segment (``DERIBIT:PERPETUAL:BTC-USD@INV``), FUTURE/OPTION drop it
(``DERIBIT:FUTURE:BTC@INV-20260710``, ``DERIBIT:OPTION:BTC@INV-20260710-48000-C``) —
matching the doc's own worked Deribit examples verbatim.

Migration mechanics (operator-decided, same doc): re-derive the corrected
instrument_id from already-captured fields (``instrument_type``, ``base_asset``,
``margin_type``, and — for per-day snapshots — ``quote_asset``/``expiry``/``strike``/
``option_type``; ``prod/catalog.parquet`` only carries ``instrument_id``/``raw_symbol``/
``base_asset``/``margin_type`` for these types, so the DDMMMYY date is re-parsed from
``raw_symbol`` there) and rewrite the parquet/partition in place — NOT a re-download
from Deribit. Real-scoping numbers (2026-07-09, ``prod/catalog.parquet``, 4.4 MiB):
263,979 real DERIBIT PERPETUAL/FUTURE/OPTION rows (26 PERPETUAL + 1,507 FUTURE +
262,446 OPTION), 0 null ``margin_type``, 0 parse failures, 0 id collisions against
this exact transform (verified locally against the full real row set before this
script existed). The much larger historical corpus lives in per-day snapshots —
``instrument_availability/by_date/day=*/venue=DERIBIT/instruments.parquet`` (both the
legacy and ``pipeline_mode=batch_instruments_service``-prefixed path shapes) —
5,342 real files (2019-03-30 to present), NOT touched by this script's ``--apply``
(catalog-only) mode; see the module-level ``PER_DAY_PREFIX_GLOB`` docstring below for
the follow-up scope.

**STAGED ROLLOUT — this script's ``--apply`` writes ONLY the rows named by
``--sample-ids`` (a small, explicit, bounded real sample) or all target rows when
``--full-sweep`` is passed. The full sweep was NOT run as part of shipping this
script (staged-rollout rule: smoke-test + measure + report, full sweep is a separate
go/no-go decision) — see ``docs/CEFI_INSTRUMENTS.md`` "Deribit @LIN/@INV migration"
for the measured throughput/ETA this scoping produced.**

Usage::

    cd instruments-service

    # Dry-run (default) — report what would change, no writes:
    .venv/bin/python scripts/canonicalize_deribit_id_markers_2026_07_09.py

    # Smoke test — migrate a small, explicit real sample in place (backup first):
    .venv/bin/python scripts/canonicalize_deribit_id_markers_2026_07_09.py --apply \\
        --sample-size 30

    # Full sweep (NOT run by this session — separate go/no-go decision):
    .venv/bin/python scripts/canonicalize_deribit_id_markers_2026_07_09.py --apply \\
        --full-sweep
"""

from __future__ import annotations

import argparse
import io
import logging
import re
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library.cloud_interface.factory import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"instruments-store-cefi-prd-{PROJECT_ID}"
CATALOG_PATH = "prod/catalog.parquet"
BACKUP_PREFIX = "prod/catalog.deribit-marker-migration."

_VENUE = "DERIBIT"
_TARGET_TYPES = ("PERPETUAL", "FUTURE", "OPTION")

_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_DDMMMYY_RE = re.compile(r"^(\d{1,2})([A-Z]{3})(\d{2})$")


def _ddmmmyy_to_yyyymmdd(token: str) -> str | None:
    """Reformat a Deribit ``DDMMMYY`` raw expiry token (e.g. ``10JUL26``) as ``YYYYMMDD``."""
    m = _DDMMMYY_RE.match(token.upper())
    if not m:
        return None
    day, month_str, year_str = int(m.group(1)), m.group(2), int(m.group(3))
    month = _MONTHS.get(month_str)
    if not month:
        return None
    return f"{2000 + year_str:04d}{month:02d}{day:02d}"


def _marker(margin_type: object) -> str | None:
    """Map a real, already-captured ``margin_type`` string to its ``@LIN``/``@INV`` token."""
    mt = str(margin_type or "").strip().lower()
    if mt == "inverse":
        return "INV"
    if mt == "linear":
        return "LIN"
    return None


def build_target_instrument_id(instrument_type: str, instrument_id: str, margin_type: object) -> tuple[str | None, str]:
    """Re-derive the target ``@LIN``/``@INV`` instrument_id from catalog columns already present.

    Returns ``(new_id_or_None, reason)`` — ``reason`` is empty on success, else a
    short skip-reason string (unclassifiable margin_type / already migrated / an
    unrecognised symbol shape). Never guesses: a row this function can't confidently
    re-derive is left untouched, not silently mismigrated.
    """
    marker = _marker(margin_type)
    if marker is None:
        return None, "no_margin_type"
    prefix, _, symbol = instrument_id.partition(":" + instrument_type + ":")
    if not symbol:
        return None, "unparseable_instrument_id"
    if "@" in symbol:
        return None, "already_migrated"
    venue_type_prefix = f"{prefix}:{instrument_type}:"
    if instrument_type == "PERPETUAL":
        return f"{venue_type_prefix}{symbol}@{marker}", ""
    if instrument_type in ("FUTURE", "OPTION"):
        parts = symbol.split("-")
        if len(parts) < 2:
            return None, "too_few_dash_parts"
        base_part = parts[0].split("_", 1)[0]  # strip _QUOTE (e.g. AVAX_USDC -> AVAX)
        yyyymmdd = _ddmmmyy_to_yyyymmdd(parts[1])
        if yyyymmdd is None:
            return None, f"unparseable_date:{parts[1]}"
        new_symbol = f"{base_part}@{marker}-{yyyymmdd}"
        tail = parts[2:]
        if tail:
            new_symbol += "-" + "-".join(tail)
        return f"{venue_type_prefix}{new_symbol}", ""
    return None, "unsupported_instrument_type"


def _select_sample(target: pd.DataFrame, sample_size: int) -> pd.DataFrame:
    """Pick a small, real, representative bounded sample spanning every
    (instrument_type, margin_type) combination present — mirrors the 2026-07-09
    OKX margin-type smoke test's "spanning all buckets" approach.
    """
    groups = target.groupby(["instrument_type", "margin_type"], observed=True)
    per_group = max(1, sample_size // max(1, len(groups)))
    parts = [g.head(per_group) for _, g in groups]
    sample = pd.concat(parts) if parts else target.head(0)
    return sample.head(sample_size)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--apply", action="store_true", help="Write the migrated catalog (default: dry-run/report only)."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=30,
        help="Bounded real sample size for --apply (default 30; ignored with --full-sweep).",
    )
    parser.add_argument(
        "--full-sweep",
        action="store_true",
        help="Migrate ALL real target rows, not just a bounded sample. NOT used in the 2026-07-09 shipping pass "
        "(staged-rollout: smoke-test + measure + report; full sweep is a separate go/no-go decision).",
    )
    args = parser.parse_args()

    storage_client = get_storage_client()
    raw = storage_client.download_bytes(BUCKET, CATALOG_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded gs://%s/%s — %d total rows", BUCKET, CATALOG_PATH, len(df))

    mask = (df["venue"] == _VENUE) & (df["instrument_type"].isin(_TARGET_TYPES))
    target = df.loc[mask]
    logger.info("Real DERIBIT PERPETUAL/FUTURE/OPTION rows in scope: %d", len(target))
    logger.info("By instrument_type: %s", target["instrument_type"].value_counts().to_dict())

    new_ids: dict[int, str] = {}
    reasons: dict[str, int] = {}
    for idx, row in target.iterrows():
        new_id, reason = build_target_instrument_id(row["instrument_type"], row["instrument_id"], row["margin_type"])
        if new_id is not None:
            new_ids[idx] = new_id
        elif reason:
            reasons[reason] = reasons.get(reason, 0) + 1

    logger.info("Re-derivable: %d / %d (%.1f%%)", len(new_ids), len(target), 100.0 * len(new_ids) / max(1, len(target)))
    if reasons:
        logger.info("Skip reasons: %s", reasons)

    collisions = pd.Series(new_ids.values()).duplicated().sum()
    if collisions:
        logger.error(
            "%d collision(s) in re-derived ids — ABORTING (never silently merge distinct instruments)", collisions
        )
        return 1
    logger.info("Collision check: 0 (each re-derived id is unique)")

    if not args.apply:
        logger.info(
            "DRY-RUN (no writes). Re-run with --apply to migrate a bounded sample, or --apply --full-sweep for all rows."
        )
        sample_preview = _select_sample(target, args.sample_size)
        for idx in sample_preview.index:
            if idx in new_ids:
                logger.info("  %s -> %s", target.loc[idx, "instrument_id"], new_ids[idx])
        return 0

    write_idx = list(target.index) if args.full_sweep else list(_select_sample(target, args.sample_size).index)
    write_idx = [i for i in write_idx if i in new_ids]
    if not write_idx:
        logger.warning("Nothing re-derivable in the selected scope — no writes performed.")
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_key = f"{BACKUP_PREFIX}{ts}.bak.parquet"
    storage_client.upload_bytes(BUCKET, backup_key, raw)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Backup written: gs://%s/%s (%d bytes)", BUCKET, backup_key, len(raw))

    before_after = [(target.loc[i, "instrument_id"], new_ids[i]) for i in write_idx]
    df = df.copy()
    for idx in write_idx:
        df.loc[idx, "instrument_id"] = new_ids[idx]
    out = io.BytesIO()
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, CATALOG_PATH, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info(
        "APPLIED — %d row(s) migrated (%s), gs://%s/%s rewritten (%d bytes)",
        len(write_idx),
        "full sweep" if args.full_sweep else f"bounded sample of {args.sample_size}",
        BUCKET,
        CATALOG_PATH,
        len(out.getvalue()),
    )
    for old_id, new_id in before_after[:10]:
        logger.info("  %s -> %s", old_id, new_id)
    if len(before_after) > 10:
        logger.info("  ... and %d more", len(before_after) - 10)

    # Verify by re-downloading and re-checking the written rows.
    verify_raw = storage_client.download_bytes(BUCKET, CATALOG_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    verify_df = pd.read_parquet(io.BytesIO(verify_raw))
    ok = all(verify_df.loc[i, "instrument_id"] == new_ids[i] for i in write_idx)
    if not ok:
        logger.error(
            "VERIFICATION FAILED — re-downloaded catalog does not match the intended write. Restore from backup: gs://%s/%s",
            BUCKET,
            backup_key,
        )
        return 1
    logger.info("Verification PASSED — all %d written row(s) confirmed correct by re-download.", len(write_idx))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
