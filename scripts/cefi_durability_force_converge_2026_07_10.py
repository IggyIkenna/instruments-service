#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod/catalog.parquet is confirmed durably converged for
#   BYBIT/KRAKEN-FUTURES/DERIBIT/OKX-SWAP/OKX-FUTURES across a real --mode
#   incremental regen cycle AND instrument_id_format_canonicalization_2026_07_08.md's
#   CeFi-durability follow-up items are marked resolved. (OKX-SWAP/OKX-FUTURES added
#   2026-07-13 — same durability gap, discovered later via a full-epic status check.)
"""Force-converge CeFi by_date + catalog durability for BYBIT/KRAKEN-FUTURES/DERIBIT/
OKX-SWAP/OKX-FUTURES.

Real root cause (diagnosed 2026-07-10, live GCS + Cloud Build/Run evidence, not
re-run blind): the two prior one-off migration scripts
(``canonicalize_bybit_kraken_futures_catalog_2026_07_09.py``,
``canonicalize_deribit_id_markers_2026_07_09.py``) wrote their per-file backups
INLINE next to the original blob (``instruments.<ts>.<tag>.bak.parquet``, same
``day=/venue=/`` directory) instead of to a quarantined ``_migration_backup/``
prefix. ``build_instrument_catalogue.py``'s ``_iter_by_date_snapshots`` walk has
NO filename filter (only checks ``.parquet`` suffix + a ``day=YYYY-MM-DD``
path segment via ``_DAY_RE``), so it silently unions all 14,902 of these stray
backup files' OLD-format rows into EVERY catalogue rebuild (full or
incremental-window) — permanently reintroducing pre-migration instrument_ids as
"active" no matter how many times the current ``instruments.parquet`` files or
``prod/catalog.parquet`` itself get fixed. This — not a code gap — is why the
2026-07-08/09 fixes were "not durable".

Separately (tracked, NOT fixed by this script — see the issue doc): the
production ``is-daily-enum-cefi`` Cloud Run job's deployed image was built
2026-07-09T00:50 UTC, BEFORE all 3 of this session's @LIN/@INV fix commits
(176d4610 09:19 UTC, 8128189e 12:43 UTC, c2d3fbbc 18:22 UTC, same day) — its
daily 13:30 UTC run (``--days-back 3``) genuinely re-wrote day=2026-07-08 back
to old-format and wrote day=2026-07-09 fresh in old-format for KRAKEN-FUTURES/
DERIBIT, confirmed via blob-timestamp correlation with the scheduler's real
``lastAttemptTime``. The live adapter CODE itself (this repo's current git
HEAD) was verified 100% canonical via a real, live Tardis-API call for all 3
venues 2026-07-10 — this is a stale-deployment problem, not a code gap.

This script does the two things fully within THIS repo's control:

1. ``--quarantine-backups``: relocate (server-side copy + delete) every real
   stray ``.bak.parquet`` file for BYBIT/KRAKEN-FUTURES/DERIBIT out of the
   walked ``instrument_availability/by_date/`` tree into
   ``_migration_backup/cefi_by_date_bak_relocation_2026_07_10/<original path>``
   — nothing is lost, it just stops polluting the catalogue walk.
2. ``--fix-by-date``: for every CURRENT (non-backup) ``instruments.parquet``
   file, re-derive ``margin_type`` (re-inferred from ``raw_symbol`` via the
   CURRENT ``_infer_margin_type`` — the STORED value is not trusted, since
   real evidence shows pre-fix rows carry a wrong stored value too, e.g.
   Kraken-Futures ``FI_``-prefixed real-inverse rows stored as
   ``margin_type=linear``) and ``instrument_key`` (via the same shared
   ``_build_canonical_perpetual_key``/``_build_canonical_future_key``/
   ``_build_canonical_option_key`` the live adapter code calls) from each
   row's own already-captured ``base_asset``/``quote_asset``/``expiry``/
   ``strike``/``option_type``/``raw_symbol`` — no re-download from the venue
   (migration-mechanics decision, instrument_id_format_canonicalization_
   2026_07_08.md). Backup-first (to the SAME quarantine prefix, never inline),
   verify-after, idempotent, shard-isolated (one file's failure never aborts
   the sweep).
3. ``--fix-expiry-canonical``: correct the stored ``expiry`` metadata column
   to the CANONICAL, symbol-derived expiry (replaying the live adapter's own
   tier-3 regex cascade — ``_canonical_regex_expiry``, mirroring
   ``tardis/adapter.py::_parse_tardis_instrument`` lines ~740-763 — against
   each row's ``raw_symbol``). This is the THIRD design for this fix, each
   superseding the last on real evidence:
     a. ``--fix-frozen-expiry`` (REMOVED): corrected stored expiry to a
        raw_symbol regex-parse whenever 2+ distinct raw_symbols collided on
        one stored expiry within a group. Wrong on two counts — a real
        collision can be a legitimate coincidence (Deribit delists multiple
        series together), and the regex target ignored a real, valid T+1
        settlement offset for genuinely-wrong rows. Corrupted 35,410
        previously-correct rows while fixing only 209 (see damage-assessment
        history below).
     b. ``--fix-expiry-ground-truth`` (REMOVED, same-day pivot): corrected
        stored expiry against Tardis's free ``availableTo`` field directly,
        treating it as ground truth. Also wrong: a live-data check
        (2026-07-12) showed ``availableTo`` legitimately differs from the
        canonical symbol-encoded date by ~1 day for currently-active-at-
        capture-time instruments (e.g. ``BTC-26JUN26`` stores expiry
        2026-06-26, matching the symbol exactly, while Tardis's
        ``availableTo`` for the same instrument is 2026-06-27 — a
        data-collection artifact in when Tardis marks an instrument's last
        observed day, not a settlement-time signal; real-world Deribit
        options/futures settle at 08:00 UTC on the date named in the
        symbol). Treating ``availableTo`` as the correction target would
        have rewritten ~266k+ already-correct rows.
     c. THIS flag (current, 2026-07-12/13 operator ruling): the raw_symbol's
        own encoding is canonical — matches the venue's real settlement
        convention and the live adapter's own tier-3 fallback exactly, so it
        needs no external API dependency to be correct — and is applied
        UNCONDITIONALLY to every row whose canonical value is parseable and
        differs from what's stored. Tardis's ``availableTo`` (free, no-auth
        ``GET /v1/exchanges/{exchange}``, fetched live, cached per venue) is
        retained ONLY as reporting telemetry: a gap of more than
        ``_ANOMALY_THRESHOLD_DAYS`` days is logged for post-hoc visibility,
        never held back or blocked. An initial same-day version of this flag
        treated any such gap as a shard failure and skipped writing the
        WHOLE file — but a 2026-07-13 sample of 40 DERIBIT files showed every
        single gap (215/68,253 rows) was one-directional early delisting
        (routine for illiquid options ahead of their scheduled expiry, not
        corruption), and file-level skip at that rate would have discarded
        corrections for 67% of DERIBIT's files (3,602/5,347) to guard
        against a pattern that isn't actually dangerous. See
        ``_detect_expiry_corrections``'s docstring.

Usage::

    cd instruments-service
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --quarantine-backups --dry-run
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --quarantine-backups --apply
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --fix-by-date --dry-run
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --fix-by-date --apply --workers 32
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --fix-expiry-canonical --dry-run
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --fix-expiry-canonical --apply --workers 32

SSOT: unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
"""

from __future__ import annotations

import argparse
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import requests
from google.cloud import storage as _gcs_sdk  # noqa: qg-deep-import — read-only bulk LISTING only (server-side

# match_glob filtering ~28s/venue vs UTL StorageClient.list_blobs's unfiltered full-prefix walk, which
# does not terminate in a reasonable window over this bucket's ~150k+ cefi by_date objects — see module
# docstring. Every actual read/write/delete of blob CONTENT below goes through UTL's StorageClient
# (get_storage_client()), not this. Migration-script carve-out, matches codex/06-coding-standards/
# quality-gates.md's "no direct google.cloud" ban intent (production service code paths), not a
# one-off backfill's read-only enumeration step.
from unified_api_contracts.internal import InstrumentType, MarginType
from unified_trading_library import get_storage_client, resolve_bucket_name

from instruments_service.reference_data.adapters.cefi.tardis import parsing as tardis_parsing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BY_DATE_PREFIX = "instrument_availability/by_date/"
QUARANTINE_PREFIX = "_migration_backup/cefi_by_date_bak_relocation_2026_07_10/"
_TARGET_VENUES: tuple[str, ...] = ("BYBIT", "KRAKEN-FUTURES", "DERIBIT", "OKX-SWAP", "OKX-FUTURES")
_EXCHANGE_FOR_VENUE: dict[str, str] = {
    "BYBIT": "bybit",
    "KRAKEN-FUTURES": "cryptofacilities",
    "DERIBIT": "deribit",
    # Added 2026-07-13: OKX-SWAP/OKX-FUTURES' per-day corpus was never force-converged
    # (excluded from the original 3-venue scope) — the live adapter code was already
    # canonical for OKX (routes through the same shared builders), but its by_date
    # historical corpus showed 0% canonical every day from 2020-01-01 through
    # 2026-07-08, confirmed via a direct GCS trace.
    "OKX-SWAP": "okex-swap",
    "OKX-FUTURES": "okex-futures",
}
_DERIVATIVE_TYPES = frozenset({"FUTURE", "OPTION", "PERPETUAL"})


# --------------------------------------------------------------------------
# Step 0: one bulk LISTING pass per venue (server-side match_glob — see the
# module-level SDK-usage note above), split into (current, backup) locally.
# --------------------------------------------------------------------------


def _list_all_for_venue(bucket: str, venue: str) -> tuple[list[str], list[str]]:
    """Return ``(current_files, stray_backup_files)`` for one venue, ONE listing call."""
    gcs_client = _gcs_sdk.Client()
    gcs_bucket = gcs_client.bucket(bucket)
    current: list[str] = []
    backups: list[str] = []
    for b in gcs_client.list_blobs(gcs_bucket, prefix=BY_DATE_PREFIX, match_glob=f"**/venue={venue}/*.parquet"):
        name = str(b.name)
        if name.endswith("/instruments.parquet"):
            current.append(name)
        else:
            backups.append(name)
    return current, backups


def _quarantine_one(st, bucket: str, blob_name: str, apply: bool) -> str:
    dest = QUARANTINE_PREFIX + blob_name
    if not apply:
        return "would_move"
    if st.blob_exists(bucket, dest):
        # Idempotent re-run: already relocated (or a same-named prior relocation) — just
        # remove the stray source copy, never overwrite an existing quarantine entry.
        st.delete_blob(bucket, blob_name)
        return "already_quarantined_source_removed"
    raw = st.download_bytes(bucket, blob_name)
    st.upload_bytes(bucket, dest, raw)
    verify = st.download_bytes(bucket, dest)
    if len(verify) != len(raw):
        return "verify_size_mismatch_source_kept"
    st.delete_blob(bucket, blob_name)
    return "moved"


def run_quarantine(st, bucket: str, apply: bool, workers: int, all_targets: list[str]) -> None:
    logger.info(
        "=== quarantine: %d stray file(s) total (mode=%s, workers=%d) ===",
        len(all_targets),
        "APPLY" if apply else "DRY-RUN",
        workers,
    )
    results: dict[str, int] = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_quarantine_one, st, bucket, blob, apply): blob for blob in all_targets}
        for i, fut in enumerate(as_completed(futures), start=1):
            blob = futures[fut]
            try:
                outcome = fut.result()
            except Exception:
                logger.exception("FAILED quarantine %s", blob)
                outcome = "error"
            results[outcome] = results.get(outcome, 0) + 1
            if i % 1000 == 0 or i == len(all_targets):
                elapsed = time.time() - t0
                logger.info("[%d/%d] elapsed=%.1fs outcomes=%s", i, len(all_targets), elapsed, results)
    logger.info("quarantine DONE: total=%d outcomes=%s elapsed=%.1fs", len(all_targets), results, time.time() - t0)


# --------------------------------------------------------------------------
# Step 2: re-derive instrument_key + margin_type on CURRENT by_date files
# --------------------------------------------------------------------------


def _to_decimal(val: object) -> Decimal | None:
    if val is None:
        return None
    try:
        if isinstance(val, float) and val != val:  # NaN
            return None
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None


def _re_derive_row(venue: str, row: pd.Series[object]) -> tuple[str | None, MarginType | None, str]:
    """Re-derive ``(new_instrument_key, new_margin_type, reason)`` for one row.

    Returns ``(None, None, reason)`` when the row cannot be safely re-derived — the
    caller leaves it UNCHANGED (never guesses). Mirrors the exact construction the
    CURRENT live adapter code uses (``tardis/adapter.py::_parse_tardis_instrument``),
    just replayed against already-captured columns instead of a fresh venue fetch.
    """
    itype_str = str(row.get("instrument_type") or "")
    if itype_str not in _DERIVATIVE_TYPES:
        return None, None, "not_a_margined_type"
    base = str(row.get("base_asset") or "").strip()
    quote = str(row.get("quote_asset") or "").strip()
    raw_symbol = str(row.get("raw_symbol") or "").strip()
    if not base or not raw_symbol:
        return None, None, "missing_base_or_raw_symbol"
    exchange = _EXCHANGE_FOR_VENUE[venue]
    inst_type = InstrumentType(itype_str)
    margin_type = tardis_parsing._infer_margin_type(inst_type, quote, raw_symbol, exchange)
    if margin_type is None:
        return None, None, "margin_type_unresolvable"

    if itype_str == "PERPETUAL":
        if not quote:
            return None, None, "perpetual_missing_quote"
        key = tardis_parsing._build_canonical_perpetual_key(venue, base, quote, margin_type)
        return key, margin_type, ""

    expiry_raw = row.get("expiry")
    expiry = expiry_raw.to_pydatetime() if hasattr(expiry_raw, "to_pydatetime") else expiry_raw
    if venue == "DERIBIT":
        # Historical capture bug (2019-era; confirmed dead in the current pipeline —
        # see cefi_durability_deribit_expiry_dup_guard_residual_2026_07_11.md): some
        # rows have `expiry` stamped to the file's own capture/snapshot day instead of
        # the instrument's real expiry, silently colliding distinct options/futures
        # (different real expiry, same strike/right) onto the same re-derived key.
        # raw_symbol always encodes the true DDMMMYY expiry for Deribit and is the same
        # fallback the live adapter itself uses when Tardis's item.expiry is absent —
        # prefer it outright rather than trying to detect the corrupt case.
        parsed_expiry = tardis_parsing._parse_deribit_symbol_expiry(raw_symbol)
        if parsed_expiry is not None:
            expiry = parsed_expiry
    if expiry is None or pd.isna(expiry):
        return None, None, "missing_expiry"
    dated_quote = "" if venue == "DERIBIT" else quote

    if itype_str == "FUTURE":
        key = tardis_parsing._build_canonical_future_key(venue, base, dated_quote, margin_type, expiry)
        return key, margin_type, ""

    # OPTION
    strike = _to_decimal(row.get("strike"))
    option_type_raw = str(row.get("option_type") or "").strip().lower()
    if strike is None or option_type_raw not in ("call", "put"):
        return None, None, "missing_strike_or_option_type"
    option_right = "C" if option_type_raw == "call" else "P"
    key = tardis_parsing._build_canonical_option_key(
        venue, base, dated_quote, margin_type, expiry, strike, option_right
    )
    return key, margin_type, ""


def _fix_frame(venue: str, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {"rows": len(df), "id_changed": 0, "margin_fixed": 0, "unresolved": 0}
    if df.empty or "instrument_key" not in df.columns:
        return df, stats
    mask = (
        df["instrument_type"].isin(_DERIVATIVE_TYPES)
        if "instrument_type" in df.columns
        else pd.Series(False, index=df.index)
    )
    orig_id = df["instrument_key"].astype(str)
    new_id = orig_id.copy()
    orig_margin = df["margin_type"].astype(str) if "margin_type" in df.columns else pd.Series("", index=df.index)
    new_margin = df["margin_type"].copy() if "margin_type" in df.columns else pd.Series(None, index=df.index)
    for idx in df.index[mask]:
        row = df.loc[idx]
        key, margin_type, reason = _re_derive_row(venue, row)
        if key is None:
            if reason != "not_a_margined_type":
                stats["unresolved"] += 1
            continue
        new_id.loc[idx] = key
        if margin_type is not None:
            new_margin.loc[idx] = margin_type.value
    stats["id_changed"] = int((new_id != orig_id).sum())
    stats["margin_fixed"] = int((new_margin.astype(str) != orig_margin).sum())
    out = df.copy()
    out["instrument_key"] = new_id
    if "margin_type" in df.columns:
        out["margin_type"] = new_margin
    return out, stats


def _would_introduce_new_collision(orig_keys: pd.Series, new_keys: pd.Series) -> bool:
    """True if any group of rows sharing a NEW ``instrument_key`` value contains 2+ rows
    that did NOT already share the same ORIGINAL ``instrument_key`` — i.e. two
    previously-distinct real instruments would be merged onto one key.

    Stricter than comparing duplicate-row COUNTS before/after
    (``.duplicated().sum()``): a resolve+introduce pair (one pre-existing collision
    fixed, one new one introduced elsewhere in the same file) can leave the total
    duplicate-row count unchanged, silently passing a scalar-count guard while still
    merging two real instruments (2026-07-13 review finding — the scalar-count version
    of this guard was confirmed exploitable and is why this function exists).
    """
    groups: dict[str, set[str]] = {}
    for orig, new in zip(orig_keys, new_keys, strict=True):
        groups.setdefault(new, set()).add(orig)
    return any(len(origs) > 1 for origs in groups.values())


def _process_one_file(st, bucket: str, venue: str, blob_name: str, apply: bool) -> dict[str, object]:
    raw = st.download_bytes(bucket, blob_name)
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = _fix_frame(venue, df)
    result: dict[str, object] = {"blob": blob_name, **stats, "written": 0}
    if len(out) != len(df):
        result["error"] = "row_count_changed"
        return result
    if stats["id_changed"] == 0 and stats["margin_fixed"] == 0:
        return result
    # Duplicate-introduction guard — never silently merge distinct instruments.
    if _would_introduce_new_collision(df["instrument_key"].astype(str), out["instrument_key"].astype(str)):
        result["error"] = "would_introduce_new_collision"
        return result
    if not apply:
        result["written"] = 1
        return result
    backup_dest = QUARANTINE_PREFIX + blob_name
    if not st.blob_exists(bucket, backup_dest):
        st.upload_bytes(bucket, backup_dest, raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    st.upload_bytes(bucket, blob_name, buf.getvalue())
    result["written"] = 1
    return result


# --------------------------------------------------------------------------
# Step 3: correct stored `expiry` against REAL Tardis ground truth (the free,
# no-auth /v1/exchanges/{exchange} endpoint) — see module docstring for why
# this supersedes the earlier same-day regex-collision-based attempt.
# --------------------------------------------------------------------------

_EXPIRING_TYPES = frozenset({"FUTURE", "OPTION"})
_ANOMALY_THRESHOLD_DAYS = 2


def _fetch_tardis_ground_truth(exchange: str) -> dict[str, datetime | None]:
    """Fetch ``GET /v1/exchanges/{exchange}`` (free, no-auth — the same endpoint the
    live adapter already calls in production, per its own module comment) and build a
    ``{raw_symbol: availableTo}`` SAFEGUARD lookup for one exchange — NOT a correction
    target (see ``_detect_expiry_corrections``'s docstring for why).

    Each ``availableSymbols[]`` entry's ``id`` is the exact ``raw_symbol`` this repo
    stores. A raw_symbol appearing more than once with disagreeing ``availableTo``
    values is dropped as ambiguous rather than guessed.
    """
    url = f"https://api.tardis.dev/v1/exchanges/{exchange}"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    payload = resp.json()
    lookup: dict[str, datetime | None] = {}
    ambiguous: set[str] = set()
    for item in payload.get("availableSymbols", []):
        raw_id = str(item.get("id") or "")
        if not raw_id:
            continue
        available_to_raw = item.get("availableTo")
        available_to = (
            datetime.fromisoformat(str(available_to_raw).replace("Z", "+00:00")) if available_to_raw else None
        )
        if raw_id in lookup and lookup[raw_id] != available_to:
            ambiguous.add(raw_id)
            continue
        lookup[raw_id] = available_to
    for raw_id in ambiguous:
        lookup.pop(raw_id, None)
    return lookup


def _canonical_regex_expiry(raw_id: str, exchange: str) -> datetime | None:
    """Symbol-derived CANONICAL expiry — replays the exact fallback cascade the live
    adapter uses for its tier-3 case (``tardis/adapter.py::_parse_tardis_instrument``,
    lines ~740-763) against a bare raw_symbol. Per 2026-07-12 operator ruling, the
    symbol encoding IS the canonical expiry: it matches the venue's real settlement
    convention (e.g. Deribit options/futures settle at 08:00 UTC on the date named in
    the symbol) and needs no external API dependency to be correct.
    """
    expiry: datetime | None = None
    if "-" in raw_id:
        expiry = tardis_parsing._parse_deribit_symbol_expiry(raw_id)
    if expiry is None and "-" in raw_id:
        expiry = tardis_parsing._parse_yymmdd_symbol_expiry(raw_id)
    if expiry is None and "_" in raw_id:
        expiry = tardis_parsing._parse_underscore_yymmdd_symbol_expiry(raw_id)
    if expiry is None and exchange == "bybit":
        expiry = tardis_parsing._parse_bybit_month_code_expiry(raw_id)
    return expiry


def _detect_expiry_corrections(
    venue: str, df: pd.DataFrame, gt_lookup: dict[str, datetime | None]
) -> tuple[dict[int, datetime], list[dict[str, object]]]:
    """Detect FUTURE/OPTION rows whose stored ``expiry`` disagrees with the CANONICAL,
    symbol-derived expiry (``_canonical_regex_expiry``) and return
    ``({row_index: corrected_expiry}, [anomaly, ...])``.

    EVERY row whose canonical expiry is parseable and differs from the stored value is
    corrected — unconditionally. Tardis's ``availableTo`` (free
    ``/v1/exchanges/{exchange}``) is retained ONLY as reporting telemetry: a row whose
    gap against canonical exceeds ``_ANOMALY_THRESHOLD_DAYS`` is appended to the
    returned anomaly list for post-hoc visibility, but is corrected exactly like any
    other row — it is NEVER held back and never blocks the rest of the file.
    2026-07-13 operator ruling, after a 40-file DERIBIT sample showed anomalies are
    common (215/68,253 rows, ~0.3%) but ENTIRELY one-directional (``availableTo``
    always BEFORE canonical, never after) — i.e. routine early delisting of illiquid
    options ahead of their scheduled expiry, not corruption. An earlier same-day
    version of this function treated any gap as a shard failure and skipped writing
    the WHOLE file, which — at DERIBIT's real anomaly-per-file rate — discarded
    corrections for 67% of files (3,602/5,347) to guard against a pattern that turned
    out to be normal, not dangerous. Supersedes ``_detect_frozen_expiry_rows`` and
    ``_detect_ground_truth_mismatches`` (both removed 2026-07-12 — see the module
    docstring for their full failure histories). A row whose canonical expiry can't be
    parsed at all is left untouched rather than guessed.
    """
    exchange = _EXCHANGE_FOR_VENUE[venue]
    mask = (
        df["instrument_type"].isin(_EXPIRING_TYPES)
        if "instrument_type" in df.columns
        else pd.Series(False, index=df.index)
    )
    corrections: dict[int, datetime] = {}
    anomalies: list[dict[str, object]] = []
    for idx in df.index[mask]:
        raw_symbol = str(df.at[idx, "raw_symbol"] or "")
        canonical = _canonical_regex_expiry(raw_symbol, exchange)
        if canonical is None:
            continue
        gt_available_to = gt_lookup.get(raw_symbol)
        if gt_available_to is not None:
            gap_days = (gt_available_to.date() - canonical.date()).days
            if abs(gap_days) > _ANOMALY_THRESHOLD_DAYS:
                anomalies.append(
                    {
                        "row": idx,
                        "raw_symbol": raw_symbol,
                        "canonical": canonical.date().isoformat(),
                        "available_to": gt_available_to.date().isoformat(),
                        "gap_days": gap_days,
                    }
                )
        stored = df.at[idx, "expiry"]
        if pd.isna(stored):
            corrections[idx] = canonical
            continue
        stored_dt = stored.to_pydatetime() if hasattr(stored, "to_pydatetime") else stored
        if stored_dt.date() != canonical.date():
            corrections[idx] = canonical
    return corrections, anomalies


def _process_expiry_canonical_file(
    st, bucket: str, venue: str, blob_name: str, apply: bool, gt_lookup: dict[str, datetime | None]
) -> dict[str, object]:
    raw = st.download_bytes(bucket, blob_name)
    df = pd.read_parquet(io.BytesIO(raw))
    corrections, anomalies = _detect_expiry_corrections(venue, df, gt_lookup)
    result: dict[str, object] = {
        "blob": blob_name,
        "rows_fixed": len(corrections),
        "written": 0,
        "anomalies": anomalies,
    }
    if not corrections:
        return result
    out = df.copy()
    for idx, new_expiry in corrections.items():
        out.at[idx, "expiry"] = pd.Timestamp(new_expiry)
    # Re-derive instrument_key/margin_type from the corrected expiry so the key stays
    # consistent with the metadata column — but ONLY for the rows THIS flag actually
    # corrected, never the whole file: _fix_frame's mask (FUTURE/OPTION/PERPETUAL) is
    # broader than the rows this flag touches (FUTURE/OPTION with a corrected expiry),
    # so its output is applied here only at `corrections`' own row indices, leaving
    # every other row's instrument_key/margin_type exactly as it already was — a no-op
    # for DERIBIT, whose key already ignores the expiry column in favor of raw_symbol's
    # own regex-derived date (see _re_derive_row); load-bearing for BYBIT/
    # KRAKEN-FUTURES, whose key IS built directly from the expiry column. Keeping this
    # scoped is what makes "Independent of --fix-by-date" (main()'s --help text)
    # actually true, and keeps rows_fixed/id_changed/margin_fixed telemetry honest
    # (2026-07-13 review finding: an earlier version applied the full re-derivation —
    # and its accompanying dup-guard — to the WHOLE file, silently rewriting rows this
    # flag was never meant to touch and under-reporting the real blast radius).
    rederived, _stats = _fix_frame(venue, out)
    fixed_frame = out.copy()
    orig_id = df["instrument_key"].astype(str)
    orig_margin = df["margin_type"].astype(str) if "margin_type" in df.columns else pd.Series("", index=df.index)
    id_changed = 0
    margin_fixed = 0
    for idx in corrections:
        new_id = str(rederived.at[idx, "instrument_key"])
        fixed_frame.at[idx, "instrument_key"] = new_id
        if new_id != orig_id.at[idx]:
            id_changed += 1
        if "margin_type" in rederived.columns:
            new_margin = rederived.at[idx, "margin_type"]
            fixed_frame.at[idx, "margin_type"] = new_margin
            if str(new_margin) != orig_margin.at[idx]:
                margin_fixed += 1
    result["id_changed"] = id_changed
    result["margin_fixed"] = margin_fixed
    # Duplicate-introduction guard — never silently merge distinct instruments.
    if _would_introduce_new_collision(orig_id, fixed_frame["instrument_key"].astype(str)):
        result["error"] = "would_introduce_new_collision"
        return result
    if not apply:
        result["written"] = 1
        return result
    backup_dest = QUARANTINE_PREFIX + blob_name
    if not st.blob_exists(bucket, backup_dest):
        st.upload_bytes(bucket, backup_dest, raw)
    buf = io.BytesIO()
    fixed_frame.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    st.upload_bytes(bucket, blob_name, buf.getvalue())
    result["written"] = 1
    return result


def run_fix_expiry_canonical(
    st, bucket: str, apply: bool, workers: int, limit: int | None, current_by_venue: dict[str, list[str]]
) -> None:
    for venue in _TARGET_VENUES:
        exchange = _EXCHANGE_FOR_VENUE[venue]
        logger.info("=== fix-expiry-canonical %s: fetching Tardis safeguard data (%s) ===", venue, exchange)
        # Non-fatal: this lookup is telemetry-only (_detect_expiry_corrections never
        # gates the correction on it — see its docstring), so a transient network
        # failure must never abort venues that haven't run yet (2026-07-13 review
        # finding: an earlier version let this exception propagate out of main()).
        try:
            gt_lookup = _fetch_tardis_ground_truth(exchange)
        except Exception:
            logger.exception(
                "%s: Tardis safeguard fetch failed — proceeding WITHOUT anomaly telemetry for this venue "
                "(corrections are unaffected, they don't depend on this data)",
                venue,
            )
            gt_lookup = {}
        logger.info("%s: %d raw_symbol(s) with known availableTo (telemetry only)", venue, len(gt_lookup))

        targets = current_by_venue[venue]
        if limit is not None:
            targets = sorted(targets)[:limit]
        logger.info(
            "=== fix-expiry-canonical %s: %d current file(s) (mode=%s, workers=%d) ===",
            venue,
            len(targets),
            "APPLY" if apply else "DRY-RUN",
            workers,
        )
        total = {"files": 0, "files_changed": 0, "rows_fixed": 0, "id_changed": 0, "margin_fixed": 0}
        errors: dict[str, int] = {}
        anomaly_rows_total = 0
        anomaly_examples: list[dict[str, object]] = []
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_process_expiry_canonical_file, st, bucket, venue, blob, apply, gt_lookup): blob
                for blob in targets
            }
            for i, fut in enumerate(as_completed(futures), start=1):
                blob = futures[fut]
                try:
                    r = fut.result()
                except Exception:
                    logger.exception("FAILED %s", blob)
                    errors["exception"] = errors.get("exception", 0) + 1
                    continue
                if r.get("error"):
                    errors[str(r["error"])] = errors.get(str(r["error"]), 0) + 1
                    continue
                total["files"] += 1
                anomalies = r.get("anomalies") or []
                anomaly_rows_total += len(anomalies)
                if anomalies and len(anomaly_examples) < 20:
                    anomaly_examples.extend(anomalies[: 20 - len(anomaly_examples)])
                if r["written"]:
                    total["files_changed"] += 1
                    total["rows_fixed"] += int(r["rows_fixed"])
                    total["id_changed"] += int(r.get("id_changed", 0))
                    total["margin_fixed"] += int(r.get("margin_fixed", 0))
                if i % 500 == 0 or i == len(targets):
                    elapsed = time.time() - t0
                    logger.info(
                        "%s [%d/%d] elapsed=%.1fs (%.3fs/file) %s errors=%s anomaly_rows_seen=%d",
                        venue,
                        i,
                        len(targets),
                        elapsed,
                        elapsed / i,
                        total,
                        errors,
                        anomaly_rows_total,
                    )
        logger.info("%s DONE: %s errors=%s elapsed=%.1fs", venue, total, errors, time.time() - t0)
        if anomaly_rows_total:
            logger.warning(
                "%s: %d row(s) had a canonical/availableTo gap > %d days — still corrected (telemetry only, "
                "for post-hoc review, never blocking). Examples: %s",
                venue,
                anomaly_rows_total,
                _ANOMALY_THRESHOLD_DAYS,
                anomaly_examples,
            )


def run_fix_by_date(
    st, bucket: str, apply: bool, workers: int, limit: int | None, current_by_venue: dict[str, list[str]]
) -> None:
    for venue in _TARGET_VENUES:
        targets = current_by_venue[venue]
        if limit is not None:
            targets = sorted(targets)[:limit]
        logger.info(
            "=== fix-by-date %s: %d current file(s) (mode=%s, workers=%d) ===",
            venue,
            len(targets),
            "APPLY" if apply else "DRY-RUN",
            workers,
        )
        total = {"files": 0, "files_changed": 0, "id_changed": 0, "margin_fixed": 0, "unresolved": 0}
        errors: dict[str, int] = {}
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process_one_file, st, bucket, venue, blob, apply): blob for blob in targets}
            for i, fut in enumerate(as_completed(futures), start=1):
                blob = futures[fut]
                try:
                    r = fut.result()
                except Exception:
                    logger.exception("FAILED %s", blob)
                    errors["exception"] = errors.get("exception", 0) + 1
                    continue
                if r.get("error"):
                    errors[str(r["error"])] = errors.get(str(r["error"]), 0) + 1
                    continue
                total["files"] += 1
                total["unresolved"] += int(r["unresolved"])
                if r["written"]:
                    total["files_changed"] += 1
                    total["id_changed"] += int(r["id_changed"])
                    total["margin_fixed"] += int(r["margin_fixed"])
                if i % 500 == 0 or i == len(targets):
                    elapsed = time.time() - t0
                    logger.info(
                        "%s [%d/%d] elapsed=%.1fs (%.3fs/file) %s errors=%s",
                        venue,
                        i,
                        len(targets),
                        elapsed,
                        elapsed / i,
                        total,
                        errors,
                    )
        logger.info("%s DONE: %s errors=%s elapsed=%.1fs", venue, total, errors, time.time() - t0)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quarantine-backups", action="store_true", help="Relocate stray .bak.parquet files.")
    ap.add_argument("--fix-by-date", action="store_true", help="Re-derive instrument_key/margin_type on current files.")
    ap.add_argument(
        "--fix-expiry-canonical",
        action="store_true",
        help="Correct stored expiry to the canonical symbol-parsed value, with live Tardis "
        "availableTo as an anomaly safeguard only (see _detect_expiry_corrections). "
        "Independent of --fix-by-date.",
    )
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry-run/report only).")
    ap.add_argument("--workers", type=int, default=32, help="Thread-pool size (default 32).")
    ap.add_argument("--limit", type=int, default=None, help="Per-venue smoke-test cap for --fix-by-date.")
    args = ap.parse_args(argv)
    if not (args.quarantine_backups or args.fix_by_date or args.fix_expiry_canonical):
        logger.error("Nothing to do — pass --quarantine-backups and/or --fix-by-date and/or --fix-expiry-canonical.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    st = get_storage_client()
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("bucket=%s ts=%s", bucket, ts)

    logger.info("=== listing (one server-side match_glob pass per venue) ===")
    current_by_venue: dict[str, list[str]] = {}
    backups_all: list[str] = []
    for venue in _TARGET_VENUES:
        t0 = time.time()
        current, backups = _list_all_for_venue(bucket, venue)
        current_by_venue[venue] = current
        backups_all.extend(backups)
        logger.info(
            "%s: %d current, %d stray backup file(s) (listed in %.1fs)",
            venue,
            len(current),
            len(backups),
            time.time() - t0,
        )

    if args.quarantine_backups:
        run_quarantine(st, bucket, args.apply, args.workers, backups_all)
    if args.fix_by_date:
        run_fix_by_date(st, bucket, args.apply, args.workers, args.limit, current_by_venue)
    if args.fix_expiry_canonical:
        run_fix_expiry_canonical(st, bucket, args.apply, args.workers, args.limit, current_by_venue)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
