#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after the CeFi canonical-completeness Phase-1 manifest migration is
#              verified live in the cefi -prd _index (canonical-fraction >= projected,
#              _verify_gate green) and the drain window is closed.
"""D4 SCRIPT 3 — complete the CeFi manifest to canonical instrument_id + de-dup coexisting forms.

Provenance: plans/active/issues/_cefi_canonical_blueprint_2026_07_17.md § 3 "SCRIPT 3 —
Manifest completion + de-dup (instruments-service)". Fork of
``relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py``.

WHAT THIS FIXES (four deltas vs the fork base):

  (i)  KEY OFF THE ROLLED-UP CATALOGUE VIA THE UAC 3-TUPLE MAP. The fork built a
       2-tuple ``(venue, raw_symbol)`` map from the ``instrument_availability/by_date``
       snapshots. That 2-tuple key EXCLUDES the marquee majors — ``(BYBIT, BTCUSDT)``
       resolves to BOTH ``BYBIT:SPOT_PAIR:BTC-USDT`` and ``BYBIT:PERPETUAL:BTC-USDT@LIN``,
       so the ambiguous-exclusion left the active BYBIT/OKX/BINANCE-FUTURES perps RAW.
       This script builds the ONE shared 3-tuple map
       ``(venue, instrument_type, raw_symbol) -> instrument_id`` from the rolled-up
       ``prod/catalog.parquet`` ``instrument_id`` column, via ``CeFiWireCanonicalMap``
       (UAC SSOT; NEVER ``canonical_instrument_id`` — that column is a raw-glued trap).
       ``instrument_type`` disambiguates the spot/perp clash, recovering the majors.

  (ii) ROLLED-UP (all-lifecycle) CATALOGUE. ``prod/catalog.parquet`` carries every
       instrument that ever listed, so delisted/expired contracts the by-date snapshot
       missed now resolve too.

  (iii) NEW DE-DUP PASS. After the relabel, coexisting spellings of ONE instrument
       (``…@LIN`` / bare ``…:BASE-QUOTE`` no-marker / bare-wire / wrapped-wire) are
       normalized onto the catalogue ``instrument_id`` and collapsed by
       ``drop_duplicates`` on the PINNED shard atom
       ``[date, venue, data_type, instrument_type, instrument_id, pipeline_mode]``
       (WITH ``pipeline_mode``), keeping the best ``capture_status`` via ``_STATUS_RANK``
       (captured > empty_confirmed > attempted_failed > expected_unattempted).

  (iv) NON-CANONICAL ENUMERATION AXES (Track 6, operator rulings 2026-07-18). The
       fork+deltas (i)-(iii) canonicalise the ``instrument_id`` COLUMN of already-
       ``{venue}:``-prefixed captured rows only. The 2026-07-18 enumeration audit
       (``market-tick-data-service/scripts/audit_cefi_manifest_noncanonical_enumeration_2026_07_18.py``)
       found three further axes Scripts 1-3-as-forked do NOT touch, all of which the
       cutover ``--apply`` must align to the catalogue SSOT or "canonical" is a lie:
         * ``instrument_type`` COLUMN drift — 3.19M BLANK + lowercase/aliased
           (``perpetual``/``spot``/``spot_pair``/``future``) + ``None`` +
           data-type-leaked (``futures_chain``/``options_chain`` written as the itype).
           BLANK itype is the ROOT: ~986k bare-wire rows fail 3-tuple resolution ONLY
           because their itype key is blank. Fixing the itype UNBLOCKS them.
         * ``:PERP:`` shorthand ids (``ASTER:PERP:CLUSDT``) — decompose the wire +
           force ``PERPETUAL`` -> ``ASTER:PERPETUAL:CL-USDT@LIN``.
         * orphans — rows whose (venue, instrument) the catalogue SSOT does not know.
       These are all served by the ONE shared ``resolve_canonical`` resolver below,
       which normalises/infers the itype, extracts the wire symbol from ANY id form,
       and forward-resolves via the same ``CeFiWireCanonicalMap``. It runs over EVERY
       row (not just captured), so the de-dup + eu-reconcile keys align across the
       normalised itype.

  (v)  BASE-QUOTE SSOT MAP + narrow reconstruction (coordinator CORRECTION 2026-07-18).
       The catalogue is COMPLETE — it holds DELISTED instruments too (e.g.
       ``BINANCE-SPOT:SPOT_PAIR:SC-USDT`` with ``available_to=2024-02-21``). The ~420k
       "bare-wire" captured misses are NOT a delisting gap; they are a KEY-FORM MISMATCH:
       the manifest carries the already-DASHED base-quote (``SC-USDT``) while
       ``CeFiWireCanonicalMap`` keys on the venue-native raw wire (``scusdt``), so
       ``canonical_for(venue, itype, "SC-USDT")`` misses. ``_build_base_quote_map`` keys a
       SECOND catalogue map on the id's own ``BASE-QUOTE`` segment (undated
       PERPETUAL/SPOT_PAIR only — DATED FUTURE/OPTION share a base-quote across expiries →
       excluded), so the dashed manifest value resolves to the EXACT catalogue id (correct
       ``@marker``, ZERO fabrication), ``instrument_type`` disambiguating the spot/perp
       clash. Reconstruction is confined to the two delimiter forms the catalogue genuinely
       does not key — Kraken slash (``XBT/USDT``→``BTC-USDT``, alias XBT→BTC) and underscore
       (``ETH_USDT``→``ETH-USDT``) — and ONLY after the SSOT map misses; it is gated on the
       canonical regex.

       MEASURED REALITY (dry-run 2026-07-18): the base-quote map recovers only ~2.6k rows,
       NOT the ~420k the "clean dashed" model expected — the unresolved-captured population is
       dominated instead by DATED contracts (~115k rows / ~41B ticks) — see (vi).

  (vi) DATED-WIRE itype-fix (operator Option A, 2026-07-18 — the 41B-tick lever). A dated
       contract is a FUTURE/OPTION, NEVER a PERPETUAL; the manifest's itype column is often
       mis-set to PERPETUAL (or blank) on a dated wire (``OKX-FUTURES``/``LTC-USD-210625``),
       so the 3-tuple wire-map — which DOES already key the venue-native dated ``raw_symbol``
       — misses. ``_resolve_itype`` now detects a GENUINE date tail (numeric ``[-_]YY[YY]MMDD``,
       DERIBIT text date ``-5APR19``, CME letter-month ``…USDH25``, option strike ``…-3250-C``)
       and overrides PERPETUAL/blank → FUTURE/OPTION, which UNBLOCKS the existing wire-map
       (measured: ~115k of ~118k dated rows / ~40.7B ticks resolve this way — the wire-map,
       not a new map, does the resolution). ``_build_base_quote_date_map`` is a fallback for a
       dated wire the exact-raw-symbol keying missed (keyed on
       ``(venue, itype, BASE-QUOTE, YYYYMMDD, strike, cp)`` → exact catalogue id). The residual
       (BITGET CME ``ETHUSDH25`` with no derivable day) gets its itype corrected but stays
       honest-raw. Also: MATIC→POL rebrand alias on the base; null-id BUNDLE shards + genuine
       bare underlyings stay KEPT.

BUNDLE shards (operator CORRECTION 2026-07-18) — ``data_type`` ∈ {futures_chain,
options_chain} are keyed on ``underlying`` and the per-row ``instrument_id`` MAY be NULL by
design. This script does NOT synthesise a bundle id; bundle rows are KEPT untouched (the
full per-contract canonical ids live INSIDE the bundle parquet, not the manifest shard key).

EU-RECONCILE (post-apply-gate fix 2026-07-18) — drops an ``expected_unattempted`` skeleton
whose 5-col shard key (date, venue, data_type, instrument_type, instrument_id) collides with
ANY captured row (captured wins; eu carries no data). Two bugs the first ``--apply`` hit —
now fixed: (1) it reconciled only eu twins of RELABELED captured rows, missing eu twins of
ALREADY-CANONICAL captured rows (and dropping nothing on an idempotent re-apply); it now uses
the FULL post-relabel captured-key set. (2) it ran on the main index only, missing cross-blob
eu/captured collisions; it now runs on every loaded blob. The 5-col key excludes
``pipeline_mode`` so a ``batch_tardis`` eu reconciles against a canonical captured twin from
another lane (measured: 100% of the residual collisions were cross-pipeline_mode, which is
why the 6-col de-dup could not catch them). Re-apply is safe + idempotent (relabel/de-dup
no-op on canonical rows; the eu-reconcile still cleans the collisions; the candidate/:PERP:
VOLUME STOP bands are skipped when the BEFORE fraction proves the index is already canonical).

KEEP-trend policy (operator CORRECTIONS 2026-07-18) — this script now DROPS almost nothing.
``KALSHI-PERP`` / ``POLYMARKET-PERP`` are KEPT (roadmap placeholder venues); blank-id rows
are KEPT (roadmap + null-id bundle shards); chain BUNDLE rows are KEPT; blank venue/data_type
are KEPT. Non-cull drops are a genuine catalogue-orphan (a resolvable-shaped bare id whose
(venue, wire) the catalogue SSOT does not know), non-captured only. ``COMBO`` is CANONICAL.

DROP-VENUE CULL (operator-CONFIRMED 2026-07-18) — every row for ``_CULL_VENUES``
(BINANCE-DELIVERY, BITSTAMP, HUOBI, GEMINI, PHEMEX, DRIFT, PACIFICA, MANGO, ZETA, FLASH,
SOLAYER, PICASSO, CAMBRIAN) is DROPPED, INCLUDING captured-with-data — the ONE operator-
authorized exception to the captured-data-safe invariant (snapshot-first ⇒ reversible). The
report prints the per-venue cull rows / captured-with-data / ticks so the operator sees the
impact (esp. BINANCE-DELIVERY's COIN-M). KALSHI-PERP/POLYMARKET-PERP/LIGHTER-ZKSYNC/
EXTENDED-STARKNET are explicitly NOT culled.

Definitive venue-suffix itype (2026-07-18): a ``-SPOT`` venue is ONLY SPOT_PAIR and a ``-SWAP``
venue ONLY PERPETUAL, so the suffix CORRECTS a mis-set column itype (fixed BYBIT-SPOT rows
carrying a stray PERPETUAL that made the wire-map miss).

  DATA-CORRECTNESS CARVE-OUT (HARD RULE, this script): a row that would DROP is PROTECTED
  (kept honest-unresolved, itype-normalised) when it is ``captured`` with ``row_count > 0``
  — dropping such a row destroys real tick data. Measured 2026-07-18: the naive drop set
  held 12,825 captured rows carrying ~7.27B tick rows (bare underlyings ``DERIBIT:ETH``,
  Kraken slash-wires ``XBT/USDT``, BITGET letter-month futures ``BTCUSDH``) — the
  "missing-quote / nc:other" class a DEDICATED decompose script (Track 6 P1) must recover,
  NOT this one. Only non-captured (or captured-empty) bookkeeping orphans actually drop.
  ``_verify_gate`` asserts ZERO captured-with-data rows were dropped.

Scope: ``asset_group=cefi`` (the whole cefi tick manifest). Inputs: the main
``_index/availability_index.parquet`` + every ``_index/per_vm/*.parquet`` shard.

SNAPSHOT-FIRST: every blob this script rewrites gets a pre-write copy under
``_index/snapshots/pre_d4_<ts>/`` before any write. Default mode is DRY-RUN (read-only);
``--apply`` mutates and is GATED by the Phase -1 catalogue verify gate (0 ``:PERP:`` ids,
0 ``instrument_id != canonical_instrument_id`` in the cefi catalogue). The dry-run reads
only the classification/de-dup columns (memory-bounded); ``--apply`` reads the full schema
to rewrite it.

STOP-ON-SURPRISE bands (halt + diagnose before ``--apply``):
  * raw-captured candidate population in ``[_CANDIDATE_MIN, _CANDIDATE_MAX]``.
  * ``:PERP:`` rewrites in ``[_PERP_MIN, _PERP_MAX]`` (a crash to ~0 = the perp venues
    fell out of the catalogue).
  * total dropped rows <= ``_MAX_TOTAL_DROPPED``.
  * ZERO captured-with-data rows in the drop set (data-loss invariant).

Usage::

    cd instruments-service

    # dry-run (default, read-only) — prints all migration counts
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/complete_cefi_manifest_canonical_dedup_2026_07_17.py

    # apply (snapshot + relabel + itype/:PERP:/orphan align + de-dup + eu-reconcile +
    # verify gate) — DRAIN-GATED
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/complete_cefi_manifest_canonical_dedup_2026_07_17.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from datetime import UTC, datetime
from typing import NamedTuple

import pandas as pd
import pyarrow.parquet as pq
from unified_api_contracts import CeFiWireCanonicalMap
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
CATALOG_BLOB = "prod/catalog.parquet"

# PINNED shard atom (WITH pipeline_mode) — the SSOT de-dup key (blueprint § 1 Phase 0a).
PIN_ATOM = ["date", "venue", "data_type", "instrument_type", "instrument_id", "pipeline_mode"]
# 5-col shard key for the RETAINED fork eu-reconcile pass. Excludes pipeline_mode by
# design so an eu skeleton written by one lane reconciles against the canonical captured
# twin written by another (native on-chain vs batch_tardis).
SHARD_KEY_COLS = ["date", "venue", "data_type", "instrument_type", "instrument_id"]

# The minimal column set the dry-run needs (classification + de-dup + eu-reconcile +
# the captured-data drop-gate). ``--apply`` reads the FULL schema to rewrite it. The load
# projects to the INTERSECTION with the blob's real schema: the per-VM ``_legacy_seed``
# shard physically lacks ``pipeline_mode``/``row_count`` (they are backfilled on read by
# ``read_availability_index``, not stored), so ``_ensure_cols`` re-materialises them.
_DRYRUN_COLS = [
    "date",
    "venue",
    "data_type",
    "instrument_type",
    "instrument_id",
    "underlying",
    "pipeline_mode",
    "capture_status",
    "row_count",
    "instrument_count",
]

# Best-status wins on a shard-atom collision.
_STATUS_RANK = {"captured": 0, "empty_confirmed": 1, "attempted_failed": 2, "expected_unattempted": 3}

# STOP-ON-SURPRISE: raw-captured candidate band across ALL blobs. Measured live
# 2026-07-17 against the rebuilt -prd manifest: main index = 490,490 (matches the
# blueprint's ~490k main-index upper bound) + the `_legacy_seed` per-VM shard = 67,560
# => 558,050 total. Bound generously around the total; halt outside.
_CANDIDATE_MIN = 400_000
_CANDIDATE_MAX = 700_000

# STOP-ON-SURPRISE: :PERP: rewrite band. Measured 2026-07-18 = 374,227 (matches the
# enumeration audit's 374,272 :PERP: rows). A crash toward 0 means the on-chain perp
# venues (ASTER/LIGHTER/EXTENDED/HYPERLIQUID) fell out of the catalogue.
_PERP_MIN = 250_000
_PERP_MAX = 500_000

# STOP-ON-SURPRISE: an upper bound on total dropped rows (blank/orphan/okx/drop-venue,
# captured-with-data EXCLUDED). Measured 2026-07-18 = ~231k. A blow-past means a
# catalogue regression made a huge population look like orphans.
_MAX_TOTAL_DROPPED = 400_000

# STOP-ON-SURPRISE: an upper bound on the drop-venue cull row count (13 venues, INCLUDING
# captured-with-data). A blow-past means a wrong venue slipped into ``_CULL_VENUES``.
_MAX_CULL_DROPPED = 800_000

# IDEMPOTENT RE-RUN: when the BEFORE captured-venue-prefixed fraction is already this high, the
# index is already canonicalised (a re-apply for the eu-reconcile cleanup), so the candidate /
# :PERP: VOLUME bands are EXPECTED to read ~0 and are skipped. All SAFETY invariants
# (captured-with-data-drop, total-dropped cap, cull cap) stay enforced. First apply saw ~83%.
_APPLIED_FRAC_THRESHOLD = 90.0

# ---------------------------------------------------------------------------
# Track-6 canonical-form constants (operator rulings 2026-07-18).
# ---------------------------------------------------------------------------

# Canonical instrument_id shape (mirrors the enumeration audit's _CANON_RE) + a COMBO arm
# (operator ruling #4: COMBO is canonical). VENUE:ITYPE:BASE-QUOTE[@LIN|@INV][-YYYYMMDD][-STRIKE-C|P].
_CANON_ID_RE = re.compile(
    r"^[A-Z0-9._-]+:(PERPETUAL|FUTURE|OPTION|SPOT_PAIR):[A-Z0-9]+-[A-Z0-9]+"
    r"(@(LIN|INV))?(-\d{8})?(-\d+(\.\d+)?-[CP])?$"
)
_COMBO_ID_RE = re.compile(r"^[A-Z0-9._-]+:COMBO:.+$")
# A trailing YYMMDD (``_210326``/``-210326``) or YYYYMMDD (``-20260401``) date = a dated future.
_DATED_RE = re.compile(r"(?:[-_]\d{6}|-\d{8})$")

# --- DATED-WIRE itype-fix (operator Option A, 2026-07-18) — a dated contract is a
# FUTURE/OPTION, NEVER a PERPETUAL; the itype column is often mis-set to PERPETUAL on a
# dated wire, so the 3-tuple wire-map (which DOES key the venue-native dated raw_symbol)
# misses. Fixing the itype unblocks it. Detect a GENUINE date tail (not any trailing digits):
_OPT_TAIL_RE = re.compile(r"-\d+(?:\.\d+)?-[CP]$")  # option strike tail (…-3250-C)
# text date DDMONYY (DERIBIT: BTC-5APR19-…):
_TXT_DATE_RE = re.compile(r"-\d{1,2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}")
# CME letter-month (BITGET: ETHUSDH25 = ETH-USD, H=Mar, 25) — quote + month-letter + year digit:
_CME_TAIL_RE = re.compile(r"(?:USD|USDT|USDC)[FGHJKMNQUVXZ]\d{1,2}$")
_MONTHS: dict[str, str] = {
    "JAN": "01",
    "FEB": "02",
    "MAR": "03",
    "APR": "04",
    "MAY": "05",
    "JUN": "06",
    "JUL": "07",
    "AUG": "08",
    "SEP": "09",
    "OCT": "10",
    "NOV": "11",
    "DEC": "12",
}
# Rebrand aliases on the BASE token — a delisted ticker → the current catalogue ticker
# (Polygon MATIC→POL 2024): the manifest carries `MATIC-BTC`, the catalogue holds `POL-BTC`.
_REBRAND_ALIASES: dict[str, str] = {"MATIC": "POL"}
# Catalogue dated-id parse (the BASE-QUOTE:ITYPE-stripped remainder): BASE-QUOTE[@LIN|@INV]-YYYYMMDD[-STRIKE-C|P].
_DATED_ID_RE = re.compile(
    r"^(?P<bq>[A-Z0-9]+-[A-Z0-9]+)(?:@(?:LIN|INV))?-(?P<date>\d{8})(?:-(?P<strike>\d+(?:\.\d+)?)-(?P<cp>[CP]))?$"
)
# Manifest dated-wire parse: BASE-QUOTE-YY[YY]MMDD[-STRIKE-C|P].
_WIRE_DATED_RE = re.compile(
    r"^(?P<bq>[A-Z0-9]+-[A-Z0-9]+)-(?P<date>\d{6}|\d{8})(?:-(?P<strike>\d+(?:\.\d+)?)-(?P<cp>[CP]))?$"
)

# itype casing + alias + data-type-leak normalization (operator ruling #1). The keys are
# already ``.strip().upper()``-ed at call time.
_ITYPE_ALIASES: dict[str, str] = {
    "SWAP": "PERPETUAL",
    "SPOT": "SPOT_PAIR",
    "FUTURES": "FUTURE",
    "OPTIONS": "OPTION",
    "PERP": "PERPETUAL",
    # data_type leaked into the instrument_type column:
    "FUTURES_CHAIN": "FUTURE",
    "OPTIONS_CHAIN": "OPTION",
}
_KEEP_ITYPES = frozenset({"PERPETUAL", "FUTURE", "OPTION", "SPOT_PAIR", "COMBO"})

# Bare (un-suffixed) venues that are derivatives-only — a LAST-RESORT itype-infer
# fallback used ONLY when the catalogue 2-tuple map has no unique itype for the wire.
# BYBIT/OKX are DELIBERATELY absent (mixed spot+deriv → cannot infer from the venue).
_DERIV_VENUES = frozenset({"DERIBIT", "BINANCE-DELIVERY", "BITMEX"})

# DROP-VENUE CULL — operator CONFIRMED 2026-07-18 ("yeah cull drop venue"). Every manifest row
# for these venues is DROPPED, INCLUDING captured-with-data: this is the ONE operator-authorized
# exception to the captured-data-safe invariant (snapshot-first, so it is reversible). NOT in the
# cull (KEPT): KALSHI-PERP, POLYMARKET-PERP, LIGHTER-ZKSYNC, EXTENDED-STARKNET.
_CULL_VENUES: frozenset[str] = frozenset(
    {
        "BINANCE-DELIVERY",
        "BITSTAMP",
        "HUOBI",
        "GEMINI",
        "PHEMEX",
        "DRIFT",
        "PACIFICA",
        "MANGO",
        "ZETA",
        "FLASH",
        "SOLAYER",
        "PICASSO",
        "CAMBRIAN",
    }
)

# bare-``OKX`` remap candidates (operator ruling #2: try each, keep the one that resolves).
_OKX_REMAP = ("OKX-SWAP", "OKX-SPOT", "OKX-FUTURES")

# ---------------------------------------------------------------------------
# Base-quote SSOT map + narrow reconstruction (coordinator CORRECTION 2026-07-18).
# The catalogue is COMPLETE (holds delisted instruments); the ~420k bare-wire misses are
# a KEY-FORM mismatch — the manifest carries the already-DASHED base-quote (``SC-USDT``)
# while ``CeFiWireCanonicalMap`` keys on the venue-native raw wire (``scusdt``). Resolve
# the dashed value against a SECOND catalogue map keyed on the id's BASE-QUOTE segment
# (undated PERPETUAL/SPOT_PAIR only). Reconstruction is confined to the two delimiter
# forms the catalogue genuinely does not key: Kraken slash + underscore.
# ---------------------------------------------------------------------------

# Trailing date tail on a construction candidate: YYYYMMDD or YYMMDD, either separator.
_DATE_TAIL_RE = re.compile(r"[-_](\d{8}|\d{6})$")

# Reconstruction margin marker from the quote (linear stables → LIN, USD inverse → INV).
_QUOTE_MARGIN: dict[str, str] = {
    "USDT": "LIN",
    "USDC": "LIN",
    "BUSD": "LIN",
    "DAI": "LIN",
    "FDUSD": "LIN",
    "TUSD": "LIN",
    "USD": "INV",
}
# Venue-native base aliases → canonical (Kraken puts XBT/XDG on the tape).
_BASE_ALIASES: dict[str, str] = {"XBT": "BTC", "XDG": "DOGE"}
# BUNDLE data_types — shards keyed on ``underlying``; the per-row ``instrument_id`` MAY be
# NULL by design (operator CORRECTION 2026-07-18: do NOT synthesise a bundle id). Kept as-is.
_BUNDLE_DATA_TYPES: frozenset[str] = frozenset({"FUTURES_CHAIN", "OPTIONS_CHAIN"})

_KEY_SEP = "\x1f"

# Majors that the 2-tuple 2026-07-16 relabel left RAW — the dry-run confirms these now
# resolve through the 3-tuple map (review blocking-risk #1).
_MAJORS_CHECK: list[tuple[str, str, str]] = [
    ("BYBIT", "SPOT_PAIR", "BTCUSDT"),
    ("BYBIT", "PERPETUAL", "BTCUSDT"),
    ("BYBIT", "SPOT_PAIR", "ETHUSDT"),
    ("BYBIT", "PERPETUAL", "ETHUSDT"),
    ("BINANCE-FUTURES", "PERPETUAL", "BTCUSDT"),
    ("BINANCE-FUTURES", "PERPETUAL", "ETHUSDT"),
]


class _Res(NamedTuple):
    """The per-(venue, itype, id, data_type)-tuple resolution decision.

    intent: ``relabel`` (id/itype/venue rewritten to ``iid``/``itype``/``venue``),
        ``keep`` (in-catalogue but unresolved — id kept, itype normalised), or
        ``drop`` (orphan/blank/okx/drop-venue — subject to the captured-data carve-out).
    via: a tag for the summary counters.
    dclass: the drop class (``blank``/``orphan``/``okx``/``drop_venue``), else "".
    """

    intent: str
    venue: str
    iid: str
    itype: str
    via: str
    dclass: str


def _load_catalog(storage: object, inst_bucket: str) -> pd.DataFrame:
    """Download the rolled-up cefi ``prod/catalog.parquet`` (all-lifecycle)."""
    raw = storage.download_bytes(inst_bucket, CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(
        io.BytesIO(raw),
        columns=["venue", "instrument_type", "raw_symbol", "instrument_id", "canonical_instrument_id"],
    )


def _build_wire_map(cat: pd.DataFrame) -> CeFiWireCanonicalMap:
    """Build the UAC 3-tuple wire->canonical map off the catalogue ``instrument_id`` column."""
    cols = cat[["venue", "instrument_type", "raw_symbol", "instrument_id"]].fillna("").astype(str)
    rows = zip(cols["venue"], cols["instrument_type"], cols["raw_symbol"], cols["instrument_id"], strict=True)
    return CeFiWireCanonicalMap.from_rows(rows)


def _build_marker_base_map(cat: pd.DataFrame) -> dict[str, str]:
    """Map ``strip_marker(instrument_id) -> instrument_id`` (catalogue), excluding collisions.

    Normalizes a legacy no-marker spelling (``BYBIT:PERPETUAL:BTC-USDT``) onto the
    catalogue's marker-bearing canonical id (``BYBIT:PERPETUAL:BTC-USDT@LIN``). Two
    catalogue ids stripping to the same base (e.g. ``@LIN`` and ``@INV``) are ambiguous
    and excluded (never guessed).
    """
    base_to_id: dict[str, str] = {}
    conflict: set[str] = set()
    for cid in cat["instrument_id"].fillna("").astype(str).unique():
        if not cid:
            continue
        base = cid.split("@", 1)[0]
        if base in conflict:
            continue
        if base in base_to_id and base_to_id[base] != cid:
            conflict.add(base)
            del base_to_id[base]
            continue
        base_to_id[base] = cid
    return base_to_id


def _build_itype_infer_map(cat: pd.DataFrame) -> dict[tuple[str, str], str]:
    """Map ``(venue, raw_symbol) -> instrument_type`` when the catalogue is UNAMBIGUOUS.

    The blank-itype inference primary path (operator ruling #1): a bare-wire manifest row
    whose itype key is blank recovers its type from the catalogue's OWN (venue, wire) ->
    type mapping. A wire that maps to >1 type (a spot/perp clash) is EXCLUDED (the caller
    falls back to venue-suffix inference), never guessed. Keys are ``.upper()``-ed to
    match ``CeFiWireCanonicalMap``'s case-insensitive lookup.
    """
    cols = cat[["venue", "raw_symbol", "instrument_type"]].fillna("").astype(str)
    by_wire: dict[tuple[str, str], set[str]] = {}
    for venue, raw, itype in zip(cols["venue"], cols["raw_symbol"], cols["instrument_type"], strict=True):
        vv, rr, ii = venue.strip().upper(), raw.strip().upper(), itype.strip().upper()
        if vv and rr and ii:
            by_wire.setdefault((vv, rr), set()).add(ii)
    return {k: next(iter(s)) for k, s in by_wire.items() if len(s) == 1}


def _build_base_quote_map(cat: pd.DataFrame) -> dict[tuple[str, str], str]:
    """Map ``(venue, instrument_type, BASE-QUOTE) -> instrument_id`` for undated perps/spots.

    The SSOT fix for the ~420k bare-wire misses (coordinator CORRECTION 2026-07-18): the
    manifest carries the already-DASHED base-quote (``SC-USDT``) while the wire-map keys on
    the venue-native raw wire (``scusdt``), so ``canonical_for`` misses even though the
    instrument (incl. delisted) IS in the catalogue. This map keys on the id's own
    ``BASE-QUOTE`` piece — ``instrument_id`` with ``VENUE:ITYPE:`` stripped and everything
    left of ``@`` — so a dashed manifest value resolves to the EXACT catalogue id (correct
    ``@marker``, no fabrication). Only PERPETUAL/SPOT_PAIR (inherently UNDATED, so BASE-QUOTE
    is unique); DATED FUTURE/OPTION share a base-quote across expiries → excluded (the
    ambiguous keys are dropped too, never guessed). ``instrument_type`` disambiguates the
    spot/perp clash (``XMR-USDT`` is SPOT_PAIR on BINANCE-SPOT, PERPETUAL@LIN on -FUTURES).
    """
    cols = cat[["venue", "instrument_type", "instrument_id"]].fillna("").astype(str)
    by_bq: dict[tuple[str, str, str], set[str]] = {}
    for venue, itype, iid in zip(cols["venue"], cols["instrument_type"], cols["instrument_id"], strict=True):
        vu, itu, idu = venue.strip().upper(), itype.strip().upper(), iid.strip()
        if itu not in {"PERPETUAL", "SPOT_PAIR"}:
            continue
        parts = idu.split(":", 2)
        if len(parts) < 3:
            continue
        bq = parts[2].split("@", 1)[0].strip().upper()
        if not bq or "-" not in bq:
            continue
        by_bq.setdefault((vu, itu, bq), set()).add(idu)
    return {k: next(iter(s)) for k, s in by_bq.items() if len(s) == 1}


def _build_base_quote_date_map(cat: pd.DataFrame) -> dict[tuple[str, str, str, str, str, str], str]:
    """``(venue, itype, BASE-QUOTE, YYYYMMDD, strike, cp) -> id`` for DATED FUTURE/OPTION.

    The fallback for a dated wire the exact-raw-symbol wire-map missed (coordinator Option A
    2026-07-18). Most dated rows resolve via the itype-fix + the existing wire-map (which DOES
    key the venue-native dated raw_symbol); this map catches a residual whose wire format
    differs but whose BASE-QUOTE + full date + strike/cp match the catalogue EXACTLY (never a
    fabrication; ambiguous keys dropped).
    """
    cols = cat[["venue", "instrument_type", "instrument_id"]].fillna("").astype(str)
    by_key: dict[tuple[str, str, str, str, str, str], set[str]] = {}
    for venue, itype, iid in zip(cols["venue"], cols["instrument_type"], cols["instrument_id"], strict=True):
        vu, itu, idu = venue.strip().upper(), itype.strip().upper(), iid.strip()
        if itu not in {"FUTURE", "OPTION"}:
            continue
        parts = idu.split(":", 2)
        if len(parts) < 3:
            continue
        m = _DATED_ID_RE.match(parts[2].strip().upper())
        if not m:
            continue
        key = (vu, itu, m.group("bq"), m.group("date"), m.group("strike") or "", m.group("cp") or "")
        by_key.setdefault(key, set()).add(idu)
    return {k: next(iter(s)) for k, s in by_key.items() if len(s) == 1}


def _parse_dated_wire(raw_symbol: str) -> tuple[str, str, str, str] | None:
    """Parse a manifest dated wire ``BASE-QUOTE-YY[YY]MMDD[-STRIKE-C|P]`` → key parts, or ``None``.

    Returns ``(BASE-QUOTE, YYYYMMDD, strike, cp)`` with the base rebrand-aliased (MATIC→POL)
    and the date normalised to YYYYMMDD. Only the numeric ``base-quote-date`` form parses
    (DERIBIT text-date + BITGET CME wires resolve via the wire-map or stay honest-raw).
    """
    m = _WIRE_DATED_RE.match(raw_symbol.strip().upper())
    if not m:
        return None
    d = m.group("date")
    yyyy, mo, dd = ("20" + d[:2], d[2:4], d[4:6]) if len(d) == 6 else (d[:4], d[4:6], d[6:8])
    if not (yyyy[:2] == "20" and "01" <= mo <= "12" and "01" <= dd <= "31"):
        return None
    base, _, quote = m.group("bq").partition("-")
    base = _REBRAND_ALIASES.get(base, base)
    return f"{base}-{quote}", yyyy + mo + dd, m.group("strike") or "", m.group("cp") or ""


def _catalog_id_set(cat: pd.DataFrame) -> set[str]:
    """Upper-cased set of every catalogue ``instrument_id`` — the 'in the catalogue' test."""
    return {s.strip().upper() for s in cat["instrument_id"].fillna("").astype(str) if s.strip()}


def _catalog_wire_set(cat: pd.DataFrame) -> set[tuple[str, str]]:
    """Upper-cased ``(venue, raw_symbol)`` set — the wire-level 'in the catalogue' test."""
    cols = cat[["venue", "raw_symbol"]].fillna("").astype(str)
    return {
        (venue.strip().upper(), raw.strip().upper())
        for venue, raw in zip(cols["venue"], cols["raw_symbol"], strict=True)
        if venue.strip() and raw.strip()
    }


def _catalogue_gate(cat: pd.DataFrame) -> tuple[bool, int, int]:
    """Phase -1 verify gate: 0 ``:PERP:`` ids, 0 ``instrument_id != canonical_instrument_id``."""
    iid = cat["instrument_id"].fillna("").astype(str)
    cid = cat["canonical_instrument_id"].fillna("").astype(str)
    n_perp = int(iid.str.contains(":PERP:", regex=False).sum())
    n_mismatch = int((iid != cid).sum())
    return (n_perp == 0 and n_mismatch == 0), n_perp, n_mismatch


# ---------------------------------------------------------------------------
# The SHARED RESOLVER (Track-6 core). Pure, catalogue-keyed, honest-None.
# ---------------------------------------------------------------------------


def _is_canonical_id(cur: str) -> bool:
    """True when ``cur`` already matches the canonical id shape (incl. COMBO)."""
    return bool(_CANON_ID_RE.match(cur) or _COMBO_ID_RE.match(cur))


def _itype_of(canon: str) -> str:
    """Parse the instrument_type segment out of a canonical id (``VENUE:ITYPE:...``)."""
    parts = canon.split(":", 2)
    return parts[1] if len(parts) >= 2 else ""


def _normalize_itype(raw_itype: str) -> str:
    """Casing + alias normalisation ONLY (no inference). Blank stays blank (honest)."""
    it = raw_itype.strip().upper()
    return _ITYPE_ALIASES.get(it, it)


def _extract_raw(cur: str) -> tuple[str, str, str | None]:
    """Classify an id form + extract its wire symbol (resolver step 2).

    Returns ``(kind, raw_symbol, forced_itype)`` where ``kind`` is one of
    ``canonical`` (already canonical — return as-is), ``perp`` (``VENUE:PERP:SYM`` — raw is
    ``SYM``, itype forced ``PERPETUAL``), ``wrapped`` (``VENUE:ITYPE:TAIL`` — raw is
    ``TAIL``), or ``bare`` (the id IS the wire symbol).
    """
    if _is_canonical_id(cur):
        return "canonical", cur, None
    if ":PERP:" in cur:
        return "perp", cur.split(":PERP:", 1)[1], "PERPETUAL"
    if cur.count(":") >= 2:
        return "wrapped", cur.split(":", 2)[2], None
    return "bare", cur, None


def _wire_date_yyyymmdd(sym: str) -> str | None:
    """Normalise a trailing ``[-_]YYMMDD``/``[-_]YYYYMMDD`` date to ``YYYYMMDD``, or ``None``.

    Conservative — validates MM ∈ 01-12, DD ∈ 01-31, and a 20xx century (so a random
    trailing 6/8-digit run is NOT mistaken for a date).
    """
    m = re.search(r"[-_](\d{6}|\d{8})$", sym)
    if not m:
        return None
    d = m.group(1)
    yyyy, mo, dd = ("20" + d[:2], d[2:4], d[4:6]) if len(d) == 6 else (d[:4], d[4:6], d[6:8])
    if yyyy[:2] == "20" and "01" <= mo <= "12" and "01" <= dd <= "31":
        return yyyy + mo + dd
    return None


def _dated_itype(raw_symbol: str) -> str | None:
    """A GENUINE date tail on the wire ⇒ ``OPTION`` (strike) / ``FUTURE`` (dated), else ``None``.

    Covers a numeric ``[-_]YY[YY]MMDD`` tail, a DERIBIT text date (``-5APR19``), and a CME
    letter-month (``…USDH25``). A strike ``…-3250-C`` ⇒ OPTION.
    """
    s = raw_symbol.strip().upper()
    if _OPT_TAIL_RE.search(s):
        return "OPTION"
    if _wire_date_yyyymmdd(s) or _TXT_DATE_RE.search(s) or _CME_TAIL_RE.search(s):
        return "FUTURE"
    return None


def _resolve_itype(
    venue: str,
    raw_itype: str,
    raw_symbol: str,
    data_type: str,
    itype_infer: dict[tuple[str, str], str],
) -> str | None:
    """Resolver steps 1 + 1b — normalise the itype, INFER when blank/unknown.

    Step 1: casing/alias/data-type-leak normalise; keep a canonical member as-is. DATED-WIRE
    override (operator Option A 2026-07-18): a dated wire whose itype column is PERPETUAL or
    blank/unknown is corrected to FUTURE/OPTION (a dated contract is never a perpetual) — this
    unblocks the wire-map, which DOES key the venue-native dated raw_symbol.
    Step 1b (operator ruling #1, MOST AGGRESSIVE): a blank/``None``/``INDEX``/unknown itype
    is inferred from (a) the catalogue 2-tuple ``(venue, wire) -> type`` map, else (b) the
    row's own chain-bundle ``data_type`` (``futures_chain`` -> ``FUTURE`` / ``options_chain``
    -> ``OPTION``), else (c) the venue suffix (``-SPOT`` -> ``SPOT_PAIR``;
    ``-FUTURES``/``-SWAP``/``-PERP`` or a bare derivatives venue -> ``FUTURE`` when the wire
    is dated else ``PERPETUAL``). A bare mixed venue (bare ``BYBIT``/``OKX``) returns ``None``.
    """
    it = _ITYPE_ALIASES.get(raw_itype.strip().upper(), raw_itype.strip().upper())
    vu = venue.strip().upper()
    # DEFINITIVE venue-suffix override (2026-07-18): a ``-SPOT`` venue trades ONLY spot and a
    # ``-SWAP`` venue ONLY perpetuals, so the suffix is authoritative — it CORRECTS a mis-set
    # column itype (measured: BYBIT-SPOT rows carrying a stray ``PERPETUAL`` itype that made the
    # wire-map miss). ``-FUTURES`` is NOT definitive (it mixes FUTURE + PERPETUAL) so it stays a
    # last-resort inference below.
    if vu.endswith("-SPOT"):
        return "SPOT_PAIR"
    if vu.endswith("-SWAP"):
        return "PERPETUAL"
    dated = _dated_itype(raw_symbol)
    if dated and (it == "PERPETUAL" or it not in _KEEP_ITYPES):
        return dated
    if it in _KEEP_ITYPES:
        return it
    inferred = itype_infer.get((vu, raw_symbol.strip().upper()))
    if inferred:
        return inferred
    dtu = data_type.strip().upper()
    if dtu == "FUTURES_CHAIN":
        return "FUTURE"
    if dtu == "OPTIONS_CHAIN":
        return "OPTION"
    # ``-SPOT``/``-SWAP`` already returned above (definitive). ``-FUTURES``/``-PERP`` + bare
    # derivatives venues are a last-resort mixed inference (FUTURE when dated, else PERPETUAL).
    if vu.endswith(("-FUTURES", "-PERP")) or vu in _DERIV_VENUES:
        return "FUTURE" if _DATED_RE.search(raw_symbol) else "PERPETUAL"
    return None


def _to_dashed_base_quote(raw_symbol: str) -> tuple[str, str] | None:
    """Normalise an UNDATED wire symbol to ``(BASE-QUOTE, delimiter_kind)`` or ``None``.

    ``delimiter_kind`` ∈ {``kraken_slash`` (``XBT/USDT``), ``underscore`` (``ETH_USDT``),
    ``dashed`` (``SC-USDT``)}. Kraken aliases (XBT→BTC, XDG→DOGE) are applied to both legs.
    A DATED wire (``ETHUSDT_210326``) returns ``None`` — dated contracts are the separate
    missing-quote/dated path, NOT the undated base-quote SSOT map. An undashed wire
    (``scusdt``) returns ``None`` too (that is the raw-wire-map's territory).
    """
    r = raw_symbol.strip().upper()
    if not r or _DATE_TAIL_RE.search(r):
        return None
    for sep, kind in (("/", "kraken_slash"), ("_", "underscore"), ("-", "dashed")):
        if sep in r:
            parts = r.split(sep)
            if len(parts) == 2 and parts[0] and parts[1]:
                base = _REBRAND_ALIASES.get(parts[0], _BASE_ALIASES.get(parts[0], parts[0]))
                quote = _BASE_ALIASES.get(parts[1], parts[1])
                return f"{base}-{quote}", kind
            return None
    return None


def _reconstruct(venue: str, itype: str, base_quote: str) -> str | None:
    """Direct-construct ``VENUE:ITYPE:BASE-QUOTE[@marker]`` — ONLY for the Kraken/underscore
    delimiter forms the catalogue does not key. Gated on the canonical regex (never fabricates).
    """
    parts = base_quote.split("-")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    v = venue.strip().upper()
    if itype == "SPOT_PAIR":
        constructed = f"{v}:SPOT_PAIR:{base_quote}"
    elif itype in {"PERPETUAL", "FUTURE"}:
        marker = _QUOTE_MARGIN.get(parts[1])
        if not marker:
            return None
        constructed = f"{v}:{itype}:{base_quote}@{marker}"
    else:
        return None
    return constructed if _CANON_ID_RE.match(constructed) else None


def _resolve_full(
    venue: str,
    raw_itype: str,
    id_or_symbol: str,
    data_type: str,
    underlying: str,
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
    itype_infer: dict[tuple[str, str], str],
    base_quote_map: dict[tuple[str, str], str],
    base_quote_date_map: dict[tuple[str, str, str, str, str, str], str],
) -> tuple[str | None, str]:
    """Resolve to ``(canonical_id, source_tag)`` — the source-aware core of ``resolve_canonical``.

    Order (first hit wins; every catalogue path precedes any construction, so the SSOT is
    NEVER overridden):
      1. already-canonical (returned as-is).
      2. raw wire-map (``CeFiWireCanonicalMap`` on the venue-native wire, e.g. ``scusdt`` /
         the dated ``ltc-usd-210625`` once the itype-fix corrects a mis-set PERPETUAL).
      3. base-quote-WITH-DATE map (a dated FUTURE/OPTION the exact-wire-map keying missed).
      4. BASE-QUOTE SSOT map (the undated dashed manifest value → the exact catalogue id).
      5. marker-base / wrapped-wire peel.
      6. reconstruction — Kraken slash + underscore ONLY, after the SSOT map missed.
    Chain BUNDLE rows are NOT resolved here (no id is synthesised — null id is valid);
    the caller KEEPS them. ``("", None)`` tags are never returned; an unresolved row is
    ``(None, "")``.
    """
    v = venue.strip()
    cur = id_or_symbol.strip()
    if not v or not cur:
        return None, ""
    kind, raw, forced_itype = _extract_raw(cur)
    if kind == "canonical":
        return cur, "already_canon"
    itype = forced_itype or _resolve_itype(v, raw_itype, raw, data_type, itype_infer)
    if itype:
        fwd = wire_map.canonical_for(v, itype, raw)
        if fwd and _is_canonical_id(fwd):
            return fwd, "catalogue"
    if itype in {"FUTURE", "OPTION"}:
        pdw = _parse_dated_wire(raw)
        if pdw:
            hit = base_quote_date_map.get((v.upper(), itype, pdw[0], pdw[1], pdw[2], pdw[3]))
            if hit and _is_canonical_id(hit):
                return hit, "base_quote_date_map"
    bqk = _to_dashed_base_quote(raw)
    if itype and bqk:
        hit = base_quote_map.get((v.upper(), itype, bqk[0]))
        if hit and _is_canonical_id(hit):
            return hit, "base_quote_map"
    mb = marker_base.get(cur.split("@", 1)[0])
    if mb:
        return mb, "catalogue"
    if itype and cur.count(":") >= 2:
        fwd = wire_map.canonical_for(v, itype, cur.split(":", 2)[2])
        if fwd and _is_canonical_id(fwd):
            return fwd, "catalogue"
    if itype and bqk and bqk[1] in {"kraken_slash", "underscore"}:
        rc = _reconstruct(v, itype, bqk[0])
        if rc:
            return rc, "reconstructed"
    return None, ""


def resolve_canonical(
    venue: str,
    raw_itype: str,
    id_or_symbol: str,
    data_type: str,
    underlying: str,
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
    itype_infer: dict[tuple[str, str], str],
    base_quote_map: dict[tuple[str, str], str],
    base_quote_date_map: dict[tuple[str, str, str, str, str, str], str],
) -> str | None:
    """Resolve a manifest row's (venue, itype, id, data_type, underlying) to its canonical id.

    The ONE shared resolver behind every Track-6 axis (thin wrapper over ``_resolve_full`` —
    every catalogue path [wire-map, dated-map, BASE-QUOTE SSOT map, marker-base, wrapped-peel]
    runs BEFORE the narrow Kraken/underscore reconstruction, so the catalogue SSOT is never
    overridden). ``None`` = "leave the value alone" (honest).
    """
    return _resolve_full(
        venue,
        raw_itype,
        id_or_symbol,
        data_type,
        underlying,
        wire_map,
        marker_base,
        itype_infer,
        base_quote_map,
        base_quote_date_map,
    )[0]


def _classify_tuple(
    venue: str,
    raw_itype: str,
    iid: str,
    data_type: str,
    underlying: str,
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
    itype_infer: dict[tuple[str, str], str],
    base_quote_map: dict[tuple[str, str], str],
    base_quote_date_map: dict[tuple[str, str, str, str, str, str], str],
    catalog_ids: set[str],
    catalog_wires: set[tuple[str, str]],
) -> _Res:
    """Decide the ``_Res`` for one distinct (venue, itype, id, data_type, underlying) tuple.

    KEEP-trend policy (operator CORRECTIONS 2026-07-18): almost everything unresolved is
    KEPT honest-unresolved — blank ids (KALSHI/POLYMARKET roadmap venues + null-id bundle
    shards), chain BUNDLE rows (null id valid, NO synthesised id), and in-catalogue rows.
    The ONLY drop is a genuine catalogue-orphan (a resolvable-shaped bare id whose
    (venue, wire) the catalogue SSOT does not know); the captured-data drop-gate downstream
    still PROTECTS any such row that is captured-with-data.
    """
    v, cur, dt = venue.strip(), iid.strip(), data_type.strip()
    norm_it = _normalize_itype(raw_itype)

    canon, via = _resolve_full(
        v, raw_itype, cur, dt, underlying, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map
    )
    if canon:
        return _Res("relabel", venue, canon, _itype_of(canon), "perp" if ":PERP:" in cur else via, "")

    if v.upper() == "OKX":
        for rv in _OKX_REMAP:
            rc = resolve_canonical(
                rv,
                raw_itype,
                cur,
                dt,
                underlying,
                wire_map,
                marker_base,
                itype_infer,
                base_quote_map,
                base_quote_date_map,
            )
            if rc:
                return _Res("relabel", rv, rc, _itype_of(rc), "okx_remap", "")

    _kind, raw, _forced = _extract_raw(cur)
    kept_it = _resolve_itype(v, raw_itype, raw, dt, itype_infer) or norm_it
    if not cur:
        # blank instrument_id → KEEP: KALSHI/POLYMARKET roadmap rows + null-id bundle shards.
        return _Res("keep", venue, iid, kept_it, "blank_id_kept", "")
    if dt.upper() in _BUNDLE_DATA_TYPES:
        # BUNDLE shard (keyed on underlying) → KEEP as-is; never synthesise an id.
        return _Res("keep", venue, iid, kept_it, "bundle_kept", "")
    if not v or not dt:
        return _Res("keep", venue, iid, kept_it, "blank_axis_kept", "")
    in_cat = (cur.upper() in catalog_ids) or ((v.upper(), raw.strip().upper()) in catalog_wires)
    if in_cat:
        return _Res("keep", venue, iid, kept_it, "unresolved_kept", "")
    return _Res("drop", venue, iid, kept_it, "drop", "orphan")


def _empty_keys() -> pd.DataFrame:
    return pd.DataFrame(columns=SHARD_KEY_COLS)


def _canonicalize_blob(
    df: pd.DataFrame,
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
    itype_infer: dict[tuple[str, str], str],
    base_quote_map: dict[tuple[str, str], str],
    base_quote_date_map: dict[tuple[str, str, str, str, str, str], str],
    catalog_ids: set[str],
    catalog_wires: set[tuple[str, str]],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Canonicalise the instrument_id + instrument_type columns; drop safe orphans.

    Operates over EVERY row (not just captured) so the de-dup + eu-reconcile keys align on
    the normalised itype. Returns ``(df_after_drops, new_captured_keys, stats)`` where
    ``new_captured_keys`` are the post-relabel 5-col keys of CAPTURED rows whose id changed
    (drives the retained eu-reconcile pass).
    """
    zero: dict[str, int] = {
        "candidates": 0,
        "relabeled": 0,
        "unresolved": 0,
        "normalized_extra": 0,
        "id_changed": 0,
        "itype_changed": 0,
        "itype_inferred": 0,
        "perp_rewritten": 0,
        "okx_remapped": 0,
        "base_quote_resolved": 0,
        "base_quote_date_resolved": 0,
        "dated_itype_fixed": 0,
        "reconstructed": 0,
        "blank_id_kept": 0,
        "bundle_kept": 0,
        "blank_axis_kept": 0,
        "unresolved_kept": 0,
        "protected_captured": 0,
        "bare_underlying_bundle": 0,
        "bare_underlying_genuine": 0,
        "dropped_orphan": 0,
        "dropped_captured_with_data": 0,
        "cull_dropped": 0,
        "cull_dropped_captured_data": 0,
        "cull_ticks": 0,
    }
    required = {
        "capture_status",
        "row_count",
        "venue",
        "instrument_type",
        "instrument_id",
        "data_type",
        "underlying",
        *SHARD_KEY_COLS,
    }
    if not required.issubset(df.columns):
        return df, _empty_keys(), zero

    kdf = df[["venue", "instrument_type", "instrument_id", "data_type", "underlying"]].fillna("").astype(str)
    orig_venue = kdf["venue"]
    orig_itype = kdf["instrument_type"]
    orig_id = kdf["instrument_id"]
    keyser = (
        orig_venue
        + _KEY_SEP
        + orig_itype
        + _KEY_SEP
        + orig_id
        + _KEY_SEP
        + kdf["data_type"]
        + _KEY_SEP
        + kdf["underlying"]
    )

    uniq = kdf.drop_duplicates()
    intent_by: dict[str, str] = {}
    venue_by: dict[str, str] = {}
    id_by: dict[str, str] = {}
    itype_by: dict[str, str] = {}
    via_by: dict[str, str] = {}
    dclass_by: dict[str, str] = {}
    for venue, itype, iid, dtype, und in uniq.itertuples(index=False):
        key = f"{venue}{_KEY_SEP}{itype}{_KEY_SEP}{iid}{_KEY_SEP}{dtype}{_KEY_SEP}{und}"
        res = _classify_tuple(
            venue,
            itype,
            iid,
            dtype,
            und,
            wire_map,
            marker_base,
            itype_infer,
            base_quote_map,
            base_quote_date_map,
            catalog_ids,
            catalog_wires,
        )
        intent_by[key] = res.intent
        venue_by[key] = res.venue
        id_by[key] = res.iid
        itype_by[key] = res.itype
        via_by[key] = res.via
        dclass_by[key] = res.dclass

    intent_ser = keyser.map(intent_by)
    via_ser = keyser.map(via_by)
    dclass_ser = keyser.map(dclass_by)
    new_id = keyser.map(id_by)
    new_itype = keyser.map(itype_by)

    captured = df["capture_status"].astype(str) == "captured"
    row_count = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
    captured_data = captured & (row_count > 0)

    # DROP-VENUE CULL (operator-CONFIRMED 2026-07-18) — every row for a culled venue is dropped,
    # INCLUDING captured-with-data (the ONE authorized exception to the captured-data-safe
    # invariant; snapshot-first makes it reversible). Match the bare venue AND its venue-chain-
    # glued form (the Solana perp DEXs are stored as ``PACIFICA-SOLANA`` etc.), but NEVER a
    # longer DIFFERENT venue (``BINANCE-DELIVERY-`` won't match ``BINANCE-FUTURES``).
    vu = orig_venue.str.upper()
    cull_mask = vu.isin(_CULL_VENUES)
    for _c in _CULL_VENUES:
        cull_mask = cull_mask | vu.str.startswith(_c + "-")

    # DATA-CORRECTNESS carve-out: a drop-intent row that is captured-with-data is PROTECTED
    # (kept, itype normalised) rather than dropped — EXCEPT on a culled venue.
    drop_intent = intent_ser == "drop"
    drop_flag = (drop_intent & (~captured_data)) | cull_mask
    protected = drop_intent & captured_data & (~cull_mask)

    # --- counters (computed BEFORE the id/itype columns are reassigned) ---
    # venue-prefixed = the id's first ':'-segment equals the venue (case-insensitive).
    id_first_seg = orig_id.str.upper().str.split(":", n=1).str[0]
    prefixed = orig_id.str.contains(":", regex=False) & (id_first_seg == orig_venue.str.upper())
    cand_mask = captured & (~prefixed)
    id_changed_mask = (new_id != orig_id) & (~drop_flag)
    itype_changed_mask = (new_itype != orig_itype) & (~drop_flag)
    # "inferred" = the ORIGINAL itype was genuinely blank/None/unknown (NOT merely a
    # casing/alias fix) AND a canonical member was recovered. Aliases are applied before
    # the KEEP test so a lowercase ``perpetual`` / ``spot`` counts as a casing fix, not an
    # inference.
    aliased_orig = orig_itype.str.strip().str.upper().replace(_ITYPE_ALIASES)
    blank_itype_orig = ~aliased_orig.isin(_KEEP_ITYPES)
    inferred_mask = blank_itype_orig & new_itype.isin(_KEEP_ITYPES) & (~drop_flag)

    stats: dict[str, int] = dict(zero)
    stats["candidates"] = int(cand_mask.sum())
    stats["relabeled"] = int((cand_mask & (intent_ser == "relabel")).sum())
    stats["unresolved"] = int(stats["candidates"] - stats["relabeled"])
    stats["normalized_extra"] = int((captured & prefixed & id_changed_mask).sum())
    stats["id_changed"] = int(id_changed_mask.sum())
    stats["itype_changed"] = int(itype_changed_mask.sum())
    stats["itype_inferred"] = int(inferred_mask.sum())
    stats["perp_rewritten"] = int((via_ser == "perp").sum())
    stats["okx_remapped"] = int((via_ser == "okx_remap").sum())
    stats["base_quote_resolved"] = int((via_ser == "base_quote_map").sum())
    stats["base_quote_date_resolved"] = int((via_ser == "base_quote_date_map").sum())
    stats["reconstructed"] = int((via_ser == "reconstructed").sum())
    stats["blank_id_kept"] = int((via_ser == "blank_id_kept").sum())
    stats["bundle_kept"] = int((via_ser == "bundle_kept").sum())
    stats["blank_axis_kept"] = int((via_ser == "blank_axis_kept").sum())
    stats["unresolved_kept"] = int((via_ser == "unresolved_kept").sum())
    stats["protected_captured"] = int(protected.sum())
    stats["dropped_orphan"] = int((drop_flag & (~cull_mask) & (dclass_ser == "orphan")).sum())
    # invariant: 0 NON-cull captured-with-data dropped (the cull is the ONLY authorized exception).
    stats["dropped_captured_with_data"] = int((drop_flag & (~cull_mask) & captured_data).sum())
    stats["cull_dropped"] = int(cull_mask.sum())
    stats["cull_dropped_captured_data"] = int((cull_mask & captured_data).sum())
    stats["cull_ticks"] = int(row_count[cull_mask].sum())
    # DATED-WIRE itype-fix (Option A): rows whose itype went PERPETUAL/blank -> FUTURE/OPTION.
    dated_fix_mask = (
        new_itype.isin({"FUTURE", "OPTION"})
        & ((aliased_orig == "PERPETUAL") | ~aliased_orig.isin(_KEEP_ITYPES))
        & (~drop_flag)
    )
    stats["dated_itype_fixed"] = int(dated_fix_mask.sum())
    # piece 3: bare-underlying captured that stayed unresolved — bundle vs genuine split.
    last_seg = orig_id.str.upper().str.rsplit(":", n=1).str[-1]
    bare_asset = last_seg.str.fullmatch(r"[A-Z0-9]{1,6}").fillna(False) & ~last_seg.str.contains(
        r"(?:USDT|USDC|USD|BUSD)$", regex=True
    )
    bare_unres = (intent_ser != "relabel") & captured & bare_asset & (orig_id.str.strip() != "")
    chain_dt = df["data_type"].astype(str).str.upper().isin(_BUNDLE_DATA_TYPES)
    stats["bare_underlying_bundle"] = int((bare_unres & chain_dt).sum())
    stats["bare_underlying_genuine"] = int((bare_unres & ~chain_dt).sum())

    # Per-venue cull impact (rows | captured-with-data | ticks) — esp. BINANCE-DELIVERY COIN-M.
    if bool(cull_mask.any()):
        cull_df = pd.DataFrame(
            {"venue": orig_venue[cull_mask].str.upper(), "rc": row_count[cull_mask], "cd": captured_data[cull_mask]}
        )
        for ven, grp in cull_df.groupby("venue", sort=False):
            logger.info(
                "  CULL %-18s rows=%d captured-with-data=%d ticks=%d",
                ven,
                len(grp),
                int(grp["cd"].sum()),
                int(grp["rc"].sum()),
            )

    # --- apply: reassign id/itype for every kept row; retarget okx-remap venues ---
    out = df.copy()
    out["instrument_id"] = new_id
    out["instrument_type"] = new_itype
    okx_mask = via_ser == "okx_remap"
    if bool(okx_mask.any()):
        out.loc[okx_mask, "venue"] = keyser[okx_mask].map(venue_by)

    out = out.loc[~drop_flag].copy()
    # ALL captured 5-col shard keys (post-relabel, post-cull) — NOT just the id-changed ones.
    # The eu-reconcile drops an eu row whose 5-col key matches ANY captured row, so an eu twin
    # of an ALREADY-CANONICAL captured row (id unchanged during relabel) is ALSO dropped, and a
    # (idempotent) re-apply still reconciles even though relabel then changes nothing. Using only
    # the id-changed subset was the bug behind the 42,915 residual eu/captured collisions.
    captured_out = out["capture_status"].astype(str) == "captured"
    captured_keys = (
        out.loc[captured_out, SHARD_KEY_COLS].drop_duplicates().reset_index(drop=True)
        if bool(captured_out.any())
        else _empty_keys()
    )
    return out, captured_keys, stats


def _dedup_blob(df: pd.DataFrame) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Collapse coexisting shard-atom spellings via drop_duplicates(PIN_ATOM, keep best status)."""
    if not set(PIN_ATOM).issubset(df.columns) or "capture_status" not in df.columns:
        return df, 0, {}
    rank = df["capture_status"].map(_STATUS_RANK).fillna(9).astype(int)
    ordered = df.assign(_rank=rank).sort_values("_rank", kind="stable")
    before = len(ordered)
    kept = ordered.drop_duplicates(subset=PIN_ATOM, keep="first")
    collapsed = before - len(kept)
    breakdown: dict[str, int] = {}
    if collapsed:
        dropped = ordered.loc[~ordered.index.isin(kept.index)]
        breakdown = {str(k): int(v) for k, v in dropped["capture_status"].value_counts().to_dict().items()}
    kept = kept.drop(columns="_rank").sort_index()
    return kept, collapsed, breakdown


def _reconcile_eu_duplicates(df: pd.DataFrame, captured_keys: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop eu rows whose 5-col shard key collides with ANY captured row (captured wins).

    ``captured_keys`` is the FULL set of captured 5-col keys (all blobs, post-relabel), NOT
    just the id-changed subset — an eu skeleton is a redundant duplicate of a captured row on
    the same (date, venue, data_type, instrument_type, instrument_id) regardless of whether the
    captured id changed during relabel. eu rows carry NO data, so dropping them is always safe.
    The 5-col key deliberately excludes ``pipeline_mode`` so a ``batch_tardis`` eu written by
    one lane reconciles against the canonical captured twin written by another.
    """
    if captured_keys.empty or not set(SHARD_KEY_COLS).issubset(df.columns) or "capture_status" not in df.columns:
        return df, 0
    eu_mask = df["capture_status"] == "expected_unattempted"
    if not bool(eu_mask.any()):
        return df, 0
    eu_keys = df.loc[eu_mask, SHARD_KEY_COLS].reset_index().rename(columns={"index": "_orig_index"})
    merged = eu_keys.merge(captured_keys.drop_duplicates().assign(_hit=True), on=SHARD_KEY_COLS, how="left")
    hit = merged["_hit"].astype("boolean").fillna(False)
    drop_idx = merged.loc[hit, "_orig_index"].tolist()
    if drop_idx:
        df = df.drop(index=drop_idx)
    return df, len(drop_idx)


def _canon_captured(df: pd.DataFrame) -> tuple[int, int, int]:
    """(venue-prefixed captured, total captured, exempt captured) for the canonical-fraction.

    ``exempt`` = captured rows that LEGITIMATELY carry no venue-prefixed canonical id — the
    chain BUNDLE shards (``data_type`` ∈ {futures_chain, options_chain}, keyed on
    ``underlying`` with a null/bare id by design) — so the ADJUSTED fraction
    ``prefixed / (total - exempt)`` measures only the rows that SHOULD be canonical, and is
    not depressed by the operator's null-id bundle ruling.
    """
    if "capture_status" not in df.columns or "instrument_id" not in df.columns or "venue" not in df.columns:
        return 0, 0, 0
    cap = df[df["capture_status"] == "captured"]
    if cap.empty:
        return 0, 0, 0
    iid = cap["instrument_id"].fillna("").astype(str)
    ven = cap["venue"].fillna("").astype(str)
    prefixed = sum(1 for s, v in zip(iid, ven, strict=True) if s.upper().startswith(v.upper() + ":"))
    if "data_type" in cap.columns:
        dtu = cap["data_type"].fillna("").astype(str).str.upper()
        exempt = int((dtu.isin(_BUNDLE_DATA_TYPES) | (iid.str.strip() == "")).sum())
    else:
        exempt = int((iid.str.strip() == "").sum())
    return prefixed, len(cap), exempt


def _load(storage: object, bucket: str, blob: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a manifest blob. ``columns`` (dry-run) projects to the classification subset.

    The projection is the INTERSECTION of ``columns`` with the blob's real schema — a
    requested column absent from THIS blob (e.g. ``pipeline_mode`` on ``_legacy_seed``)
    is skipped here and re-materialised by ``_ensure_cols``, not a hard read failure.
    """
    raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
    if columns is None:
        return pd.read_parquet(io.BytesIO(raw))
    present = set(pq.read_schema(io.BytesIO(raw)).names)
    proj = [c for c in columns if c in present]
    return pd.read_parquet(io.BytesIO(raw), columns=proj)


def _ensure_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Re-materialise ``pipeline_mode`` / ``row_count`` when a blob does not store them.

    Mirrors ``read_availability_index``'s read-side backfill: ``pipeline_mode`` -> "" and
    ``row_count`` -> ``instrument_count`` (the index-side materialised written row count).
    Returns ``(df, added_cols)`` so ``--apply`` can DROP the synthesised columns before
    write, preserving the blob's on-disk schema.
    """
    added: list[str] = []
    out = df
    if "pipeline_mode" not in out.columns:
        out = out.assign(pipeline_mode="")
        added.append("pipeline_mode")
    if "row_count" not in out.columns:
        if "instrument_count" in out.columns:
            base = pd.to_numeric(out["instrument_count"], errors="coerce").fillna(0).astype("int64")
        else:
            base = pd.Series(0, index=out.index, dtype="int64")
        out = out.assign(row_count=base)
        added.append("row_count")
    return out, added


def _write(storage: object, bucket: str, blob: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, blob, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]


def _snapshot(storage: object, bucket: str, blob: str, run_ts: str) -> str:
    label = blob.rsplit("/", 1)[-1].removesuffix(".parquet")
    snap_blob = f"_index/snapshots/pre_d4_{run_ts}/{label}.parquet"
    logger.info("Snapshotting gs://%s/%s -> gs://%s/%s", bucket, blob, bucket, snap_blob)
    _write(storage, bucket, snap_blob, _load(storage, bucket, blob))
    return snap_blob


def _verify_gate(
    storage: object,
    bucket: str,
    blobs: list[str],
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
    itype_infer: dict[tuple[str, str], str],
    base_quote_map: dict[tuple[str, str], str],
    base_quote_date_map: dict[tuple[str, str, str, str, str, str], str],
) -> int:
    """Post-apply: 0 further-resolvable captured rows, 0 eu/captured 5-col collisions, 0 dropped-with-data."""
    logger.info("Verifying post-apply gate on gs://%s ...", bucket)
    residual_resolvable = 0
    for blob in blobs:
        try:
            df = _load(storage, bucket, blob)
        except Exception as exc:  # broad-except-ok — per-object isolation (transient per-VM shard)
            if blob == INDEX_BLOB:
                raise
            logger.warning("Verify: skipping per-VM shard gs://%s/%s (vanished): %s", bucket, blob, exc)
            continue
        if "capture_status" not in df.columns or "venue" not in df.columns:
            continue
        cap = df[df["capture_status"] == "captured"]
        venue = cap["venue"].fillna("").astype(str)
        itype = cap["instrument_type"].fillna("").astype(str)
        cur = cap["instrument_id"].fillna("").astype(str)
        dt = cap["data_type"].fillna("").astype(str) if "data_type" in cap.columns else pd.Series([""] * len(cap))
        und = cap["underlying"].fillna("").astype(str) if "underlying" in cap.columns else pd.Series([""] * len(cap))
        still = sum(
            1
            for v, t, c, d, u in zip(venue, itype, cur, dt, und, strict=True)
            if not _is_canonical_id(c.strip())
            and (
                nv := resolve_canonical(
                    v, t, c, d, u, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map
                )
            )
            is not None
            and nv != c
        )
        residual_resolvable += still
        if still:
            logger.error("GATE FAIL: gs://%s/%s still has %d further-resolvable captured rows.", bucket, blob, still)

    main_df = _load(storage, bucket, INDEX_BLOB)
    residual_eu = 0
    if set(SHARD_KEY_COLS).issubset(main_df.columns) and "capture_status" in main_df.columns:
        cap_keys = main_df.loc[main_df["capture_status"] == "captured", SHARD_KEY_COLS].drop_duplicates()
        eu_keys = main_df.loc[main_df["capture_status"] == "expected_unattempted", SHARD_KEY_COLS]
        merged = eu_keys.merge(cap_keys.assign(_hit=True), on=SHARD_KEY_COLS, how="left")
        residual_eu = int(merged["_hit"].astype("boolean").fillna(False).sum())
        if residual_eu:
            logger.error("GATE FAIL: main index still has %d eu rows colliding with a captured 5-col key.", residual_eu)

    if residual_resolvable or residual_eu:
        return 1
    logger.info("GATE PASSED: 0 further-resolvable captured rows; 0 eu/captured 5-col collisions.")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply",
        action="store_true",
        help="Relabel + itype/:PERP:/orphan align + de-dup + reconcile (snapshot first). Default: dry-run.",
    )
    p.add_argument("--bucket", default=None, help="Override the cefi market-data bucket.")
    p.add_argument("--instruments-bucket", default=None, help="Override the cefi instruments-store bucket.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    inst_bucket = args.instruments_bucket or resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="cefi"
    )
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    cat = _load_catalog(storage, inst_bucket)
    wire_map = _build_wire_map(cat)
    marker_base = _build_marker_base_map(cat)
    itype_infer = _build_itype_infer_map(cat)
    base_quote_map = _build_base_quote_map(cat)
    base_quote_date_map = _build_base_quote_date_map(cat)
    catalog_ids = _catalog_id_set(cat)
    catalog_wires = _catalog_wire_set(cat)
    logger.info(
        "3-tuple wire map: %d forward keys, %d ambiguous; marker-base: %d bases; itype-infer: %d wires; "
        "base-quote SSOT map: %d keys; base-quote-DATE map: %d keys; catalogue: %d ids / %d wires.",
        len(wire_map.canonical_by_wire),
        len(wire_map.ambiguous_wire_keys),
        len(marker_base),
        len(itype_infer),
        len(base_quote_map),
        len(base_quote_date_map),
        len(catalog_ids),
        len(catalog_wires),
    )

    gate_ok, n_perp, n_mismatch = _catalogue_gate(cat)
    logger.info(
        "Phase -1 catalogue gate: ':PERP:'=%d, instrument_id!=canonical=%d, GREEN=%s", n_perp, n_mismatch, gate_ok
    )
    if args.apply and not gate_ok:
        logger.error(
            "PRECONDITION GUARD: catalogue verify gate is RED — refusing --apply. Rebuild the catalogue first."
        )
        return 1

    for v, t, r in _MAJORS_CHECK:
        resolved = wire_map.canonical_for(v, t, r)
        marker = "RESOLVES" if resolved else "STILL-RAW"
        logger.info("  majors-check [%s]: (%s, %s, %s) -> %s", marker, v, t, r, resolved)

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [INDEX_BLOB, *per_vm_names]
    load_cols = None if args.apply else _DRYRUN_COLS

    dfs: dict[str, pd.DataFrame] = {}
    added_cols: dict[str, list[str]] = {}
    all_captured_keys: list[pd.DataFrame] = []
    totals: dict[str, int] = {}
    main_candidates = 0
    total_collapsed = 0
    collapse_breakdown: dict[str, int] = {}
    before_pref = before_cap = before_exempt = 0

    # Pass 1 — canonicalise id + itype off the resolver, per blob; collect new canonical keys.
    for blob in blobs:
        try:
            df = _load(storage, bucket, blob, columns=load_cols)
        except Exception as exc:  # broad-except-ok — per-object isolation (transient per-VM shard)
            # A live backfill VM's per-VM shard can be consolidated + deleted between the
            # list_blobs snapshot and this download (404). The main index MUST load; a
            # vanished per-VM shard is skipped (its rows are already in / headed for the index).
            if blob == INDEX_BLOB:
                raise
            logger.warning("Skipping per-VM shard gs://%s/%s (vanished mid-run — consolidated): %s", bucket, blob, exc)
            continue
        df, added_cols[blob] = _ensure_cols(df)
        bp, bc, be = _canon_captured(df)
        before_pref += bp
        before_cap += bc
        before_exempt += be

        df, captured_keys, stats = _canonicalize_blob(
            df, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map, catalog_ids, catalog_wires
        )
        dfs[blob] = df
        if not captured_keys.empty:
            all_captured_keys.append(captured_keys)
        for k, val in stats.items():
            totals[k] = totals.get(k, 0) + val
        if blob == INDEX_BLOB:
            main_candidates = stats["candidates"]
        logger.info(
            "gs://%s/%s: candidates=%d relabeled=%d itype_changed=%d dated_itype_fixed=%d "
            "base_quote=%d base_quote_date=%d perp=%d reconstructed=%d bundle_kept=%d "
            "unresolved_kept=%d protected=%d dropped_orphan=%d",
            bucket,
            blob,
            stats["candidates"],
            stats["relabeled"],
            stats["itype_changed"],
            stats["dated_itype_fixed"],
            stats["base_quote_resolved"],
            stats["base_quote_date_resolved"],
            stats["perp_rewritten"],
            stats["reconstructed"],
            stats["bundle_kept"],
            stats["unresolved_kept"],
            stats["protected_captured"],
            stats["dropped_orphan"],
        )

    # Only blobs that actually loaded (a transient per-VM shard may have vanished mid-run).
    loaded_blobs = [b for b in blobs if b in dfs]

    # Pass 2 — eu-reconcile: drop eu rows colliding (5-col) with ANY captured row, in EVERY
    # loaded blob (not main-only — an eu row can twin a captured row in another blob). The
    # captured key set is the FULL post-relabel set, so it also catches eu twins of
    # already-canonical captured rows (the 42,915-collision bug) and reconciles on re-apply.
    combined_captured_keys = (
        pd.concat(all_captured_keys, ignore_index=True).drop_duplicates() if all_captured_keys else _empty_keys()
    )
    n_eu_dropped = 0
    for blob in loaded_blobs:
        dfs[blob], n_dropped = _reconcile_eu_duplicates(dfs[blob], combined_captured_keys)
        n_eu_dropped += n_dropped

    # Pass 3 — de-dup coexisting shard-atom spellings (PINNED 6-col atom, keep best status).
    after_pref = after_cap = after_exempt = 0
    for blob in loaded_blobs:
        deduped, collapsed, breakdown = _dedup_blob(dfs[blob])
        dfs[blob] = deduped
        total_collapsed += collapsed
        for k, val in breakdown.items():
            collapse_breakdown[k] = collapse_breakdown.get(k, 0) + val
        ap, ac, ae = _canon_captured(deduped)
        after_pref += ap
        after_cap += ac
        after_exempt += ae

    before_frac = 100.0 * before_pref / before_cap if before_cap else 0.0
    after_frac = 100.0 * after_pref / after_cap if after_cap else 0.0
    before_adj = 100.0 * before_pref / (before_cap - before_exempt) if (before_cap - before_exempt) > 0 else 0.0
    after_adj = 100.0 * after_pref / (after_cap - after_exempt) if (after_cap - after_exempt) > 0 else 0.0
    total_dropped = totals.get("dropped_orphan", 0)

    logger.info("=" * 78)
    logger.info("SUMMARY across %d blob(s):", len(blobs))
    logger.info(
        "  raw-captured candidates : %d  (main index=%d + per-VM=%d)",
        totals.get("candidates", 0),
        main_candidates,
        totals.get("candidates", 0) - main_candidates,
    )
    logger.info("  captured relabeled      : %d", totals.get("relabeled", 0))
    logger.info(
        "  captured honest-unres.  : %d  (delisted/ambiguous candidates, left as-is)", totals.get("unresolved", 0)
    )
    logger.info(
        "  normalized_extra        : %d  (already-prefixed captured no-marker/wrapped -> catalogue id)",
        totals.get("normalized_extra", 0),
    )
    logger.info("  --- Track-6 axes (ALL rows) ---")
    logger.info("  instrument_id changed   : %d", totals.get("id_changed", 0))
    logger.info(
        "  instrument_type changed : %d  (of which blank/unknown -> inferred: %d)",
        totals.get("itype_changed", 0),
        totals.get("itype_inferred", 0),
    )
    logger.info("  :PERP: -> :PERPETUAL:   : %d", totals.get("perp_rewritten", 0))
    logger.info("  bare-OKX remapped       : %d", totals.get("okx_remapped", 0))
    logger.info("  --- DATED-WIRE itype-fix (operator Option A 2026-07-18 — the 41B-tick lever) ---")
    logger.info(
        "  itype -> FUTURE/OPTION   : %d  (from PERPETUAL/blank: dated-wire override [OKX mislabel] + chain-data_type inference)",
        totals.get("dated_itype_fixed", 0),
    )
    logger.info(
        "  base-quote-DATE resolved: %d  (dated FUTURE/OPTION the exact-wire-map missed -> exact catalogue id)",
        totals.get("base_quote_date_resolved", 0),
    )
    logger.info("  --- base-quote SSOT recovery + rebrand ---")
    logger.info(
        "  base-quote SSOT resolved: %d  (dashed bare-wire -> exact catalogue id; incl. MATIC->POL rebrand)",
        totals.get("base_quote_resolved", 0),
    )
    logger.info(
        "  reconstructed           : %d  (Kraken slash + underscore ONLY — forms the catalogue does not key)",
        totals.get("reconstructed", 0),
    )
    logger.info(
        "  bare-underlying split   : bundle=%d (chain shards, KEPT null-id) / genuine=%d (no-quote single instrument, honest-raw)",
        totals.get("bare_underlying_bundle", 0),
        totals.get("bare_underlying_genuine", 0),
    )
    logger.info("  --- KEEP (operator CORRECTIONS 2026-07-18 — nothing dropped bar genuine orphans) ---")
    logger.info(
        "  blank-id KEPT           : %d  (KALSHI/POLYMARKET roadmap venues + null-id shards — never dropped)",
        totals.get("blank_id_kept", 0),
    )
    logger.info(
        "  bundle KEPT (no-synth)  : %d  (futures_chain/options_chain shards keyed on underlying; null id valid)",
        totals.get("bundle_kept", 0),
    )
    logger.info(
        "  blank-axis KEPT         : %d  (blank venue/data_type — kept, not dropped)", totals.get("blank_axis_kept", 0)
    )
    logger.info("  unresolved kept (in-cat): %d", totals.get("unresolved_kept", 0))
    logger.info(
        "  PROTECTED captured-data : %d  (unresolved orphans carrying real ticks -> KEPT honest-unresolved)",
        totals.get("protected_captured", 0),
    )
    logger.info(
        "  DROPPED (orphans)       : %d  (genuine catalogue-orphans, non-captured only; NON-cull captured-with-data in set=%d [MUST be 0])",
        total_dropped,
        totals.get("dropped_captured_with_data", 0),
    )
    logger.info(
        "  DROP-VENUE CULL         : %d rows (operator-CONFIRMED; incl. %d captured-with-data / %d ticks — the ONE authorized captured-data-drop exception, snapshot-first)",
        totals.get("cull_dropped", 0),
        totals.get("cull_dropped_captured_data", 0),
        totals.get("cull_ticks", 0),
    )
    logger.info("  --- reconcile + de-dup ---")
    logger.info(
        "  eu-reconcile dropped    : %d  (ALL blobs; eu whose 5-col key twins ANY captured row — captured wins)",
        n_eu_dropped,
    )
    logger.info("  de-dup collapsed        : %d  by status: %s", total_collapsed, collapse_breakdown)
    logger.info("  canonical-fraction (raw): %.2f%% -> %.2f%% (captured venue-prefixed)", before_frac, after_frac)
    logger.info(
        "  canonical-fraction (adj): %.2f%% -> %.2f%%  (excl. %d captured null-id BUNDLE/blank shards that are canonically null by design)",
        before_adj,
        after_adj,
        after_exempt,
    )
    logger.info("=" * 78)

    # STOP-ON-SURPRISE guards.
    surprised = False
    # An idempotent re-apply on an already-canonicalised index reads ~0 candidates / :PERP:
    # (the first apply already did that work) — the VOLUME bands would false-halt, so skip them
    # when the BEFORE fraction proves the index is already canonical. SAFETY invariants below
    # (data-loss, over-drop, cull cap) are ALWAYS enforced.
    already_applied = before_frac >= _APPLIED_FRAC_THRESHOLD
    if already_applied:
        logger.info(
            "RE-RUN on an already-canonicalised index (before-fraction %.2f%% >= %.1f%%) — candidate/:PERP: VOLUME "
            "bands EXPECTED ~0 (idempotent); skipping those two bands, all safety invariants retained.",
            before_frac,
            _APPLIED_FRAC_THRESHOLD,
        )
    if not already_applied and not (_CANDIDATE_MIN <= totals.get("candidates", 0) <= _CANDIDATE_MAX):
        logger.error(
            "STOP-ON-SURPRISE: raw-captured candidate count %d outside band [%d, %d].",
            totals.get("candidates", 0),
            _CANDIDATE_MIN,
            _CANDIDATE_MAX,
        )
        surprised = True
    if not already_applied and not (_PERP_MIN <= totals.get("perp_rewritten", 0) <= _PERP_MAX):
        logger.error(
            "STOP-ON-SURPRISE: :PERP: rewrite count %d outside band [%d, %d] — perp venues may have left the catalogue.",
            totals.get("perp_rewritten", 0),
            _PERP_MIN,
            _PERP_MAX,
        )
        surprised = True
    if total_dropped > _MAX_TOTAL_DROPPED:
        logger.error(
            "STOP-ON-SURPRISE: total dropped %d exceeds cap %d — a catalogue regression may be over-orphaning.",
            total_dropped,
            _MAX_TOTAL_DROPPED,
        )
        surprised = True
    if totals.get("dropped_captured_with_data", 0) != 0:
        logger.error(
            "STOP-ON-SURPRISE (DATA LOSS): %d NON-cull captured-with-data rows are in the drop set — the carve-out failed.",
            totals.get("dropped_captured_with_data", 0),
        )
        surprised = True
    if totals.get("cull_dropped", 0) > _MAX_CULL_DROPPED:
        logger.error(
            "STOP-ON-SURPRISE: drop-venue cull count %d exceeds cap %d — a wrong venue may be in _CULL_VENUES.",
            totals.get("cull_dropped", 0),
            _MAX_CULL_DROPPED,
        )
        surprised = True
    if surprised:
        logger.error("Diagnose before --apply.")
        return 1

    if not args.apply:
        logger.info(
            "DRY-RUN (default) — no write. --apply would relabel %d, change %d itypes, rewrite %d :PERP:, "
            "collapse %d dupes, drop %d eu rows + %d orphans (snapshot-first).",
            totals.get("relabeled", 0),
            totals.get("itype_changed", 0),
            totals.get("perp_rewritten", 0),
            total_collapsed,
            n_eu_dropped,
            total_dropped,
        )
        return 0

    if (
        totals.get("id_changed", 0) == 0
        and totals.get("itype_changed", 0) == 0
        and total_collapsed == 0
        and n_eu_dropped == 0
        and total_dropped == 0
    ):
        logger.info("Nothing to change.")
        return 0

    for blob in loaded_blobs:
        _snapshot(storage, bucket, blob, run_ts)
        # Drop any column synthesised by _ensure_cols to preserve the blob's on-disk schema.
        drop_synth = [c for c in added_cols.get(blob, []) if c in dfs[blob].columns]
        out_df = dfs[blob].drop(columns=drop_synth) if drop_synth else dfs[blob]
        _write(storage, bucket, blob, out_df)
        logger.info("gs://%s/%s: written (%d rows).", bucket, blob, len(out_df))

    rc = _verify_gate(
        storage, bucket, loaded_blobs, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map
    )
    if rc == 0:
        logger.info(
            "APPLY COMPLETE: id_changed=%d, itype_changed=%d, perp=%d, de-dup-collapsed=%d, eu-dropped=%d, "
            "orphans-dropped=%d, canonical-fraction %.2f%%->%.2f%%.",
            totals.get("id_changed", 0),
            totals.get("itype_changed", 0),
            totals.get("perp_rewritten", 0),
            total_collapsed,
            n_eu_dropped,
            total_dropped,
            before_frac,
            after_frac,
        )
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
