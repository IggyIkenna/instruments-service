#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 source=mdps_odds_horizon_bucket expected_unattempted rows in
#   instruments-store-sports-{env} carry a blank/null venue -- i.e. every such
#   row has already been dropped (stale/superseded by a real capture) or
#   relabeled to the writer's real "ODDS_API" venue (verified post-apply,
#   this script's own dry-run count == 0).
"""reconcile_mdps_odds_horizon_bucket_venue_grain_2026_07_14.py -- one-off
cleanup of the ~200,259 STALE blank-venue ``source=mdps_odds_horizon_bucket``
``expected_unattempted`` rows left behind by a SECOND, DIFFERENT grain
mismatch in ``enumerate_expected_universe.py`` (see
``instruments-service@<this-commit>``, "fix(sports): realign
mdps_odds_horizon_bucket expected-universe venue grain to writer's ODDS_API
venue").

BACKGROUND -- this is the sibling/follow-up to
``reconcile_mdps_odds_horizon_bucket_eu_grain_2026_07_13.py`` (the
``data_type`` casing fix, 2026-07-13). That fix + its one-off reconciliation
correctly realigned the ``data_type`` dimension (``ODDS_HORIZON_BUCKET`` ->
``odds_horizon_bucket``) and a subsequent full 4-VM historical backfill
(``launch-mdps-sports-bucket-vm.sh``, completed 2026-07-14: 1,930 succeeded +
293 legitimately empty of 2,230 backlog dates) genuinely captured the missing
data. Despite this, the live canonical manifest's ``expected_unattempted``
count for this source did NOT drop -- confirmed via a live manifest read
2026-07-14: 200,259 ``expected_unattempted`` rows, 100% ``venue=""``
(blank), alongside 143,594 ``captured`` rows, 100% ``venue="ODDS_API"``. 0
overlap on ``venue`` between the two sets for the exact same ``(league_id,
date)`` cells.

ROOT CAUSE (fixed going forward by the enumerator code change above, NOT by
this script): every OTHER sports source's real captured atom carries a BLANK
``venue`` (the "sports is league-grain, venue is blank" convention this
module documents throughout), so the enumerator's per-league seeding
hard-coded ``venue=""`` for every sports data_type. ``mdps_odds_horizon_bucket``
is the ONE exception -- its writer
(``market-data-processing-service/scripts/reprocess_sports_odds.py``,
``_MANIFEST_VENUE = "ODDS_API"``) stamps a real, non-blank
``venue="ODDS_API"`` on every captured row for this source (a deliberate
fixed source-label: the script aggregates raw per-bookmaker odds into one
per-(date, league_id, timeframe) view and reuses ``ODDS_API`` as a source
token, not a real venue). So a blank-venue ``expected_unattempted`` seed for
THIS source's cell never lines up with the real captured atom -- the exact
``data_type`` bug class from 2026-07-13, but on the ``venue`` dimension
instead. The enumerator code fix makes FUTURE runs match correctly -- but it
is a per-VM-shard ADDITIVE writer (``_write_absent_rows`` only ever writes
newly-computed ``absent_rows``; it never deletes or relabels a pre-existing
manifest row). So the 200,259 stale rows already sitting in the canonical
manifest from every PRIOR enumerator run are untouched by the code fix alone
-- this script is the one-off backfill-equivalent that reconciles them, run
once, directly against the canonical index.

WHAT THIS SCRIPT DOES to every ``source=mdps_odds_horizon_bucket``,
``capture_status=expected_unattempted`` row with a blank/null ``venue``
(confirmed via a live dry-run 2026-07-14, 200,259 rows, 100%
``venue=""`` / ``timeframe=None``):

  1. **STALE (DROP)** -- the row's ``(league_id, date)`` atom already has a
     real ``capture_status=captured``, ``venue="ODDS_API"`` row for this
     source (i.e. some ``T-*`` timeframe was actually captured that day).
     This is a genuinely-captured cell that a pre-fix run mis-seeded as
     "still pending" because of the venue mismatch -- the manifest's own
     oscillation rule (a seeder never overrides a numerator fact, "captured
     outranks expected_unattempted") means this stale sentinel should never
     have existed once the atom was captured, so it is DELETED outright (not
     relabeled -- there is nothing honest left for it to say once a captured
     row for the same atom exists). Confirmed via live probe: 633 of the
     200,259 rows.
  2. **SURVIVING (RELABEL)** -- every other row: no captured atom exists for
     this ``(league_id, date)`` -- a genuine, still-open gap. Kept as
     ``expected_unattempted`` (this script does NOT judge whether the gap is
     "legitimate" -- that's the enumerator's/per-source-rule's job, not a
     one-off script's), but its ``venue`` is RELABELED from blank to the
     writer's real ``"ODDS_API"`` so it is grain-consistent with (a) every
     other row for this source already in the manifest and (b) any FUTURE
     enumerator run's seeding (self-consistency: a future run must never
     re-seed a duplicate for an atom this script already seeded). Confirmed
     via live probe: 199,626 of the 200,259 rows.

The atom used for the drop/relabel split is ``(league_id, date)`` only --
NOT ``(league_id, date, timeframe)`` -- because every EU row for this source
carries ``timeframe=None`` (confirmed live: 100% of the 200,259 rows), while
real captured rows carry a per-bucket ``timeframe`` value (``T-1h`` etc, one
row per bucket) -- keying on ``timeframe`` would never match anything and
would misclassify every genuinely-captured cell as "surviving". This mirrors
``reconcile_mdps_odds_horizon_bucket_eu_grain_2026_07_13.py``'s identical
``(league_id, date)`` atom choice for the exact same reason.

No other ``capture_status`` (``captured`` / ``empty_confirmed`` /
``attempted_failed``) or any OTHER source's rows are touched.

SAFETY -- confirmed via a live dry-run probe before writing this script:
  - 200,259 blank-venue expected_unattempted rows for this source, 100%
    distinct on ``(league_id, date)`` (0 self-duplicates already) -- so
    "atom" and "row" are 1:1 for this source's blank-venue EU slice, no
    ambiguity in the drop/relabel split.
  - 143,594 captured rows, 100% ``venue="ODDS_API"``; exactly 633 of the
    200,259 blank-venue EU atoms match a captured ``(league_id, date)`` atom
    (the STALE set above); the remaining 199,626 do not (the SURVIVING set
    above). ``633 + 199,626 == 200,259``.
  - Relabeling never creates a new duplicate-dedup-key group: for all 199,626
    SURVIVING atoms, 0 already have ANY other row (captured / empty_confirmed
    / attempted_failed, any source) carrying ``venue="ODDS_API"`` at the same
    ``(league_id, date)`` -- verified via a live probe cross-checking the
    post-relabel key against the whole manifest slice for this source.

CAS-SAFE DIRECT REWRITE (per this workspace's direct-canonical-rewrite rule):
this script DROPS and RELABELS existing manifest rows -- a direct canonical
rewrite, NOT a per-VM shard add -- so a plain gcsfs read/write is unsafe
against ``instruments-store-sports``'s live per-minute manifest consolidator
(a concurrent consolidator cycle could silently clobber this script's write,
exactly as documented in
``reconcile_mdps_odds_horizon_bucket_eu_grain_2026_07_13.py``'s "CAS-SAFE
DIRECT REWRITE" section, this script's direct template for the CAS
mechanics). Uses the UTL generation-precondition CAS primitive
(``StorageClient.download_bytes_with_generation`` /
``StorageClient.conditional_upload_bytes(if_generation_match=...)``) in a
bounded retry loop: read target + generation together, recompute the
drop/relabel against the FRESH snapshot (so a fact the consolidator itself
added in the interim -- e.g. a genuinely new capture landing between retries
-- is honored), attempt an atomic compare-and-swap write; a
``PreconditionFailed`` means a concurrent writer won the race -- re-read and
retry rather than blind-overwriting.

DRY-RUN by default; ``--apply`` writes (via the CAS retry loop).

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/reconcile_mdps_odds_horizon_bucket_venue_grain_2026_07_14.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import time

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reconcile_mdps_odds_horizon_bucket_venue_grain")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_SOURCE_NAME = "mdps_odds_horizon_bucket"
_WRITER_VENUE = "ODDS_API"

# The live sports manifest consolidator cron fires every ~60s (30-90s runtime
# per cycle) -- bound the CAS retry loop generously past a few full cycles
# rather than failing on the first collision with an in-flight run (mirrors
# reconcile_mdps_odds_horizon_bucket_eu_grain_2026_07_13.py's bound).
_MAX_CAS_ATTEMPTS = 30
_CAS_RETRY_SLEEP_SECONDS = 5.0


def _plan_drop_and_relabel(tgt: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Compute the reconciled frame against a fresh ``tgt`` snapshot.

    Returns ``(new_df, n_dropped, n_relabeled)``. Pure function of ``tgt`` so
    it can be re-run against a freshly-read snapshot on every CAS retry.
    """
    is_source = tgt["source"] == _SOURCE_NAME
    is_eu = tgt["capture_status"] == "expected_unattempted"
    venue_blank = tgt["venue"].fillna("").astype(str) == ""
    eu_mask = is_source & is_eu & venue_blank

    captured_mask = (
        is_source & (tgt["capture_status"] == "captured") & (tgt["venue"].fillna("").astype(str) == _WRITER_VENUE)
    )
    captured_atoms = set(
        zip(
            tgt.loc[captured_mask, "league_id"].fillna("").astype(str),
            tgt.loc[captured_mask, "date"].astype(str),
            strict=True,
        )
    )

    eu_league = tgt.loc[eu_mask, "league_id"].fillna("").astype(str)
    eu_date = tgt.loc[eu_mask, "date"].astype(str)
    eu_atom = list(zip(eu_league, eu_date, strict=True))
    is_stale = pd.Series([atom in captured_atoms for atom in eu_atom], index=tgt.loc[eu_mask].index)

    stale_index = tgt.loc[eu_mask].index[is_stale.to_numpy()]
    relabel_index = tgt.loc[eu_mask].index[~is_stale.to_numpy()]

    new_df = tgt.drop(index=stale_index).copy()
    new_df.loc[new_df.index.isin(relabel_index), "venue"] = _WRITER_VENUE

    return new_df, len(stale_index), len(relabel_index)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the reconciled index back to the canonical target.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    tgt_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not tgt_bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved target bucket %s is not the expected env-short shape. Refusing.", tgt_bucket)
        return 1

    client = get_storage_client(provider="gcp")

    if not args.apply:
        data, generation = client.download_bytes_with_generation(tgt_bucket, _MANIFEST_BLOB)
        if data is None:
            logger.error("Refusing: target object gs://%s/%s does not exist.", tgt_bucket, _MANIFEST_BLOB)
            return 1
        tgt = pd.read_parquet(io.BytesIO(data))
        logger.info(
            "Read gs://%s/%s at generation=%d: %d total rows.", tgt_bucket, _MANIFEST_BLOB, generation, len(tgt)
        )
        before_eu = int(
            (
                (tgt["source"] == _SOURCE_NAME)
                & (tgt["capture_status"] == "expected_unattempted")
                & (tgt["venue"].fillna("").astype(str) == "")
            ).sum()
        )
        new_df, n_dropped, n_relabeled = _plan_drop_and_relabel(tgt)
        logger.info(
            "DRY RUN -- source=%s expected_unattempted blank-venue before=%d: %d STALE (would DROP, already "
            "captured under this atom) + %d SURVIVING (would RELABEL venue '' -> %s). rows_in=%d rows_out=%d "
            "(delta=%d). Re-run with --apply to commit.",
            _SOURCE_NAME,
            before_eu,
            n_dropped,
            n_relabeled,
            _WRITER_VENUE,
            len(tgt),
            len(new_df),
            len(new_df) - len(tgt),
        )
        return 0

    # --apply: atomic CAS retry loop (see module docstring "CAS-SAFE DIRECT
    # REWRITE" for why a plain gcsfs read/write is unsafe against this
    # bucket's live per-minute manifest consolidator).
    for attempt in range(1, _MAX_CAS_ATTEMPTS + 1):
        logger.info(
            "Reading canonical target gs://%s/%s (CAS attempt %d/%d)",
            tgt_bucket,
            _MANIFEST_BLOB,
            attempt,
            _MAX_CAS_ATTEMPTS,
        )
        data, generation = client.download_bytes_with_generation(tgt_bucket, _MANIFEST_BLOB)
        if data is None:
            logger.error("Refusing: target object gs://%s/%s does not exist.", tgt_bucket, _MANIFEST_BLOB)
            return 1
        tgt = pd.read_parquet(io.BytesIO(data))
        before_eu = int(
            (
                (tgt["source"] == _SOURCE_NAME)
                & (tgt["capture_status"] == "expected_unattempted")
                & (tgt["venue"].fillna("").astype(str) == "")
            ).sum()
        )
        logger.info(
            "Target read at generation=%d: %d total rows, %d source=%s expected_unattempted blank-venue.",
            generation,
            len(tgt),
            before_eu,
            _SOURCE_NAME,
        )

        new_df, n_dropped, n_relabeled = _plan_drop_and_relabel(tgt)
        logger.info(
            "Plan: %d STALE (DROP) + %d SURVIVING (RELABEL venue '' -> %s). rows_in=%d rows_out=%d.",
            n_dropped,
            n_relabeled,
            _WRITER_VENUE,
            len(tgt),
            len(new_df),
        )

        buf = io.BytesIO()
        new_df.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        new_generation = client.conditional_upload_bytes(
            tgt_bucket,
            _MANIFEST_BLOB,
            buf.getvalue(),
            if_generation_match=generation,
            content_type="application/octet-stream",
        )
        if new_generation is not None:
            logger.info(
                "APPLIED on attempt %d/%d (generation %d -> %d). Dropped=%d Relabeled=%d rows_in=%d rows_out=%d.",
                attempt,
                _MAX_CAS_ATTEMPTS,
                generation,
                new_generation,
                n_dropped,
                n_relabeled,
                len(tgt),
                len(new_df),
            )
            return 0

        logger.warning(
            "CAS precondition failed on attempt %d/%d (generation %d changed underneath us -- a "
            "concurrent writer, likely the manifest consolidator, won the race). Retrying in %.0fs.",
            attempt,
            _MAX_CAS_ATTEMPTS,
            generation,
            _CAS_RETRY_SLEEP_SECONDS,
        )
        time.sleep(_CAS_RETRY_SLEEP_SECONDS)

    logger.error("Exhausted %d CAS attempts without a successful write. Refusing.", _MAX_CAS_ATTEMPTS)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
