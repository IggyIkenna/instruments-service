#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 source=mdps_odds_horizon_bucket expected_unattempted rows in
#   instruments-store-sports-{env} carry data_type="ODDS_HORIZON_BUCKET"
#   (uppercase) -- i.e. every such row has already been dropped (stale/
#   superseded by a real capture) or relabeled to the writer's lowercase
#   "odds_horizon_bucket" (verified post-apply, this script's own dry-run
#   count == 0).
"""reconcile_mdps_odds_horizon_bucket_eu_grain_2026_07_13.py -- one-off cleanup
of the 209,526 STALE ``source=mdps_odds_horizon_bucket`` ``expected_unattempted``
rows left behind by the pre-fix grain mismatch in
``enumerate_expected_universe.py`` (see ``instruments-service@<this-commit>``,
"fix(sports): realign mdps_odds_horizon_bucket expected-universe grain to
writer's lowercase data_type").

ROOT CAUSE (fixed going forward by the enumerator code change above, NOT by
this script): the v2 sports enumerator's per-league present-set match and its
seeded ``expected_unattempted`` rows both used the UAC-uppercase
``ODDS_HORIZON_BUCKET`` axis key verbatim, while MDPS's real writer
(``market-data-processing-service/scripts/reprocess_sports_odds.py``,
``_MANIFEST_DATA_TYPE``) stamps the manifest ``data_type`` column lower-case
(``"odds_horizon_bucket"``). The enumerator code fix makes FUTURE runs match
correctly -- but it is a per-VM-shard ADDITIVE writer (``_write_absent_rows``
only ever writes newly-computed ``absent_rows``; it never deletes or relabels
a pre-existing manifest row). So the 209,526 stale rows already sitting in the
canonical manifest from every PRIOR enumerator run are untouched by the code
fix alone -- this script is the one-off backfill-equivalent that reconciles
them, run once, directly against the canonical index.

WHAT THIS SCRIPT DOES to every ``source=mdps_odds_horizon_bucket``,
``capture_status=expected_unattempted`` row (confirmed via a live dry-run
2026-07-13, 209,526 rows, 100% ``data_type=ODDS_HORIZON_BUCKET`` / ``venue=""``
/ ``timeframe=None``):

  1. **STALE (DROP)** -- the row's ``(league_id, date)`` atom already has a
     real ``capture_status=captured`` row for this source (same league, same
     day, i.e. some ``T-*`` timeframe was actually captured that day). This is
     a genuinely-captured cell that a pre-fix run mis-seeded as "still
     pending" because of the case mismatch -- the manifest's own oscillation
     rule (a seeder never overrides a numerator fact, "captured outranks
     expected_unattempted") means this stale sentinel should never have
     existed once the atom was captured, so it is DELETED outright (not
     relabeled -- there is nothing honest left for it to say once a captured
     row for the same atom exists). Confirmed via live probe: 9,361 of the
     209,526 rows.
  2. **SURVIVING (RELABEL)** -- every other row: no captured atom exists for
     this ``(league_id, date)`` -- a genuine, still-open gap. Kept as
     ``expected_unattempted`` (this script does NOT judge whether the gap is
     "legitimate" -- that's the enumerator's/per-source-rule's job, not a
     one-off script's), but its ``data_type`` is RELABELED from the
     UAC-uppercase ``"ODDS_HORIZON_BUCKET"`` to the writer's real
     ``"odds_horizon_bucket"`` so it is grain-consistent with (a) every other
     row for this source already in the manifest and (b) any FUTURE
     enumerator run's present-set match (self-consistency: a future run must
     never re-seed a duplicate for an atom this script already seeded).
     Confirmed via live probe: 200,165 of the 209,526 rows. Nothing else on
     the row changes (``league_id``/``date``/``reason``/``capture_status``/
     every other column stays exactly as-is).

No other ``capture_status`` (``captured`` / ``empty_confirmed`` /
``attempted_failed``) or any OTHER source's rows are touched.

SAFETY -- confirmed via a live dry-run probe before writing this script:
  - 209,526 expected_unattempted rows for this source, 100% distinct on
    ``(league_id, date)`` (0 self-duplicates already) -- so "atom" and "row"
    are 1:1 for this source's EU slice, no ambiguity in the drop/relabel split.
  - 14,417 distinct captured ``(league_id, date)`` atoms; exactly 9,361 of the
    209,526 EU atoms match one of them (the STALE set above); the remaining
    200,165 do not (the SURVIVING set above). ``9,361 + 200,165 == 209,526``.
  - Relabeling never creates a new duplicate-dedup-key group: the canonical
    manifest-consolidator dedup key includes ``timeframe`` (unset/None on
    every EU row, both before and after this script) and ``venue`` (blank on
    every EU row, unchanged) -- only ``data_type`` casing changes, and no
    other row for this source already carries
    ``(venue="", data_type="odds_horizon_bucket", timeframe=None,
    league_id=<L>, date=<D>)`` for any surviving atom (verified: the 123,642
    captured rows all carry a REAL ``timeframe`` value, never ``None``/blank,
    so a relabeled EU row's key can never collide with a captured row's key).

CAS-SAFE DIRECT REWRITE (per this workspace's direct-canonical-rewrite rule):
this script DROPS and RELABELS existing manifest rows -- a direct canonical
rewrite, NOT a per-VM shard add -- so a plain gcsfs read/write is unsafe
against ``instruments-store-sports``'s live per-minute manifest consolidator
(a concurrent consolidator cycle could silently clobber this script's write,
exactly as documented in
``migrate_orphaned_mtds_odds_api_bucket_rows_2026_07_13.py``'s "DEVIATION..."
section, this script's direct template for the CAS mechanics). Uses the UTL
generation-precondition CAS primitive
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
      .venv/bin/python scripts/reconcile_mdps_odds_horizon_bucket_eu_grain_2026_07_13.py [--apply]
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
logger = logging.getLogger("reconcile_mdps_odds_horizon_bucket_eu_grain")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_SOURCE_NAME = "mdps_odds_horizon_bucket"
_STALE_DATA_TYPE = "ODDS_HORIZON_BUCKET"
_WRITER_DATA_TYPE = "odds_horizon_bucket"

# The live sports manifest consolidator cron fires every ~60s (30-90s runtime
# per cycle) -- bound the CAS retry loop generously past a few full cycles
# rather than failing on the first collision with an in-flight run (mirrors
# migrate_orphaned_mtds_odds_api_bucket_rows_2026_07_13.py's bound).
_MAX_CAS_ATTEMPTS = 30
_CAS_RETRY_SLEEP_SECONDS = 5.0


def _plan_drop_and_relabel(tgt: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Compute the reconciled frame against a fresh ``tgt`` snapshot.

    Returns ``(new_df, n_dropped, n_relabeled)``. Pure function of ``tgt`` so
    it can be re-run against a freshly-read snapshot on every CAS retry.
    """
    is_source = tgt["source"] == _SOURCE_NAME
    is_eu = tgt["capture_status"] == "expected_unattempted"
    is_stale_dt = tgt["data_type"] == _STALE_DATA_TYPE
    eu_mask = is_source & is_eu & is_stale_dt

    captured_mask = is_source & (tgt["capture_status"] == "captured")
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
    new_df.loc[new_df.index.isin(relabel_index), "data_type"] = _WRITER_DATA_TYPE

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
        before_eu = int(((tgt["source"] == _SOURCE_NAME) & (tgt["capture_status"] == "expected_unattempted")).sum())
        new_df, n_dropped, n_relabeled = _plan_drop_and_relabel(tgt)
        logger.info(
            "DRY RUN -- source=%s expected_unattempted before=%d: %d STALE (would DROP, already captured "
            "under this atom) + %d SURVIVING (would RELABEL data_type %s -> %s). rows_in=%d rows_out=%d "
            "(delta=%d). Re-run with --apply to commit.",
            _SOURCE_NAME,
            before_eu,
            n_dropped,
            n_relabeled,
            _STALE_DATA_TYPE,
            _WRITER_DATA_TYPE,
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
        before_eu = int(((tgt["source"] == _SOURCE_NAME) & (tgt["capture_status"] == "expected_unattempted")).sum())
        logger.info(
            "Target read at generation=%d: %d total rows, %d source=%s expected_unattempted.",
            generation,
            len(tgt),
            before_eu,
            _SOURCE_NAME,
        )

        new_df, n_dropped, n_relabeled = _plan_drop_and_relabel(tgt)
        logger.info(
            "Plan: %d STALE (DROP) + %d SURVIVING (RELABEL %s -> %s). rows_in=%d rows_out=%d.",
            n_dropped,
            n_relabeled,
            _STALE_DATA_TYPE,
            _WRITER_DATA_TYPE,
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
