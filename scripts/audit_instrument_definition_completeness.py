#!/usr/bin/env python3
"""IS completeness gate — is ``instrument_availability/by_date/`` 100% complete per AG?

The lifecycle catalogue (``build_instrument_catalogue.py``) and every downstream "could-exist"
computation (``expected_unattempted``, coverage denominators, instrument-existence guards) are only
trustworthy when the per-date instrument-definition layer they roll up is itself complete — i.e. no
``attempted_failed`` cells in the instruments-service availability ``_index`` for the asset group.

This tool reads the consolidated availability ``_index`` for one asset group and tabulates the
instrument-definition cells by ``capture_status``, surfacing every ``attempted_failed`` cell as a
gap. ``empty_confirmed`` is honest absence (a legitimately-empty source response), NOT a gap.

DELICATE — PROVISIONAL VERDICT (operator, 2026-06-04): the current ``_index`` is pre-migration
(v8 / mixed schema; cefi is 100% v8, see ``cefi_manifest_canonicalisation_2026_06_01.md``). A
"complete" verdict NOW is therefore provisional — it surfaces gross gaps (``attempted_failed``
cells that exist today) but cannot prove the *expected* universe is fully covered until the IS
manifest canonicalisation lands. Re-run this as a HARD gate AFTER that migration. No
catalogue/enumerator output can be trusted while this is RED for an asset group.

This is the best-effort half of the plan's [AUDIT] P0. The hard-gate half (full UAC
expected-universe diff: every ``venue x instrument-defn data_type x date`` the universe says should
exist) is the post-migration re-run.

Plan: proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (Phase 0 — [AUDIT] P0).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from dataclasses import dataclass, field

import pandas as pd
from unified_trading_library import get_bucket_name, read_availability_index

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

#: The four honest-coverage capture states (manifest v5+).
_CAPTURED = "captured"
_EMPTY_CONFIRMED = "empty_confirmed"
_ATTEMPTED_FAILED = "attempted_failed"
_EXPECTED_UNATTEMPTED = "expected_unattempted"


@dataclass
class CompletenessReport:
    """Best-effort completeness summary for one asset group's instrument-definition layer."""

    asset_group: str
    total_rows: int
    status_counts: dict[str, int]
    #: ``attempted_failed`` count per venue (the concrete "not 100% complete" signal).
    failed_by_venue: dict[str, int] = field(default_factory=dict)
    #: A bounded sample of ``(venue, date, data_type)`` gap tuples for triage.
    gap_sample: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def attempted_failed(self) -> int:
        """Total ``attempted_failed`` cells (the gap count)."""
        return self.status_counts.get(_ATTEMPTED_FAILED, 0)

    @property
    def is_complete(self) -> bool:
        """True when no ``attempted_failed`` cell exists.

        PROVISIONAL — "complete" here means "no failed cell in the current ``_index``", NOT
        "the full expected universe is covered" (that requires the post-migration hard gate).
        """
        return self.attempted_failed == 0


def _coerce_status(value: object) -> str:
    """Normalise a ``capture_status`` cell to a lowercase string.

    Legacy rows may carry a blank / NaN ``capture_status``; the manifest read path coerces those
    to ``CAPTURED``, so we mirror that here rather than invent a phantom gap.
    """
    if value is None:
        return _CAPTURED
    try:
        if pd.isna(value):  # pyright: ignore[reportArgumentType]
            return _CAPTURED
    except (TypeError, ValueError):
        pass
    text = str(value).strip().lower()
    return text or _CAPTURED


def summarise_completeness(
    index_df: pd.DataFrame,
    asset_group: str,
    *,
    gap_sample_limit: int = 50,
) -> CompletenessReport:
    """Tabulate the instrument-definition ``_index`` by ``capture_status`` and list gaps.

    Args:
        index_df: the availability ``_index`` DataFrame (columns include ``date`` / ``venue`` /
            ``data_type`` / ``capture_status``). For an instruments-store bucket every row IS an
            instrument-definition cell, so no data_type filtering is applied.
        asset_group: the asset group being audited (for the report header).
        gap_sample_limit: cap on the number of ``(venue, date, data_type)`` gap tuples retained.

    Returns:
        A :class:`CompletenessReport`.
    """
    total = len(index_df)
    if total == 0:
        return CompletenessReport(asset_group=asset_group, total_rows=0, status_counts={})

    statuses = [_coerce_status(v) for v in index_df.get("capture_status", pd.Series(dtype=object))]
    status_counts = dict(Counter(statuses))

    failed_by_venue: Counter[str] = Counter()
    gap_sample: list[tuple[str, str, str]] = []
    has_venue = "venue" in index_df.columns
    has_date = "date" in index_df.columns
    has_dtype = "data_type" in index_df.columns
    records: list[dict[str, object]] = index_df.to_dict("records")  # pyright: ignore[reportAssignmentType]
    for row in records:
        if _coerce_status(row.get("capture_status")) != _ATTEMPTED_FAILED:
            continue
        venue = str(row.get("venue", "")) if has_venue else ""
        day = str(row.get("date", "")) if has_date else ""
        dtype = str(row.get("data_type", "")) if has_dtype else ""
        failed_by_venue[venue] += 1
        if len(gap_sample) < gap_sample_limit:
            gap_sample.append((venue, day, dtype))

    return CompletenessReport(
        asset_group=asset_group,
        total_rows=total,
        status_counts=status_counts,
        failed_by_venue=dict(failed_by_venue),
        gap_sample=gap_sample,
    )


def _print_report(report: CompletenessReport) -> None:
    """Log a human-readable provisional completeness report."""
    logger.info("=" * 72)
    logger.info("IS instrument-definition completeness — asset_group=%s", report.asset_group)
    logger.info("PROVISIONAL (pre-migration _index) — re-run as a hard gate post-canonicalisation")
    logger.info("=" * 72)
    logger.info("total instrument-definition cells: %d", report.total_rows)
    for status in (_CAPTURED, _EMPTY_CONFIRMED, _ATTEMPTED_FAILED, _EXPECTED_UNATTEMPTED):
        logger.info("  %-22s %d", status, report.status_counts.get(status, 0))
    other = {
        k: v
        for k, v in report.status_counts.items()
        if k not in {_CAPTURED, _EMPTY_CONFIRMED, _ATTEMPTED_FAILED, _EXPECTED_UNATTEMPTED}
    }
    for status, count in sorted(other.items()):
        logger.info("  %-22s %d (unexpected status)", status, count)

    if report.attempted_failed:
        logger.warning("attempted_failed cells by venue:")
        for venue, count in sorted(report.failed_by_venue.items(), key=lambda kv: (-kv[1], kv[0])):
            logger.warning("  %-28s %d", venue or "<blank>", count)
        logger.warning("gap sample (venue, date, data_type), up to %d:", len(report.gap_sample))
        for venue, day, dtype in report.gap_sample:
            logger.warning("  %s | %s | %s", venue or "<blank>", day or "<blank>", dtype or "<blank>")

    verdict = "COMPLETE (provisional)" if report.is_complete else "INCOMPLETE"
    logger.info("VERDICT: %s — %d attempted_failed gap(s)", verdict, report.attempted_failed)
    logger.info("=" * 72)


def run_audit(asset_group: str, *, gap_sample_limit: int = 50) -> int:
    """Read the ``_index`` for ``asset_group`` and report completeness. Returns an exit code.

    Exit 0 = provisionally complete (no ``attempted_failed`` cells); exit 2 = incomplete (gaps
    present). Exit 2 (not 1) keeps "incomplete data" distinct from a tool/IO error.
    """
    bucket = get_bucket_name("instruments", asset_group)
    logger.info("Reading availability _index from bucket %s", bucket)
    index_df = read_availability_index(bucket)
    report = summarise_completeness(index_df, asset_group, gap_sample_limit=gap_sample_limit)
    _print_report(report)
    return 0 if report.is_complete else 2


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Best-effort (provisional) completeness check for the IS instrument-definition layer.",
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=["cefi", "defi", "tradfi", "sports", "prediction"],
        help="Asset group to audit (lowercase).",
    )
    parser.add_argument(
        "--gap-sample-limit",
        type=int,
        default=50,
        help="Max number of (venue, date, data_type) gap tuples to list (default: 50).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = _parse_args(argv)
    asset_group: str = args.asset_group
    gap_sample_limit: int = args.gap_sample_limit
    return run_audit(asset_group, gap_sample_limit=gap_sample_limit)


if __name__ == "__main__":
    sys.exit(main())
