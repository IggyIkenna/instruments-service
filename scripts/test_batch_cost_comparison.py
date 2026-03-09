#!/usr/bin/env python3
"""
Databento Batch API Cost Comparison Script

Compares the cost of a fresh batch download vs a re-download of the same data.
Key insight: Databento batch re-downloads within 30 days are FREE.

Uses DatabentoBaseClient from unified-trading-library for:
- Deterministic API key selection (same params -> same key)
- Expanded state checking (queued/processing/done)
- GCS job cache for cross-shard deduplication

Usage:
    # Dry-run (default) - estimate cost only, no jobs submitted:
    python scripts/test_batch_cost_comparison.py --date 2026-02-06

    # Actually submit a batch job:
    python scripts/test_batch_cost_comparison.py --date 2026-02-06 --execute

    # Run again to verify FREE re-download:
    python scripts/test_batch_cost_comparison.py --date 2026-02-06 --execute
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from uuid import uuid4

from unified_config_interface import UnifiedCloudConfig
from unified_internal_contracts import EnhancedError, ErrorCategory, ErrorRecoveryStrategy, ErrorSeverity
from unified_internal_contracts.schemas.errors import ErrorContext
from unified_market_interface import DatabentoBaseClient
from unified_trading_library import get_secret_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def format_cost(cost_usd: float | None) -> str:
    """Format a dollar amount for display."""
    if cost_usd is None:
        return "N/A"
    if cost_usd == 0.0:
        return "$0.00 (FREE)"
    return f"${cost_usd:.4f}"


def print_separator(title: str = "") -> None:
    """Log a visual separator with optional title."""
    if title:
        logger.info("=" * 60)
        logger.info("  %s", title)
        logger.info("=" * 60)
    else:
        logger.info("-" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Databento batch download cost: fresh vs re-download",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Dry-run estimate only (safe, no cost):
  python scripts/test_batch_cost_comparison.py --date 2026-02-06

  # Submit a real batch job:
  python scripts/test_batch_cost_comparison.py --date 2026-02-06 --execute

  # Re-run to verify FREE re-download:
  python scripts/test_batch_cost_comparison.py --date 2026-02-06 --execute
""",
    )
    parser.add_argument("--dataset", default="GLBX.MDP3", help="Databento dataset (default: GLBX.MDP3)")
    parser.add_argument("--symbols", default="ES.FUT", help="Comma-separated symbols (default: ES.FUT)")
    parser.add_argument("--schema", default="definition", help="Schema name (default: definition)")
    parser.add_argument("--stype-in", default="parent", help="Symbol type input (default: parent)")
    parser.add_argument("--date", required=True, help="Date to query (YYYY-MM-DD, e.g. 2026-02-06)")
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually submit a batch job (default is dry-run/estimate only)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout in minutes waiting for batch job (default: 30)",
    )

    args = parser.parse_args()

    # Parse date range
    try:
        date_start = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format: %s. Use YYYY-MM-DD.", args.date)
        sys.exit(1)

    date_end = date_start + timedelta(days=1)
    start_str = date_start.strftime("%Y-%m-%d")
    end_str = date_end.strftime("%Y-%m-%d")
    symbols_list = [s.strip() for s in args.symbols.split(",")]

    # Initialize base client (handles API key resolution via Secret Manager + multi-key rotation)
    print_separator("DATABENTO BATCH COST COMPARISON")
    logger.info("  Dataset:   %s", args.dataset)
    logger.info("  Symbols:   %s", symbols_list)
    logger.info("  Schema:    %s", args.schema)
    logger.info("  Stype-in:  %s", args.stype_in)
    logger.info("  Date:      %s -> %s", start_str, end_str)
    logger.info("  Mode:      %s", "EXECUTE (will submit job)" if args.execute else "DRY-RUN (estimate only)")

    # API key via get_secret_client (Secret Manager first, env fallback) per instruments-and-api-keys-standard
    config = UnifiedCloudConfig()
    project_id = config.gcp_project_id
    api_key = (
        get_secret_client(
            secret_name="databento-api-key",
            project_id=project_id or "",
        )
        if project_id
        else None
    )
    base_client = DatabentoBaseClient(api_key=api_key, project_id=project_id)

    # Also get a raw db.Historical for metadata calls (cost estimate, billable size)
    raw_client = base_client.client
    logger.info("[OK] DatabentoBaseClient initialized (deterministic key selection enabled)")

    batch_key_index = base_client._get_batch_key_index(args.dataset, args.schema, symbols_list, start_str, end_str)
    logger.info("[OK] Deterministic batch key index for these params: %s", batch_key_index)

    # ---- Step 1: Estimate cost via metadata.get_cost() ----
    print_separator("STEP 1: Cost Estimate (metadata.get_cost)")
    try:
        estimated_cost = raw_client.metadata.get_cost(
            dataset=args.dataset,
            symbols=symbols_list,
            schema=args.schema,
            stype_in=args.stype_in,
            start=start_str,
            end=end_str,
        )
        logger.info("  Estimated cost for FRESH download: %s", format_cost(estimated_cost))
    except Exception as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("get_cost() failed: %s", e)
        estimated_cost = None
    # ---- Step 2: Estimated billable size ----
    print_separator("STEP 2: Billable Size (metadata.get_billable_size)")
    try:
        billable_size = raw_client.metadata.get_billable_size(
            dataset=args.dataset,
            symbols=symbols_list,
            schema=args.schema,
            start=start_str,
            end=end_str,
        )
        if billable_size is not None:
            size_kb = billable_size / 1024
            size_mb = size_kb / 1024
            logger.info("  Billable size: %s bytes (%.2f KB / %.4f MB)", f"{billable_size:,}", size_kb, size_mb)
        else:
            logger.info("  Billable size: N/A")
    except Exception as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        logger.warning("get_billable_size() failed: %s", e)
        billable_size = None
    # ---- Step 3: Check for existing batch job (using base client with expanded states) ----
    print_separator("STEP 3: Check Existing Batch Jobs (queued/processing/done)")
    existing_job = base_client.find_matching_batch_job(
        dataset=args.dataset,
        schema=args.schema,
        symbols=symbols_list,
        stype_in=args.stype_in,
        start=start_str,
        end=end_str,
    )

    if existing_job:
        job_id = existing_job.get("id", "unknown")
        job_cost = existing_job.get("cost_usd")
        job_billed = existing_job.get("billed_size")
        job_state = existing_job.get("state", "unknown")
        logger.info("  FOUND existing batch job!")
        logger.info("    Job ID:      %s", job_id)
        logger.info("    State:       %s", job_state)
        logger.info("    Cost:        %s", format_cost(job_cost))
        logger.info("    Billed size: %s", f"{job_billed:,} bytes" if job_billed else "N/A")
        if job_state == "done":
            logger.info("  --> Re-downloading this job is FREE (within 30 days)")
        else:
            logger.info("  --> Job is still %s, will wait for completion", job_state)
    else:
        logger.info("  No existing batch job found for these parameters.")
        logger.info("  A new batch job submission will incur the estimated cost above.")

    # ---- Step 4: Dry-run gate ----
    if not args.execute:
        print_separator("DRY-RUN COMPLETE")
        logger.info("  No batch job was submitted (safe mode).")
        logger.info("  To actually submit a batch job, re-run with --execute:")
        logger.info("    python scripts/test_batch_cost_comparison.py --date %s --execute", args.date)
        if existing_job:
            state = existing_job.get("state", "unknown")
            if state == "done":
                logger.info("  NOTE: An existing done job was found. Re-running with --execute will")
                logger.info("  re-download it for FREE (no new cost).")
            else:
                logger.info("  NOTE: An existing %s job was found. Re-running with --execute", state)
                logger.info("  will wait for it to complete, then download (no new cost).")
        else:
            logger.warning("  Submitting a new job will cost ~%s", format_cost(estimated_cost))
        return

    # ---- Step 5: Execute using base client's unified batch orchestration ----
    print_separator("STEP 4: Submitting / Re-downloading Batch Job")
    if existing_job:
        job_id = existing_job.get("id", "unknown")
        job_state = existing_job.get("state", "unknown")
        if job_state == "done":
            logger.info("  Re-downloading existing job %s (FREE)...", job_id)
            final_job = existing_job
        else:
            logger.info("  Waiting for in-flight job %s (state=%s)...", job_id, job_state)
            try:
                final_job = base_client.wait_for_batch_job(
                    str(job_id),
                    args.dataset,
                    args.schema,
                    symbols_list,
                    start_str,
                    end_str,
                    timeout_minutes=args.timeout,
                )
                logger.info("  Job completed!")
            except Exception as e:
                _err = EnhancedError(
                    message=str(e),
                    category=ErrorCategory.SERVER_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                    correlation_id=str(uuid4()),
                    context=ErrorContext(extra={"exc_type": type(e).__name__}),
                )
                logger.error("Waiting for job failed: %s", e)
                sys.exit(1)
    else:
        logger.info("  Submitting NEW batch job (via base client)...")
        try:
            new_job = base_client.submit_batch_job(
                dataset=args.dataset,
                schema=args.schema,
                symbols=symbols_list,
                stype_in=args.stype_in,
                start=start_str,
                end=end_str,
            )
            job_id = new_job.get("id") if isinstance(new_job, dict) else getattr(new_job, "id", "unknown")
            logger.info("  Submitted job: %s", job_id)
            logger.info("  Waiting for completion (timeout: %s min)...", args.timeout)
            final_job = base_client.wait_for_batch_job(
                str(job_id),
                args.dataset,
                args.schema,
                symbols_list,
                start_str,
                end_str,
                timeout_minutes=args.timeout,
            )
            logger.info("  Job completed!")
        except Exception as e:
            _err = EnhancedError(
                message=str(e),
                category=ErrorCategory.SERVER_ERROR,
                severity=ErrorSeverity.MEDIUM,
                recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
                correlation_id=str(uuid4()),
                context=ErrorContext(extra={"exc_type": type(e).__name__}),
            )
            logger.error("Batch job submission/wait failed: %s", e)
            sys.exit(1)
    # ---- Step 6: Report actual cost of the job ----
    print_separator("STEP 5: Actual Job Cost")

    # Use the batch client for this key index to refresh job info
    job_id = final_job.get("id") if isinstance(final_job, dict) else getattr(final_job, "id", "unknown")
    batch_client = base_client._get_client_for_batch(args.dataset, args.schema, symbols_list, start_str, end_str)
    try:
        jobs = batch_client.batch.list_jobs(states=["done"])
        refreshed_job = next(
            (j for j in jobs if (j.get("id") if isinstance(j, dict) else getattr(j, "id", None)) == job_id),
            final_job,
        )
    except Exception as e:
        _err = EnhancedError(
            message=str(e),
            category=ErrorCategory.SERVER_ERROR,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=ErrorRecoveryStrategy.FALLBACK,
            correlation_id=str(uuid4()),
            context=ErrorContext(extra={"exc_type": type(e).__name__}),
        )
        refreshed_job = final_job
    actual_cost = (
        refreshed_job.get("cost_usd") if isinstance(refreshed_job, dict) else getattr(refreshed_job, "cost_usd", None)
    )
    actual_billed = (
        refreshed_job.get("billed_size")
        if isinstance(refreshed_job, dict)
        else getattr(refreshed_job, "billed_size", None)
    )

    logger.info("  Job ID:        %s", job_id)
    logger.info("  Actual cost:   %s", format_cost(actual_cost))
    logger.info("  Billed size:   %s", f"{actual_billed:,} bytes" if actual_billed else "N/A")
    dataset_val = (
        refreshed_job.get("dataset", "N/A")
        if isinstance(refreshed_job, dict)
        else getattr(refreshed_job, "dataset", "N/A")
    )
    schema_val = (
        refreshed_job.get("schema", "N/A")
        if isinstance(refreshed_job, dict)
        else getattr(refreshed_job, "schema", "N/A")
    )
    symbols_val = (
        refreshed_job.get("symbols", "N/A")
        if isinstance(refreshed_job, dict)
        else getattr(refreshed_job, "symbols", "N/A")
    )
    start_val = (
        refreshed_job.get("start", "N/A") if isinstance(refreshed_job, dict) else getattr(refreshed_job, "start", "N/A")
    )
    end_val = (
        refreshed_job.get("end", "N/A") if isinstance(refreshed_job, dict) else getattr(refreshed_job, "end", "N/A")
    )
    logger.info("  Dataset:       %s", dataset_val)
    logger.info("  Schema:        %s", schema_val)
    logger.info("  Symbols:       %s", symbols_val)
    logger.info("  Start:         %s", start_val)
    logger.info("  End:           %s", end_val)

    # ---- Step 7: Summary ----
    print_separator("SUMMARY")
    if estimated_cost is not None and actual_cost is not None:
        logger.info("  Estimated cost (get_cost):   %s", format_cost(estimated_cost))
        logger.info("  Actual cost (batch job):     %s", format_cost(actual_cost))
        if estimated_cost > 0 and actual_cost == 0:
            logger.info("  This was a FREE re-download of an existing batch job!")
            logger.info("  You saved %s by reusing the batch result.", format_cost(estimated_cost))
        elif actual_cost > 0:
            logger.info("  This was a FRESH batch job (first download).")
    else:
        logger.info("  Estimated cost: %s", format_cost(estimated_cost))
        logger.info("  Actual cost:    %s", format_cost(actual_cost))

    logger.info("  -------------------------------------------------------")
    logger.info("  Run this script again with the same --date to test")
    logger.info("  FREE re-download (batch jobs are free within 30 days):")
    logger.info("    python scripts/test_batch_cost_comparison.py --date %s --execute", args.date)
    logger.info("  -------------------------------------------------------")


if __name__ == "__main__":
    main()
