#!/usr/bin/env python3
"""
Databento Batch API Cost Comparison Script

Compares the cost of a fresh batch download vs a re-download of the same data.
Key insight: Databento batch re-downloads within 30 days are FREE.

Uses DatabentoBaseClient from unified-cloud-services for:
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
import os
import sys
from datetime import datetime, timedelta

from unified_cloud_services import DatabentoBaseClient


def format_cost(cost_usd: float | None) -> str:
    """Format a dollar amount for display."""
    if cost_usd is None:
        return "N/A"
    if cost_usd == 0.0:
        return "$0.00 (FREE)"
    return f"${cost_usd:.4f}"


def print_separator(title: str = "") -> None:
    """Print a visual separator with optional title."""
    if title:
        print(f"\n{'=' * 60}")
        print(f"  {title}")
        print(f"{'=' * 60}")
    else:
        print(f"\n{'-' * 60}")


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
        print(f"[ERROR] Invalid date format: {args.date}. Use YYYY-MM-DD.")
        sys.exit(1)

    date_end = date_start + timedelta(days=1)
    start_str = date_start.strftime("%Y-%m-%d")
    end_str = date_end.strftime("%Y-%m-%d")
    symbols_list = [s.strip() for s in args.symbols.split(",")]

    # Initialize base client (handles API key resolution via Secret Manager + multi-key rotation)
    print_separator("DATABENTO BATCH COST COMPARISON")
    print(f"  Dataset:   {args.dataset}")
    print(f"  Symbols:   {symbols_list}")
    print(f"  Schema:    {args.schema}")
    print(f"  Stype-in:  {args.stype_in}")
    print(f"  Date:      {start_str} -> {end_str}")
    print(f"  Mode:      {'EXECUTE (will submit job)' if args.execute else 'DRY-RUN (estimate only)'}")

    # Use env var API key if available, otherwise let base client resolve via Secret Manager
    api_key = os.environ.get("DATABENTO_API_KEY")
    base_client = DatabentoBaseClient(api_key=api_key)

    # Also get a raw db.Historical for metadata calls (cost estimate, billable size)
    raw_client = base_client.client
    print("[OK] DatabentoBaseClient initialized (deterministic key selection enabled)")

    batch_key_index = base_client._get_batch_key_index(args.dataset, args.schema, symbols_list, start_str, end_str)
    print(f"[OK] Deterministic batch key index for these params: {batch_key_index}")

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
        print(f"  Estimated cost for FRESH download: {format_cost(estimated_cost)}")
    except Exception as e:
        print(f"  [WARN] get_cost() failed: {e}")
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
            print(f"  Billable size: {billable_size:,} bytes ({size_kb:.2f} KB / {size_mb:.4f} MB)")
        else:
            print("  Billable size: N/A")
    except Exception as e:
        print(f"  [WARN] get_billable_size() failed: {e}")
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
        print("  FOUND existing batch job!")
        print(f"    Job ID:      {job_id}")
        print(f"    State:       {job_state}")
        print(f"    Cost:        {format_cost(job_cost)}")
        print(f"    Billed size: {job_billed:,} bytes" if job_billed else "    Billed size: N/A")
        print()
        if job_state == "done":
            print("  --> Re-downloading this job is FREE (within 30 days)")
        else:
            print(f"  --> Job is still {job_state}, will wait for completion")
    else:
        print("  No existing batch job found for these parameters.")
        print("  A new batch job submission will incur the estimated cost above.")

    # ---- Step 4: Dry-run gate ----
    if not args.execute:
        print_separator("DRY-RUN COMPLETE")
        print("  No batch job was submitted (safe mode).")
        print()
        print("  To actually submit a batch job, re-run with --execute:")
        print(f"    python scripts/test_batch_cost_comparison.py --date {args.date} --execute")
        print()
        if existing_job:
            state = existing_job.get("state", "unknown")
            if state == "done":
                print("  NOTE: An existing done job was found. Re-running with --execute will")
                print("  re-download it for FREE (no new cost).")
            else:
                print(f"  NOTE: An existing {state} job was found. Re-running with --execute")
                print("  will wait for it to complete, then download (no new cost).")
        else:
            print(f"  WARNING: Submitting a new job will cost ~{format_cost(estimated_cost)}")
        return

    # ---- Step 5: Execute using base client's unified batch orchestration ----
    print_separator("STEP 4: Submitting / Re-downloading Batch Job")
    if existing_job:
        job_id = existing_job.get("id", "unknown")
        job_state = existing_job.get("state", "unknown")
        if job_state == "done":
            print(f"  Re-downloading existing job {job_id} (FREE)...")
            final_job = existing_job
        else:
            print(f"  Waiting for in-flight job {job_id} (state={job_state})...")
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
                print("  Job completed!")
            except Exception as e:
                print(f"  [ERROR] Waiting for job failed: {e}")
                sys.exit(1)
    else:
        print("  Submitting NEW batch job (via base client)...")
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
            print(f"  Submitted job: {job_id}")
            print(f"  Waiting for completion (timeout: {args.timeout} min)...")
            final_job = base_client.wait_for_batch_job(
                str(job_id),
                args.dataset,
                args.schema,
                symbols_list,
                start_str,
                end_str,
                timeout_minutes=args.timeout,
            )
            print("  Job completed!")
        except Exception as e:
            print(f"  [ERROR] Batch job submission/wait failed: {e}")
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
    except Exception:
        refreshed_job = final_job

    actual_cost = (
        refreshed_job.get("cost_usd") if isinstance(refreshed_job, dict) else getattr(refreshed_job, "cost_usd", None)
    )
    actual_billed = (
        refreshed_job.get("billed_size")
        if isinstance(refreshed_job, dict)
        else getattr(refreshed_job, "billed_size", None)
    )

    print(f"  Job ID:        {job_id}")
    print(f"  Actual cost:   {format_cost(actual_cost)}")
    print(f"  Billed size:   {actual_billed:,} bytes" if actual_billed else "  Billed size:   N/A")
    print(
        f"  Dataset:       {refreshed_job.get('dataset', 'N/A') if isinstance(refreshed_job, dict) else getattr(refreshed_job, 'dataset', 'N/A')}"
    )
    print(
        f"  Schema:        {refreshed_job.get('schema', 'N/A') if isinstance(refreshed_job, dict) else getattr(refreshed_job, 'schema', 'N/A')}"
    )
    print(
        f"  Symbols:       {refreshed_job.get('symbols', 'N/A') if isinstance(refreshed_job, dict) else getattr(refreshed_job, 'symbols', 'N/A')}"
    )
    print(
        f"  Start:         {refreshed_job.get('start', 'N/A') if isinstance(refreshed_job, dict) else getattr(refreshed_job, 'start', 'N/A')}"
    )
    print(
        f"  End:           {refreshed_job.get('end', 'N/A') if isinstance(refreshed_job, dict) else getattr(refreshed_job, 'end', 'N/A')}"
    )

    # ---- Step 7: Summary ----
    print_separator("SUMMARY")
    if estimated_cost is not None and actual_cost is not None:
        print(f"  Estimated cost (get_cost):   {format_cost(estimated_cost)}")
        print(f"  Actual cost (batch job):     {format_cost(actual_cost)}")
        if estimated_cost > 0 and actual_cost == 0:
            print()
            print("  This was a FREE re-download of an existing batch job!")
            print(f"  You saved {format_cost(estimated_cost)} by reusing the batch result.")
        elif actual_cost > 0:
            print()
            print("  This was a FRESH batch job (first download).")
    else:
        print(f"  Estimated cost: {format_cost(estimated_cost)}")
        print(f"  Actual cost:    {format_cost(actual_cost)}")

    print()
    print("  -------------------------------------------------------")
    print("  Run this script again with the same --date to test")
    print("  FREE re-download (batch jobs are free within 30 days):")
    print()
    print(f"    python scripts/test_batch_cost_comparison.py --date {args.date} --execute")
    print("  -------------------------------------------------------")


if __name__ == "__main__":
    main()
