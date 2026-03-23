#!/usr/bin/env python3
"""Geocode-venues CLI — add lat/lon to stadium mappings via Google Maps API.

Usage:
    python scripts/geocode_venues.py --secret-name GOOGLE_MAPS_API_KEY
    python scripts/geocode_venues.py --secret-name GOOGLE_MAPS_API_KEY --output venues_geocoded.json

Fetches API key from Secret Manager via secret_name. Requires GCP_PROJECT_ID.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Geocode stadium/venue names to lat/lon")
    parser.add_argument(
        "--secret-name", default="GOOGLE_MAPS_API_KEY", help="Secret Manager secret for Google Maps API key"
    )
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--dry-run", action="store_true", help="List venues without calling API")
    args = parser.parse_args()

    from unified_api_contracts.external.api_football import API_FOOTBALL_TO_CANONICAL_STADIUMS

    stadium_mappings = API_FOOTBALL_TO_CANONICAL_STADIUMS

    venues = list(stadium_mappings.values())
    logger.info("Venues to geocode: %d", len(venues))

    if args.dry_run:
        for v in sorted(set(venues)):
            logger.info(v)
        return 0

    from unified_config_interface import UnifiedCloudConfig

    config = UnifiedCloudConfig()
    project_id = config.gcp_project_id
    if not project_id or not str(project_id).strip():
        logger.error("GCP_PROJECT_ID required")
        return 1

    from unified_trading_library import get_secret_client

    api_key = get_secret_client(project_id=project_id, secret_name=args.secret_name)
    if not api_key:
        logger.error("Failed to fetch secret %s", args.secret_name)
        return 1

    try:
        import urllib.parse
        import urllib.request

        results: dict[str, dict[str, float]] = {}
        for venue in sorted(set(venues)):
            q = urllib.parse.quote(venue.replace("_", " "))
            url = f"https://maps.googleapis.com/maps/api/geocode/json?address={q}&key={api_key}"
            with urllib.request.urlopen(url) as resp:
                data = json.loads(resp.read())
            if data.get("results"):
                loc = data["results"][0]["geometry"]["location"]
                results[venue] = {"lat": loc["lat"], "lon": loc["lng"]}
            else:
                results[venue] = {"lat": 0.0, "lon": 0.0}
                logger.warning("No result for %s", venue)

        out = json.dumps(results, indent=2)
        if args.output:
            with open(args.output, "w") as f:
                f.write(out)
            logger.info("Wrote %s", args.output)
        else:
            logger.info(out)
        return 0
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
