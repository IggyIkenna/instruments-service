"""Instrument Writer for Instruments Service.

Extends BaseGCSWriter from unified-cloud-services with instrument specific path structure.
"""

import logging
from datetime import datetime

from unified_cloud_services.io import BaseGCSWriter

from instruments_service.config import get_service_config
from instruments_service.schemas.output_schemas import INSTRUMENTS_SCHEMA

logger = logging.getLogger(__name__)


class InstrumentWriter(BaseGCSWriter):
    """
    Writer for instrument definitions.

    Extends BaseGCSWriter with instrument specific path:
    gs://{bucket}/by_date/day={date}/instrument_id={instrument_id}.parquet
    """

    def __init__(self, category: str, dry_run: bool = False):
        """
        Initialize instrument writer.

        Args:
            category: Market category (CEFI, TRADFI, DEFI)
            dry_run: If True, write to local data/sample/ instead of GCS
        """
        config = get_service_config()
        bucket = f"instruments-store-{category.lower()}-{config.gcp_project_id}"

        super().__init__(
            bucket_name=bucket,
            schema=INSTRUMENTS_SCHEMA,
            dry_run=dry_run,
            validate_schema=True,
        )

        self.category = category.upper()
        logger.info(f"Initialized InstrumentWriter: category={category}, dry_run={dry_run}")

    def build_path(
        self,
        date: datetime,
        instrument_id: str,
    ) -> str:
        """
        Build GCS path for instrument output.

        Args:
            date: Processing date
            instrument_id: Instrument identifier

        Returns:
            Relative path: by_date/day={date}/instrument_id={instrument_id}.parquet
        """
        date_str = date.strftime("%Y-%m-%d")
        return f"by_date/day={date_str}/instrument_id={instrument_id}.parquet"
