"""Instrument Writer for Instruments Service.

Extends BaseGCSWriter from unified-cloud-services with instrument specific path structure.
"""

import logging
from datetime import datetime

from unified_cloud_services import BaseGCSWriter

from instruments_service.config import InstrumentsServiceConfig, get_service_config
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
        config: InstrumentsServiceConfig = get_service_config()
        bucket = f"instruments-store-{category.lower()}-{config.gcp_project_id}"

        super().__init__(  # pyright: ignore[reportUnknownMemberType]
            bucket_name=bucket,
            schema=INSTRUMENTS_SCHEMA,
            dry_run=dry_run,
            validate_schema=True,
        )

        self.category = category.upper()
        logger.info(f"Initialized InstrumentWriter: category={category}, dry_run={dry_run}")

    def build_path(self, **kwargs: object) -> str:
        """
        Build GCS path for instrument output.

        Args (via kwargs):
            date: Processing date (datetime)
            instrument_id: Instrument identifier

        Returns:
            Relative path: by_date/day={date}/instrument_id={instrument_id}.parquet
        """
        date_val = kwargs.get("date")
        instrument_id = kwargs.get("instrument_id")
        if not isinstance(date_val, datetime):
            raise TypeError("build_path requires date: datetime")
        if not isinstance(instrument_id, str):
            raise TypeError("build_path requires instrument_id: str")
        date_str = date_val.strftime("%Y-%m-%d")
        return f"by_date/day={date_str}/instrument_id={instrument_id}.parquet"
