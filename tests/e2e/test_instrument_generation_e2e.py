"""
End-to-end test for instrument generation.

Tests the complete workflow:
1. Download instruments from Tardis API for 2023-05-23 to 2023-05-24
2. Upload to test bucket (instruments-store-test-*)
3. Verify authentication (GCP credentials + Secret Manager)
4. Verify CSV samples generated
5. Verify data integrity (can query back from test bucket)
"""

import pytest
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path

from instruments_service.app.core.instrument_processing_service import (
    InstrumentProcessingService,
)
from instruments_service.app.core.cloud_instrument_storage import CloudInstrumentStorage
from instruments_service.app.core.batch_processor import InstrumentBatchProcessor
from instruments_service.config import VenueMapping
from unified_cloud_services import get_config


# Test configuration
START_DATE = datetime(2023, 5, 23, tzinfo=timezone.utc)
END_DATE = datetime(2023, 5, 24, tzinfo=timezone.utc)


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_instrument_generation_e2e(
    gcp_credentials,
    gcp_project_id,
    test_bucket_name,
    test_cloud_target,
    tardis_api_key,
    csv_sample_dir,
):
    """
    E2E test: Generate instruments for 2023-05-23 to 2023-05-24.

    Verifies:
    - Instrument download from Tardis API (via Secret Manager)
    - GCS upload to test bucket
    - Authentication works
    - CSV samples generated
    - Data integrity (can query back)
    """
    # Verify we're using test bucket (not prod)
    assert (
        "test" in test_bucket_name.lower()
    ), f"Must use test bucket for E2E test, got {test_bucket_name}"

    # Verify API key retrieved from Secret Manager
    assert tardis_api_key is not None, "Tardis API key must be retrieved from Secret Manager"
    assert len(tardis_api_key) > 0, "API key must not be empty"

    # Initialize services
    # Note: Service will use Secret Manager automatically if tardis_api_key not provided
    # We pass it here for explicit testing, but in production it's not needed
    config = {
        "project_id": gcp_project_id,
        "tardis_api_key": tardis_api_key,  # Explicit for testing, but Secret Manager works without this
        "enable_ccxt_integration": False,  # Disable for faster testing
        "enable_metadata_caching": True,
    }

    processing_service = InstrumentProcessingService(config)
    # Use test_cloud_target fixture to ensure test bucket is used
    storage = CloudInstrumentStorage(cloud_target=test_cloud_target)
    batch_processor = InstrumentBatchProcessor(config)

    # Verify test bucket is being used
    assert (
        storage.cloud_target.gcs_bucket == test_bucket_name
    ), f"Storage must use test bucket {test_bucket_name}, got {storage.cloud_target.gcs_bucket}"

    # Get all supported exchanges
    venue_mapping = VenueMapping()
    all_exchanges = venue_mapping.all_tardis_exchanges

    # Process instruments for each date in range
    all_instruments = {}
    dates_processed = []

    current_date = START_DATE
    while current_date <= END_DATE:
        dates_processed.append(current_date)

        # Generate instruments for all exchanges for this date
        date_instruments = await processing_service.generate_instruments_for_exchanges(
            exchanges=all_exchanges, target_date=current_date
        )

        all_instruments.update(date_instruments)

        # Convert to DataFrame
        if date_instruments:
            instruments_list = []
            for inst_key, inst_obj in date_instruments.items():
                if hasattr(inst_obj, "model_dump"):
                    instruments_list.append(inst_obj.model_dump())
                else:
                    instruments_list.append(inst_obj)

            instruments_df = pd.DataFrame(instruments_list)

            # Store to test bucket
            storage_result = storage.store_instruments(
                instruments_df=instruments_df,
                table_name="instruments_e2e_test",
                date=current_date,
            )

            assert (
                storage_result
            ), f"Failed to store instruments for {current_date.strftime('%Y-%m-%d')}"

            # Verify CSV sample was generated (if enabled)
            if get_config("ENABLE_CSV_SAMPLING", "false").lower() == "true":
                date_str = current_date.strftime("%Y%m%d")
                sample_files = list(csv_sample_dir.glob(f"instruments_{date_str}_*.csv"))
                assert len(sample_files) > 0, f"CSV sample should be generated for {date_str}"

        current_date += timedelta(days=1)

    # Verify we got instruments
    assert len(all_instruments) > 0, "Should have generated at least some instruments"

    # Verify we processed both dates
    assert len(dates_processed) == 2, f"Should process 2 dates, processed {len(dates_processed)}"

    # Verify data integrity: Query back from test bucket using GCS download
    # Since BigQuery queries were removed, use CloudDataProvider to download from GCS
    from instruments_service.app.core.cloud_data_provider import CloudDataProvider

    data_provider = CloudDataProvider(cloud_target=storage.cloud_target)

    # Download instruments for one of the processed dates
    test_date = dates_processed[0]
    queried_instruments = data_provider.get_instruments_from_gcs(test_date)

    assert (
        len(queried_instruments) > 0
    ), "Should be able to download instruments back from test bucket"

    # Verify test bucket isolation: Check that we're not writing to prod bucket
    prod_bucket = get_config("INSTRUMENTS_GCS_BUCKET", "market-data-tick")
    assert (
        storage.cloud_target.gcs_bucket != prod_bucket
    ), f"Must not write to prod bucket {prod_bucket}, got {storage.cloud_target.gcs_bucket}"

    print(f"\n✅ E2E Test Summary:")
    print(f"   - Dates processed: {len(dates_processed)}")
    print(f"   - Total instruments: {len(all_instruments)}")
    print(f"   - Test bucket: {storage.cloud_target.gcs_bucket}")
    print(f"   - Queried instruments: {len(queried_instruments)}")
    print(f"   - CSV samples: {len(list(csv_sample_dir.glob('instruments_*.csv')))}")


@pytest.mark.e2e
def test_test_bucket_isolation(test_bucket_name, prod_bucket_name):
    """Verify test bucket is different from prod bucket."""
    assert (
        test_bucket_name != prod_bucket_name
    ), f"Test bucket ({test_bucket_name}) must be different from prod bucket ({prod_bucket_name})"
    assert (
        "test" in test_bucket_name.lower()
    ), f"Test bucket name must contain 'test': {test_bucket_name}"


@pytest.mark.e2e
def test_secret_manager_access(tardis_api_key):
    """Verify Secret Manager access works."""
    assert tardis_api_key is not None, "Must be able to retrieve API key from Secret Manager"
    assert isinstance(tardis_api_key, str), "API key must be a string"
    assert len(tardis_api_key) > 10, "API key must be reasonable length"


@pytest.mark.e2e
def test_gcp_credentials(gcp_credentials):
    """Verify GCP credentials are configured."""
    assert gcp_credentials is not None, "GCP credentials must be configured"
    assert Path(gcp_credentials).exists(), f"Credentials file must exist: {gcp_credentials}"
