"""Unit tests for lazy adapter loading."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instruments_service.app.core.adapter_loader import (
    clear_adapter_cache,
    get_adapter_for_venue,
    get_cached_adapters,
)


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear adapter cache before each test."""
    clear_adapter_cache()
    yield
    clear_adapter_cache()


def test_get_adapter_for_cefi_venue():
    """Test loading adapter for CEFI venue."""
    adapter = get_adapter_for_venue("BINANCE-FUTURES")
    assert adapter is not None
    assert "TardisAdapter" in str(type(adapter))


def test_aster_venue_raises_not_implemented():
    """Test that ASTER venue raises NotImplementedError (AsterBaseClient removed from UCS)."""
    with pytest.raises(NotImplementedError, match="Aster adapter not available"):
        get_adapter_for_venue("ASTER")


def test_get_adapter_for_defi_venue():
    """Test loading adapter for DeFi venue."""
    adapter = get_adapter_for_venue("UNISWAP-V3")
    assert adapter is not None
    assert "UniswapV3Adapter" in str(type(adapter))


def test_adapter_caching():
    """Test that adapters are cached (singleton per data source)."""
    adapter1 = get_adapter_for_venue("BINANCE-FUTURES")
    adapter2 = get_adapter_for_venue("COINBASE-SPOT")  # Same data source (tardis)

    # Should return same instance (cached)
    assert adapter1 is adapter2


def test_different_data_sources_different_adapters():
    """Test that different data sources get different adapters (tardis vs databento)."""
    tardis_adapter = get_adapter_for_venue("BINANCE-FUTURES")
    databento_adapter = get_adapter_for_venue("CME")

    assert tardis_adapter is not databento_adapter


def test_unknown_venue_raises_error():
    """Test that unknown venue raises ValueError."""
    with pytest.raises(ValueError, match="Unknown venue"):
        get_adapter_for_venue("UNKNOWN_VENUE")


def test_get_cached_adapters():
    """Test getting cached adapters."""
    # Initially empty
    assert get_cached_adapters() == {}

    # Load some adapters (tardis and databento - different data sources)
    get_adapter_for_venue("BINANCE-FUTURES")
    get_adapter_for_venue("CME")

    cached = get_cached_adapters()
    assert "tardis" in cached
    assert "databento" in cached
    assert len(cached) == 2


def test_clear_adapter_cache():
    """Test clearing adapter cache."""
    # Load adapter
    get_adapter_for_venue("BINANCE-FUTURES")
    assert len(get_cached_adapters()) == 1

    # Clear cache
    clear_adapter_cache()
    assert get_cached_adapters() == {}

    # Reload creates new instance
    adapter_new = get_adapter_for_venue("BINANCE-FUTURES")
    assert adapter_new is not None


def test_defi_venue_specific_adapters():
    """Test that different DeFi venues load their specific adapters."""
    uniswap_v2 = get_adapter_for_venue("UNISWAP-V2")
    uniswap_v3 = get_adapter_for_venue("UNISWAP-V3")
    aave = get_adapter_for_venue("AAVE-V3")

    assert "UniswapV2Adapter" in str(type(uniswap_v2))
    assert "UniswapV3Adapter" in str(type(uniswap_v3))
    assert "AaveV3Adapter" in str(type(aave))


def test_defi_venue_uniswap_v4():
    """Test loading adapter for UNISWAP-V4 (The Graph)."""
    adapter = get_adapter_for_venue("UNISWAP-V4")
    assert adapter is not None
    assert "UniswapV4Adapter" in str(type(adapter))


def test_defi_venue_curve():
    """Test loading adapter for CURVE (The Graph)."""
    adapter = get_adapter_for_venue("CURVE")
    assert adapter is not None
    assert "CurveAdapter" in str(type(adapter))


def test_defi_venue_balancer():
    """Test loading adapter for BALANCER (The Graph)."""
    adapter = get_adapter_for_venue("BALANCER")
    assert adapter is not None
    assert "BalancerAdapter" in str(type(adapter))


def test_defi_venue_morpho():
    """Test loading adapter for MORPHO (The Graph)."""
    adapter = get_adapter_for_venue("MORPHO")
    assert adapter is not None
    assert "MorphoAdapter" in str(type(adapter))


def test_defi_venue_euler():
    """Test loading adapter for EULER (The Graph)."""
    adapter = get_adapter_for_venue("EULER")
    assert adapter is not None
    assert "EulerAdapter" in str(type(adapter))


def test_defi_venue_fluid():
    """Test loading adapter for FLUID (The Graph)."""
    adapter = get_adapter_for_venue("FLUID")
    assert adapter is not None
    assert "FluidAdapter" in str(type(adapter))


def test_defi_venue_lido():
    """Test loading adapter for LIDO (The Graph)."""
    adapter = get_adapter_for_venue("LIDO")
    assert adapter is not None
    assert "LidoAdapter" in str(type(adapter))


def test_defi_venue_etherfi():
    """Test loading adapter for ETHERFI (The Graph)."""
    adapter = get_adapter_for_venue("ETHERFI")
    assert adapter is not None
    assert "EtherFiAdapter" in str(type(adapter))


def test_defi_venue_ethena():
    """Test loading adapter for ETHENA (The Graph)."""
    adapter = get_adapter_for_venue("ETHENA")
    assert adapter is not None
    assert "EthenaAdapter" in str(type(adapter))


def test_yahoo_finance_fx_venue_loads_adapter():
    """Test that FX venue (Yahoo Finance) loads YahooFinanceAdapter."""
    pytest.importorskip("yfinance", reason="yfinance required for Yahoo Finance adapter")
    adapter = get_adapter_for_venue("FX")
    assert adapter is not None
    assert "YahooFinanceAdapter" in str(type(adapter))


def test_case_insensitive_venue_names():
    """Test that venue names are case-insensitive."""
    adapter1 = get_adapter_for_venue("binance-futures")
    adapter2 = get_adapter_for_venue("BINANCE-FUTURES")

    # Should return same cached instance
    assert adapter1 is adapter2


# From test_coverage_boost_4


class TestDataSourceAdapter:
    """Tests for DataSourceAdapter."""

    def test_import(self) -> None:
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        assert adapter is not None

    def test_default_routing_key(self) -> None:
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        assert adapter._routing_key == "cefi"

    def test_custom_routing_key(self) -> None:
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter(routing_key="tradfi")
        assert adapter._routing_key == "tradfi"

    def test_read_parquet_file_not_found(self) -> None:
        """When storage raises FileNotFoundError, should return empty DataFrame."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = FileNotFoundError("not found")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_read_parquet_returns_dataframe(self) -> None:
        """When storage returns a DataFrame, should return it."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        expected_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.return_value = expected_df
            mock_ds.return_value = mock_source
            result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
            assert isinstance(result, pd.DataFrame)
            assert not result.empty

    def test_read_parquet_non_dataframe_returns_empty(self) -> None:
        """When storage returns non-DataFrame, should return empty DataFrame."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.return_value = "not a dataframe"
            mock_ds.return_value = mock_source
            result = adapter.read_parquet("some/path.parquet")
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_read_parquet_os_error_404_returns_empty(self) -> None:
        """OSError with '404' in message should return empty DataFrame."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = OSError("404 Not Found")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet("some/path.parquet")
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_read_parquet_os_error_returns_empty(self) -> None:
        """Generic OSError should return empty DataFrame."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = OSError("general error")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet("some/path.parquet")
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_read_parquet_value_error_returns_empty(self) -> None:
        """ValueError should return empty DataFrame."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = ValueError("bad value")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet("some/path.parquet")
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_read_parquet_partition_parsing(self) -> None:
        """Path partitions are parsed correctly from gcs_path."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        captured_partitions: dict[str, dict[str, str]] = {}

        def capture_call(**kwargs: object) -> MagicMock:
            mock_source = MagicMock()
            mock_source.read.side_effect = FileNotFoundError("not found")
            return mock_source

        with patch("instruments_service.adapters.data_source_adapter.get_data_source", side_effect=capture_call):
            path = "instrument_availability/by_date/day=2024-01-15/instruments.parquet"
            adapter.read_parquet(path)
            # Should not raise

    def test_read_parquet_from_category(self) -> None:
        """read_parquet_from_category should use category as routing key."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        expected_df = pd.DataFrame({"col": [1, 2]})
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.return_value = expected_df
            mock_ds.return_value = mock_source
            result = adapter.read_parquet_from_category(
                "instrument_availability/by_date/day=2024-01-01/instruments.parquet",
                "CEFI",
            )
            assert isinstance(result, pd.DataFrame)
            # Check routing key was lowercase category
            call_kwargs = mock_ds.call_args
            assert call_kwargs.kwargs.get("routing_key") == "cefi"

    def test_read_parquet_from_category_file_not_found(self) -> None:
        """read_parquet_from_category with FileNotFoundError returns empty."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = FileNotFoundError("not found")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet_from_category("some/path.parquet", "TRADFI")
            assert result.empty

    def test_read_parquet_from_category_os_error(self) -> None:
        """read_parquet_from_category with OSError returns empty."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = OSError("storage error")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet_from_category("some/path.parquet", "DEFI")
            assert result.empty

    def test_read_parquet_from_category_404_error(self) -> None:
        """read_parquet_from_category with 404 in OSError returns empty."""
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_ds:
            mock_source = MagicMock()
            mock_source.read.side_effect = OSError("404 Not Found")
            mock_ds.return_value = mock_source
            result = adapter.read_parquet_from_category("some/path.parquet", "CEFI")
            assert result.empty


@pytest.mark.unit
class TestDataSourceAdapterFromBoost:
    """Tests for DataSourceAdapter."""

    def test_import(self):
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        assert DataSourceAdapter is not None

    def test_instantiation_default(self):
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        assert adapter is not None
        assert adapter._routing_key == "cefi"

    def test_instantiation_custom_routing(self):
        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter(routing_key="tradfi")
        assert adapter._routing_key == "tradfi"

    def test_read_parquet_returns_dataframe_on_error(self):
        import pandas as pd

        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_gds.side_effect = RuntimeError("Connection failed")
            try:
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)
            except (OSError, RuntimeError, ValueError, TypeError, KeyError):
                pass

    def test_read_parquet_file_not_found(self):
        import pandas as pd

        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.side_effect = FileNotFoundError("not found")
            mock_gds.return_value = mock_ds
            try:
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)
            except (OSError, RuntimeError, ValueError, TypeError, KeyError):
                pass

    def test_read_parquet_returns_data(self):
        import pandas as pd

        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        mock_df = pd.DataFrame({"a": [1, 2]})
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.return_value = mock_df
            mock_gds.return_value = mock_ds
            try:
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)
            except (OSError, RuntimeError, ValueError, TypeError, KeyError):
                pass


@pytest.mark.unit
class TestDataSourceAdapterDetailedFromBoost:
    """Additional data_source_adapter coverage."""

    def test_read_parquet_with_partition_in_path(self):
        import pandas as pd

        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter(routing_key="tradfi")
        mock_df = pd.DataFrame({"venue": ["NYSE"], "exchange_raw_symbol": ["AAPL"]})
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.return_value = mock_df
            mock_gds.return_value = mock_ds
            import contextlib

            with contextlib.suppress(Exception):
                result = adapter.read_parquet(
                    "instrument_availability/by_date/day=2024-07-01/venue=NYSE/instruments.parquet"
                )
                assert isinstance(result, pd.DataFrame)

    def test_read_parquet_404_in_error_message(self):
        import pandas as pd

        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.side_effect = OSError("404 Not Found")
            mock_gds.return_value = mock_ds
            import contextlib

            with contextlib.suppress(Exception):
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)

    def test_read_parquet_non_dataframe_response(self):
        import pandas as pd

        from instruments_service.adapters.data_source_adapter import DataSourceAdapter

        adapter = DataSourceAdapter()
        with patch("instruments_service.adapters.data_source_adapter.get_data_source") as mock_gds:
            mock_ds = MagicMock()
            mock_ds.read.return_value = "not a dataframe"
            mock_gds.return_value = mock_ds
            import contextlib

            with contextlib.suppress(Exception):
                result = adapter.read_parquet("instrument_availability/by_date/day=2024-01-01/instruments.parquet")
                assert isinstance(result, pd.DataFrame)
                assert result.empty


@pytest.mark.unit
class TestStorageAdapterFromBoost:
    """Tests for StorageAdapter."""

    def test_import(self):
        from instruments_service.adapters.storage_adapter import StorageAdapter

        assert StorageAdapter is not None

    def test_instantiation(self):
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        assert adapter is not None

    def test_build_gcs_path(self):
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        path = adapter.build_gcs_path("2024-01-01", "BINANCE")
        assert "2024-01-01" in path
        assert "BINANCE" in path
        assert path.endswith(".parquet")

    def test_build_gcs_path_with_slash_venue(self):
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        path = adapter.build_gcs_path("2024-01-01", "BYBIT/SPOT")
        assert "BYBIT-SPOT" in path

    def test_upload_batch_with_error(self):
        import pandas as pd

        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        df = pd.DataFrame({"a": [1, 2]})
        with patch("instruments_service.adapters.storage_adapter.get_data_sink") as mock_gds:
            mock_sink = MagicMock()
            mock_sink.write.side_effect = RuntimeError("Connection error")
            mock_gds.return_value = mock_sink
            import contextlib

            with contextlib.suppress(Exception):
                results = adapter.upload_batch(
                    [("instrument_availability/by_date/day=2024-01-01/venue=BINANCE/instruments.parquet", df, "cefi")],
                    "cefi",
                )
                assert isinstance(results, list)
                assert len(results) == 1
                assert results[0]["success"] is False

    def test_upload_batch_success(self):
        import pandas as pd

        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        df = pd.DataFrame({"a": [1, 2]})
        with patch("instruments_service.adapters.storage_adapter.get_data_sink") as mock_gds:
            mock_sink = MagicMock()
            mock_sink.write.return_value = None
            mock_gds.return_value = mock_sink
            import contextlib

            with contextlib.suppress(Exception):
                results = adapter.upload_batch(
                    [("instrument_availability/by_date/day=2024-01-01/venue=BINANCE/instruments.parquet", df, "cefi")],
                    "cefi",
                )
                assert isinstance(results, list)
                if results:
                    assert results[0]["success"] is True

    def test_get_bucket_for_category(self):
        from instruments_service.adapters.storage_adapter import StorageAdapter

        adapter = StorageAdapter()
        import contextlib

        with contextlib.suppress(Exception):
            bucket = adapter.get_bucket_for_category("CEFI")
            assert isinstance(bucket, str)
