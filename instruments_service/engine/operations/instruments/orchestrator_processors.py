"""
Orchestrator Market Processors

Contains processing logic for different market types (CeFi, TradFi, DeFi).
Split from orchestrator.py for better maintainability.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, cast

from unified_market_interface import InstrumentDefinition, get_adapter

from instruments_service.config import (
    DEFI_PROTOCOLS,
    DEFI_VENUE_TO_PROTOCOL,
    UnifiedInstrumentConfig,
)
from instruments_service.engine.venues.special_instruments import (
    create_bitcoin_etf_instrument_definition,
    create_krwusd_instrument_definition,
    create_vix_instrument_definition,
    get_us_equity_trading_hours,
)

logger = logging.getLogger(__name__)


class MarketProcessors:
    """
    Market-specific processing logic for different market types.

    Handles CeFi, TradFi, and DeFi instrument processing.
    """

    async def process_cefi(
        self, date: datetime, exchanges: list[str] | None, force: bool, venues_filter: list[str]
    ) -> dict[str, InstrumentDefinition]:
        """
        Process CeFi (Tardis) exchanges.

        Args:
            date: Target date for instruments
            exchanges: List of exchanges to process
            force: Force refresh
            venues_filter: Venue filter list

        Returns:
            Dictionary of instrument definitions
        """
        cefi_exchanges: list[str] = (
            cast(list[str], exchanges)
            if exchanges is not None
            else cast(list[str], self.venue_mapping.all_tardis_exchanges)
        )

        # Apply venue filtering for CEFI
        if venues_filter:
            tardis_venue_values: set[str] = set(cast(dict[str, str], self.venue_mapping.tardis_to_venue).values())
            cefi_venues: list[str] = [v for v in venues_filter if v in tardis_venue_values]

            if cefi_venues:
                venue_to_exchanges: dict[str, list[str]] = cast(
                    dict[str, list[str]],
                    self.venue_mapping.get_venue_to_tardis_exchanges(),
                )

                filtered_exchanges: list[str] = []
                for canonical_venue in cefi_venues:
                    if canonical_venue in venue_to_exchanges:
                        raw_exchanges_for_venue: list[str] = venue_to_exchanges[canonical_venue]
                        for raw_exchange in raw_exchanges_for_venue:
                            if raw_exchange in cefi_exchanges and raw_exchange not in filtered_exchanges:
                                filtered_exchanges.append(raw_exchange)
                                logger.debug(f"  Mapped {canonical_venue} -> raw exchange {raw_exchange}")

                if filtered_exchanges:
                    cefi_exchanges = filtered_exchanges
                    logger.info(f"🔍 Filtered CEFI exchanges by canonical venues {cefi_venues}: {cefi_exchanges}")
                else:
                    logger.warning(f"⚠️ No matching CEFI exchanges found for canonical venues: {cefi_venues}")
                    cefi_exchanges = []
            else:
                logger.info("🔍 No CEFI venues in filter, skipping CEFI processing")
                cefi_exchanges = []
        else:
            logger.debug(f"🔍 No venue filter specified, processing exchanges: {cefi_exchanges}")

        if cefi_exchanges:
            # Pre-flight check: Validate venue access
            cefi_exchanges = await self._check_venue_access(cefi_exchanges)

        all_instruments: dict[str, InstrumentDefinition] = {}

        if not cefi_exchanges:
            logger.info("⏭️ Skipping CEFI processing - no exchanges to process")
        else:
            logger.info(f"🚀 Processing {len(cefi_exchanges)} CeFi exchanges in parallel...")

            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)

            async def process_single_exchange(exchange: str) -> dict[str, InstrumentDefinition]:
                try:
                    logger.info(f"🔍 Processing CeFi exchange {exchange}...")
                    adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
                    instruments_raw = await adapter.fetch_instruments(
                        exchange=exchange,
                        target_date=date,
                        force_refresh=force,
                        normalize=True,
                    )
                    result: dict[str, InstrumentDefinition] = {}
                    for d in cast(list[dict[str, Any]], instruments_raw):
                        try:
                            inst = InstrumentDefinition(**d)
                            key = cast(str, d.get("instrument_key") or inst.instrument_key)
                            result[key] = inst
                        except Exception as e:
                            logger.warning(
                                f"Failed to create InstrumentDefinition for {d.get('instrument_key', 'unknown')}: {e}"
                            )
                    if result:
                        logger.info(f"✅ Processed {len(result)} instruments from {exchange}")
                    return result
                except Exception as e:
                    logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
                    return {}

            results: list[dict[str, InstrumentDefinition]] = await asyncio.gather(
                *[process_single_exchange(ex) for ex in cefi_exchanges],
                return_exceptions=False,
            )

            for result in results:
                if result:
                    all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        # Process on-chain CLOB venues
        all_instruments.update(await self._process_onchain_clob(date, venues_filter))

        return all_instruments

    async def process_tradfi(
        self, date: datetime, venues_filter: list[str], tradfi_venues: list[str] | None
    ) -> dict[str, InstrumentDefinition]:
        """
        Process TradFi (Databento) exchanges.

        Args:
            date: Target date for instruments
            venues_filter: Venue filter list
            tradfi_venues: Specific TradFi venues to process

        Returns:
            Dictionary of instrument definitions
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            databento_config = UnifiedInstrumentConfig()
            all_databento_exchanges: list[str] = cast(list[str], self.venue_mapping.all_databento_venues)

            databento_exchanges: list[str]
            if tradfi_venues:
                filtered_tradfi_venues = [v for v in tradfi_venues if v in all_databento_exchanges]
                if filtered_tradfi_venues:
                    databento_exchanges = filtered_tradfi_venues
                    logger.info(f"🔍 Processing specified TradFi venues: {databento_exchanges}")
                else:
                    logger.warning(
                        f"⚠️ No valid TradFi venues in tradfi_venues={tradfi_venues}, skipping TRADFI processing"
                    )
                    databento_exchanges = []
            elif venues_filter:
                filtered_tradfi_venues = [v for v in venues_filter if v in all_databento_exchanges]
                if filtered_tradfi_venues:
                    databento_exchanges = filtered_tradfi_venues
                    logger.info(f"🔍 Filtered TRADFI exchanges by venues: {databento_exchanges}")
                else:
                    logger.info("🔍 No TRADFI venues in filter, skipping TRADFI processing")
                    databento_exchanges = []
            else:
                databento_exchanges = ["CME", "ICE", "CBOE", "NASDAQ", "NYSE", "FX"]
                logger.info(f"🔍 No venue filter, processing default TRADFI exchanges: {databento_exchanges}")

            if not databento_exchanges:
                logger.info("⏭️ Skipping TRADFI processing - no exchanges to process")
            else:
                logger.info(f"🚀 Processing {len(databento_exchanges)} TradFi venues...")

                results = await asyncio.gather(
                    *[self._process_databento_exchange(ex, date, databento_config) for ex in databento_exchanges],
                    return_exceptions=False,
                )

                for result in results:
                    if result:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], result))

        except Exception as e:
            logger.error(f"❌ Failed to initialize Databento processing: {e}", exc_info=True)

        return all_instruments

    async def process_defi(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """
        Process DeFi protocols.

        Args:
            date: Target date for instruments
            venues_filter: Venue filter list

        Returns:
            Dictionary of instrument definitions
        """
        all_instruments: dict[str, InstrumentDefinition] = {}

        try:
            if venues_filter:
                defi_venues: list[str] = [
                    v for v in venues_filter if v in cast(list[str], self.venue_mapping.all_defi_venues)
                ]

                if defi_venues:
                    defi_protocols: list[tuple[str, str | None]] = []
                    for venue in defi_venues:
                        venue_key: str = venue.upper() if venue else ""
                        if venue_key in DEFI_VENUE_TO_PROTOCOL:
                            protocol, chain = DEFI_VENUE_TO_PROTOCOL[venue_key]
                            if (protocol, chain) not in defi_protocols:
                                defi_protocols.append((protocol, chain))
                    if defi_protocols:
                        defi_protocol_names: list[str] = [p[0] for p in defi_protocols]
                        logger.info(f"🔍 Filtered DEFI protocols by venues {defi_venues}: {defi_protocol_names}")
                    else:
                        logger.warning(f"⚠️ No matching DEFI protocols found for venues: {defi_venues}")
                        defi_protocols = []
                else:
                    logger.info("🔍 No DEFI venues in filter, skipping DEFI processing")
                    defi_protocols = []
            else:
                defi_protocols = DEFI_PROTOCOLS
                logger.info("🔍 No venue filter specified, processing all DEFI protocols")

            if not defi_protocols:
                logger.info("⏭️ Skipping DEFI processing - no protocols to process")
            else:
                for protocol, chain in defi_protocols:
                    try:
                        if chain:
                            defi_instruments = self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                chain=chain,
                                target_date=date,
                            )
                        else:
                            defi_instruments = self.processing_service.fetch_defi_instruments(
                                protocol=protocol,
                                target_date=date,
                            )
                        if defi_instruments:
                            all_instruments.update(cast(dict[str, InstrumentDefinition], defi_instruments))
                            logger.info(f"✅ Processed {len(defi_instruments)} instruments from {protocol}")
                    except Exception as e:
                        logger.error(f"❌ Failed to process {protocol}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"❌ Failed to initialize DeFi processing: {e}", exc_info=True)

        return all_instruments

    async def _check_venue_access(self, cefi_exchanges: list[str]) -> list[str]:
        """Check venue access and filter out blocked venues."""
        adapter: object | None = None
        try:
            api_key = getattr(self.processing_service, "api_key", None)
            project_id = getattr(self.processing_service, "_tardis_project_id", None)
            tardis_adapter = get_adapter("tardis", "tradfi", api_key=api_key, project_id=project_id)
            if hasattr(tardis_adapter, "base_client"):
                base_client = tardis_adapter.base_client
                if hasattr(base_client, "check_venues_access"):
                    adapter = tardis_adapter
        except (ValueError, Exception) as e:
            logger.warning(f"⚠️ Cannot check venue access: {e}")

        if adapter is not None and hasattr(adapter, "base_client"):
            base_client = adapter.base_client
            if hasattr(base_client, "check_venues_access"):
                access_results: dict[str, tuple[bool, str]] = cast(
                    dict[str, tuple[bool, str]],
                    base_client.check_venues_access(cefi_exchanges),
                )
                blocked: list[str] = [ex for ex, (ok, _) in access_results.items() if not ok]
                if blocked:
                    for ex in blocked:
                        _, error_msg = access_results[ex]
                        logger.warning(f"⚠️ Venue access blocked: {ex} - {error_msg}")
                    accessible = [ex for ex in cefi_exchanges if ex not in blocked]
                    if not accessible:
                        logger.error(f"❌ All {len(cefi_exchanges)} CEFI venues blocked - skipping CEFI processing")
                        return []
                    else:
                        logger.warning(
                            f"⚠️ {len(blocked)}/{len(cefi_exchanges)} venues blocked, "
                            f"continuing with {len(accessible)} accessible venues"
                        )
                        return accessible

        return cefi_exchanges

    async def _process_onchain_clob(self, date: datetime, venues_filter: list[str]) -> dict[str, InstrumentDefinition]:
        """Process on-chain CLOB venues (Hyperliquid, Aster)."""
        all_instruments: dict[str, InstrumentDefinition] = {}

        cefi_onchain_clob_venues: list[str] = cast(list[str], self.venue_mapping.all_cefi_onchain_clob_venues)
        cefi_clob_protocols: list[tuple[str, None]] = []

        if venues_filter:
            for venue in venues_filter:
                if venue.upper() in cefi_onchain_clob_venues:
                    protocol_name: str = venue.lower()
                    cefi_clob_protocols.append((protocol_name, None))
        else:
            for venue in cefi_onchain_clob_venues:
                cefi_clob_protocols.append((venue.lower(), None))

        if cefi_clob_protocols:
            protocol_names: list[str] = [p[0] for p in cefi_clob_protocols]
            logger.info(f"🚀 Processing {len(cefi_clob_protocols)} on-chain CLOB venues (CEFI): {protocol_names}")
            for protocol, _chain in cefi_clob_protocols:
                try:
                    clob_instruments = self.processing_service.fetch_defi_instruments(
                        protocol=protocol,
                        target_date=date,
                    )
                    if clob_instruments:
                        all_instruments.update(cast(dict[str, InstrumentDefinition], clob_instruments))
                        logger.info(
                            f"✅ Processed {len(clob_instruments)} instruments from {protocol} (CEFI on-chain CLOB)"
                        )
                except Exception as e:
                    logger.error(f"❌ Failed to process on-chain CLOB {protocol}: {e}", exc_info=True)

        return all_instruments

    async def _process_databento_exchange(
        self, exchange: str, date: datetime, databento_config: UnifiedInstrumentConfig
    ) -> dict[str, InstrumentDefinition]:
        """Process a single Databento exchange."""
        try:
            if exchange == "CBOE":
                vix_def_dict: dict[str, Any] | None = create_vix_instrument_definition(date)
                if vix_def_dict:
                    vix_def = InstrumentDefinition(**vix_def_dict)
                    logger.info(f"✅ Created VIX: {vix_def.instrument_key}")
                    return {vix_def.instrument_key: vix_def}
                return {}
            elif exchange == "FX":
                krwusd_def_dict: dict[str, Any] | None = create_krwusd_instrument_definition(date)
                if krwusd_def_dict:
                    krwusd_def = InstrumentDefinition(**krwusd_def_dict)
                    logger.info(f"✅ Created KRW/USD: {krwusd_def.instrument_key}")
                    return {krwusd_def.instrument_key: krwusd_def}
                return {}
            elif exchange in ["NASDAQ", "NYSE"]:
                instruments: dict[str, InstrumentDefinition] = {}
                symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                bitcoin_etf_launch_date = datetime(2024, 1, 11, tzinfo=UTC)
                bitcoin_etf_tickers = ["IBIT", "FBTC", "ARKB"]

                if date >= bitcoin_etf_launch_date:
                    for ticker in bitcoin_etf_tickers:
                        if ticker in symbols:
                            etf_def_dict: dict[str, Any] | None = create_bitcoin_etf_instrument_definition(
                                ticker, date, get_us_equity_trading_hours
                            )
                            if etf_def_dict:
                                etf_def = InstrumentDefinition(**etf_def_dict)
                                instruments[etf_def.instrument_key] = etf_def
                                logger.info(f"✅ Created Bitcoin ETF: {etf_def.instrument_key}")

                non_btc_etf_symbols = [s for s in symbols if s not in bitcoin_etf_tickers]
                if non_btc_etf_symbols:
                    _raw_other: dict[str, InstrumentDefinition] = cast(
                        dict[str, InstrumentDefinition],
                        await self.processing_service.fetch_databento_instruments(
                            exchange=exchange,
                            symbols=non_btc_etf_symbols,
                            target_date=date,
                        )
                        or {},
                    )
                    if _raw_other:
                        instruments.update(_raw_other)
                        logger.info(f"✅ Processed {len(_raw_other)} additional instruments from {exchange}")

                if instruments:
                    logger.info(f"✅ Processed {len(instruments)} total instruments from {exchange}")
                return instruments
            else:
                # CME, ICE processing
                symbols: list[str] = databento_config.get_symbols_for_venue(exchange)

                if not symbols:
                    logger.warning(f"⚠️ No symbols configured for {exchange}")
                    return {}

                instruments = cast(
                    dict[str, InstrumentDefinition],
                    await self.processing_service.fetch_databento_instruments(
                        exchange=exchange,
                        symbols=symbols,
                        target_date=date,
                    )
                    or {},
                )

                if instruments:
                    logger.info(f"✅ Processed {len(instruments)} instruments from {exchange}")
                return instruments

        except Exception as e:
            logger.error(f"❌ Failed to process {exchange}: {e}", exc_info=True)
            return {}
