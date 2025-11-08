# Instruments Service Documentation

Welcome to the Instruments Service documentation. This directory contains comprehensive guides for using, developing, and maintaining the instruments-service.

## Quick Navigation

### Getting Started
- **[QUICK_START.md](./QUICK_START.md)** - Get started in 5 minutes
- **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** - Detailed setup and installation instructions

### Core Documentation
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Architecture documentation and design decisions
- **[usage/USAGE_GUIDE.md](./usage/USAGE_GUIDE.md)** - Comprehensive usage guide
- **[reference/API_REFERENCE.md](./reference/API_REFERENCE.md)** - Complete API reference

### Testing
- **[testing/TESTING.md](./testing/TESTING.md)** - Testing strategy and procedures
- **[testing/TEST_FAILURES_ANALYSIS.md](./testing/TEST_FAILURES_ANALYSIS.md)** - Test failure analysis
- **[testing/TESTING_GCP_SETUP.md](./testing/TESTING_GCP_SETUP.md)** - GCP setup for testing

### Feature Guides
- **[INSTRUMENT_KEY.md](./INSTRUMENT_KEY.md)** - Instrument ID format, implementation details, and DeFi enrichment patterns
- **[MVP_DEFI_INSTRUMENTS.md](./MVP_DEFI_INSTRUMENTS.md)** - DeFi instruments MVP guide
- **[DATABENTO_DEFI_INTEGRATION.md](./DATABENTO_DEFI_INTEGRATION.md)** - Databento & DeFi integration guide (TradFi and DeFi adapters)
- **[VENUE_ADAPTER_ARCHITECTURE.md](./VENUE_ADAPTER_ARCHITECTURE.md)** - Venue adapter pattern architecture

### Batch Processing
- **[batch_processing/BATCH_PROCESSING.md](./batch_processing/BATCH_PROCESSING.md)** - Batch processing guide

## Canonical References

- **[docs/INSTRUMENT_VENUE_SPECIFICATION.md](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md)** - Complete canonical instrument ID specification
- **[docs/UNIFIED_ARCHITECTURE_SPEC.md](../../docs/UNIFIED_ARCHITECTURE_SPEC.md)** - Complete system architecture
- **[docs/UNIFIED_REPOSITORY_STRUCTURE.md](../../docs/UNIFIED_REPOSITORY_STRUCTURE.md)** - Repository structure standards
- **[docs/DATA_ACCESS_PATTERNS.md](../../docs/DATA_ACCESS_PATTERNS.md)** - Data access patterns architecture

## Examples

See the `examples/` directory in the repository root for:
- Batch generation examples
- Query examples
- Usage patterns

## Related Services

- **unified-cloud-services**: Cloud infrastructure library (GCS, BigQuery, Secret Manager)
- **market-tick-data-handler**: Consumes instruments data
- **market-data-processing-service**: Consumes instruments data
- **features-data-service**: Consumes instruments data

---

*Last Updated: 2025-01-15*

## Recent Updates

### Databento & DeFi Integration (2025-01-15)
- ✅ Added Databento adapter for TradFi instruments (CME, NASDAQ, NYSE)
- ✅ Added The Graph integration for DeFi DEX pools (Uniswap V3, Curve)
- ✅ Added protocol SDKs integration (AAVE V3, EtherFi, Lido)
- ✅ Updated schema to support DeFi-specific fields (contract addresses, pool addresses)
- ✅ Integrated Secret Manager for secure API key management
- 📖 See [DATABENTO_DEFI_INTEGRATION.md](./DATABENTO_DEFI_INTEGRATION.md) for details

