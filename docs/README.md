# Instruments Service Documentation

Welcome to the Instruments Service documentation. This directory contains comprehensive guides for using, developing, and maintaining the instruments-service.

## Documentation Structure

### Getting Started
- **[QUICK_START.md](./QUICK_START.md)** - Get started in 5 minutes
- **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** - Detailed setup and installation instructions

### Core Documentation
- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Service architecture, design decisions, and system context
- **[INSTRUMENT_SPECIFICATION.md](./INSTRUMENT_SPECIFICATION.md)** - Complete instrument ID specification and formats
- **[VENUE_ADAPTERS.md](./VENUE_ADAPTERS.md)** - Venue adapter pattern and supported data sources
- **[DEFI_GUIDE.md](./DEFI_GUIDE.md)** - DeFi protocols, data sources, and integration guide

### Usage & Reference
- **[USAGE_GUIDE.md](./USAGE_GUIDE.md)** - Comprehensive usage examples for clients and developers
- **[API_REFERENCE.md](./API_REFERENCE.md)** - Complete API documentation

### Testing & Status
- **[TESTING.md](./TESTING.md)** - Testing strategy, procedures, and GCP setup
- **[STATUS.md](./STATUS.md)** - Current implementation status and quality gates

### MVP & Performance
- **[MVP_INSTRUMENTS.md](./MVP_INSTRUMENTS.md)** - MVP instrument lists and performance benchmarks

## Quick Links

**New to the service?** Start with [QUICK_START.md](./QUICK_START.md)

**Understanding instrument IDs?** See [INSTRUMENT_SPECIFICATION.md](./INSTRUMENT_SPECIFICATION.md)

**Working with DeFi?** See [DEFI_GUIDE.md](./DEFI_GUIDE.md)

**Adding a new venue?** See [VENUE_ADAPTERS.md](./VENUE_ADAPTERS.md)

## Related Services

- **unified-cloud-services**: Cloud infrastructure library (GCS, BigQuery, Secret Manager) - Required dependency
- **market-tick-data-handler**: Consumes instruments data for market data download
- **market-data-processing-service**: Consumes instruments data for feature generation
- **strategy-service**: Consumes instruments data for strategy execution

## Examples

See the `examples/` directory in the repository root for:
- Batch generation examples
- Query examples
- Usage patterns

---

*Last Updated: 2025-01-15*
