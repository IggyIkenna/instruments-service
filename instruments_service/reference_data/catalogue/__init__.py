"""Per-asset-group canonical instrument catalogue.

Produces lists of UAC ``InstrumentRecord`` with populated
``available_from_datetime`` / ``available_to_datetime`` and canonical
``instrument_key`` (built via UAC ``build_instrument_id``) for downstream
point-in-time lookups via ``get_instruments_available_on``.

The catalogue is written to
``gs://{instruments-bucket}/reference_data/instruments/{asset_group}/all.parquet``
by :class:`CatalogueBuilder`.
"""

from .catalogue_builder import CATALOGUE_SUPPORTED_ASSET_GROUPS, CatalogueBuilder

__all__ = ["CATALOGUE_SUPPORTED_ASSET_GROUPS", "CatalogueBuilder"]
