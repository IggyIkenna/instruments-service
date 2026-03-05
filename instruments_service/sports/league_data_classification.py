"""League classification raw data.

LEAGUE_CLASSIFICATION_DATA: dict mapping API-Football league ID to
classification metadata. Extracted from league_classification.py to keep
the registry module within the 900-line file size limit.
"""

from __future__ import annotations

from .league_data_classification_a import LEAGUE_CLASSIFICATION_DATA_A
from .league_data_classification_b import LEAGUE_CLASSIFICATION_DATA_B

LEAGUE_CLASSIFICATION_DATA = {**LEAGUE_CLASSIFICATION_DATA_A, **LEAGUE_CLASSIFICATION_DATA_B}
