"""Typed runtime accessor for the tardis adapter package namespace.

The tardis package keeps the exact single-namespace semantics of the former
monolithic ``adapters/cefi/tardis.py``: every shared symbol (collaborator
imports, constants, sibling functions) is an attribute of the PACKAGE module,
and submodules resolve them through this proxy at call time.  That preserves
``unittest.mock.patch("instruments_service.reference_data.adapters.cefi.tardis.<name>")``
targets and cross-module monkeypatching without a module-level import cycle:
the package module is fetched lazily from ``sys.modules`` on every access.
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import cast, final, override

_PKG_NAME = "instruments_service.reference_data.adapters.cefi.tardis"


def _pkg() -> ModuleType:
    return sys.modules[_PKG_NAME]


@final
class PackageNamespaceProxy:
    """Forwards attribute get/set to the live tardis package module."""

    @override
    def __setattr__(self, name: str, value: object) -> None:
        setattr(_pkg(), name, value)

    def __getattr__(self, name: str) -> object:
        return cast("object", getattr(_pkg(), name))


tardis_namespace = PackageNamespaceProxy()
