"""Typed runtime accessor for the orchestrator package namespace.

The orchestrator package keeps the exact single-namespace semantics of the
former monolithic ``engine/orchestrator.py``: every shared symbol (collaborator
imports, constants, mutable caches, sibling functions) is an attribute of the
PACKAGE module, and submodules resolve them through this proxy at call time.
That preserves ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching without a module-level import cycle:
the package module is fetched lazily from ``sys.modules`` on every access.
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import cast, final, override

_PKG_NAME = "instruments_service.engine.orchestrator"


def _pkg() -> ModuleType:
    return sys.modules[_PKG_NAME]


@final
class PackageNamespaceProxy:
    """Forwards attribute get/set to the live orchestrator package module."""

    @override
    def __setattr__(self, name: str, value: object) -> None:
        setattr(_pkg(), name, value)

    def __getattr__(self, name: str) -> object:
        return cast("object", getattr(_pkg(), name))


orch_namespace = PackageNamespaceProxy()
