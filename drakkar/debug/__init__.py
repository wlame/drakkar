"""Debug web UI — public façade.

The implementation lives in :mod:`drakkar.debug.server`. This module
re-exports the public API so existing call sites continue to work.
"""

from __future__ import annotations

from drakkar.debug.server import DebugServer, create_debug_app

__all__ = ['DebugServer', 'create_debug_app']
