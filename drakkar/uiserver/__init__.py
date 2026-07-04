"""Debug web UI — public façade.

The implementation lives in :mod:`drakkar.uiserver.server`. This module
re-exports the public API so existing call sites continue to work.
"""

from __future__ import annotations

from drakkar.uiserver.server import UIServer, create_ui_app

__all__ = ['UIServer', 'create_ui_app']
