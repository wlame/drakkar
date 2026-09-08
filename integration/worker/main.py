"""Worker entry point — starts the Drakkar application.

Usage:
    WORKER_ID=worker-1 DK_CONFIG=/app/drakkar.yaml python main.py
    WORKER_HANDLER=http-search DK_CONFIG=/app/drakkar-http.yaml python main.py

One image serves every worker in the harness; ``WORKER_HANDLER`` picks
which handler it runs, and ``DK_CONFIG`` the config that matches it.
"""

import os
import sys
from collections.abc import Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from handler import RipgrepHandler
from http_handler import HttpSearchHandler

import drakkar as dk

# Handler registry: WORKER_HANDLER value -> factory. A table rather than a
# chain of ifs so the accepted values and the error message below cannot
# drift apart.
HANDLERS: dict[str, Callable[[], dk.BaseDrakkarHandler]] = {
    # Kafka-driven worker: the full pipeline, every sink.
    'ripgrep': RipgrepHandler,
    # HTTP-only worker: same ripgrep pipeline behind POST requests,
    # mirroring one aggregate record to a single Kafka sink.
    'http-search': HttpSearchHandler,
}

DEFAULT_HANDLER = 'ripgrep'


def main() -> None:
    """Build the configured handler and run the app, or exit on a bad name."""
    config_path = os.environ.get('DK_CONFIG', '/app/drakkar.yaml')
    handler_name = os.environ.get('WORKER_HANDLER', DEFAULT_HANDLER)

    try:
        handler = HANDLERS[handler_name]()
    except KeyError:
        raise SystemExit(f'unknown WORKER_HANDLER {handler_name!r}; expected one of {sorted(HANDLERS)}') from None

    app = dk.DrakkarApp(
        handler=handler,
        config_path=config_path,
    )
    app.run()


if __name__ == '__main__':
    main()
