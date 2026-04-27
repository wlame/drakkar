"""Worker entry point — starts the Drakkar application.

Usage:
    WORKER_ID=worker-1 DK_CONFIG=/app/drakkar.yaml python main.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from handler import RipgrepHandler

import drakkar as dk


def main() -> None:
    config_path = os.environ.get('DK_CONFIG', '/app/drakkar.yaml')

    app = dk.DrakkarApp(
        handler=RipgrepHandler(),
        config_path=config_path,
    )
    app.run()


if __name__ == '__main__':
    main()
