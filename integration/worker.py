"""Integration test worker entry point."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drakkar import DrakkarApp
from handler import RipgrepHandler


def main():
    config_path = os.environ.get("DRAKKAR_CONFIG", "/app/drakkar.yaml")

    app = DrakkarApp(
        handler=RipgrepHandler(),
        config_path=config_path,
    )
    app.run()


if __name__ == "__main__":
    main()
