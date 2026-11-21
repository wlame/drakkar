"""Split worker entry point: large requests are split into parts across the fleet."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from handler import SplitHandler

import drakkar as dk


def main() -> None:
    config_path = os.environ.get('DK_CONFIG', '/app/drakkar.yaml')

    app = dk.DrakkarApp(
        handler=SplitHandler(),
        config_path=config_path,
    )
    app.run()


if __name__ == '__main__':
    main()
