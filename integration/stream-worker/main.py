"""Stream worker entry point: one message turns into many groups of tasks."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from handler import StreamHandler

import drakkar as dk


def main() -> None:
    config_path = os.environ.get('DK_CONFIG', '/app/drakkar.yaml')

    app = dk.DrakkarApp(
        handler=StreamHandler(),
        config_path=config_path,
    )
    app.run()


if __name__ == '__main__':
    main()
