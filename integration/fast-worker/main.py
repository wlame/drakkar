"""Fast worker entry point — counts symbols in files."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from handler import SymbolCountHandler

import drakkar as dk


def main() -> None:
    config_path = os.environ.get('DRAKKAR_CONFIG', '/app/drakkar.yaml')

    app = dk.DrakkarApp(
        handler=SymbolCountHandler(),
        config_path=config_path,
    )
    app.run()


if __name__ == '__main__':
    main()
