"""``python -m drakkar.agent``: the same command as ``drakkar-agent``."""

import sys

from drakkar.agent.cli import run

if __name__ == '__main__':
    sys.exit(run())
