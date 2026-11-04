"""Files of the shared folder that this worker reads now, so the UI delete can refuse them.

A merge result being sent to a client or read as the source of a merge is in
use: deleting it then would end the download early, and on a network mount
it can fail the reader. Another worker's reads are not known here.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable


class FilesInUse:
    """Counts the current users of each shared-folder path (relative to the folder, as the routes take it).

    Changed only on the API server's loop, so a plain counter is enough.
    """

    def __init__(self) -> None:
        self._users: Counter[str] = Counter()

    def hold(self, paths: Iterable[str]) -> Callable[[], None]:
        """Count one user of each of ``paths`` until the returned release is called (once; later calls do nothing)."""
        held = list(paths)
        self._users.update(held)
        released: list[bool] = []

        def release() -> None:
            if released:
                return
            released.append(True)
            self._users.subtract(held)
            for path in held:
                if self._users[path] <= 0:
                    self._users.pop(path, None)

        return release

    def in_use(self, path: str) -> bool:
        """Whether ``path`` has a user now."""
        return self._users[path] > 0
