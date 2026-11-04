"""Files of the shared folder that this worker reads now, so the UI delete can refuse them.

A merge result being sent to a client or read as the source of a merge is in
use: deleting it then would end the download early, and on a network mount
it can fail the reader. Another worker's reads are not known here.

Files are keyed by their identity on disk (``(st_dev, st_ino)``), never by
the request text: two spellings of one file (a case-insensitive mount) are
one key. A delete marks its file first, so a download or a merge that starts
while the unlink runs is refused instead of reading a file on its way out.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Hashable, Iterable


class FilesInUse:
    """Counts the current users of each file, and the files being deleted.

    Changed only on the API server's loop, so plain containers are enough:
    a check and the change that follows it run with no await in between.
    """

    def __init__(self) -> None:
        self._users: Counter[Hashable] = Counter()
        self._deleting: set[Hashable] = set()

    def hold(self, keys: Iterable[Hashable]) -> Callable[[], None] | None:
        """Count one user of each of ``keys`` until the returned release is called (once; later calls do nothing).

        ``None``, and nothing counted, when one of them is being deleted.
        """
        held = list(keys)
        if any(key in self._deleting for key in held):
            return None
        self._users.update(held)
        released: list[bool] = []

        def release() -> None:
            if released:
                return
            released.append(True)
            self._users.subtract(held)
            for key in held:
                if self._users[key] <= 0:
                    self._users.pop(key, None)

        return release

    def in_use(self, key: Hashable) -> bool:
        """Whether ``key`` has a user now."""
        return self._users[key] > 0

    def begin_delete(self, key: Hashable) -> Callable[[], None]:
        """Mark ``key`` as being deleted until the returned end is called; holds of it are refused meanwhile."""
        self._deleting.add(key)
        return lambda: self._deleting.discard(key)
