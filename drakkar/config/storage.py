"""Where a worker keeps its files: its own folder and the folder the cluster shares.

``storage.dir`` holds one own folder per worker,
``<storage.dir>/<sanitized cluster>/<worker>/``, with that worker's live
files; only that worker opens them, and an owner lock in the folder keeps a
second process with the same name out. It must be a local filesystem: the
lock and SQLite's WAL mode depend on local file locks.
``storage.shared_dir`` is the folder every worker of a cluster sees:
registry cards, sealed history, archives and merge results.
"""

from pydantic import Field

from drakkar.config.base import ConfigModel


class StorageConfig(ConfigModel):
    """This worker's own files and the folder its cluster shares."""

    dir: str = Field(
        default='/tmp/drakkar',
        description=(
            'Local folder that holds one own folder per worker, <dir>/<sanitized cluster>/<worker>, '
            "for that worker's live files (live recorder file, cache file, crash marker). Only that "
            'worker opens them; an owner lock in the folder refuses a second process with the same name. '
            'Must be a local filesystem, not a network mount. The worker creates missing folders with '
            'mode 0700. Empty: no own files and no cache file; with recorder.enabled and provider '
            'sqlite the load fails.'
        ),
    )
    shared_dir: str = Field(
        default='',
        description=(
            'Folder that every worker of the cluster sees (for example a ReadWriteMany volume): '
            'registry cards, sealed history, archives and merge results. Empty means the '
            'same folder as dir, which is right for workers on one host.'
        ),
    )
