"""Where a worker keeps its files: its own folder and the folder the cluster shares.

``storage.dir`` holds this worker's own live files; only this worker opens
them. ``storage.shared_dir`` is the folder every worker of a cluster sees:
registry cards, sealed history, archives and merge results.
"""

from pydantic import Field

from drakkar.config.base import ConfigModel


class StorageConfig(ConfigModel):
    """This worker's own files and the folder its cluster shares."""

    dir: str = Field(
        default='/tmp/drakkar',
        description=(
            "Folder for this worker's own files (live recorder file, cache file, crash marker). "
            'Only this worker opens them. The worker creates it with mode 0700. Empty: no own files and '
            'no cache file; with recorder.enabled and provider sqlite the load fails.'
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
