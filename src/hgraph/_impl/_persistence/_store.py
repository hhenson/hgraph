import os
import pickle
from datetime import datetime
from enum import IntEnum
from functools import wraps
from pathlib import Path
from typing import Dict

from hgraph import NodeSignature
from hgraph._runtime import Node
from hgraph._types._time_series_types import TimeSeriesOutput
from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._lifecycle import ComponentLifeCycle, stop_guard, start_guard


def time_guard(fn):
    @wraps(fn)
    def _start(self: "PersistenceStore", tm: datetime, *args, **kwargs):
        if self._last_timestamp > tm:
            raise RuntimeError(f"The tm provided '{tm}' is before the last processed time '{self._last_timestamp}'")
        return fn(self, tm, *args, **kwargs)


class IndexCommand(IntEnum):
    TS_DATA = 0
    STATE_DATA = 1
    META_DATA = 2
    CYCLE_END = 3  # Useful to deal with corrupt files when we have a failure to assist with detecting "transaction" boundaries


class PersistenceStore(ComponentLifeCycle):

    def __init__(self, path: Path, store_name: str):
        super().__init__()
        self._path = path
        self._store_name = store_name
        self._index = None
        self._metadata = None
        self._data = None
        self._last_id: int = -1
        self._last_timestamp: datetime = MIN_DT
        self._time_series_to_id: Dict[TimeSeriesOutput, int] = {}

    @start_guard
    def start(self):
        as_of: str = f"{datetime.utcnow():%Y-%m-%d_%H-%M-%S}"
        self._index = os.open(self._path.joinpath(f"{self._store_name}_{as_of}.ndx"),
                              os.O_WRONLY | os.O_CREAT | os.O_BINARY)
        self._metadata = os.open(self._path.joinpath(f"{self._store_name}_{as_of}.meta"),
                                 os.O_WRONLY | os.O_CREAT | os.O_BINARY)
        self._data = os.open(self._path.joinpath(f"{self._store_name}_{as_of}.dat"),
                             os.O_RDWR | os.O_CREAT | os.O_BINARY)

    @stop_guard
    def stop(self):
        self._index.close()
        self._metadata.close()
        self._data.close()

    @time_guard
    def register_node(self, tm: datetime, node: Node):
        """
        Register the time-series for persistence, will keep this to the level of output associated to node.
        This can reduce the index space.
        """
        self._last_id += 1
        self._write_index(tm, IndexCommand.META_DATA, self._last_id, get_file_length(self._metadata))
        return self._last_id

    def _write_index(self, tm: datetime, cmd: IndexCommand, id: int, offset: int) -> None:
        # We can do some space-saving by realising that we treat time as either the same or increasing.
        # But for now lets keep it simple and use a fixed record to hold the details
        self._index.seek(0, os.SEEK_END)  # Make sure we are at the end of the file

        # This creates a 128 bit index
        self._index.write(int(tm.timestamp()*1e6).to_bytes(32//8))
        self._index.write(cmd.to_bytes(1))
        self._index.write(id.to_bytes(32//8-1)) # This is fine for small graphs, but not for large ones, we could make this a template parameter?
        self._index.write(offset.to_bytes(64 // 8))

    def _write_metadata(self, node: Node):
        self._metadata.seek(0, os.SEEK_END)
        signature: NodeSignature = node.signature
        self._metadata.seek(0, os.SEEK_END)
        self._metadata.write(node.node_ndx)
        pickle.dump(signature, self._metadata)

    def _write_data(self, node: Node):
        ...


def get_file_length(file):
    current_position = file.tell()  # Save the current file position
    file.seek(0, os.SEEK_END)  # Go to the end of file
    length = file.tell()  # Get the file position at the end (this is the length)
    file.seek(current_position, os.SEEK_SET)  # Restore the file position
    return length
