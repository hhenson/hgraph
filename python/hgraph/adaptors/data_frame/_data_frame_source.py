from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import Iterator, TypeVar

import pyarrow as pa

from hgraph import GlobalState

__all__ = (
    "DataFrameSource",
    "ArrowDataFrameSource",
    "PolarsDataFrameSource",
    "SqlDataFrameSource",
    "DataStore",
    "DataConnectionStore",
    "DATA_FRAME_SOURCE",
)


def _as_arrow_table(value) -> pa.Table:
    from hgraph._frame import as_arrow_table

    table = as_arrow_table(value)
    if not isinstance(table, pa.Table):
        raise TypeError(f"dataframe sources must produce an Arrow Table, got {type(value)!r}")
    return table


class DataFrameSource(ABC):
    """A wiring-time provider of Arrow tables or record batches."""

    def _prepare_for_graph(self) -> "DataFrameSource":
        """Bind wiring-time dependencies needed by runtime iteration."""
        return self

    @abstractmethod
    def data_frame(
        self, start_time: datetime = None, end_time: datetime = None
    ) -> pa.Table:
        ...

    @property
    def schema(self) -> pa.Schema:
        return _as_arrow_table(self.data_frame()).schema

    def iter_frames(
        self, start_time: datetime = None, end_time: datetime = None
    ) -> Iterator[pa.Table]:
        return iter((_as_arrow_table(self.data_frame(start_time, end_time)),))


DATA_FRAME_SOURCE = TypeVar("DATA_FRAME_SOURCE", bound=DataFrameSource)


class DataStore:
    """Data-source instances held in the Python seed/result GlobalState."""

    _STATE_KEY = ":adaptors:data_frame:sources"
    _MISSING = object()

    def __init__(self):
        self._data_frame_sources = {}
        self._previous = self._MISSING

    @classmethod
    def instance(cls) -> "DataStore":
        state = GlobalState.instance()
        store = state.get(cls._STATE_KEY)
        if store is None:
            store = cls()
            state[cls._STATE_KEY] = store
        return store

    def set_data_source(
        self, dfs: type[DATA_FRAME_SOURCE], dfs_instance: DATA_FRAME_SOURCE
    ):
        self._data_frame_sources[dfs] = dfs_instance

    def get_data_source(
        self, dfs: type[DATA_FRAME_SOURCE]
    ) -> DATA_FRAME_SOURCE:
        source = self._data_frame_sources.get(dfs)
        if source is None:
            source = dfs()
            self._data_frame_sources[dfs] = source
        return source

    def __enter__(self):
        state = GlobalState.instance()
        self._previous = state.get(self._STATE_KEY, self._MISSING)
        state[self._STATE_KEY] = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        state = GlobalState.instance()
        if self._previous is self._MISSING:
            state.pop(self._STATE_KEY, None)
        else:
            state[self._STATE_KEY] = self._previous
        self._previous = self._MISSING
        return False


class ArrowDataFrameSource(DataFrameSource):
    def __init__(self, frame):
        self._frame = _as_arrow_table(frame)

    def data_frame(
        self, start_time: datetime = None, end_time: datetime = None
    ) -> pa.Table:
        return self._frame


class PolarsDataFrameSource(ArrowDataFrameSource):
    """Compatibility name; converts a Polars value to Arrow immediately."""

    def __init__(self, df):
        super().__init__(df)


class DataConnectionStore:
    _STATE_KEY = ":adaptors:data_frame:connections"
    _MISSING = object()

    def __init__(self):
        self._connections = {}
        self._previous = self._MISSING

    @classmethod
    def instance(cls) -> "DataConnectionStore":
        state = GlobalState.instance()
        store = state.get(cls._STATE_KEY)
        if store is None:
            store = cls()
            state[cls._STATE_KEY] = store
        return store

    def get_connection(self, name: str):
        try:
            return self._connections[name]
        except KeyError as error:
            raise ValueError(f"no connection found with name {name!r}") from error

    def has_connection(self, name: str) -> bool:
        return name in self._connections

    def set_connection(self, name: str, connection):
        self._connections[name] = connection

    def __enter__(self):
        state = GlobalState.instance()
        self._previous = state.get(self._STATE_KEY, self._MISSING)
        state[self._STATE_KEY] = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        state = GlobalState.instance()
        if self._previous is self._MISSING:
            state.pop(self._STATE_KEY, None)
        else:
            state[self._STATE_KEY] = self._previous
        self._previous = self._MISSING
        return False


class SqlDataFrameSource(DataFrameSource):
    """A dependency-neutral DB-API/Arrow SQL query source."""

    def __init__(self, query: str, connection: str, batch_size: int = -1, **kwargs):
        self._query = query
        self._connection = connection
        self._batch_size = batch_size
        self._kwargs = kwargs
        self._frame = None
        self._bound_connection = None

    def _prepare_for_graph(self) -> "SqlDataFrameSource":
        self._bound_connection = DataConnectionStore.instance().get_connection(
            self._connection
        )
        return self

    @property
    def connection(self):
        if self._bound_connection is not None:
            return self._bound_connection
        return DataConnectionStore.instance().get_connection(self._connection)

    def _execute(self, query: str = None):
        query = self._query if query is None else query
        execute = self.connection.execute
        try:
            return execute(query, **self._kwargs)
        except TypeError:
            return execute(query)

    @staticmethod
    def _result_table(result) -> pa.Table:
        fetch_arrow_table = getattr(result, "fetch_arrow_table", None)
        if fetch_arrow_table is not None:
            return _as_arrow_table(fetch_arrow_table())
        fetch_record_batch = getattr(result, "fetch_record_batch", None)
        if fetch_record_batch is not None:
            return _as_arrow_table(fetch_record_batch())
        rows = result.fetchall()
        names = [column[0] for column in result.description]
        return pa.Table.from_pylist([dict(zip(names, row)) for row in rows])

    def data_frame(
        self, start_time: datetime = None, end_time: datetime = None
    ) -> pa.Table:
        if self._frame is None:
            self._frame = self._result_table(self._execute())
        return self._frame

    @cached_property
    def schema(self) -> pa.Schema:
        return self._result_table(self._execute(self._query + " LIMIT 1")).schema

    def iter_frames(
        self, start_time: datetime = None, end_time: datetime = None
    ) -> Iterator[pa.Table]:
        if self._frame is not None or self._batch_size < 0:
            return iter((self.data_frame(start_time, end_time),))

        result = self._execute()
        fetchmany = getattr(result, "fetchmany", None)
        if fetchmany is None:
            return iter((self._result_table(result),))
        names = [column[0] for column in result.description]

        def batches():
            while rows := fetchmany(self._batch_size):
                yield pa.Table.from_pylist([dict(zip(names, row)) for row in rows])

        return batches()
