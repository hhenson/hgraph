"""The 0.5 ``DataFrameStorage`` compatibility surface (RFC 0025,
checkpoint 4): the Python frame-store adaptation moved here with the store.
``hgraph.adaptors.data_frame`` keeps its released import paths as a guarded
re-export over this module."""

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from hgraph import (
    GlobalState,
    get_table_schema_as_of_key,
    get_table_schema_date_key,
)

from . import _hgraph_persistence

__all__ = (
    "WriteMode",
    "DataFrameStorage",
    "BaseDataFrameStorage",
    "FileBasedDataFrameStorage",
    "MemoryDataFrameStorage",
)

_STORAGE_KEY = ":data_frame:__storage__"


class WriteMode(Enum):
    EXTEND = 1
    OVERWRITE = 2
    MERGE = 3


class DataFrameStorage(ABC):
    _MISSING = object()

    def __init__(self):
        self._previous = self._MISSING

    @classmethod
    def instance(cls, global_state=None):
        state = global_state if global_state is not None else GlobalState.instance()
        return state.get(_STORAGE_KEY)

    def set_as_instance(self):
        state = GlobalState.instance()
        self._previous = state.get(_STORAGE_KEY, self._MISSING)
        self._date_key = get_table_schema_date_key(state._impl)
        self._as_of_key = get_table_schema_as_of_key(state._impl)
        state[_STORAGE_KEY] = self
        _hgraph_persistence._set_python_frame_store(state._impl, self)

    def release_as_instance(self):
        state = GlobalState.instance()
        _hgraph_persistence._restore_python_frame_store(state._impl)
        if self._previous is self._MISSING:
            state.pop(_STORAGE_KEY, None)
        else:
            state[_STORAGE_KEY] = self._previous
        self._previous = self._MISSING

    # The native compatibility seam is deliberately smaller than this legacy
    # storage API. It neither asks Python to enforce native immutability nor
    # exposes native segmentation: one call stores or loads one complete frame.
    def store(self, path, frame):
        frame = _as_arrow(frame)
        self.write_frame(path, frame, mode=WriteMode.OVERWRITE)
        names = set(frame.schema.names)
        self.set_schema_info(
            path,
            self._date_key if self._date_key in names else None,
            self._as_of_key if self._as_of_key in names else None,
        )

    def load(self, path):
        try:
            return _as_arrow(self.read_frame(path))
        except (FileNotFoundError, KeyError):
            return None

    def has(self, path):
        exists = getattr(self, "_exists", None)
        if exists is not None:
            return exists(path)
        try:
            return self.load(path) is not None
        except (FileNotFoundError, KeyError):
            return False

    def __enter__(self):
        self.set_as_instance()
        return self

    def __exit__(self, *_):
        self.release_as_instance()
        return False

    @abstractmethod
    def read_frame(self, path, start_time=None, end_time=None, as_of=None):
        raise NotImplementedError

    @abstractmethod
    def write_frame(
        self, path, df, mode=WriteMode.OVERWRITE, as_of=None, global_state=None
    ):
        raise NotImplementedError

    @abstractmethod
    def set_schema_info(self, path, date_time_col=None, as_of_col=None):
        raise NotImplementedError


class BaseDataFrameStorage(DataFrameStorage, ABC):
    def __init__(self):
        super().__init__()
        self._schema = {}

    def read_frame(self, path, start_time=None, end_time=None, as_of=None):
        from hgraph._frame import as_user_frame

        frame = self._read(path)
        date_col, as_of_col = self._get_schema_info(path)
        mask = None
        if start_time is not None or end_time is not None:
            date_col = date_col or "date"
            values = frame[date_col]
            if start_time is not None:
                mask = pc.greater_equal(values, pa.scalar(start_time))
            if end_time is not None:
                upper = pc.less_equal(values, pa.scalar(end_time))
                mask = upper if mask is None else pc.and_(mask, upper)
        if as_of is not None and as_of_col is not None:
            as_of_mask = pc.less_equal(frame[as_of_col], pa.scalar(as_of))
            mask = as_of_mask if mask is None else pc.and_(mask, as_of_mask)
        # A framework surface: user code reads recordings through this, so
        # the result presents in the user-facing form (issue #80); internal
        # replay normalizes back through _as_arrow.
        return as_user_frame(frame.filter(mask) if mask is not None else frame)

    def write_frame(
        self, path, df, mode=WriteMode.OVERWRITE, as_of=None, global_state=None
    ):
        if mode is WriteMode.MERGE:
            raise RuntimeError("WriteMode.MERGE is not supported")
        frame = _as_arrow(df)
        if self._get_schema_info(path) == (None, None):
            from hgraph._wiring._state import _is_runtime_active

            # Preserve the original default-name runtime behavior without
            # consulting the wiring-only GlobalState singleton. Configured
            # runtime names come from the callback's explicit injectable.
            if global_state is None and _is_runtime_active():
                date_key, as_of_key = "__date_time__", "__as_of__"
            else:
                date_key = get_table_schema_date_key(global_state)
                as_of_key = get_table_schema_as_of_key(global_state)
            self.set_schema_info(
                path,
                date_key,
                as_of_key,
            )
        if mode is WriteMode.EXTEND and self._exists(path):
            previous = self._read(path)
            frame = pa.concat_tables([previous, frame], promote_options="default")
        self._write(path, frame)
        return frame

    def _exists(self, path):
        try:
            return self._read(path) is not None
        except (FileNotFoundError, KeyError):
            return False

    @abstractmethod
    def _read(self, path):
        raise NotImplementedError

    @abstractmethod
    def _write(self, path, frame):
        raise NotImplementedError

    @abstractmethod
    def _get_schema_info(self, path):
        raise NotImplementedError


class FileBasedDataFrameStorage(BaseDataFrameStorage):
    def __init__(self, path):
        super().__init__()
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _data_path(self, path):
        return self.path / f"{path}.parquet"

    def _schema_path(self, path):
        return self.path / f"{path}.schema"

    def _exists(self, path):
        return self._data_path(path).exists()

    def _read(self, path):
        return pq.read_table(self._data_path(path))

    def _write(self, path, frame):
        pq.write_table(frame, self._data_path(path))

    def set_schema_info(self, path, date_time_col=None, as_of_col=None):
        self._schema_path(path).write_text(
            f"date_time_col: {date_time_col}\nas_of_col: {as_of_col}"
        )

    def _get_schema_info(self, path):
        schema = self._schema_path(path)
        if not schema.exists():
            return None, None
        date_col, as_of_col = schema.read_text().splitlines()
        if date_col.startswith("date_time_col: "):
            date_col = date_col.removeprefix("date_time_col: ")
            as_of_col = as_of_col.removeprefix("as_of_col: ")
            return (
                None if date_col == "None" else date_col,
                None if as_of_col == "None" else as_of_col,
            )
        # Read the short-lived hg_cpp pre-parity format as a migration aid.
        return date_col or None, as_of_col or None


class MemoryDataFrameStorage(BaseDataFrameStorage):
    def __init__(self):
        super().__init__()
        self._frames = {}

    def _exists(self, path):
        return str(path) in self._frames

    def _read(self, path):
        return self._frames[str(path)]

    def _write(self, path, frame):
        self._frames[str(path)] = frame

    def _get_schema_info(self, path):
        return self._schema.get(str(path), (None, None))

    def set_schema_info(self, path, date_time_col=None, as_of_col=None):
        self._schema[str(path)] = (date_time_col, as_of_col)


def _as_arrow(value):
    from hgraph._frame import as_arrow_table

    table = as_arrow_table(value)
    if not isinstance(table, pa.Table):
        raise TypeError(f"dataframe storage requires an Arrow-compatible frame, got {type(value)!r}")
    return table


