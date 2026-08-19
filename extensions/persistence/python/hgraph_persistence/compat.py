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
    "DATA_FRAME_RECORD_REPLAY_PATH",
    "DATA_FRAME_RECORD_OVERRIDES",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "get_data_frame_record_overrides",
)

# The GlobalState keys are public upstream surface (imported by user code);
# the private aliases below are retained for the existing internal call sites.
DATA_FRAME_RECORD_REPLAY_PATH = ":data_frame:__path__"
DATA_FRAME_RECORD_OVERRIDES = ":data_frame:__overrides__"
_PATH_KEY = DATA_FRAME_RECORD_REPLAY_PATH
_OVERRIDES_KEY = DATA_FRAME_RECORD_OVERRIDES
_STORAGE_KEY = ":data_frame:__storage__"


def set_data_frame_record_path(path):
    GlobalState.instance()[_PATH_KEY] = Path(path)


class _OverrideState:
    def __init__(self):
        self.data = {
            "all": {
                "track_as_of": True,
                "track_removes": False,
                "partition_keys": None,
                "remove_partition_keys": None,
            },
            "key": {},
            "recordable_id": {},
            "key_recordable_id": {},
        }


def _overrides(state=None):
    state = state or GlobalState.instance()
    value = state.get(_OVERRIDES_KEY)
    if value is None:
        value = _OverrideState()
        state[_OVERRIDES_KEY] = value
    return value.data


def set_data_frame_overrides(
    key=None,
    recordable_id=None,
    track_as_of=None,
    track_removes=None,
    partition_keys=None,
    remove_partition_keys=None,
):
    overrides = _overrides()
    if key is None and recordable_id is None:
        target = overrides["all"]
    elif key is None:
        target = overrides["recordable_id"].setdefault(recordable_id, {})
    elif recordable_id is None:
        target = overrides["key"].setdefault(key, {})
    else:
        target = overrides["key_recordable_id"].setdefault((recordable_id, key), {})
    target.update(
        track_as_of=True if track_as_of is None else track_as_of,
        track_removes=True if track_removes is None else track_removes,
        partition_keys=partition_keys,
        remove_partition_keys=remove_partition_keys,
    )


def get_data_frame_record_overrides(key, recordable_id, global_state=None):
    overrides = _overrides(global_state)
    return (
        overrides["all"]
        | overrides["recordable_id"].get(recordable_id, {})
        | overrides["key"].get(key, {})
        | overrides["key_recordable_id"].get((recordable_id, key), {})
    )



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


def _legacy_record_replay_kwargs(name, args, kwargs):
    """Translate the 0.5 override registry into native call-site options.

    This is wiring-only compatibility. The native recorder still receives
    explicit immutable scalar options and does not consult Python state at
    runtime.
    """
    from hgraph import DATA_FRAME

    if not GlobalState.has_instance():
        return kwargs
    state = GlobalState.instance()
    if state.get("__record_replay_model__") != DATA_FRAME:
        return kwargs
    if DataFrameStorage.instance(state) is None:
        return kwargs

    key_position = 1 if name == "record" else 0
    key = kwargs.get("key", args[key_position] if len(args) > key_position else "out")
    recordable_id_position = 2 if name == "record" else 1
    recordable_id = kwargs.get(
        "recordable_id",
        args[recordable_id_position] if len(args) > recordable_id_position else None,
    )
    options = get_data_frame_record_overrides(key, recordable_id, state)
    translated = dict(kwargs)

    if name == "record":
        from . import RecordAsOf, RecordRemoves

        translated.setdefault(
            # The 0.5 flag controls whether the column is present; it does not
            # override a fixed graph-level ``set_as_of`` value.  ``INHERIT``
            # preserves that distinction while still tracking evaluation time
            # when no fixed value is configured.  Callers can continue to pass
            # ``RecordAsOf.TRACK`` explicitly when they want that override.
            "as_of", RecordAsOf.INHERIT if options.get("track_as_of", True) else RecordAsOf.OMIT
        )
        translated.setdefault(
            "removes", RecordRemoves.TRACK if options.get("track_removes", False) else RecordRemoves.OMIT
        )
    if name in ("record", "replay"):
        if options.get("partition_keys") is not None:
            translated.setdefault("partition_names", tuple(options["partition_keys"]))
        if options.get("remove_partition_keys") is not None:
            translated.setdefault("removed_names", tuple(options["remove_partition_keys"]))
    return translated


# Core wiring must not import adaptor modules (RFC 0025 checkpoint 5): the
# 0.5 translation registers itself when this compatibility surface loads —
# the only route to an ACTIVE DataFrameStorage, without which it is a no-op.
from hgraph._wiring._core import register_record_replay_wiring_adapter

register_record_replay_wiring_adapter(_legacy_record_replay_kwargs)


