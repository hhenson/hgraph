from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from hgraph import (
    AUTO_RESOLVE,
    MAX_DT,
    OUT,
    Frame,
    GlobalState,
    get_table_schema_as_of_key,
    get_table_schema_date_key,
    graph,
    operator_function,
    table_schema,
)

__all__ = (
    "DATA_FRAME_RECORD_REPLAY",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "replay_data_frame",
    "WriteMode",
    "DataFrameStorage",
    "BaseDataFrameStorage",
    "FileBasedDataFrameStorage",
    "MemoryDataFrameStorage",
)


DATA_FRAME_RECORD_REPLAY = ":data_frame:__data_frame_record_replay__"

#: Rows buffered before a recording flush.
#:
#: Each flush costs one Arrow table build and one concat, so this trades a
#: bounded amount of retained Python data for taking both off the per-tick
#: path. Module-level so a deployment can tune it and tests can shrink it.
RECORD_BATCH_ROWS = 8192
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


# These names are compatibility views over the C++-owned record/replay nodes.
record_to_data_frame = operator_function("record")
replay_from_data_frame = operator_function("replay")
replay_const_from_data_frame = operator_function("replay_const")
_replay_data_frame_native = operator_function("replay_data_frame")


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
        state[_STORAGE_KEY] = self

    def release_as_instance(self):
        state = GlobalState.instance()
        if self._previous is self._MISSING:
            state.pop(_STORAGE_KEY, None)
        else:
            state[_STORAGE_KEY] = self._previous
        self._previous = self._MISSING

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


# --------------------------------------------------------------------------
# DATA_FRAME-model record/replay overloads (upstream parity, Arrow-native).
#
# Built on the native TABLE protocol: ``to_table`` supplies canonical rows
# (date, as_of[, removed, key], value...) and ``from_table`` rebuilds the
# time series; this module maps rows <-> Arrow frames in DataFrameStorage,
# honouring set_data_frame_overrides (column renames / dropped columns).
# --------------------------------------------------------------------------

def _df_model_active(m):
    return (GlobalState.has_instance()
            and GlobalState.instance().get("__record_replay_model__") == DATA_FRAME_RECORD_REPLAY)


def _configured_as_of(default=None, global_state=None):
    if global_state is not None:
        return global_state.get("__as_of__", default)
    if GlobalState.has_instance():
        return GlobalState.instance().get("__as_of__", default)
    return default


def _frame_columns_for(schema, overrides):
    """[(row_index, stored_column_name)] honouring the overrides."""
    partition_iter = iter(list(overrides.get("partition_keys", ()) or ()))
    removed_iter = iter(list(overrides.get("remove_partition_keys", ()) or ()))
    track_as_of = overrides.get("track_as_of", True)
    track_removes = overrides.get("track_removes", True)

    columns = []
    for index, key in enumerate(schema.keys):
        if key == schema.as_of_key:
            if track_as_of:
                columns.append((index, key))
        elif key in schema.removed_keys:
            if track_removes:
                columns.append((index, next(removed_iter, key)))
        elif key in schema.partition_keys:
            columns.append((index, next(partition_iter, key)))
        else:
            columns.append((index, key))
    return columns


def _read_row_groups(frame, schema, overrides):
    """Stored frame -> canonical row tuples grouped by consecutive date."""
    columns = _frame_columns_for(schema, overrides)
    stored = {index: frame[name].to_pylist() for index, name in columns}
    date_index = schema.keys.index(schema.date_time_key)
    as_of_index = schema.keys.index(schema.as_of_key) if schema.as_of_key in schema.keys else None
    groups = []
    for row_number in range(frame.num_rows):
        row = tuple(
            stored[index][row_number] if index in stored
            else (stored[date_index][row_number] if index == as_of_index
                  else (False if key in schema.removed_keys else None))
            for index, key in enumerate(schema.keys))
        when = row[date_index]
        if groups and groups[-1][0] == when:
            groups[-1][1].append(row)
        else:
            groups.append((when, [row]))
    return groups


def _storage_frame(key, recordable_id, global_state):
    # Internal replay consumption: a user-implemented storage (or a
    # user-supplied literal frame) may return the user-facing form —
    # normalize back to the canonical Arrow representation (issue #80).
    storage = DataFrameStorage.instance(global_state)
    if storage is None:
        raise RuntimeError("data-frame record/replay requires an active DataFrameStorage")
    return _as_arrow(storage.read_frame(
        f"{recordable_id}.{key}", as_of=_configured_as_of(global_state=global_state)))


_REPLAY_SCHEMAS = {}


def _register_data_frame_record_replay():
    from hgraph import (
        AUTO_RESOLVE, TIME_SERIES_TYPE, TS, OUT, STATE, graph,
        sink_node, generator, table_schema, to_table, from_table,
    )
    from hgraph._wiring._compose import _TsExprFor
    from hgraph._wiring._core import _unwrap

    def _flush_record_batch(_state, storage, path):
        """Write the buffered rows as ONE Arrow table, then drop them."""
        columns = _state.columns
        if not columns or not columns[0]:
            return
        frame = pa.table(dict(zip(_state.col_names, columns)))
        mode = WriteMode.EXTEND if _state.started else WriteMode.OVERWRITE
        _state.started = True
        storage.write_frame(path, frame, mode=mode)
        for column in columns:
            column.clear()

    @sink_node
    def _df_record_rows(rows: TIME_SERIES_TYPE, key: str, recordable_id: str,
                        col_indices: tuple, col_names: tuple, multi_row: bool,
                        _state: STATE = None,
                        global_state: GlobalState = None):
        # Buffer into plain Python lists and build Arrow in batches.
        #
        # Recording used to build a one-row `pa.table` every tick and write it
        # with EXTEND, and EXTEND re-read the whole accumulated table and
        # concatenated. That is O(n^2) in work, and the memory cost is worse
        # than the row count suggests: `concat_tables` does not combine chunks,
        # so the result carried ONE CHUNK PER TICK PER COLUMN, each with its own
        # 64-byte-aligned data and validity buffers plus per-chunk object
        # overhead. A few bytes of payload per tick retained hundreds.
        storage = DataFrameStorage.instance(global_state)
        if storage is None:
            raise RuntimeError("data-frame record requires an active DataFrameStorage")
        if not hasattr(_state, "columns"):
            _state.columns = [[] for _ in col_names]
            _state.col_names = list(col_names)
            _state.started = False
        row_values = rows.value if multi_row else (rows.value,)
        columns = _state.columns
        for position, index in enumerate(col_indices):
            column = columns[position]
            for row in row_values:
                column.append(row[index])
        if len(columns[0]) >= RECORD_BATCH_ROWS:
            _flush_record_batch(_state, storage, f"{recordable_id}.{key}")

    @_df_record_rows.stop
    def _df_record_rows_stop(key: str, recordable_id: str, _state: STATE = None,
                             global_state: GlobalState = None):
        # The tail. Without this the last partial batch is lost, so the flush
        # threshold would silently truncate a recording.
        storage = DataFrameStorage.instance(global_state)
        if storage is None or not hasattr(_state, "columns"):
            return
        _flush_record_batch(_state, storage, f"{recordable_id}.{key}")

    @graph(overloads="record", requires=_df_model_active)
    def _record_to_data_frame(ts: TIME_SERIES_TYPE, key: str = "out",
                              recordable_id: str = None,
                              _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
                              global_state: GlobalState = None):
        # registry dispatch re-wraps the port as whole-value TS; the resolved
        # typevar carries the true endpoint type (TSD vs TS)
        tp = _tp if _tp is not AUTO_RESOLVE else _TsExprFor(_unwrap(ts).ts_type)
        schema = table_schema(tp).value
        overrides = get_data_frame_record_overrides(
            key, recordable_id, global_state
        )
        storage = DataFrameStorage.instance(global_state)
        if storage is not None:
            storage.set_schema_info(
                f"{recordable_id}.{key}", schema.date_time_key,
                schema.as_of_key if overrides.get("track_as_of", True) else None,
            )
        columns = _frame_columns_for(schema, overrides)
        _df_record_rows(to_table(ts), key=key, recordable_id=recordable_id,
                        col_indices=tuple(index for index, _ in columns),
                        col_names=tuple(name for _, name in columns),
                        multi_row=bool(schema.partition_keys) or schema.is_multi_row)

    def _replay_rows_type(m, key, recordable_id):
        schema, _, _ = _REPLAY_SCHEMAS[(key, recordable_id)]
        from hgraph._table import table_shape_from_schema
        return TS[table_shape_from_schema(schema)]

    @generator(resolvers={TIME_SERIES_TYPE: _replay_rows_type})
    def _df_replay_rows(key: str, recordable_id: str,
                        multi_row: bool,
                        global_state: GlobalState = None) -> TIME_SERIES_TYPE:
        # upstream filter: rows strictly before the evaluation start are
        # dropped (dt_col >= start_time)
        schema, overrides, start = _REPLAY_SCHEMAS[(key, recordable_id)]
        frame = _storage_frame(key, recordable_id, global_state)
        for when, rows in _read_row_groups(frame, schema, overrides):
            if start is not None and when < start:
                continue
            yield when, tuple(rows) if multi_row else rows[0]

    @generator(resolvers={TIME_SERIES_TYPE: _replay_rows_type})
    def _df_replay_const_rows(key: str, recordable_id: str,
                              multi_row: bool,
                              global_state: GlobalState = None) -> TIME_SERIES_TYPE:
        # const semantics (upstream replay_const_from_data_frame): the FIRST
        # group at/after the evaluation start, emitted once.
        schema, overrides, start = _REPLAY_SCHEMAS[(key, recordable_id)]
        frame = _storage_frame(key, recordable_id, global_state)
        for when, rows in _read_row_groups(frame, schema, overrides):
            if start is not None and when < start:
                continue
            yield when, tuple(rows) if multi_row else rows[0]
            return

    def _wire_replay(node, tp, key, recordable_id):
        schema = table_schema(tp).value
        overrides = get_data_frame_record_overrides(key, recordable_id)
        start = GlobalState.instance().get("__start_time__")
        _REPLAY_SCHEMAS[(key, recordable_id)] = (schema, overrides, start)
        return node(key=key, recordable_id=recordable_id,
                    multi_row=bool(schema.partition_keys) or schema.is_multi_row)

    @graph(overloads="replay", requires=_df_model_active)
    def _replay_from_data_frame(key: str = "out", recordable_id: str = None,
                                to: type[OUT] = AUTO_RESOLVE) -> OUT:
        return from_table[to](_wire_replay(_df_replay_rows, to, key, recordable_id))

    @graph(overloads="replay_const", requires=_df_model_active)
    def _replay_const_from_data_frame(key: str = "out", recordable_id: str = None,
                                      to: type[OUT] = AUTO_RESOLVE) -> OUT:
        return from_table[to](_wire_replay(_df_replay_const_rows, to, key, recordable_id))

_register_data_frame_record_replay()


@graph
def replay_data_frame(
    data_frame: Frame,
    schema: object = None,
    as_of_time: datetime = None,
    tp: type[OUT] = AUTO_RESOLVE,
) -> OUT:
    """Replay a raw bitemporal frame through the native table protocol.

    A custom :class:`~hgraph.TableSchema` is a wiring-time column contract;
    it is projected and renamed to the canonical C++ layout before the
    native source performs start/as-of filtering and revision selection.
    """
    frame = _as_arrow(data_frame)
    canonical = table_schema(tp).value
    if schema is not None:
        if len(schema.keys) != len(canonical.keys):
            raise ValueError(
                "replay_data_frame schema has "
                f"{len(schema.keys)} columns; output requires {len(canonical.keys)}"
            )
        try:
            frame = frame.select(schema.keys)
        except KeyError as error:
            raise ValueError(
                "replay_data_frame input does not satisfy the supplied schema"
            ) from error
        frame = frame.rename_columns(canonical.keys)
    cutoff = MAX_DT if as_of_time is None else as_of_time
    return _replay_data_frame_native[tp](frame, cutoff)
