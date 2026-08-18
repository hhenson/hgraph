from datetime import datetime
from pathlib import Path

import pyarrow as pa

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


# Preserve the 0.5 public sentinel: downstream code imports it directly and
# uses it with ``set_record_replay_model``. The state setter translates this
# compatibility name to the native ``DataFrame`` model.
DATA_FRAME_RECORD_REPLAY = ":data_frame:__data_frame_record_replay__"
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


# The 0.5 override-registry translation into native call-site options moved
# to hgraph_persistence.compat (RFC 0025 checkpoint 5): it only acts under an
# ACTIVE DataFrameStorage, and core wiring must not import adaptor modules —
# the compat surface registers the wiring adapter itself when it loads.


# These names are compatibility views over the C++-owned record/replay nodes.
record_to_data_frame = operator_function("record")
replay_from_data_frame = operator_function("replay")
replay_const_from_data_frame = operator_function("replay_const")
_replay_data_frame_native = operator_function("replay_data_frame")


_STORAGE_EXPORTS = (
    "WriteMode",
    "DataFrameStorage",
    "BaseDataFrameStorage",
    "FileBasedDataFrameStorage",
    "MemoryDataFrameStorage",
)


def __getattr__(name):
    # The storage surface lives in the optional hgraph-persistence
    # distribution (RFC 0025); the pointed install error fires when a
    # durable name is USED, never at import of this module.
    if name not in _STORAGE_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        from hgraph_persistence import compat
    except ModuleNotFoundError as error:
        if error.name != "hgraph_persistence":
            raise
        raise ModuleNotFoundError(
            "hgraph.adaptors.data_frame durable storage is provided by the optional "
            "'hgraph-persistence' distribution; install it with `pip install hgraph-persistence`",
            name="hgraph_persistence",
        ) from error
    value = getattr(compat, name)
    globals()[name] = value
    return value


def _as_arrow(value):
    from hgraph._frame import as_arrow_table

    table = as_arrow_table(value)
    if not isinstance(table, pa.Table):
        raise TypeError(f"dataframe storage requires an Arrow-compatible frame, got {type(value)!r}")
    return table


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
