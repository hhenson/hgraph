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
    "DATA_FRAME_RECORD_REPLAY_PATH",
    "DATA_FRAME_RECORD_OVERRIDES",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "get_data_frame_record_overrides",
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
#
# Served through __getattr__ rather than bound eagerly so that reading it can
# warn: it is a shim like the rest of this surface, and a module constant
# assigned here would be fetched without passing through any code of ours.
_DATA_FRAME_RECORD_REPLAY = ":data_frame:__data_frame_record_replay__"
# The 0.5 override-registry translation into native call-site options moved
# to hgraph_persistence.compat (RFC 0025 checkpoint 5): it only acts under an
# ACTIVE DataFrameStorage, and core wiring must not import adaptor modules —
# the compat surface registers the wiring adapter itself when it loads.


# These names are compatibility views over the C++-owned record/replay nodes.
record_to_data_frame = operator_function("record")
replay_from_data_frame = operator_function("replay")
replay_const_from_data_frame = operator_function("replay_const")
_replay_data_frame_native = operator_function("replay_data_frame")


# Every durable name is served by hgraph-persistence (RFC 0025 checkpoint 5:
# the durable recording state moved with the implementation that reads it).
# They resolve on USE, never at import, so this module still imports in a
# core-only install.
_STORAGE_EXPORTS = (
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


def __getattr__(name):
    # The storage surface lives in the optional hgraph-persistence
    # distribution (RFC 0025); the pointed install error fires when a
    # durable name is USED, never at import of this module.
    from ..._deprecation import (
        warn_deprecated_compat_name,
        warn_moved_to_persistence,
    )

    if name == "DATA_FRAME_RECORD_REPLAY":
        # The replacement is NOT in hgraph_persistence.compat, which does not
        # define this name: a caller selecting a backend wants the id.
        warn_deprecated_compat_name(
            "hgraph.adaptors.data_frame.DATA_FRAME_RECORD_REPLAY",
            "hgraph_persistence.FRAME_BACKEND",
        )
        globals()[name] = _DATA_FRAME_RECORD_REPLAY
        return _DATA_FRAME_RECORD_REPLAY
    if name not in _STORAGE_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    warn_moved_to_persistence(
        f"hgraph.adaptors.data_frame.{name}", f"hgraph_persistence.compat.{name}"
    )
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
