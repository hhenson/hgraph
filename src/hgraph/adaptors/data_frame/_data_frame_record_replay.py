from pathlib import Path

import polars as pl

from hgraph import (
    graph,
    record,
    TIME_SERIES_TYPE,
    to_table,
    to_table_schema,
    AUTO_RESOLVE,
    TableSchema,
    sink_node,
    TS,
    STATE,
    GlobalState,
)
from hgraph._operators._record_replay import record_replay_model_restriction
from hgraph._runtime._traits import Traits


__all__ = ("DATA_FRAME_RECORD_REPLAY", "set_data_frame_record_path")

DATA_FRAME_RECORD_REPLAY = ":data_frame:__data_frame_record_replay__"
DATA_FRAME_RECORD_REPLAY_PATH = ":data_frame:__path__"


def set_data_frame_record_path(path: Path):
    """Sets the location where the files are to be written"""
    GlobalState.instance()[DATA_FRAME_RECORD_REPLAY_PATH] = path


@graph(overloads=record, requires=record_replay_model_restriction(DATA_FRAME_RECORD_REPLAY))
def record_to_data_frame(
    ts: TIME_SERIES_TYPE,
    key: str,
    tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
):
    """
    converts the ts to a table and then records the table to memory, writing to a parquet file in the end.
    delta_value are ignored when working in the tabular space for now.
    """
    tbl = to_table(ts)
    schema = to_table_schema(tp)
    _record_to_data_frame(tbl, schema, key)


@sink_node
def _record_to_data_frame(
    ts: TIME_SERIES_TYPE,
    schema: TS[TableSchema],
    key: str,
    _state: STATE = None,
    _traits: Traits = None,
):
    _state.value.append(ts.value)


@_record_to_data_frame.start
def _record_to_data_frame_start(key: str, _state: STATE, _traits: Traits = None):
    _state.value = []
    recordable_id = _traits.get_trait_or("recordable_id", None)
    _state.recordable_id = f":data_frame:{recordable_id}"


@_record_to_data_frame.stop
def _record_to_data_frame_stop(_state: STATE, schema: TS[TableSchema]):
    schema: TableSchema = schema.value
    df = pl.from_records(_state.value, schema=[(k, t) for k, t in zip(schema.keys, schema.types)], orient="row")
    path: Path = GlobalState.instance().get(DATA_FRAME_RECORD_REPLAY_PATH, Path("."))
    _write_df(df, path, _state.recordable_id)


def _write_df(df, path, recordable_id):
    """Separate the writing logic into a function to simplify testing"""
    df.write_parquet(path.joinpath(recordable_id + ".parquet"))
