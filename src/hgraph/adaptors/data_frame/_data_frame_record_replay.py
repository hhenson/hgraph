from datetime import datetime
from pathlib import Path

import polars as pl

from hgraph import (
    graph,
    record,
    TIME_SERIES_TYPE,
    to_table,
    table_schema,
    AUTO_RESOLVE,
    TableSchema,
    sink_node,
    TS,
    STATE,
    GlobalState,
    generator,
    OUT,
    get_table_schema_date_key,
    from_table,
)
from hgraph._operators._record_replay import record_replay_model_restriction, replay
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
    recordable_id: str = None,
    tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
):
    """
    converts the ts to a table and then records the table to memory, writing to a parquet file in the end.
    delta_value are ignored when working in the tabular space for now.
    """
    tbl = to_table(ts)
    schema = table_schema(tp)
    _record_to_data_frame(tbl, schema, key)


@sink_node
def _record_to_data_frame(
    ts: TIME_SERIES_TYPE,
    schema: TS[TableSchema],
    key: str,
    recordable_id: str = None,
    _state: STATE = None,
    _traits: Traits = None,
):
    _state.value.append(ts.value)


@_record_to_data_frame.start
def _record_to_data_frame_start(key: str, recordable_id: str, _state: STATE, _traits: Traits = None):
    _state.value = []
    recordable_id = recordable_id if recordable_id is not None else _traits.get_trait_or("recordable_id", None)
    _state.recordable_id = f":data_frame:{recordable_id}"


@_record_to_data_frame.stop
def _record_to_data_frame_stop(_state: STATE, schema: TS[TableSchema]):
    schema: TableSchema = schema.value
    df = pl.from_records(_state.value, schema=[(k, t) for k, t in zip(schema.keys, schema.types)], orient="row")
    path: Path = GlobalState.instance().get(DATA_FRAME_RECORD_REPLAY_PATH, Path("."))
    _write_df(df, path, _state.recordable_id)


def _write_df(df: pl.DataFrame, path: Path, recordable_id: str):
    """Separate the writing logic into a function to simplify testing"""
    df.write_parquet(path.joinpath(recordable_id + ".parquet"))


def _read_df(path: Path, recordable_id: str) -> pl.DataFrame:
    """Separate the reading logic into a function to simplify testing"""
    return pl.read_parquet(path.joinpath(recordable_id + ".parquet"))


@graph(overloads=replay, requires=record_replay_model_restriction(DATA_FRAME_RECORD_REPLAY))
def replay_from_data_frame(key: str, tp: type[OUT] = AUTO_RESOLVE, recordable_id: str = None) -> OUT:
    values = _replay_from_data_frame(key, tp, recordable_id)
    return from_table[tp](values)


def _get_df(key, recordable_id: str, traits: Traits) -> pl.DataFrame:
    recordable_id = traits.get_trait_or("recordable_id", None) if recordable_id is None else recordable_id
    path: Path = GlobalState.instance().get(DATA_FRAME_RECORD_REPLAY_PATH, Path("."))
    return _read_df(path, recordable_id)


def _replay_from_data_frame_output_shape(m, s):
    tp = m[TIME_SERIES_TYPE].py_type
    schema: TableSchema = table_schema(tp).value
    if schema.partition_keys:
        return tuple[tuple[*schema.types], ...]
    else:
        return tuple[*schema.types]


@generator(resolvers={OUT: _replay_from_data_frame_output_shape})
def _replay_from_data_frame(
    key: str, tp: type[TIME_SERIES_TYPE], recordable_id: str = None, _traits: Traits = None
) -> TS[OUT]:
    schema: TableSchema = table_schema(tp)
    df_source = _get_df(key, recordable_id, _traits)
    dt_col = pl.col(get_table_schema_date_key())
    df: pl.DataFrame
    dt: datetime
    for (dt,), df in df_source.group_by(dt_col, maintain_order=True):
        out = tuple(df.iter_rows())
        if out:
            if schema.partition_keys:
                yield dt, out
            else:
                yield dt, out[0]  # If there are no partition keys we should only return a single value
