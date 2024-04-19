"""
Tools to extract data and write data to data frames and data bases.
"""
from datetime import date, datetime, time, timedelta

import polars as pl

from hgraph import TS_SCHEMA, TSB, TIME_SERIES_TYPE, TSD, HgScalarTypeMetaData, UnNamedTimeSeriesSchema, \
    generator, HgTSTypeMetaData, graph, sink_node, GlobalState, CustomMessageWiringError, AUTO_RESOLVE, K
from hgraph.nodes._record import record, get_recorded_value

__all__ = ("from_polars", "to_polars", "get_polars_df")


def from_polars(df: pl.DataFrame, time_col: str, as_bundle: bool = True, include_time_in_bundle: bool = False) \
        -> TSB[TS_SCHEMA] | TSD[K, TIME_SERIES_TYPE]:
    """
    Create a data source from a Polars data frame.
    """
    schema = df.schema
    include_time = as_bundle and include_time_in_bundle
    hg_types = {k: HgTSTypeMetaData(_polars_type_to_hgraph_type(v)) for k, v in schema.items() if
                include_time or k != time_col}
    if as_bundle:
        hg_schema = UnNamedTimeSeriesSchema.create_resolved_schema(hg_types)
        output_type = TSB[hg_schema]
    else:
        tp_ = next(iter(hg_types.values()))
        if not all(tp_.matches(v) for v in hg_types.values()):
            raise CustomMessageWiringError("All columns must have the same type when creating a TSD output type")
        output_type = TSD[str, tp_]

    @generator
    def _from_polars() -> output_type:
        for row in df.iter_rows(named=True):
            if include_time:
                yield row[time_col], row
            else:
                yield row.pop(time_col), row

    return _from_polars()


def _polars_type_to_hgraph_type(tp: pl.datatypes.PolarsDataType) -> HgScalarTypeMetaData:
    types: dict[pl.datatypes.PolarsDataType, HgScalarTypeMetaData] = {
        pl.datatypes.Int8: HgScalarTypeMetaData.parse_type(int),
        pl.datatypes.Int16: HgScalarTypeMetaData.parse_type(int),
        pl.datatypes.Int32: HgScalarTypeMetaData.parse_type(int),
        pl.datatypes.Int64: HgScalarTypeMetaData.parse_type(int),
        pl.datatypes.Float32: HgScalarTypeMetaData.parse_type(float),
        pl.datatypes.Float64: HgScalarTypeMetaData.parse_type(float),
        pl.datatypes.Utf8: HgScalarTypeMetaData.parse_type(str),
        pl.datatypes.Date: HgScalarTypeMetaData.parse_type(date),
        pl.datatypes.Datetime: HgScalarTypeMetaData.parse_type(datetime),
        pl.datatypes.Time: HgScalarTypeMetaData.parse_type(time),
        pl.datatypes.Duration: HgScalarTypeMetaData.parse_type(timedelta),
        pl.datatypes.Boolean: HgScalarTypeMetaData.parse_type(bool),
        # pl.datatypes.Categorical: HgScalarTypeMetaData.parse(str),
    }
    return types[tp]


@graph
def to_polars(ts: TSB[TS_SCHEMA], time_col: str = "time", location: str = "polars.df",
              schema: type[TS_SCHEMA] = AUTO_RESOLVE):
    """
    Create a Polars data frame from a time series.
    """
    # Dataframes are largely immutable things (at least from the perspective of row-by-row addition operations)
    # and as such all reports indicate that construction from existing python lists / dicts are much faster
    # than trying to do a row-by-row concatenation / append. So we just record and then at the end construct
    # the data frame from the recorded data.

    # Split up columns and record them independently
    for k, v in schema._schema_items():
        if type(v) is not HgTSTypeMetaData:
            raise CustomMessageWiringError(
                f"Schema '{ts.schema}' is not suitable for polars conversion as it is not a simple structure")
        record(ts[k], label=f"{location}::{k}")

    @sink_node(valid=(), active=())
    def _stub(ts: TSB[TS_SCHEMA], location: str, schema: type[TS_SCHEMA]):
        """A stub sink node, it does nothing, but allows us to hook a stop behaviour to dump out the DF when done."""

    @_stub.stop
    def _stub_stop(location: str, schema: type[TS_SCHEMA]):
        df_s = [pl.DataFrame(get_recorded_value(label=f"{location}::{k}"), schema=[time_col, k]) for k in schema._schema_keys()]
        df = df_s[0]
        for df_other in df_s[1:]:
            df = df.join(df_other, on=time_col, how="outer_coalesce")
        GlobalState.instance()[f"nodes.{to_polars.signature.name}.{location}"] = df

    _stub(ts, location, schema)


def get_polars_df(location: str = "polars.df") -> pl.DataFrame:
    """
    Get the Polars data frame from the given location.
    """
    return GlobalState.instance()[f"nodes.{to_polars.signature.name}.{location}"]