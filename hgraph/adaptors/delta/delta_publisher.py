import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import polars as pl
from polars import DataFrame, Series

from hgraph.adaptors.delta.delta_adaptor import DeltaSchemaMode, DeltaWriteMode, delta_write_adaptor
from hgraph.adaptors.data_catalogue.publish import publish_impl_from_graph, publish_impl_to_graph
from hgraph.stream.stream import Stream, Data
from hgraph.adaptors.data_catalogue.catalogue import DataSink, DataCatalogueEntry
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph import Frame, SCHEMA, compute_node, TS, AUTO_RESOLVE, TSB

logger = logging.getLogger(__name__)


__all__ = ['DeltaDataSink']


@dataclass(frozen=True)
class DeltaDataSink(DataSink):
    table: str
    write_mode: DeltaWriteMode
    schema_mode: DeltaSchemaMode
    column_mappings: Mapping[str, str] = None
    keys: tuple[str, ...] = None
    partition: tuple[str, ...] = None
    partition_key_from: dict[str, str] = None

    def map_columns(self, frame: Frame[SCHEMA]) -> DataFrame:
        try:
            return frame.rename(self.column_mappings) if self.column_mappings else frame
        except:
            logger.exception(f"Error mapping columns for {self.__class__.__name__} {self.sink_path}")
            logger.error(f"- column mappings: {self.column_mappings}")
            logger.error(f"- frame: {frame}")


def map_partition_key(col: Series) -> Series:
    if col.dtype == pl.Datetime:
        return col.dt.date()
    
    assert False, f"Unsupported auto-build partition key type {col.dtype}"


@compute_node
def render_frame(
    ds: TS[DeltaDataSink], data: TS[Frame[SCHEMA]], scope: TS[Mapping[str, Scope]], options: TS[dict[str, object]]
) -> TS[DataFrame]:
    # Additional columns are defined in the options and are added to the data as literal values, that is the same value
    # is used for all history.
    scope = scope.value
    options = options.value
    data = data.value.with_columns(
        **{k: pl.lit(v.adjust(options[k]) if k in options else v.default()) for k, v in scope.items()}
    )
    if ds.value.partition_key_from:
        data = data.with_columns(
            **{k: map_partition_key(data[ds.value.partition_key_from[k]]) for k in ds.value.partition}
        )
    return ds.value.map_columns(data)


@publish_impl_from_graph
def publish_delta_from_graph(
    dce: DataCatalogueEntry,
    data_sink: TS[DeltaDataSink],
    options: TS[dict[str, object]],
    request_id: TS[int],
    data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return delta_write_adaptor.from_graph(
        path=dce.store.sink_path,
        table=data_sink.table,
        data=render_frame(data_sink, data, dce.scope, options),
        write_mode=data_sink.write_mode,
        schema_mode=data_sink.schema_mode,
        keys=data_sink.keys,
        partition=data_sink.partition,
        __request_id__=request_id,
    )


@publish_impl_to_graph
def publish_delta_to_graph(
    dce: DataCatalogueEntry,
    data_sink: TS[DeltaDataSink],
    options: TS[dict[str, object]],
    request_id: TS[int],
    data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]:
    return delta_write_adaptor.to_graph(path=dce.store.sink_path, __request_id__=request_id, __no_ts_inputs__=True)
