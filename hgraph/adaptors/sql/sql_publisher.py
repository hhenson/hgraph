import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import polars as pl
from hgraph.adaptors.data_catalogue.catalogue import DataSink, DataCatalogueEntry
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph.adaptors.data_catalogue.publish import publish_impl_from_graph, publish_impl_to_graph
from hgraph.adaptors.sql import SqlAdaptorConnection
from hgraph.adaptors.sql.sql_adaptor import sql_write_adaptor
from hgraph.adaptors.sql.sql_adaptor_raw import SQLWriteMode
from hgraph.stream.stream import Stream, Data
from polars import DataFrame

from hgraph import Frame, SCHEMA, compute_node, TS, AUTO_RESOLVE, TSB

logger = logging.getLogger(__name__)


__all__ = ['SqlDataSink']


@dataclass(frozen=True)
class SqlDataSink(DataSink):
    table: str
    column_mappings: Mapping[str, str]
    mode: SQLWriteMode

    def map_columns(self, frame: Frame[SCHEMA]) -> DataFrame:
        try:
            return frame.rename(self.column_mappings)
        except:
            logger.exception(f"Error mapping columns for {self.__class__.__name__} {self.sink_path}")
            logger.error(f"- column mappings: {self.column_mappings}")
            logger.error(f"- frame: {frame}")


@compute_node
def render_frame(
    ds: TS[SqlDataSink], data: TS[Frame[SCHEMA]], scope: TS[Mapping[str, Scope]], options: TS[dict[str, object]]
) -> TS[DataFrame]:
    # Additional columns are defined in the options and are added to the data as literal values, that is the same value
    # is used for all history.
    scope = scope.value
    options = options.value
    data = data.value.with_columns(
        **{k: pl.lit(v.adjust(options[k]) if k in options else v.default()) for k, v in scope.items()}
    )
    return ds.value.map_columns(data)


@publish_impl_from_graph
def publish_sql_from_graph(
    dce: DataCatalogueEntry,
    data_sink: TS[SqlDataSink],
    options: TS[dict[str, object]],
    request_id: TS[int],
    data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return sql_write_adaptor.from_graph(
        path=dce.store.sink_path,
        table=data_sink.table,
        data=render_frame(data_sink, data, dce.scope, options),
        mode=data_sink.mode,
        __request_id__=request_id,
    )


@publish_impl_to_graph
def publish_sql_to_graph(
    dce: DataCatalogueEntry,
    data_sink: TS[SqlDataSink],
    options: TS[dict[str, object]],
    request_id: TS[int],
    data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]:
    return sql_write_adaptor.to_graph(path=dce.store.sink_path, __request_id__=request_id, __no_ts_inputs__=True)
