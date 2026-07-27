from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import pyarrow as pa

from hgraph import AUTO_RESOLVE, SCHEMA, Frame, TS, TSB, compute_node, null_sink
from hgraph.adaptors.data_catalogue.catalogue import DataCatalogueEntry, DataSink
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph.adaptors.data_catalogue.publish import publish_impl_from_graph, publish_impl_to_graph
from hgraph.stream import Data, Stream

from .sql_adaptor import SQLWriteMode, sql_write_adaptor

__all__ = ("SqlDataSink",)


@dataclass(frozen=True)
class SqlDataSink(DataSink):
    table: str
    column_mappings: Mapping[str, str] = None
    mode: SQLWriteMode = SQLWriteMode.APPEND


@compute_node
def _render_frame(
    data_sink: TS[SqlDataSink], data: TS[Frame[SCHEMA]],
    scope: TS[Mapping[str, Scope]], options: TS[dict[str, object]],
) -> TS[Frame[SCHEMA]]:
    from hgraph._frame import as_arrow_table

    frame = as_arrow_table(data.value)
    values = options.value or {}
    for name, item in (scope.value or {}).items():
        if name not in frame.column_names:
            value = item.adjust(values[name]) if name in values else item.default()
            frame = frame.append_column(name, pa.array([value] * frame.num_rows))
    mappings = data_sink.value.column_mappings
    if mappings:
        frame = frame.rename_columns([mappings.get(name, name) for name in frame.column_names])
    return frame


@publish_impl_from_graph
def publish_sql_from_graph(
    dce: DataCatalogueEntry, data_sink: TS[SqlDataSink],
    options: TS[dict[str, object]], request_id: TS[int],
    data: TS[Frame[SCHEMA]], _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return sql_write_adaptor.from_graph(
        path=dce.store.sink_path,
        table=data_sink.table,
        data=_render_frame(data_sink, data, dce.scope, options),
        mode=data_sink.mode,
        __request_id__=request_id,
    )


@publish_impl_to_graph
def publish_sql_to_graph(
    dce: DataCatalogueEntry, data_sink: TS[SqlDataSink],
    options: TS[dict[str, object]], request_id: TS[int],
    data: TS[Frame[SCHEMA]], _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]:
    return sql_write_adaptor[SCHEMA:_schema].to_graph(
        path=dce.store.sink_path, __request_id__=request_id,
        __no_ts_inputs__=True,
    )
