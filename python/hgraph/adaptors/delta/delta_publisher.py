from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc

from hgraph import AUTO_RESOLVE, SCHEMA, Frame, TS, TSB, compute_node
from hgraph.adaptors.data_catalogue.catalogue import DataCatalogueEntry, DataSink
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph.adaptors.data_catalogue.publish import publish_impl_from_graph, publish_impl_to_graph
from hgraph.stream import Data, Stream

from .delta_adaptor import delta_write_adaptor
from .delta_adaptor_raw import DeltaSchemaMode, DeltaWriteMode

__all__ = ("DeltaDataSink",)


@dataclass(frozen=True)
class DeltaDataSink(DataSink):
    table: str
    write_mode: DeltaWriteMode = DeltaWriteMode.APPEND
    schema_mode: DeltaSchemaMode = DeltaSchemaMode.MERGE
    column_mappings: Mapping[str, str] = None
    keys: tuple[str, ...] = ()
    partition: tuple[str, ...] = ()
    partition_key_from: dict[str, str] = None


@compute_node
def _render_frame(
    data_sink: TS[DeltaDataSink], data: TS[Frame[SCHEMA]],
    scope: TS[Mapping[str, Scope]], options: TS[dict[str, object]],
) -> TS[Frame[SCHEMA]]:
    frame = data.value
    values = options.value or {}
    for name, item in (scope.value or {}).items():
        value = item.adjust(values[name]) if name in values else item.default()
        column = pa.array([value] * frame.num_rows)
        if name in frame.column_names:
            frame = frame.set_column(frame.column_names.index(name), name, column)
        else:
            frame = frame.append_column(name, column)
    if data_sink.value.partition_key_from:
        for name in data_sink.value.partition:
            source = frame[data_sink.value.partition_key_from[name]]
            if not pa.types.is_timestamp(source.type):
                raise AssertionError(
                    f"Unsupported auto-build partition key type {source.type}")
            column = pc.cast(source, pa.date32())
            if name in frame.column_names:
                frame = frame.set_column(frame.column_names.index(name), name, column)
            else:
                frame = frame.append_column(name, column)
    mappings = data_sink.value.column_mappings
    if mappings:
        frame = frame.rename_columns([mappings.get(name, name) for name in frame.column_names])
    return frame


@publish_impl_from_graph
def publish_delta_from_graph(
    dce: DataCatalogueEntry, data_sink: TS[DeltaDataSink],
    options: TS[dict[str, object]], request_id: TS[int],
    data: TS[Frame[SCHEMA]], _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return delta_write_adaptor.from_graph(
        path=dce.store.sink_path,
        table=data_sink.table,
        data=_render_frame(data_sink, data, dce.scope, options),
        write_mode=data_sink.write_mode,
        schema_mode=data_sink.schema_mode,
        keys=data_sink.keys,
        partition=data_sink.partition,
        __request_id__=request_id,
    )


@publish_impl_to_graph
def publish_delta_to_graph(
    dce: DataCatalogueEntry, data_sink: TS[DeltaDataSink],
    options: TS[dict[str, object]], request_id: TS[int],
    data: TS[Frame[SCHEMA]], _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]:
    return delta_write_adaptor[SCHEMA:_schema].to_graph(
        path=dce.store.sink_path, __request_id__=request_id,
        __no_ts_inputs__=True,
    )
