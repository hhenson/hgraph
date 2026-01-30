from dataclasses import dataclass
from datetime import timedelta

from hgraph.adaptors.json.json_adaptor import json_adaptor
from hgraph.adaptors.data_catalogue.subscribe import subscriber_impl_from_graph, subscriber_impl_to_graph
from hgraph.stream.stream import Stream, Data
from hgraph.adaptors.data_catalogue.catalogue import DataSource, DataCatalogueEntry
from hgraph import TS, SCHEMA, AUTO_RESOLVE, TSB, Frame, convert, combine, sample, default, schedule, MIN_DT


__all__ = ['JsonDataSource']


@dataclass(frozen=True)
class JsonDataSource(DataSource):
    file: str


@subscriber_impl_from_graph
def subscribe_json_from_graph(
    dce: DataCatalogueEntry,
    ds: TS[JsonDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE
):
    poll_delay = default(convert[TS[timedelta]](options["poll"]), dce.scope['poll'].default() if 'poll' in dce.scope else None)
    return json_adaptor[_schema].from_graph(
        path=dce.store.source_path,
        file=sample(default(schedule(poll_delay, start=MIN_DT, use_wall_clock=True), True), ds.file),
        __request_id__=request_id
    )


@subscriber_impl_to_graph
def subscribe_json_to_graph(
    dce: DataCatalogueEntry,
    ds: TS[JsonDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    json = json_adaptor[_schema].to_graph(path=dce.store.source_path, __request_id__=request_id, __no_ts_inputs__=True)
    return combine[TSB[Stream[Data[Frame[_schema]]]]](
        values=convert[TS[Frame[_schema]]](json.values),
        status=json.status,
        status_msg=json.status_msg,
        timestamp=json.timestamp,
    )
