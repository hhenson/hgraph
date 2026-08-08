from dataclasses import dataclass
from datetime import timedelta

from hgraph import (
    AUTO_RESOLVE, MIN_DT, SCHEMA, TS, TSB, Frame, convert, default,
    null_sink, sample, schedule,
)
from hgraph.adaptors.data_catalogue.catalogue import DataCatalogueEntry, DataSource
from hgraph.adaptors.data_catalogue.subscribe import (
    subscriber_impl_from_graph, subscriber_impl_to_graph,
)
from hgraph.adaptors.json.json_adaptor import json_adaptor
from hgraph.stream import Data, Stream

__all__ = ("JsonDataSource",)


@dataclass(frozen=True)
class JsonDataSource(DataSource):
    file: str


@subscriber_impl_from_graph
def subscribe_json_from_graph(
    dce: DataCatalogueEntry, ds: TS[JsonDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    null_sink(request_id)


@subscriber_impl_to_graph
def subscribe_json_to_graph(
    dce: DataCatalogueEntry, ds: TS[JsonDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    file = ds.file
    if "poll" in dce.scope:
        poll_delay = convert[TS[timedelta]](options["poll"])
        if (poll_default := dce.scope["poll"].default()) is not None:
            poll_delay = default(poll_delay, poll_default)
        file = sample(
            default(schedule(poll_delay, start=MIN_DT, use_wall_clock=True), True),
            file,
        )
    return json_adaptor[_schema](dce.store.source_path, file)
