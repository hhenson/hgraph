from dataclasses import dataclass
from typing import Mapping

from hgraph import (
    AUTO_RESOLVE, MIN_DT, SCHEMA, SCHEDULER, EvaluationClock, Frame, TS,
    TSB, compute_node,
)
from hgraph.adaptors.data_catalogue import DataCatalogueEntry, DataSource
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph.adaptors.data_catalogue.subscribe import (
    subscriber_impl_from_graph, subscriber_impl_to_graph,
)
from hgraph.stream import Data, Stream

from .delta_adaptor import delta_read_adaptor

__all__ = ("DeltaDataSource",)


@dataclass(frozen=True)
class DeltaDataSource(DataSource):
    table: str
    query: tuple[tuple[str, str, str], ...] = ()
    sort: tuple[tuple[str, bool], ...] = ()

    def render(self, **options):
        return tuple(
            (column, operator, options[value])
            if value in options and options[value] is not None
            else (column, operator, value)
            for column, operator, value in self.query
            if value not in options or options[value] is not None)


@compute_node
def _render_filters(
    ds: TS[DeltaDataSource], scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]], _scheduler: SCHEDULER = None,
    _clock: EvaluationClock = None,
) -> TS[tuple[tuple[str, str, object], ...]]:
    scope_value = scope.value
    options_value = {} if not scope_value else (options.value if options.valid else None)
    if options_value is None:
        return

    poll = scope_value.get("poll")
    poll = poll.default() if poll else None
    if (interval := options_value.get("poll", poll)) is not None:
        next_time = (1 + (_clock.now - MIN_DT) // interval) * interval + MIN_DT
        _scheduler.schedule(next_time, "_", on_wall_clock=True)

    return ds.value.render(**{
        key: value.adjust(options_value[key]) if key in options_value else value.default()
        for key, value in scope_value.items()
    })


@subscriber_impl_from_graph
def subscribe_delta_from_graph(
    dce: DataCatalogueEntry, ds: TS[DeltaDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return delta_read_adaptor[_schema].from_graph(
        path=dce.store.source_path,
        table=ds.table,
        filters=_render_filters(ds, dce.scope, options),
        sort=ds.sort,
        __request_id__=request_id,
    )


@subscriber_impl_to_graph
def subscribe_delta_to_graph(
    dce: DataCatalogueEntry, ds: TS[DeltaDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    return delta_read_adaptor[_schema].to_graph(
        path=dce.store.source_path,
        __request_id__=request_id,
        __no_ts_inputs__=True,
    )
