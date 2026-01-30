import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from frozendict import frozendict

from hgraph.adaptors.delta.delta_adaptor import delta_query_adaptor, delta_query_adaptor_impl
from hgraph.adaptors.delta.delta_adaptor_raw import delta_query_adaptor_raw_impl
from hgraph.adaptors.data_catalogue.subscribe import (
    subscribe,
    subscribe_adaptor_impl,
    subscriber_impl_from_graph,
    subscriber_impl_to_graph,
)
from hgraph.adaptors.data_catalogue.catalogue import DataCatalogueEntry, DataSource, DataEnvironment, DataEnvironmentEntry
from hgraph.adaptors.data_catalogue.data_scopes import DateTimeScope, Scope
from hgraph import (
    GraphConfiguration,
    date,
    evaluate_graph,
    graph,
    TS,
    TSB,
    Frame,
    SCHEMA,
    CompoundScalar,
    register_adaptor,
    debug_print,
    EvaluationMode,
    compute_node,
    AUTO_RESOLVE,
    SCHEDULER,
    MIN_DT,
    EvaluationClock,
)
from hgraph.stream.stream import Stream, Data

logger = logging.getLogger(__name__)


__all__ = ['DeltaQueryDataSource']


@dataclass(frozen=True)
class DeltaQueryDataSource(DataSource):
    tables: frozenset[str]
    query: str

    def render(self, **options) -> str:
        try:
            return self.query.format(**options)
        except:
            logger.exception(f"Error rendering query for {self.__class__.__name__} {self.source_path}")
            logger.error(f"- Query: {self.query}")
            logger.error(f"- Options: {options}")


@compute_node(valid=("ds", "scope"))
def render_query(
    ds: TS[DeltaQueryDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]],
    _scheduler: SCHEDULER = None,
    _clock: EvaluationClock = None,
) -> TS[str]:
    scope = scope.value
    if len(scope) == 0:
        options_v = {}
    elif not options.valid:
        return
    else:
        options_v: dict = options.value

    poll = scope.get("poll", None)
    if poll:
        poll = poll.default()

    if (interval := options_v.get("poll", poll)) is not None:
        use_wall_clock = True
        next = (
            1 + ((_clock.now if use_wall_clock else _clock.evaluation_time) - MIN_DT) // interval
        ) * interval + MIN_DT
        _scheduler.schedule(next, "_", on_wall_clock=use_wall_clock)

    return ds.value.render(
        **{k: v.adjust(options_v[k]) if k in options_v else v.default() for k, v in scope.items()}
    )



@subscriber_impl_from_graph
def subscribe_delta_query_from_graph(
    dce: DataCatalogueEntry,
    ds: TS[DeltaQueryDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return delta_query_adaptor[_schema].from_graph(
        path=dce.store.source_path, tables=ds.tables, query=render_query(ds, dce.scope, options), __request_id__=request_id
    )


@subscriber_impl_to_graph
def subscribe_delta_to_graph(
    dce: DataCatalogueEntry,
    ds: TS[DeltaQueryDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    return delta_query_adaptor[_schema].to_graph(
        path=dce.store.source_path, __request_id__=request_id, __no_ts_inputs__=True
    )


if __name__ == "__main__":
    @dataclass
    class GraphPerformance(CompoundScalar):
        time: datetime
        evaluation_time: datetime
        cycles: float
        avg_cycle: float
        avg_os_cycle: float
        max_cycle: float
        graph_time: float
        os_graph_time: float
        graph_load: float
        avg_lag: float
        max_lag: float
        inspection_time: float
        memory: int
        virt_memory: int
        graph_memory: int
        date: date

    de = DataEnvironment()
    if os.environ.get("CODER", "false").lower() == "true":
        tempfile.tempdir = r'/home/data/_data'
    else:
        tempfile.gettempdir()

    de.add_entry(DataEnvironmentEntry(source_path="table_history_path", environment_path=os.path.join(tempfile.tempdir, "table_history")))
    de.set_current(de)

    dce = DataCatalogueEntry[DataSource](
        GraphPerformance,
        "graph_performance",
        store=DeltaQueryDataSource(
            source_path="table_history_path",
            tables=frozenset({"graph_performance"}),
            query="SELECT * FROM graph_performance WHERE date >= '{start}' AND date < '{end}' ORDER BY time",
        ),
        scope=frozendict(
            {
                "start": DateTimeScope(),
                "end": DateTimeScope(),
            }
        ),
    )

    @graph
    def g():
        register_adaptor(None, subscribe_adaptor_impl)
        register_adaptor(None, delta_query_adaptor_impl)
        register_adaptor(None, delta_query_adaptor_raw_impl)

        data = subscribe[GraphPerformance]("graph_performance", start=date(2025, 6, 24), end=date(2025, 6, 25))
        debug_print("data", data)

    evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME))
