import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from frozendict import frozendict
from hgraph.adaptors.data_catalogue import (
    DataCatalogueEntry,
    DataSource,
    DataEnvironment,
    DataEnvironmentEntry,
    DateTimeScope,
    Scope,
    subscribe,
    subscribe_adaptor_impl,
    subscriber_impl_from_graph,
    subscriber_impl_to_graph,
)
from hgraph.adaptors.delta.delta_adaptor import delta_read_adaptor, delta_read_adaptor_impl
from hgraph.adaptors.delta.delta_adaptor_raw import delta_read_adaptor_raw_impl
from hgraph.stream.stream import Stream, Data

from hgraph import (
    GraphConfiguration,
    Tuple,
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

logger = logging.getLogger(__name__)

__all__ = ["DeltaDataSource"]


DELTA_QUERY = Tuple[Tuple[str, str, object], ...]


@dataclass(frozen=True)
class DeltaDataSource(DataSource):
    table: str
    query: Tuple[Tuple[str, str, str], ...]
    sort: Tuple[Tuple[str, bool]] = None

    def render(self, **options) -> DELTA_QUERY:
        query = []
        for k, o, v in self.query:
            if v in options:
                if options[v] is not None:
                    query.append((k, o, options[v]))
                else:
                    pass
            else:
                query.append((k, o, v))

        return tuple(query)


@compute_node(valid=("ds", "scope"))
def render_query(
    ds: TS[DeltaDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]],
    _scheduler: SCHEDULER = None,
    _clock: EvaluationClock = None,
) -> TS[DELTA_QUERY]:
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

    return ds.value.render(**{k: v.adjust(options_v[k]) if k in options_v else v.default() for k, v in scope.items()})


@subscriber_impl_from_graph
def subscribe_delta_from_graph(
    dce: DataCatalogueEntry,
    ds: TS[DeltaDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return delta_read_adaptor[_schema].from_graph(
        path=dce.store.source_path,
        table=ds.table,
        filters=render_query(ds, dce.scope, options),
        sort=ds.sort,
        __request_id__=request_id,
    )


@subscriber_impl_to_graph
def subscribe_delta_to_graph(
    dce: DataCatalogueEntry,
    ds: TS[DeltaDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    return delta_read_adaptor[_schema].to_graph(
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
        tempfile.tempdir = r"/home/data/_data"
    else:
        tempfile.gettempdir()

    de.add_entry(
        DataEnvironmentEntry(
            source_path="table_history_path", environment_path=os.path.join(tempfile.tempdir, "table_history")
        )
    )
    de.set_current(de)

    dce = DataCatalogueEntry[DataSource](
        GraphPerformance,
        "graph_performance",
        store=DeltaDataSource(
            source_path="table_history_path",
            table="graph_performance",
            query=(
                ("date", ">", "start"),
                ("date", "<", "end"),
            ),
        ),
        scope=frozendict({
            "start": DateTimeScope(),
            "end": DateTimeScope(),
        }),
    )

    @graph
    def g():
        register_adaptor(None, subscribe_adaptor_impl)
        register_adaptor(None, delta_read_adaptor_impl)
        register_adaptor(None, delta_read_adaptor_raw_impl)

        data = subscribe[GraphPerformance]("graph_performance", start=date(2025, 6, 21), end=date(2025, 6, 25))
        debug_print("data", data)

    evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME))
