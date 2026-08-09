import logging
from dataclasses import dataclass
from typing import Mapping

from hgraph import (
    AUTO_RESOLVE, LOGGER, MIN_DT, SCHEMA, SCHEDULER, STATE, EvaluationClock,
    Frame, TS, TSB, compute_node, feedback, null_sink,
)
from hgraph.adaptors.data_catalogue.catalogue import DataCatalogueEntry, DataSource
from hgraph.adaptors.data_catalogue.data_scopes import RetryOptions, Scope
from hgraph.adaptors.data_catalogue.subscribe import (
    subscriber_impl_from_graph, subscriber_impl_to_graph,
)
from hgraph.stream import Data, Stream, StreamStatus

from .sql_adaptor import sql_read_adaptor

__all__ = ("SqlDataSource",)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlDataSource(DataSource):
    query: str

    def render(self, **options):
        return self.query.format(**options)


@compute_node(
    valid=("ds", "scope"),
    active=("ds", "scope", "options"),
)
def _render_query(
    ds: TS[SqlDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]],
    response_status: TS[StreamStatus] = None,
    _scheduler: SCHEDULER = None,
    _clock: EvaluationClock = None,
    _state: STATE = None,
    _logger: LOGGER = None,
) -> TS[str]:
    scope_value = scope.value
    options_value = {} if not scope_value else (options.value if options.valid else None)
    if options_value is None:
        return

    poll_scope = scope_value.get("poll")
    poll = poll_scope.default() if poll_scope else None
    retry = scope_value.get("retry")
    retry = retry.default() if retry else None
    retry_options = options_value.get("retry", retry)
    inputs_modified = ds.modified or scope.modified or options.modified

    if retry_options is not None:
        assert poll is None, "Cannot have both 'poll' and 'retry' options set"
        assert isinstance(retry_options, RetryOptions)
        response_status.make_active()
        if inputs_modified:
            if hasattr(_state, "retry"):
                _scheduler.un_schedule("_")
                del _state.retry

    if response_status.modified and not inputs_modified:
        if retry_options is not None:
            if response_status.value is StreamStatus.ERROR:
                retry_state = getattr(_state, "retry", retry_options.create())
                _state.retry = retry_state
                now = _clock.now
                if (next_time := retry_state.next(now)) is not None:
                    _logger.info(
                        "Will retry SQL query in %s at %s after error",
                        next_time - now, next_time,
                    )
                    _scheduler.schedule(next_time, "_", on_wall_clock=True)
                else:
                    _logger.info("Max retries reached for SQL query")
            elif hasattr(_state, "retry"):
                _scheduler.un_schedule("_")
                del _state.retry
        return

    if (interval := options_value.get("poll", poll)) is not None:
        next_time = (1 + (_clock.now - MIN_DT) // interval) * interval + MIN_DT
        _scheduler.schedule(next_time, "_", on_wall_clock=True)

    return ds.value.render(**{
        key: value.adjust(options_value[key]) if key in options_value else value.default()
        for key, value in scope_value.items()
    })


@subscriber_impl_from_graph
def subscribe_sql_from_graph(
    dce: DataCatalogueEntry, ds: TS[SqlDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    null_sink(request_id)


@subscriber_impl_to_graph
def subscribe_sql_to_graph(
    dce: DataCatalogueEntry, ds: TS[SqlDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    response_status = feedback(TS[StreamStatus])
    response = sql_read_adaptor[_schema](
        _render_query(ds, dce.scope, options, response_status()),
        path=dce.store.source_path,
    )
    response_status(response.status)
    return response
