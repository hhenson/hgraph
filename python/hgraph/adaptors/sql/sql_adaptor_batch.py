import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping

from frozendict import frozendict

from hgraph import (
    AUTO_RESOLVE,
    DEFAULT,
    LOGGER,
    SCHEMA,
    Frame,
    TS,
    TSB,
    TSD,
    compute_node,
    convert,
    exception_time_series,
    feedback,
    flip,
    graph,
    if_then_else,
    map_,
    partition,
    rekey,
    service_adaptor,
    service_adaptor_impl,
    throttle,
    unpartition,
    valid,
)
from hgraph.adaptors.data_catalogue import DataEnvironment, DataSource
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph.reflection import fields
from hgraph.stream import (
    Data,
    Stream,
    StreamStatus,
)

from .sql_adaptor_raw import sql_read_adaptor_raw
from .sql_connection import _require_polars

logger = logging.getLogger(__name__)

__all__ = (
    "BatchSqlDataSource",
    "sql_adaptor_batch",
    "sql_adaptor_batch_impl",
)


@dataclass(frozen=True)
class BatchSqlDataSource(DataSource):
    name: str
    query: str
    filters: frozendict[str, str] = frozendict()

    def __post_init__(self):
        if not isinstance(self.filters, frozendict):
            object.__setattr__(self, "filters", frozendict(self.filters))

    def render(self, **options):
        return self.query.format(**options)


@service_adaptor
def sql_adaptor_batch(
    ds: TS[BatchSqlDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]],
    path: str,
    _schema: type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=sql_adaptor_batch)
def sql_adaptor_batch_impl(
    ds: TSD[int, TS[BatchSqlDataSource]],
    scope: TSD[int, TS[Mapping[str, Scope]]],
    options: TSD[int, TS[dict[str, object]]],
    path: str,
    batch_period: timedelta = timedelta(seconds=1),
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    environment = DataEnvironment.current()
    if environment is None:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = environment.get_entry(path).environment_path

    ds_batch = throttle(ds, period=batch_period, delay_first_tick=True)
    options_batch = throttle(options, period=batch_period, delay_first_tick=True)
    scope_batch = throttle(scope, period=batch_period, delay_first_tick=True)

    request_ids = flip(ds_batch, unique=False)
    partitioned_options = partition(options_batch, ds_batch)
    rekeyed_scope = rekey(scope_batch, ds_batch)
    grouped_queries = map_(
        lambda key, scoped, opts: render_batch_query(
            ds=key, scope=scoped, options=opts),
        scoped=rekeyed_scope,
        opts=partitioned_options,
        __keys__=request_ids.key_set,
    )
    queries = unpartition(grouped_queries)

    requests = map_(
        lambda query: sql_read_adaptor_raw.from_graph(
            path=connection_string, query=query),
        query=queries,
    )
    results = map_(
        lambda request: sql_read_adaptor_raw.to_graph(
            path=connection_string, __request_id__=request,
            __no_ts_inputs__=True),
        feedback(requests)(),
    )

    return extract_data(
        results,
        feedback(ds_batch)(),
        feedback(options_batch)(),
        feedback(scope_batch)(),
        _schema=_schema,
    )


@compute_node(active=("options",))
def render_batch_query(
    ds: TS[BatchSqlDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TSD[int, TS[dict[str, object]]],
) -> TSD[tuple[int, ...], TS[str]]:
    source = ds.value
    collected = {}
    for _, changed_options in options.modified_items():
        for name, value in changed_options.value.items():
            if name in source.filters:
                collected.setdefault(name, []).append(value)
            else:
                collected[name] = value

    if not collected:
        return

    adjusted = {
        name: item.adjust(collected[name]) if name in collected else item.default()
        for name, item in scope.value.items()
    }
    if any(value is None for value in adjusted.values()):
        logger.error(
            "Collected None values for a batch query: %s, incoming options: %s",
            adjusted,
            options.delta_value,
        )
        return

    return {tuple(options.modified_keys()): source.render(**adjusted)}


@graph
def extract_data(
    data: TSD[tuple[int, ...], TSB[Stream[Data[Frame]]]],
    ds: TSD[int, TS[BatchSqlDataSource]],
    options: TSD[int, TS[dict[str, object]]],
    scope: TSD[int, TS[Mapping[str, Scope]]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    return map_(
        filter_data,
        data=unpack_data(data),
        ds=ds,
        options=options,
        scope=scope,
        _schema=_schema,
    )


@compute_node
def unpack_data(
    data: TSD[tuple[int, ...], TSB[Stream[Data[Frame]]]],
) -> TSD[int, TSB[Stream[Data[Frame]]]]:
    output = {}
    for keys, value in data.modified_items():
        if bundle := value.value:
            output.update({key: bundle for key in keys})
    return output or None


@graph
def filter_data(
    data: TSB[Stream[Data[Frame]]],
    ds: TS[BatchSqlDataSource],
    options: TS[dict[str, object]],
    scope: TS[Mapping[str, Scope]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    filtered_raw = filter_data_(data.values, ds, options, scope, _schema)
    filtered = convert[TS[Frame[_schema]]](filtered_raw)
    error = exception_time_series(filtered)
    output_type = TSB[Stream[Data[Frame[_schema]]]]
    return output_type.from_ts(
        status=if_then_else(
            valid(error),
            StreamStatus.ERROR,
            data.status,
        ),
        status_msg=if_then_else(
            valid(error),
            error.error_msg,
            data.status_msg,
        ),
        timestamp=data.timestamp,
        values=filtered,
    )


@compute_node(active=("data",))
def filter_data_(
    data: TS[object],
    ds: TS[BatchSqlDataSource],
    options: TS[dict[str, object]],
    scope: TS[Mapping[str, Scope]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
    _logger: LOGGER = None,
) -> TS[Frame]:
    from hgraph._frame import as_arrow_table

    pl = _require_polars()
    source = ds.value
    scope_value = scope.value
    options_value = options.value
    expressions = []
    for name, value in options_value.items():
        if name in source.filters:
            adjusted = scope_value[name].adjust(value)
            expression = source.filters[name].format(
                adjusted, **{name: adjusted})
            expressions.append(pl.sql_expr(expression))

    frame = pl.from_arrow(as_arrow_table(data.value))
    if expressions:
        frame = frame.filter(*expressions)
    schema_columns = set(fields(_schema))
    frame = frame.select(
        column for column in frame.columns if column in schema_columns)

    if frame.is_empty():
        adjusted_options = {
            name: scope_value[name].adjust(value)
            for name, value in options_value.items()
            if name in scope_value
        }
        _logger.warning(
            "No data returned from %s with options %s",
            source.source_path,
            adjusted_options,
        )
    return frame.to_arrow()
