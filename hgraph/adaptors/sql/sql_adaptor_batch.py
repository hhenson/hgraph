import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Type, Mapping

import polars as pl
from frozendict import frozendict
from polars import DataFrame

from hgraph.adaptors.sql.sql_adaptor_raw import sql_read_adaptor_raw
from hgraph.stream.stream import Stream, combine_statuses, combine_status_messages, StreamStatus, Data
from hgraph.adaptors.data_catalogue.catalogue import DataSource, DataEnvironment
from hgraph.adaptors.data_catalogue.data_scopes import Scope
from hgraph import (
    service_adaptor,
    TS,
    TSB,
    SCHEMA,
    DEFAULT,
    Frame,
    service_adaptor_impl,
    TSD,
    AUTO_RESOLVE,
    compute_node,
    map_,
    feedback,
    flip,
    partition,
    rekey,
    graph,
    exception_time_series,
    if_then_else,
    valid,
    unpartition,
    HgScalarTypeMetaData,
    combine,
    throttle,
    LOGGER,
)

logger = logging.getLogger(__name__)


__all__ = ['BatchSqlDataSource', 'sql_adaptor_batch', 'sql_adaptor_batch_impl']

@dataclass(frozen=True)
class BatchSqlDataSource(DataSource):
    name: str
    query: str
    filters: frozendict[str, str]

    def render(self, **options):
        try:
            return self.query.format(**options)
        except:
            logger.exception(f"Error rendering query for {self.__class__.__name__} {self.name} {self.source_path}")
            logger.error(f"- Query: {self.query}")
            logger.error(f"- Options: {options}")
            logger.error(f"- Filters: {self.filters}")


@service_adaptor
def sql_adaptor_batch(
    ds: TS[BatchSqlDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]],
    path: str,
    _schema: Type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[Frame[SCHEMA]]]]: ...


@service_adaptor_impl(interfaces=sql_adaptor_batch)
def sql_adaptor_batch_impl(
    ds: TSD[int, TS[BatchSqlDataSource]],
    scope: TSD[int, TS[Mapping[str, Scope]]],
    options: TSD[int, TS[dict[str, object]]],
    path: str,
    batch_period: timedelta = timedelta(seconds=1),
    _schema: Type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    de = DataEnvironment.current()
    if not de:
        raise RuntimeError(f"No DataEnvironment set up for {path}")
    connection_string = de.get_entry(path).environment_path

    ds_batch = throttle(ds, period=batch_period, delay_first_tick=True)
    options_batch = throttle(options, period=batch_period, delay_first_tick=True)
    scope_batch = throttle(scope, period=batch_period, delay_first_tick=True)

    request_ids = flip(ds_batch, unique=False)
    options_partitioned = partition(options_batch, ds_batch)
    scope_rekeyed = rekey(scope_batch, ds_batch)

    ds_queries = map_(
        lambda key, s, o: render_batch_query(ds=key, scope=s, options=o),
        s=scope_rekeyed,
        o=options_partitioned,
        __keys__=request_ids.key_set,
    )
    queries = unpartition(ds_queries)

    inputs = map_(lambda q: sql_read_adaptor_raw.from_graph(path=connection_string, query=q), q=queries)

    results = map_(
        lambda r: sql_read_adaptor_raw.to_graph(path=connection_string, __request_id__=r, __no_ts_inputs__=True),
        feedback(inputs)(),
    )

    return extract_data(
        results, feedback(ds_batch)(), feedback(options_batch)(), feedback(scope_batch)(), _schema=_schema
    )


@compute_node(active=("options",))
def render_batch_query(
    ds: TS[BatchSqlDataSource], scope: TS[Mapping[str, Scope]], options: TSD[int, TS[dict[str, object]]]
) -> TSD[tuple[int, ...], TS[str]]:
    ds = ds.value
    options_collected = {}
    for k, changed_options in options.modified_items():
        for param, value in changed_options.value.items():
            if param in ds.filters:
                options_collected.setdefault(param, []).append(value)
            else:
                options_collected[param] = value

    if not options_collected:
        return

    adjusted = {
        k: v.adjust(options_collected[k]) if k in options_collected else v.default() for k, v in scope.value.items()
    }

    if any(v is None for v in adjusted.values()):
        logger.error(f"collected None values for a batch query: {adjusted}, incoming options: {options.delta_value}")
        return

    return {tuple(options.modified_keys()): ds.render(**adjusted)}


@graph
def extract_data(
    data: TSD[tuple[int, ...], TSB[Stream[Data[DataFrame]]]],
    ds: TSD[int, TS[BatchSqlDataSource]],
    options: TSD[int, TS[dict[str, object]]],
    scope: TSD[int, TS[Mapping[str, Scope]]],
    _schema: Type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    unpacked_data = unpack_data(data)
    return map_(filter_data, options=options, ds=ds, data=unpacked_data, scope=scope, _schema=_schema)


@compute_node
def unpack_data(data: TSD[tuple[int, ...], TSB[Stream[Data[DataFrame]]]]) -> TSD[int, TSB[Stream[Data[DataFrame]]]]:
    out = {}
    for keys, v in data.modified_items():
        if v_ := v.value:
            out.update({k: v_ for k in keys})
    if out:
        return out


@graph
def filter_data(
    data: TSB[Stream[Data[DataFrame]]],
    ds: TS[BatchSqlDataSource],
    options: TS[dict[str, object]],
    scope: TS[Mapping[str, Scope]],
    _schema: Type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    error = exception_time_series(data)
    filtered_data = filter_data_(data.values, ds, options, scope, _schema)
    return combine[TSB[Stream[Data[Frame[_schema]]]]](
        status=if_then_else(valid(error), StreamStatus.ERROR, combine_statuses(data.status, filtered_data.status)),
        status_msg=if_then_else(
            valid(error), error.error_msg, combine_status_messages(data.status_msg, filtered_data.status_msg)
        ),
        timestamp=data.timestamp,
        values=filtered_data.values,
    )


@compute_node(active=("data",))
def filter_data_(
    data: TS[DataFrame],
    ds: TS[BatchSqlDataSource],
    options: TS[dict[str, object]],
    scope: TS[Mapping[str, Scope]],
    _schema: Type[SCHEMA],
    logger: LOGGER = None,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ds = ds.value
    scope = scope.value
    options = options.value
    filters = [
        pl.sql_expr(ds.filters[param].format(**{param: scope[param].adjust(value)}))
        for param, value in options.items()
        if param in ds.filters
    ]
    meta = HgScalarTypeMetaData.parse_type(_schema)
    data = data.value
    out = data.filter(*filters).drop(col for col in data.columns if col not in meta.meta_data_schema)
    if out.is_empty():
        options_adjusted = {k: scope[k].adjust(v) for k, v in options.items()}
        msg = f"No data returned from {ds.source_path} with options {options_adjusted}"
        logger.warning(msg)
    return {"status": StreamStatus.OK, "status_msg": "", "values": out}
