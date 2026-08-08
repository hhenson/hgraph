from dataclasses import dataclass

from frozendict import frozendict

from hgraph import AUTO_RESOLVE, SCHEMA, TS, TSB, Frame, compute_node, null_sink
from hgraph.adaptors.data_catalogue import DataSource, DataCatalogueEntry
from hgraph.adaptors.data_catalogue.subscribe import subscriber_impl_from_graph, subscriber_impl_to_graph
from hgraph.stream import Data, Stream

from .sql_adaptor import sql_read_adaptor, sql_read_adaptor_impl

__all__ = ("BatchSqlDataSource", "sql_adaptor_batch", "sql_adaptor_batch_impl")


@dataclass(frozen=True)
class BatchSqlDataSource(DataSource):
    name: str
    query: str
    filters: frozendict[str, str] = frozendict()

    def render(self, **options):
        return self.query.format(**options)


@subscriber_impl_from_graph
def subscribe_batch_sql_from_graph(
    dce: DataCatalogueEntry, ds: TS[BatchSqlDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    null_sink(request_id)


@subscriber_impl_to_graph
def subscribe_batch_sql_to_graph(
    dce: DataCatalogueEntry, ds: TS[BatchSqlDataSource],
    options: TS[dict[str, object]], request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    return sql_adaptor_batch[_schema](
        ds, dce.scope, options, path=dce.store.source_path)


@compute_node
def _render_batch_query(
    ds: TS[BatchSqlDataSource], scope: TS[object], options: TS[object]
) -> TS[str]:
    values = options.value or {}
    scopes = scope.value or {}
    adjusted = {
        name: item.adjust(values[name]) if name in values else item.default()
        for name, item in scopes.items()
    }
    return ds.value.render(**adjusted)


class _BatchSqlAdaptor:
    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        return _BatchSqlAdaptor(schema)

    def __call__(self, ds, scope, options, *, path):
        adaptor = sql_read_adaptor if self.schema is None else sql_read_adaptor[self.schema]
        return adaptor(_render_batch_query(ds, scope, options), path=path)


sql_adaptor_batch = _BatchSqlAdaptor()
sql_adaptor_batch_impl = sql_read_adaptor_impl
