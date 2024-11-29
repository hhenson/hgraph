from collections import defaultdict
from typing import Type, Mapping

from hgraph import (
    adaptor,
    TIME_SERIES_TYPE,
    TSD,
    K,
    service_adaptor,
    SCALAR,
    TS,
    adaptor_impl,
    service_adaptor_impl,
    rekey,
    WiringGraphContext,
    WiringNodeInstance,
    CustomMessageWiringError,
    register_adaptor, flip, map_, TSS, graph, REF, TSB, SCHEMA, TS_SCHEMA, race, nothing, combine, len_, emit, assert_,
    pass_through, sink_node, reduce_tsd_with_race, reduce_tsd_of_bundles_with_race, operator,
)
from hgraph._wiring._wiring_node_class._service_adaptor_node_class import ServiceAdaptorNodeClass
from hgraph.adaptors.perspective._perspetive_publish import _publish_table, _receive_table_edits, TableEdits

__all__ = (
    "publish_table",
    "publish_table_editable",
    "publish_multitable",
    "publish_table_impl",
    "publish_table_editable_impl",
    "publish_multitable_impl",
    "register_perspective_adaptors",
)


@adaptor
def publish_table(path: str, ts: TSD[K, TIME_SERIES_TYPE], index_col_name: str, history: int = None): ...


@adaptor_impl(interfaces=publish_table)
def publish_table_impl(path: str, ts: TSD[K, TIME_SERIES_TYPE], index_col_name: str, history: int = None):
    _assert_unique_type_per_path(publish_table)

    _publish_table(path, ts, index_col_name=index_col_name, history=history)


@adaptor
def publish_table_editable(
    path: str, ts: TSD[K, TIME_SERIES_TYPE], index_col_name: str, history: int = None, empty_row: bool = False
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]: ...


@graph
def publish_join_table(path: str, schema: Mapping[str, type], index: str, description: object):
    _publish_join_table(path, schema, index, description, nothing(TS[bool]))


@sink_node
def _publish_join_table(path: str, schema: dict[str, type], index: str, description: object, _: TS[bool]):
    ...


@_publish_join_table.start
def _publish_join_table_start(path: str, schema: dict[str, type], index: str, description: object, _: TS[bool]):
    from hgraph.adaptors.perspective import PerspectiveTablesManager
    PerspectiveTablesManager.current().add_join(path, schema, index, description)


@adaptor_impl(interfaces=publish_table_editable)
def publish_table_editable_impl(
    path: str, ts: TSD[K, TIME_SERIES_TYPE], index_col_name: str, history: int = None, empty_row: bool = False
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]:
    _assert_unique_type_per_path(publish_table_editable)

    _publish_table(path, ts, index_col_name=index_col_name, history=history, editable=True, empty_row=empty_row)
    return _receive_table_edits(path, tp=ts.output_type.dereference().py_type, index_col_name=index_col_name, empty_row=empty_row)


@service_adaptor
def publish_multitable(path: str, key: TS[SCALAR], ts: TIME_SERIES_TYPE, unique: bool, index_col_name: str, history: int = None): ...


@service_adaptor_impl(interfaces=publish_multitable)
def publish_multitable_impl(
    path: str, key: TSD[int, TS[SCALAR]], ts: TSD[int, TIME_SERIES_TYPE], unique: bool, index_col_name: str, history: int = None
):
    _assert_unique_type_per_path(publish_multitable)

    if not unique:
        @operator
        def merge_references(keys: TSS[int], ts: TSD[int, REF[TIME_SERIES_TYPE]], _schema: Type[TS_SCHEMA] = TS_SCHEMA) -> REF[TIME_SERIES_TYPE]:
            ...

        @graph(overloads=merge_references)
        def merge_references_tsb(keys: TSS[int], ts: TSD[int, REF[TSB[TS_SCHEMA]]], _schema: Type[TS_SCHEMA] = TS_SCHEMA) -> TSB[TS_SCHEMA]:
            selection = ts[keys]
            return reduce_tsd_of_bundles_with_race(tsd=selection)

        @graph(overloads=merge_references)
        def merge_references_no_tsb(keys: TSS[int], ts: TSD[int, REF[TS[SCALAR]]]) -> REF[TS[SCALAR]]:
            assert_(len_(keys), 1, "Only bundles are allowed to be published as multi-tables with repeating keys")
            return ts[emit(keys)]

        keys = flip(key, unique=False)
        table = map_(lambda keys, ts: merge_references(keys, ts), keys, pass_through(ts))
        _publish_table(path, table, index_col_name=index_col_name, history=history)

    else:
        _publish_table(path, rekey(ts, key), index_col_name=index_col_name, history=history)


def _assert_unique_type_per_path(adaptor_type):
    adaptors_dedup = defaultdict(lambda: defaultdict(set))
    all_clients = WiringGraphContext.__stack__[0].registered_service_clients(adaptor_type)
    for path, type_map, _, receive in all_clients:
        path = path.replace("/from_graph", "").replace("/to_graph", "")
        for k, t in type_map.items():
            adaptors_dedup[(path, receive)][k].add(t)

    errors = []
    for (path, item), types in adaptors_dedup.items():
        for k, v in types.items():
            if len(v) > 1:
                errors.append(
                    f"For {adaptor_type} at path '{path}' not every client provided the same type for {item}:"
                )
                for t in v:
                    errors.append(f"\tsome provided {t}")

    if errors:
        raise CustomMessageWiringError("\n".join(errors))


def register_perspective_adaptors():
    register_adaptor(None, publish_table_impl)
    register_adaptor(None, publish_table_editable_impl)
    register_adaptor(None, publish_multitable_impl)
