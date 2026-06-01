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
    register_adaptor,
    flip,
    map_,
    TSS,
    graph,
    REF,
    TSB,
    SCHEMA,
    TS_SCHEMA,
    race,
    nothing,
    combine,
    len_,
    emit,
    assert_,
    pass_through,
    sink_node,
    reduce_tsd_with_race,
    reduce_tsd_of_bundles_with_race,
    operator,
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
def publish_table(path: str, ts: TSD[K, TIME_SERIES_TYPE], index_col_name: str, history: int = None):
    """
    Publish a time-series dictionary as a read-only table via the Perspective adaptor.

    Publishes the entries of ``ts`` as rows in a table identified by ``path``. Each key in the TSD
    becomes a row, with the key value placed in the column named by ``index_col_name``.

    :param path: Unique identifier for the table. Must be unique per adaptor usage.
    :param ts: A time-series dictionary whose entries form the table rows.
    :param index_col_name: Name of the column that holds the TSD key values.
    :param history: If set, the number of historical ticks to retain. ``None`` means no history.
    """
    ...


@adaptor_impl(interfaces=publish_table)
def publish_table_impl(path: str, ts: TSD[K, TIME_SERIES_TYPE], index_col_name: str, history: int = None):
    """Implementation of :func:`publish_table`. Delegates to the internal ``_publish_table`` helper."""
    _assert_unique_type_per_path(publish_table)

    _publish_table(path, ts, index_col_name=index_col_name, history=history)


@adaptor
def publish_table_editable(
    path: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    index_col_name: str,
    history: int = None,
    edit_role: str = None,
    empty_row: bool = False,
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]:
    """
    Publish a time-series dictionary as an editable table via the Perspective adaptor.

    Similar to :func:`publish_table`, but allows the UI to send edits back into the graph.
    Returns a ``TSB[TableEdits]`` bundle containing an ``edits`` TSD (modified rows) and a
    ``removes`` TSS (removed keys).

    :param path: Unique identifier for the table. Must be unique per adaptor usage.
    :param ts: A time-series dictionary whose entries form the table rows.
    :param index_col_name: Name of the column that holds the TSD key values.
    :param history: If set, the number of historical ticks to retain. ``None`` means no history.
    :param edit_role: Optional role name that restricts who may edit the table.
    :param empty_row: When ``True``, the UI can create new rows via an empty-row placeholder.
    :return: A time-series bundle of edits and removals received from the UI.
    """
    ...


@adaptor_impl(interfaces=publish_table_editable)
def publish_table_editable_impl(
    path: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    index_col_name: str,
    history: int = None,
    edit_role: str = None,
    empty_row: bool = False,
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]:
    """
    Implementation of :func:`publish_table_editable`.

    Publishes the table with ``editable=True`` and returns a bundle that streams back
    edits and removals from the UI.
    """
    _assert_unique_type_per_path(publish_table_editable)

    _publish_table(
        path,
        ts,
        index_col_name=index_col_name,
        history=history,
        editable=True,
        empty_row=empty_row,
        edit_role=edit_role,
    )
    return _receive_table_edits(
        path, tp=ts.output_type.dereference().py_type, index_col_name=index_col_name, empty_row=empty_row
    )


@service_adaptor
def publish_multitable(
    path: str, key: TS[SCALAR], ts: TIME_SERIES_TYPE, unique: bool, index_col_name: str, history: int = None
):
    """
    Publish time-series data from multiple graph clients into a single shared table.

    This is the multi-client variant of :func:`publish_table`. Each client contributes rows
    keyed by ``key``. When ``unique`` is ``True``, every client must supply distinct keys;
    when ``False``, rows from different clients with the same key are merged (bundles are
    merged with a race strategy, scalars require uniqueness).

    :param path: Unique identifier for the shared table.
    :param key: A time-series scalar that identifies this client's contribution.
    :param ts: The time-series data to publish as table rows.
    :param unique: Whether each client is expected to provide distinct key values.
    :param index_col_name: Name of the column that holds the key values.
    :param history: If set, the number of historical ticks to retain. ``None`` means no history.
    """
    ...


@service_adaptor_impl(interfaces=publish_multitable)
def publish_multitable_impl(
    path: str,
    key: TSD[int, TS[SCALAR]],
    ts: TSD[int, TIME_SERIES_TYPE],
    unique: bool,
    index_col_name: str,
    history: int = None,
):
    """
    Implementation of :func:`publish_multitable`.

    When ``unique`` is ``False``, merges data from multiple clients sharing the same key using
    a race-reduction strategy for bundles. When ``unique`` is ``True``, re-keys the data
    directly.
    """
    _assert_unique_type_per_path(publish_multitable)

    if not unique:

        @operator
        def merge_references(
            keys: TSS[int], ts: TSD[int, REF[TIME_SERIES_TYPE]], _schema: Type[TS_SCHEMA] = TS_SCHEMA
        ) -> REF[TIME_SERIES_TYPE]: ...

        @graph(overloads=merge_references)
        def merge_references_tsb(
            keys: TSS[int], ts: TSD[int, REF[TSB[TS_SCHEMA]]], _schema: Type[TS_SCHEMA] = TS_SCHEMA
        ) -> TSB[TS_SCHEMA]:
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
