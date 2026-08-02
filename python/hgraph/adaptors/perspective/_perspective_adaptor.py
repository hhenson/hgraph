"""Public Perspective adaptor interfaces and their default implementations."""

from collections import defaultdict

from hgraph import (
    K,
    REF,
    SCALAR,
    TIME_SERIES_TYPE,
    TS,
    TSB,
    TSD,
    TSS,
    TS_SCHEMA,
    WiringGraphContext,
    WiringError,
    adaptor,
    adaptor_impl,
    assert_,
    emit,
    flip,
    graph,
    len_,
    map_,
    operator,
    pass_through,
    reduce_tsd_of_bundles_with_race,
    register_adaptor,
    rekey,
    service_adaptor,
    service_adaptor_impl,
)

from ._perspective import perspective_web
from ._perspective_publish import (
    TableEdits,
    _publish_table,
    _receive_table_edits,
)

__all__ = (
    "publish_table",
    "publish_table_editable",
    "publish_multitable",
    "publish_table_impl",
    "publish_table_editable_impl",
    "publish_multitable_impl",
    "register_perspective_adaptors",
    "perspective_web",
)


@adaptor
def publish_table(
    path: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    index_col_name: str,
    history: int = None,
) -> None:
    """Publish a TSD as a read-only Perspective table."""


@adaptor_impl(interfaces=publish_table)
def publish_table_impl(
    path: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    index_col_name: str,
    history: int = None,
) -> None:
    _assert_unique_type_per_path(publish_table)
    _publish_table(
        path, ts, index_col_name=index_col_name, history=history)


@adaptor
def publish_table_editable(
    path: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    index_col_name: str,
    history: int = None,
    edit_role: str = None,
    empty_row: bool = False,
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]:
    """Publish an editable TSD and return its typed edits and removals."""


@adaptor_impl(interfaces=publish_table_editable)
def publish_table_editable_impl(
    path: str,
    ts: TSD[K, TIME_SERIES_TYPE],
    index_col_name: str,
    history: int = None,
    edit_role: str = None,
    empty_row: bool = False,
) -> TSB[TableEdits[K, TIME_SERIES_TYPE]]:
    _assert_unique_type_per_path(publish_table_editable)
    plan = _publish_table(
        path,
        ts,
        index_col_name=index_col_name,
        history=history,
        editable=True,
        edit_role=edit_role,
        empty_row=empty_row,
    )
    return _receive_table_edits(
        path,
        ts,
        index_col_name=index_col_name,
        empty_row=empty_row,
        plan=plan,
    )


@service_adaptor
def publish_multitable(
    path: str,
    key: TS[SCALAR],
    ts: TIME_SERIES_TYPE,
    unique: bool,
    index_col_name: str,
    history: int = None,
) -> None:
    """Publish multiple clients into one shared Perspective table."""


@operator
def _merge_multitable_references(
    keys: TSS[int],
    ts: TSD[int, REF[TIME_SERIES_TYPE]],
) -> REF[TIME_SERIES_TYPE]: ...


@graph(overloads=_merge_multitable_references)
def _merge_multitable_bundles(
    keys: TSS[int],
    ts: TSD[int, REF[TSB[TS_SCHEMA]]],
) -> TSB[TS_SCHEMA]:
    return reduce_tsd_of_bundles_with_race(tsd=ts[keys])


@graph(overloads=_merge_multitable_references)
def _merge_multitable_scalars(
    keys: TSS[int],
    ts: TSD[int, REF[TS[SCALAR]]],
) -> REF[TS[SCALAR]]:
    assert_(
        len_(keys),
        1,
        "Only bundles may be published as multi-tables with repeating keys",
    )
    return ts[emit(keys)]


@service_adaptor_impl(interfaces=publish_multitable)
def publish_multitable_impl(
    path: str,
    key: TSD[int, TS[SCALAR]],
    ts: TSD[int, TIME_SERIES_TYPE],
    unique: bool,
    index_col_name: str,
    history: int = None,
) -> None:
    _assert_unique_type_per_path(publish_multitable)
    if unique:
        table = rekey(ts, key)
    else:
        keys = flip(key, unique=False)
        table = map_(
            lambda keys, ts: _merge_multitable_references(keys, ts),
            keys,
            pass_through(ts),
        )
    _publish_table(
        path, table, index_col_name=index_col_name, history=history)


def _assert_unique_type_per_path(adaptor_type):
    by_path = defaultdict(lambda: defaultdict(set))
    for path, type_map, _, receive in \
            WiringGraphContext.instance().registered_service_clients(adaptor_type):
        path = path.removesuffix("/from_graph").removesuffix("/to_graph")
        for variable, concrete in type_map.items():
            by_path[(path, receive)][variable].add(concrete)

    errors = []
    for (path, item), variables in by_path.items():
        for variable, concrete in variables.items():
            if len(concrete) <= 1:
                continue
            errors.append(
                f"For {adaptor_type.__name__} at path {path!r}, clients "
                f"provided different {variable} types for {item}:")
            errors.extend(f"  {value}" for value in concrete)
    if errors:
        raise WiringError("\n".join(errors))


def register_perspective_adaptors():
    """Register the default implementations for the current wiring graph."""
    register_adaptor(None, publish_table_impl)
    register_adaptor(None, publish_table_editable_impl)
    register_adaptor(None, publish_multitable_impl)
