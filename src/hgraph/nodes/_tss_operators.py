import hgraph
from hgraph import compute_node, contains_, REF, TSS, TS, STATE, PythonTimeSeriesReference, not_, graph, \
    KEYABLE_SCALAR, PythonSetDelta
from hgraph.nodes._operators import len_
from hgraph.nodes._set_operators import is_empty


__all__ = ("tss_contains", "tss_is_empty", "tss_not_", "tss_len", "tss_intersection", "tss_union", "tss_difference")


@compute_node(overloads=contains_)
def tss_contains(ts: REF[TSS[KEYABLE_SCALAR]], item: TS[KEYABLE_SCALAR], _state: STATE = None) \
        -> REF[TS[bool]]:
    """Perform a time-series contains check of an item in the given time-series set"""
    # If the tss is set then we should de-register the old contains.
    if _state.tss is not None:
        _state.tss.release_contains_output(_state.item, _state.requester)
    _state.tss = ts.value.output
    _state.item = item.value
    return PythonTimeSeriesReference(
        None if _state.tss is None else _state.tss.get_contains_output(_state.item, _state.requester))


@tss_contains.start
def _tss_contains_start(_state: STATE):
    _state.requester = object()
    _state.tss = None
    _state.item = None


@compute_node(overloads=is_empty)
def tss_is_empty(ts: REF[TSS[KEYABLE_SCALAR]]) -> REF[TS[bool]]:
    """A time-series ticking with the empty state of the TSS supplied is modified"""
    # NOTE: Since the TSS output is currently a fixed output we don't need to track state.
    return PythonTimeSeriesReference(ts.value.output.is_empty_output() if ts.value.valid else None)


@graph(overloads=not_)
def tss_not_(ts: TSS[KEYABLE_SCALAR]) -> TS[bool]:
    return tss_is_empty(ts)


@compute_node(overloads=len_)
def tss_len(ts: TSS[KEYABLE_SCALAR]) -> TS[int]:
    return len(ts.value)


@compute_node(overloads=hgraph.add_)
def tss_union(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    added = lhs.added() | rhs.added()
    lhs_value = lhs.value
    removed = lhs.removed() - rhs.value
    for i in rhs.removed():
        if i not in lhs_value:
            removed.add(i)
    return PythonSetDelta(added, removed)


@compute_node(overloads=hgraph.sub_)
def tss_difference(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    added = set()
    removed = set()
    lhs_value = lhs.value
    rhs_value = rhs.value
    for i in lhs.added():
        if i not in rhs_value:
            added.add(i)
    for i in lhs.removed():
        removed.add(i)
    for i in rhs.added():
        if i in lhs_value:
            removed.add(i)
    for i in rhs.removed():
        if i in lhs_value:
            added.add(i)
    return PythonSetDelta(added, removed)


@compute_node
def tss_intersection(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    added = set()
    removed = lhs.removed() | rhs.removed()
    lhs_value = lhs.value
    rhs_value = rhs.value
    for i in lhs.added():
        if i in rhs_value:
            added.add(i)
    for i in rhs.added():
        if i in lhs_value:
            added.add(i)
    return PythonSetDelta(added, removed)
