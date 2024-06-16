from statistics import stdev, variance
from typing import Type

from hgraph._impl._types._ref import PythonTimeSeriesReference
from hgraph._impl._types._tss import PythonSetDelta
from hgraph._operators import contains_, is_empty, len_, bit_or, sub_, bit_and, bit_xor, eq_, and_, or_, min_, max_, \
    sum_, zero, std, var, str_, not_, mean
from hgraph._types._ref_type import REF
from hgraph._types._scalar_types import STATE, KEYABLE_SCALAR
from hgraph._types._ts_type import TS
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._types._tss_type import TSS
from hgraph._wiring._decorators import compute_node, graph

__all__ = ()


@compute_node(overloads=contains_)
def contains_tss(ts: REF[TSS[KEYABLE_SCALAR]], item: TS[KEYABLE_SCALAR], _state: STATE = None) -> REF[TS[bool]]:
    """
    Perform a time-series contains check of an item in the given time-series set
    """
    # If the tss is set then we should de-register the old contains.
    if _state.tss is not None:
        _state.tss.release_contains_output(_state.item, _state.requester)
    _state.tss = ts.value.output
    _state.item = item.value
    return PythonTimeSeriesReference(
        None if _state.tss is None else _state.tss.get_contains_output(_state.item, _state.requester)
    )


@contains_tss.start
def _tss_contains_start(_state: STATE):
    _state.requester = object()
    _state.tss = None
    _state.item = None


@compute_node(overloads=is_empty)
def is_empty_tss(ts: REF[TSS[KEYABLE_SCALAR]]) -> REF[TS[bool]]:
    """
    A time-series ticking with the empty state of the TSS
    """
    # NOTE: Since the TSS output is currently a fixed output we don't need to track state.
    return PythonTimeSeriesReference(ts.value.output.is_empty_output() if ts.value.valid else None)


@graph(overloads=not_)
def not_tss(ts: TSS[KEYABLE_SCALAR]) -> TS[bool]:
    return is_empty_tss(ts)


@compute_node(overloads=len_)
def len_tss(ts: TSS[KEYABLE_SCALAR]) -> TS[int]:
    return len(ts.value)


@compute_node(overloads=bit_or)
def bit_or_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    added = lhs.added() | rhs.added()
    lhs_value = lhs.value
    removed = lhs.removed() - rhs.value
    for i in rhs.removed():
        if i not in lhs_value:
            removed.add(i)
    return PythonSetDelta(added, removed)


@compute_node(overloads=sub_)
def sub_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    added = set()
    removed = set()
    lhs_value = lhs.value
    rhs_value = rhs.value
    for i in lhs_value:
        if i not in rhs_value:
            added.add(i)
    for i in lhs.removed():
        removed.add(i)
    for i in rhs_value:
        if i in lhs_value:
            removed.add(i)
    for i in rhs.removed():
        if i in lhs_value:
            added.add(i)
    return PythonSetDelta(added, removed)


@compute_node(overloads=bit_and)
def bit_and_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    removed = lhs.removed() | rhs.removed()
    added = rhs.value.intersection(lhs.value)
    return PythonSetDelta(added, removed)


@compute_node(overloads=bit_xor)
def bit_xor_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    # Symmetrical difference - i.e. items which are in either but not both TSS's
    added = set()
    removed = set()
    lhs_value = lhs.value
    rhs_value = rhs.value
    for i in lhs_value:
        if i in rhs_value:
            removed.add(i)
        else:
            added.add(i)
    for i in rhs_value:
        if i in lhs_value:
            removed.add(i)
        else:
            added.add(i)
    for i in lhs.removed():
        if i in rhs_value:
            added.add(i)
        else:
            removed.add(i)
    for i in rhs.removed():
        if i in lhs_value:
            added.add(i)
        else:
            removed.add(i)
    return PythonSetDelta(added, removed)


@compute_node(overloads=eq_)
def eq_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TSS[KEYABLE_SCALAR]:
    return lhs.value == rhs.value


@compute_node(overloads=and_)
def and_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TS[bool]:
    return bool(lhs.value and rhs.value)


@compute_node(overloads=or_)
def or_tsss(lhs: TSS[KEYABLE_SCALAR], rhs: TSS[KEYABLE_SCALAR]) -> TS[bool]:
    return bool(lhs.value or rhs.value)


@compute_node(overloads=min_)
def min_tss_unary(tss: TSS[KEYABLE_SCALAR], default_value: TS[KEYABLE_SCALAR] = None) -> TS[KEYABLE_SCALAR]:
    # TODO - default_value.value gives UnSet if default_value is not valid, rather than None.
    default = default_value.value if default_value.valid else None
    return min(tss.value, default=default)


@compute_node(overloads=max_)
def max_tss_unary(tss: TSS[KEYABLE_SCALAR], default_value: TS[KEYABLE_SCALAR] = None) -> TS[KEYABLE_SCALAR]:
    default = default_value.value if default_value.valid else None
    return max(tss.value, default=default)


@graph(overloads=sum_)
def sum_tss_unary(tss: TSS[KEYABLE_SCALAR], tp: Type[TS[KEYABLE_SCALAR]] = AUTO_RESOLVE) -> TS[KEYABLE_SCALAR]:
    return _sum_tss_unary(tss, zero(tp, sum_))


@compute_node
def _sum_tss_unary(tss: TSS[KEYABLE_SCALAR], zero_ts: TS[KEYABLE_SCALAR]) -> TS[KEYABLE_SCALAR]:
    """
    Unary sum for TSS
    The sum is the sum of the latest set value
    """
    return sum(tss.value, start=zero_ts.value)


@compute_node(overloads=mean)
def mean_tss_unary(tss: TSS[KEYABLE_SCALAR]) -> TS[float]:
    """
    Unary mean for TSS
    The mean is the mean of the latest set value
    """
    tss = tss.value
    if len(tss) > 0:
        return float(sum(tss) / len(tss))
    else:
        return float("NaN")


@compute_node(overloads=std)
def std_tss_unary(tss: TSS[KEYABLE_SCALAR]) -> TS[float]:
    """
    Unary std for TSS
    The standard deviation is that of the latest set value
    """
    tss = tss.value
    if len(tss) <= 1:
        return 0.0
    else:
        return float(stdev(tss))


@compute_node(overloads=var)
def std_tss_unary(tss: TSS[KEYABLE_SCALAR]) -> TS[float]:
    """
    Unary variance for TSS
    The variance is that of the latest set value
    """
    tss = tss.value
    if len(tss) <= 1:
        return 0.0
    else:
        return float(variance(tss))


@compute_node(overloads=str_)
def str_tss(tss: TSS[KEYABLE_SCALAR]) -> TS[str]:
    return str(set(tss.value))
