from statistics import variance, stdev
from typing import Type

from hgraph import TS, SCALAR, min_, compute_node, max_, str_, sum_, graph, AUTO_RESOLVE, zero, mean, std, var

__all__ = ()


@compute_node(overloads=min_)
def min_frozenset_unary(ts: TS[frozenset[SCALAR]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    return min(ts.value, default=default_value.value)


@compute_node(overloads=max_)
def max_frozenset_unary(ts: TS[frozenset[SCALAR]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    return max(ts.value, default=default_value.value)


@graph(overloads=sum_)
def sum_frozenset_unary(ts: TS[frozenset[SCALAR]], tp: Type[TS[SCALAR]] = AUTO_RESOLVE) -> TS[SCALAR]:
    return _sum_frozenset_unary(ts, zero(tp, sum_))


@compute_node
def _sum_frozenset_unary(ts: TS[frozenset[SCALAR]], zero_ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Unary sum for frozenset
    The sum is the sum of the latest value
    If the set is empty the typed zero is returned
    """
    return sum(ts.value, start=zero_ts.value)


@compute_node(overloads=mean)
def mean_frozenset_unary(ts: TS[frozenset[SCALAR]]) -> TS[float]:
    """
    Unary mean for frozenset
    The mean is the mean of the latest value
    """
    ts = ts.value
    len_ts = len(ts)
    if len_ts == 0:
        return float("NaN")
    elif len_ts == 1:
        return next(iter(ts))
    else:
        return sum(ts) / len_ts


@compute_node(overloads=std)
def std_frozenset_unary(ts: TS[frozenset[SCALAR]]) -> TS[float]:
    """
    Unary standard deviation for frozenset
    The standard deviation is that of the latest value
    """
    ts = ts.value
    if len(ts) <= 1:
        return 0.0
    else:
        return float(stdev(ts))


@compute_node(overloads=var)
def var_frozenset_unary(ts: TS[frozenset[SCALAR]]) -> TS[float]:
    """
    Unary standard deviation for frozenset
    The standard deviation is that of the latest value
    """
    ts = ts.value
    if len(ts) <= 1:
        return 0.0
    else:
        return float(variance(ts))


@compute_node(overloads=str_)
def str_frozenset(ts: TS[frozenset[SCALAR]]) -> TS[str]:
    return str(set(ts.value))
