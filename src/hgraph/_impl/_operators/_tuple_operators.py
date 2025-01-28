from statistics import stdev, variance
from typing import Type, Tuple

from hgraph import (
    SCALAR,
    TS,
    IncorrectTypeBinding,
    compute_node,
    HgTupleFixedScalarType,
    HgTupleCollectionScalarType,
    mul_,
    and_,
    or_,
    AUTO_RESOLVE,
    graph,
    mean,
    var,
    std,
    getitem_,
    min_,
    max_,
    sum_,
    zero,
    TUPLE,
    index_of,
)

__all__ = tuple()


def _item_type(tuple_tp: Type[TUPLE], index: int) -> Type:
    if isinstance(tuple_tp, HgTupleFixedScalarType):
        return tuple_tp.element_types[index]
    elif isinstance(tuple_tp, HgTupleCollectionScalarType):
        return tuple_tp.element_type
    raise IncorrectTypeBinding(TUPLE, tuple_tp)


@compute_node(
    overloads=getitem_, resolvers={SCALAR: lambda mapping, scalars: _item_type(mapping[TUPLE], scalars["key"])}
)
def getitem_tuple_fixed(ts: TS[TUPLE], key: int) -> TS[SCALAR]:
    return ts.value[key]


@compute_node(overloads=getitem_)
def getitem_tuple(ts: TS[Tuple[SCALAR, ...]], key: TS[int]) -> TS[SCALAR]:
    """
    Retrieve the tuple item indexed by key from the timeseries of scalar tuples
    """
    return ts.value[key.value]


@compute_node(overloads=mul_)
def mul_tuple_int(lhs: TS[Tuple[SCALAR, ...]], rhs: TS[int]) -> TS[Tuple[SCALAR, ...]]:
    return lhs.value * rhs.value


@compute_node(overloads=and_)
def and_tuples(lhs: TS[Tuple[SCALAR, ...]], rhs: TS[Tuple[SCALAR, ...]]) -> TS[bool]:
    return bool(lhs.value and rhs.value)


@compute_node(overloads=or_)
def or_tuples(lhs: TS[Tuple[SCALAR, ...]], rhs: TS[Tuple[SCALAR, ...]]) -> TS[bool]:
    return bool(lhs.value or rhs.value)


@compute_node(overloads=min_)
def min_tuple_unary(ts: TS[Tuple[SCALAR, ...]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    return min(ts.value, default=default_value.value)


@compute_node(overloads=max_)
def max_tuple_unary(ts: TS[Tuple[SCALAR, ...]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    return max(ts.value, default=default_value.value)


@graph(overloads=sum_)
def sum_tuple_unary(ts: TS[Tuple[SCALAR, ...]], tp: Type[TS[SCALAR]] = AUTO_RESOLVE) -> TS[SCALAR]:
    return _sum_tuple_unary(ts, zero(tp, sum_))


@compute_node
def _sum_tuple_unary(ts: TS[Tuple[SCALAR, ...]], zero_ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Unary sum for timeseries of tuples
    The sum is the sum of the latest value
    """
    return sum(ts.value, start=zero_ts.value)


@compute_node(overloads=mean)
def mean_tuple_unary(ts: TS[Tuple[SCALAR, ...]]) -> TS[float]:
    """
    Unary mean for timeseries of tuples
    The mean is the mean of the latest value
    """
    ts = ts.value
    if len(ts) > 0:
        return float(sum(ts) / len(ts))
    else:
        return float("NaN")


@compute_node(overloads=std)
def std_tuple_unary(ts: TS[Tuple[SCALAR, ...]]) -> TS[float]:
    """
    Unary standard deviation for timeseries of tuples
    The standard deviation is that of the latest value
    """
    ts = ts.value
    if len(ts) <= 1:
        return 0.0
    else:
        return float(stdev(ts))


@compute_node(overloads=var)
def var_tuple_unary(ts: TS[Tuple[SCALAR, ...]]) -> TS[float]:
    """
    Unary standard deviation for timeseries of tuples
    The standard deviation is that of the latest value
    """
    ts = ts.value
    if len(ts) <= 1:
        return 0.0
    else:
        return float(variance(ts))


@compute_node(overloads=index_of)
def index_of_tuple(ts: TS[tuple[SCALAR, ...]], item: TS[SCALAR]) -> TS[int]:
    try:
        return ts.value.index(item.value)
    except ValueError:
        return -1
