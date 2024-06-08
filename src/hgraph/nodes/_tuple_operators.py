from collections import deque
from dataclasses import dataclass, field
from typing import Type, TypeVar, Generic, Tuple

from hgraph import SCALAR, TS, HgTypeMetaData, WiringContext, MissingInputsError, IncorrectTypeBinding, compute_node, \
    with_signature, TimeSeries, HgTupleFixedScalarType, HgTupleCollectionScalarType, TSL, STATE, CompoundScalar, \
    SCHEDULER, MIN_TD, mul_, and_, or_, AUTO_RESOLVE, graph
from hgraph import getitem_, min_, max_, sum_, zero
from hgraph.nodes import flatten_tsl_values


__all__ = ("TUPLE", "getitem_tuple")


TUPLE = TypeVar("TUPLE", bound=tuple)


def _item_type(tuple_tp: Type[TUPLE], index: int) -> Type:
    if isinstance(tuple_tp, HgTupleFixedScalarType):
        return tuple_tp.element_types[index]
    elif isinstance(tuple_tp, HgTupleCollectionScalarType):
        return tuple_tp.element_type
    raise IncorrectTypeBinding(TUPLE, tuple_tp)


@compute_node(overloads=getitem_)
def getitem_tuple(ts: TS[Tuple[SCALAR, ...]], key: TS[int]) -> TS[SCALAR]:
    """
    Retrieve the tuple item indexed by key from the timeseries of scalar tuples
    """
    return ts.value[key.value]


@dataclass
class UnrollState(CompoundScalar, Generic[SCALAR]):
    buffer: deque[SCALAR] = field(default_factory=deque)


@compute_node
def unroll(ts: TS[tuple[SCALAR, ...]],
           _state: STATE[UnrollState[SCALAR]] = None, _schedule: SCHEDULER = None) -> TS[SCALAR]:
    """
    The values contained in the tuple are unpacked and returned one at a time until all values are unpacked.
    """
    if ts.modified:
        _state.buffer.extend(ts.value)

    if _state.buffer:
        d: deque[SCALAR] = _state.buffer
        v = d.popleft()
        if d:
            _schedule.schedule(MIN_TD)
        return v


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
def max_tuple(ts: TS[Tuple[SCALAR, ...]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
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
