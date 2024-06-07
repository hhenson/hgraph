from collections import deque
from dataclasses import dataclass, field
from typing import Type, TypeVar, Generic

from hgraph import SCALAR, TS, HgTypeMetaData, WiringContext, MissingInputsError, IncorrectTypeBinding, compute_node, \
    with_signature, TimeSeries, HgTupleFixedScalarType, HgTupleCollectionScalarType, TSL, STATE, CompoundScalar, \
    SCHEDULER, MIN_TD
from hgraph._operators._operators import getitem_
from hgraph.nodes import flatten_tsl_values


__all__ = ("TUPLE", "getitem_tuple")


TUPLE = TypeVar("TUPLE", bound=tuple)


def _item_type(tuple_tp: Type[TUPLE], index: int) -> Type:
    if isinstance(tuple_tp, HgTupleFixedScalarType):
        return tuple_tp.element_types[index]
    elif isinstance(tuple_tp, HgTupleCollectionScalarType):
        return tuple_tp.element_type
    raise IncorrectTypeBinding(TUPLE, tuple_tp)


@compute_node(overloads=getitem_, resolvers={SCALAR: lambda mapping, scalars: _item_type(mapping[TUPLE],scalars['key'])})
def getitem_tuple(ts: TS[TUPLE], key: int) -> TS[SCALAR]:
    return ts.value[key]


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

