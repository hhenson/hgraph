from collections import defaultdict
from dataclasses import dataclass, field

from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._operators._operators import bit_and, bit_or
from hgraph._operators._control import race
from hgraph._wiring._decorators import graph, compute_node
from hgraph._operators._control import all_, any_, merge
from hgraph._types._scalar_types import CompoundScalar, STATE
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._ts_type import TS
from hgraph._types._ref_type import REF
from hgraph._types._time_series_types import OUT
from hgraph._wiring._reduce import reduce
from hgraph._runtime._constants import MAX_DT
from hgraph._runtime._evaluation_clock import EvaluationClock


__all__ = tuple()


@graph(overloads=all_)
def all_default(*args: TSL[TS[bool], SIZE]) -> TS[bool]:
    return reduce(bit_and, args, False)


@graph(overloads=any_)
def any_default(*args: TSL[TS[bool], SIZE]) -> TS[bool]:
    """
    Graph version of python `any` operator
    """
    return reduce(bit_or, args, False)


@compute_node(overloads=merge)
def merge_default(*tsl: TSL[OUT, SIZE]) -> OUT:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """
    return next(tsl.modified_values()).delta_value


@dataclass
class _RaceState(CompoundScalar):
    first_valid_times: dict = field(default_factory=lambda: defaultdict(lambda: MAX_DT))
    winner: REF[OUT] = None


@compute_node(overloads=race)
def race_default(
    *tsl: TSL[REF[OUT], SIZE],
    _state: STATE[_RaceState] = None,
    _ec: EvaluationClock = None,
    _sz: type[SIZE] = AUTO_RESOLVE,
) -> REF[OUT]:

    # Keep track of the first time each input goes valid (and invalid)
    for i in range(_sz.SIZE):
        if _tsl_ref_item_valid(tsl, i):
            if i not in _state.first_valid_times:
                _state.first_valid_times[i] = _ec.now
        else:
            _state.first_valid_times.pop(i, None)

    # Forward the input with the earliest valid time
    winner = _state.winner
    if winner not in _state.first_valid_times:
        # Find a new winner - old one has gone invalid
        winner = min(_state.first_valid_times.items(), default=None, key=lambda item: item[1])
        if winner is not None:
            _state.winner = winner[0]
            return tsl[_state.winner].value
    elif tsl[_state.winner].modified:
        # Forward the winning value
        return tsl[_state.winner].delta_value


def _tsl_ref_item_valid(tsl, i):
    if not tsl.valid:
        return False
    if not (tsli := tsl[i]).valid:
        return False
    tsli_value = tsli.value
    if (output := tsli_value.output) is not None:
        return output.valid
    elif (items := getattr(tsli_value, "items", None)) is not None:
        return items[i].valid
    else:
        return False

