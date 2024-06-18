from collections import defaultdict
from dataclasses import dataclass, field

from hgraph._impl._types._ref import PythonTimeSeriesReference
from hgraph._operators._flow_control import all_, any_, merge, index_of
from hgraph._operators._flow_control import race, BoolResult, if_, route_by_index, if_true, if_then_else
from hgraph._operators._operators import bit_and, bit_or
from hgraph._runtime._constants import MAX_DT
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._types._ref_type import REF
from hgraph._types._scalar_types import CompoundScalar, STATE
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._types._tsb_type import TSB
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import graph, compute_node
from hgraph._wiring._reduce import reduce

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


@compute_node(valid=("condition",), overloads=if_)
def if_impl(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSB[BoolResult[TIME_SERIES_TYPE]]:
    """
    Forwards a timeseries value to one of two bundle outputs: "true" or "false", according to whether
    the condition is true or false
    """
    if condition.value:
        return {"true": ts.value if ts.valid else PythonTimeSeriesReference(), "false": PythonTimeSeriesReference()}
    else:
        return {"false": ts.value if ts.valid else PythonTimeSeriesReference(), "true": PythonTimeSeriesReference()}


@compute_node(valid=("index_ts",), overloads=route_by_index)
def route_by_index_impl(
    index_ts: TS[int], ts: REF[TIME_SERIES_TYPE], _sz: type[SIZE] = AUTO_RESOLVE
) -> TSL[REF[TIME_SERIES_TYPE], SIZE]:
    """
    Forwards a timeseries value to the 'nth' output according to the value of index_ts
    """
    out = [PythonTimeSeriesReference()] * _sz.SIZE
    index_ts = index_ts.value
    if 0 <= index_ts < _sz.SIZE and ts.valid:
        out[index_ts] = ts.value
    return out


@compute_node(overloads=if_true)
def if_true_impl(condition: TS[bool], tick_once_only: bool = False) -> TS[bool]:
    """
    Emits a tick with value True when the input condition ticks with True.
    If tick_once_only is True then this will only tick once, otherwise this will tick with every tick of the condition,
    when the condition is True.
    """
    if condition.value:
        if tick_once_only:
            condition.make_passive()
        return True


@compute_node(valid=("condition",), overloads=if_then_else)
def if_then_else_impl(
    condition: TS[bool], true_value: REF[TIME_SERIES_TYPE], false_value: REF[TIME_SERIES_TYPE]
) -> REF[TIME_SERIES_TYPE]:
    """
    If the condition is true the output ticks with the true_value, otherwise it ticks with the false_value.
    """
    condition_value = condition.value
    if condition.modified:
        if condition_value:
            if true_value.valid:
                return true_value.value
        else:
            if false_value.valid:
                return false_value.value

    if condition_value and true_value.modified:
        return true_value.value

    if not condition_value and false_value.modified:
        return false_value.value


@compute_node(overloads=index_of)
def index_of_impl(tsl: TSL[TIME_SERIES_TYPE, SIZE], ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    Return the index of the leftmost time-series in the TSL with value equal to ts
    """
    return next((i for i, t in enumerate(tsl) if t.valid and t.value == ts.value), -1)

