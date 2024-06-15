from collections import defaultdict
from typing import Generic, cast, Type

from hgraph import graph, all_, TSL, TS, SIZE, reduce, bit_and, any_, bit_or, compute_node, merge, TIME_SERIES_TYPE, \
    REF, TSB, PythonTimeSeriesReference, TimeSeriesSchema, Size, AUTO_RESOLVE, STATE, EvaluationClock, MAX_DT, race

__all__ = ("if_", "if_true", "if_then_else", "route_by_index", "BoolResult")


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
def merge_default(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """
    return next(tsl.modified_values()).delta_value


@compute_node(overloads=race)
def race_default(*tsl: TSL[REF[TIME_SERIES_TYPE], SIZE],
                 state: STATE = None,
                 ec: EvaluationClock = None,
                 sz: Type[SIZE] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:

    # Keep track of the first time each input goes valid (and invalid)
    for i in range(sz.SIZE):
        if _tsl_ref_item_valid(tsl, i):
            if i not in state.first_valid_times:
                state.first_valid_times[i] = ec.now
        else:
            state.first_valid_times.pop(i, None)

    # Forward the input with the earliest valid time
    winner = state.winner
    if winner not in state.first_valid_times:
        # Find a new winner - old one has gone invalid
        winner = min(state.first_valid_times.items(), default=None, key=lambda item: item[1])
        if winner is not None:
            state.winner = winner[0]
            return tsl[state.winner].value
    elif tsl[state.winner].modified:
        # Forward the winning value
        return tsl[state.winner].delta_value


@race_default.start
def _(state: STATE):
    state.first_valid_times = defaultdict(lambda: MAX_DT)  # time the item in the TSL first went valid, by list index
    state.winner = None  # index of item which so far has gone valid first


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


class BoolResult(TimeSeriesSchema, Generic[TIME_SERIES_TYPE]):
    true: REF[TIME_SERIES_TYPE]
    false: REF[TIME_SERIES_TYPE]


@compute_node(valid=("condition",))
def if_(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSB[BoolResult[TIME_SERIES_TYPE]]:
    """
    Forwards a timeseries value to one of two bundle outputs: "true" or "false", according to whether
    the condition is true or false
    """
    if condition.value:
        return {'true': ts.value if ts.valid else PythonTimeSeriesReference(),
                'false': PythonTimeSeriesReference()}
    else:
        return {'false': ts.value if ts.valid else PythonTimeSeriesReference(),
                'true': PythonTimeSeriesReference()}


@compute_node(valid=("index_ts",))
def route_by_index(index_ts: TS[int],
                   ts: REF[TIME_SERIES_TYPE],
                   _sz: Type[SIZE] = AUTO_RESOLVE) -> TSL[REF[TIME_SERIES_TYPE], SIZE]:
    """
    Forwards a timeseries value to the 'nth' output according to the value of index_ts
    """
    out = [PythonTimeSeriesReference()] * _sz.SIZE
    index_ts = index_ts.value
    if 0 <= index_ts < _sz.SIZE and ts.valid:
        out[index_ts] = ts.value
    return out


@compute_node
def if_true(condition: TS[bool], tick_once_only: bool = False) -> TS[bool]:
    """
    Emits a tick with value True when the input condition ticks with True.
    If tick_once_only is True then this will only tick once, otherwise this will tick with every tick of the condition,
    when the condition is True.
    """
    if condition.value:
        if tick_once_only:
            condition.make_passive()
        return True


@compute_node(valid=("condition",))
def if_then_else(condition: TS[bool],
                 true_value: REF[TIME_SERIES_TYPE],
                 false_value: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
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


@compute_node(deprecated="Use if_ instead.")
def route_ref(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSL[REF[TIME_SERIES_TYPE], Size[2]]:
    return cast(TSL,
                (ts.value, PythonTimeSeriesReference()) if condition.value else (PythonTimeSeriesReference(), ts.value))
