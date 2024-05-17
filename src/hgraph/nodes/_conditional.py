from typing import cast, Generic

from hgraph import TS, compute_node, TIME_SERIES_TYPE, REF, TSL, PythonTimeSeriesReference, Size, TimeSeriesSchema, TSB

__all__ = ("if_then_else", "if_true", "route_ref", "filter_")


@compute_node(valid=("condition",))
def if_then_else(condition: TS[bool], true_value: REF[TIME_SERIES_TYPE], false_value: REF[TIME_SERIES_TYPE]) \
        -> REF[TIME_SERIES_TYPE]:
    """
    If the condition is true the output is bound to the true_value, otherwise it is bound to the false_value.
    This just connects the time-series values.
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


class BoolResult(TimeSeriesSchema, Generic[TIME_SERIES_TYPE]):
    true: REF[TIME_SERIES_TYPE]
    false: REF[TIME_SERIES_TYPE]


@compute_node(valid=("condition",))
def if_(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSB[BoolResult[TIME_SERIES_TYPE]]:
    """
    Emits a tick with value True when the input condition ticks with False.
    If tick_once_only is True then this will only tick once, otherwise this will tick with every tick of the condition,
    when the condition is False.
    """
    if condition.value:
        return {'true': ts.value if ts.valid else PythonTimeSeriesReference(),
                'false': PythonTimeSeriesReference()}
    else:
        return {'false': ts.value if ts.valid else PythonTimeSeriesReference(),
                'true': PythonTimeSeriesReference()}


@compute_node(deprecated="Use if_ instead.")
def route_ref(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSL[REF[TIME_SERIES_TYPE], Size[2]]:
    return cast(TSL,
                (ts.value, PythonTimeSeriesReference()) if condition.value else (PythonTimeSeriesReference(), ts.value))


@compute_node
def filter_(condition: TS[bool], ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    if condition.value:
        return ts.value if condition.modified else ts.delta_value