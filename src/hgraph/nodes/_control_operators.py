from collections import defaultdict
from dataclasses import dataclass, field
from typing import Generic, cast, Type

from hgraph import (
    graph,
    all_,
    TSL,
    TS,
    SIZE,
    reduce,
    bit_and,
    any_,
    bit_or,
    compute_node,
    merge,
    TIME_SERIES_TYPE,
    REF,
    TSB,
    PythonTimeSeriesReference,
    TimeSeriesSchema,
    Size,
    AUTO_RESOLVE,
    STATE,
    EvaluationClock,
    MAX_DT,
    race,
    CompoundScalar,
)

__all__ = ("if_", "if_true", "if_then_else", "route_by_index", "BoolResult")


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
        return {"true": ts.value if ts.valid else PythonTimeSeriesReference(), "false": PythonTimeSeriesReference()}
    else:
        return {"false": ts.value if ts.valid else PythonTimeSeriesReference(), "true": PythonTimeSeriesReference()}


@compute_node(valid=("index_ts",))
def route_by_index(
    index_ts: TS[int], ts: REF[TIME_SERIES_TYPE], _sz: Type[SIZE] = AUTO_RESOLVE
) -> TSL[REF[TIME_SERIES_TYPE], SIZE]:
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
def if_then_else(
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


@compute_node(deprecated="Use if_ instead.")
def route_ref(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSL[REF[TIME_SERIES_TYPE], Size[2]]:
    return cast(
        TSL, (ts.value, PythonTimeSeriesReference()) if condition.value else (PythonTimeSeriesReference(), ts.value)
    )
