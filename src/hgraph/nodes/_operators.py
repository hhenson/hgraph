from hgraph import compute_node, TIME_SERIES_TYPE, TS, TS_OUT

__all__ = ("eq_", "if_")


@compute_node
def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    """
    Compares two time-series values for equality, emits True when a tick results in equality and False otherwise.
    This will tick with every tick of either of the input values.
    """
    return lhs.value == rhs.value


@compute_node(valid=('condition',))
def if_(condition: TS[bool], true_value: TIME_SERIES_TYPE, false_value: TIME_SERIES_TYPE,
        tick_on_condition: bool = True) -> TIME_SERIES_TYPE:
    """
    Conditionally merges two time-series values, by default if the tick_on_condition is True then this will
    tick a full copy of the value when the condition changes.
    If the condition is true and the true_value is modified then this will tick a delta value of the true_value.
    If the condition is false and the false_value is modified then this will tick a delta value of the false_value.
    """
    if condition.modified and tick_on_condition:
        return true_value.value if condition.value else false_value.value
    if condition.value and true_value.modified:
        return true_value.delta_value
    if not condition.value and false_value.modified:
        return false_value.delta_value


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
