from hgraph import TS, compute_node, TIME_SERIES_TYPE, REF


__all__ = ("if_then_else", "if_true")


@compute_node(valid=("condition",))
def if_then_else(condition: TS[bool], true_value: REF[TIME_SERIES_TYPE], false_value: REF[TIME_SERIES_TYPE]) \
        -> REF[TIME_SERIES_TYPE]:
    """
    If the condition is true the output is bound ot the true_value, otherwise it is bound to the false_value.
    This just connect the time-series values.
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
