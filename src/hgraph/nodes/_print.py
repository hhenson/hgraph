from hgraph import sink_node, TIME_SERIES_TYPE, EvaluationClock


__all__ = ("debug_print",)


@sink_node
def debug_print(label: str, ts: TIME_SERIES_TYPE, print_delta: bool = True, _clock: EvaluationClock = None):
    """
    Use this to help debug code, this will print the value of the supplied time-series to the standard out.
    It will include the engine time in the print. Do not leave these lines in production code.

    :param label: The label to print before the value
    :param ts: The time-series to print
    :param print_delta: If true, print the delta value, otherwise print the value
    :param _clock: The evaluation clock (to be injected)
    """
    if print_delta:
        value = ts.delta_value
    else:
        value = ts.value
    print(f"[{_clock.now}][{_clock.evaluation_time}] {label}: {value}")