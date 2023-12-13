from hgraph import sink_node, TIME_SERIES_TYPE, EvaluationClock


@sink_node
def debug_print(label: str, ts: TIME_SERIES_TYPE, print_delta: bool = True, context: EvaluationClock = None):
    """
    Use this to help debug code, this will print the value of the supplied time-series to the standard out.
    It will include the engine time in the print. Do not leave these lines in production code.

    :param label: The label to print before the value
    :param ts: The time-series to print
    :param print_delta: If true, print the delta value, otherwise print the value
    :param context: The execution context, this is supplied by the runtime and should not be supplied by the user.
    """
    if print_delta:
        value = ts.delta_value
    else:
        value = ts.value
    print(f"[{context.now}][{context.evaluation_time}] {label}: {value}")