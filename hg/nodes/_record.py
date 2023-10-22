from hg import sink_node, TIME_SERIES_TYPE, ScalarValue


@sink_node
def record(ts: TIME_SERIES_TYPE, results: tuple[ScalarValue], record_delta_values: bool = True):
    """
    This node will record the values of the time series into the provided list.
    """
    results.append(ts.delta_scalar_value if record_delta_values else ts.scalar_value)