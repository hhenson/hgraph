from hgraph import compute_node, TIME_SERIES_TYPE


@compute_node
def drop_dups(ts: TIME_SERIES_TYPE, output: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    """
    Drops duplicate values from a time-series.
    """
    if output.valid:
        if ts.value != output.value:
            return ts.delta_value
    else:
        return ts.value