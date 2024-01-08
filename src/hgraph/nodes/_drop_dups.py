from hgraph import compute_node, TIME_SERIES_TYPE


__all__ = ("drop_dups",)


@compute_node
def drop_dups(ts: TIME_SERIES_TYPE, _output: TIME_SERIES_TYPE = None) -> TIME_SERIES_TYPE:
    """
    Drops duplicate values from a time-series.
    """
    if _output.valid:
        if ts.value != _output.value:
            return ts.delta_value
    else:
        return ts.value