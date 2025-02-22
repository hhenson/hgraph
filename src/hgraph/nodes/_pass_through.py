from hgraph import compute_node, TIME_SERIES_TYPE


__all__ = ("pass_through_node",)


@compute_node
def pass_through_node(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """Just passes the value through, this is useful for testing and for rank adjustment"""
    return ts.delta_value
