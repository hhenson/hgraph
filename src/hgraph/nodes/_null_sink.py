from hgraph import TIME_SERIES_TYPE, sink_node

__all__ = ("null_sink",)


@sink_node
def null_sink(ts: TIME_SERIES_TYPE):
    """
    A sink node that will consume the time-series and do nothing with it.
    This is useful when you want to consume a time-series but do not want to do anything with it.
    """
    pass
