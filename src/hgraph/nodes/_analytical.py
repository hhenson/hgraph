from hgraph import TS, graph, NUMBER, lag

__all__ = ("pct_change",)


@graph
def pct_change(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    pct_change = (ts.value - ts_1.value) / ts_1.value
    The result is in fractional percentage values
    """
    l = lag(ts, period=1)
    return (ts - l) / l
