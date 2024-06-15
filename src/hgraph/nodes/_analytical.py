from hgraph import TS, graph, NUMBER, lag

__all__ = ("center_of_mass_to_alpha", "span_to_alpha", "pct_change")


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_to_alpha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)


@graph
def pct_change(ts: TS[NUMBER]) -> TS[NUMBER]:
    """
    pct_change = (ts.value - ts_1.value) / ts_1.value
    The result is in fractional percentage values
    """
    l = lag(ts, period=1)
    return (ts - l) / l
