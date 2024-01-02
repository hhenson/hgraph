from hgraph import compute_node, TS, STATE


@compute_node
def ewma(ts: TS[float], alpha: float, min_periods: int = 0, _state: STATE = None) -> TS[float]:
    """Exponential Weighted Moving Average"""
    x_t = ts.value
    s_prev = _state.s_prev

    if s_prev is None:
        s = x_t
    else:
        s = s_prev + alpha * (x_t - s_prev)

    _state.s_prev = s
    _state.count += 1

    if _state.count > min_periods:
        return s


@ewma.start
def ewma_start(alpha: float, _state: STATE):
    if 0.0 <= alpha <= 1.0:
        _state.s_prev = None
        _state.count  = 0
    else:
        raise ValueError("alpha must be between 0 and 1")


def center_of_mass_to_alpha(com: float) -> float:
    if com <= 0:
        raise ValueError(f"Center of mass must be positive, got {com}")
    return 1.0 / (com + 1.0)


def span_top_aplha(span: float) -> float:
    if span <= 0:
        raise ValueError(f"Span must be positive, got {span}")
    return 2.0 / (span + 1.0)
