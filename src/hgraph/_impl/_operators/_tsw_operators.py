from hgraph import compute_node, sum_, TSW, SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN, TS, abs_

__all__ = ("sum_tsw",)


@compute_node(overloads=sum_)
def sum_tsw(ts: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[SCALAR]:
    return int(sum(ts.value))


@compute_node(overloads=abs_)
def abs_tsw(ts: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN]:
    return int(abs(ts.delta_value))
