__all__ = ["cumsum"]

import numpy as np

from hgraph import TS, lift, graph, AUTO_RESOLVE
from hgraph.numpy._constants import ARRAY


@graph
def cumsum(a: TS[ARRAY], axis: int = None, _tp: type[ARRAY] = AUTO_RESOLVE) -> TS[ARRAY]:
    if axis is None:
        return lift(np.cumsum, {"a": TS[_tp]}, TS[_tp])(a=a)
    else:
        return lift(np.cumsum, {"a": TS[_tp], "axis": TS[int]}, TS[_tp])(a=a, axis=axis)



