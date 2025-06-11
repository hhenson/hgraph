__all__ = ["cumsum"]

from functools import wraps

import numpy as np

from hgraph import TS, lift, graph, AUTO_RESOLVE
from hgraph.numpy._constants import ARRAY
from hgraph.numpy._utils import add_docs


@graph
@add_docs(np.cumsum)
def cumsum(a: TS[ARRAY], axis: int = None, _tp: type[ARRAY] = AUTO_RESOLVE) -> TS[ARRAY]:
    if axis is None:
        return lift(np.cumsum, {"a": TS[_tp]}, TS[_tp])(a=a)
    else:
        return lift(np.cumsum, {"a": TS[_tp], "axis": TS[int]}, TS[_tp])(a=a, axis=axis)



