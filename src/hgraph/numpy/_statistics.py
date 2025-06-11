__all__ = ["corrcoef",]

import numpy as np

from hgraph import TS, graph, lift, AUTO_RESOLVE, Array, Size, compute_node, SCALAR
from hgraph.numpy._constants import ARRAY, ARRAY_1
from hgraph.numpy._utils import extract_dimensions_from_array, extract_type_from_array, add_docs


@graph
@add_docs(np.corrcoef)
def corrcoef(
        x: TS[ARRAY],
        y: TS[ARRAY] = None,
        rowvar: bool = True,
        tp_a: type[ARRAY] = AUTO_RESOLVE,
) -> TS[SCALAR]:
    tp = extract_type_from_array(tp_a)
    dimensions = extract_dimensions_from_array(tp_a)
    if rowvar:
        sz = len(dimensions)
    else:
        sz = dimensions[0]

    if y is None:
        return _corrcoef_no_y[SCALAR: Array[tp, Size[sz], Size[sz]] if sz > 1 else tp](x, rowvar)
    else:
        sz *= 2
        return _corrcoef[SCALAR: Array[tp, Size[sz], Size[sz]]](x, y, rowvar)


@compute_node
def _corrcoef_no_y(x: TS[ARRAY], rowvar: bool) -> TS[SCALAR]:
    v = np.corrcoef(x.value, rowvar=rowvar)
    return v


@compute_node
def _corrcoef(x: TS[ARRAY], y: TS[ARRAY], rowvar: bool) -> TS[SCALAR]:
    return np.corrcoef(x.value, y.value, rowvar=rowvar)