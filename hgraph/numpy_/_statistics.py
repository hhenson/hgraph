import numpy as np

from hgraph import TS, graph, AUTO_RESOLVE, Array, Size, compute_node, SCALAR, NUMBER, operator
from hgraph import TSW, WINDOW_SIZE, WINDOW_SIZE_MIN, OUT, SIZE, TIME_SERIES_TYPE
from hgraph.numpy_._constants import ARRAY
from hgraph.numpy_._utils import extract_dimensions_from_array, extract_type_from_array, add_docs

__all__ = [
    "corrcoef", "quantile"
]


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
        return _corrcoef_no_y[SCALAR : Array[tp, Size[sz], Size[sz]] if sz > 1 else tp](x, rowvar)
    else:
        sz *= 2
        return _corrcoef[SCALAR : Array[tp, Size[sz], Size[sz]]](x, y, rowvar)


@compute_node
def _corrcoef_no_y(x: TS[ARRAY], rowvar: bool) -> TS[SCALAR]:
    v = np.corrcoef(x.value, rowvar=rowvar)
    return v


@compute_node
def _corrcoef(x: TS[ARRAY], y: TS[ARRAY], rowvar: bool) -> TS[SCALAR]:
    return np.corrcoef(x.value, y.value, rowvar=rowvar)


@operator
@add_docs(np.quantile)
def quantile(
    a: TIME_SERIES_TYPE,
    q: TS[SCALAR],
    method: str = "linear",
    keepdims: bool = False,
) -> OUT:
    """
    This will wrap most common usages of np.quantile, but not all. The base case of array and single float q is supported.
    """
    

@compute_node(overloads=quantile)
def quantile_array_scalar(
    a: TS[Array[NUMBER, SIZE]],
    q: TS[float],
    method: str = "linear",
    keepdims: bool = False,
) -> TS[float]:
    v = np.quantile(a.value, q.value, method=method, keepdims=keepdims)
    return float(v)


@compute_node(overloads=quantile, all_valid=("a",))
def quantile_tsw_scalar(
    a: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE_MIN],
    q: TS[float],
    method: str = "linear",
    keepdims: bool = False,
) -> TS[float]:
    v = np.quantile(a.value, q.value, method=method, keepdims=keepdims)
    return float(v)

