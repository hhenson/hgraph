__all__ = ["as_array"]

import numpy as np

from hgraph import compute_node, TSW, SCALAR, SIZE, TS, Array, WINDOW_SIZE_MIN, graph, AUTO_RESOLVE, WINDOW_SIZE, Size
import hgraph


@graph(
    resolvers={SIZE: lambda m, s: Size[m[WINDOW_SIZE].py_type.SIZE]},
    requires=lambda m, s: True if m[WINDOW_SIZE].py_type.FIXED_SIZE else "Only fixed size windows are supported.",
)
def as_array(tsw: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
             zero: SCALAR = None, 
             tp: type[SCALAR] = AUTO_RESOLVE,
             sz: type[WINDOW_SIZE] = AUTO_RESOLVE,
             m_sz: type[WINDOW_SIZE_MIN] = AUTO_RESOLVE) -> TS[Array[SCALAR, SIZE]]:
    """
    Converts the values from TSW into a numpy array output of size ``SIZE``. If the min-size is smaller than ``SIZE``
    then the array is zero padded. By default, if not provided, zero is obtained using::
    
        zero(tp, sum_)
        
    Forcing the shape of the array to be consistent with ``SIZE``.
    """
    if sz.SIZE == m_sz.SIZE:
        return _as_array[SIZE: Size[sz.SIZE]](tsw)
    else:
        from hgraph import sum_
        return _as_array_with_padding[SIZE: Size[sz.SIZE]](tsw, hgraph.zero(TS[tp], sum_) if zero is None else zero, sz=sz.SIZE)
    
    
@compute_node(all_valid=("tsw",))
def _as_array(tsw: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE]) -> TS[Array[SCALAR, SIZE]]:
    return tsw.value


@compute_node(all_valid=("tsw",))
def _as_array_with_padding(tsw: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN], zero: TS[SCALAR], sz: int) -> TS[Array[SCALAR, SIZE]]:
    v = tsw.value
    if len(v) == sz:
        return v
    else:
        z = zero.value
        zeros = np.full(sz - len(v), z)
        return np.concatenate((v, zeros))
