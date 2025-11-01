import numpy as np

import hgraph
from hgraph import (
    compute_node,
    TSW,
    SCALAR,
    SIZE,
    TS,
    Array,
    WINDOW_SIZE_MIN,
    graph,
    AUTO_RESOLVE,
    WINDOW_SIZE,
    Size,
    operator,
    OUT,
)
from hgraph.numpy_._constants import ARRAY
from hgraph.numpy_._utils import extract_type_from_array, extract_dimensions_from_array

__all__ = ["as_array", "get_item"]


@graph(
    resolvers={SIZE: lambda m, s: Size[m[WINDOW_SIZE].py_type.SIZE]},
    requires=lambda m, s: True if m[WINDOW_SIZE].py_type.FIXED_SIZE else "Only fixed size windows are supported.",
)
def as_array(
    tsw: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN],
    zero: SCALAR = None,
    _tp: type[SCALAR] = AUTO_RESOLVE,
    _sz: type[WINDOW_SIZE] = AUTO_RESOLVE,
    _m_sz: type[WINDOW_SIZE_MIN] = AUTO_RESOLVE,
) -> TS[Array[SCALAR, SIZE]]:
    """
    Converts the values from TSW into a numpy array output of size ``SIZE``. If the min-size is smaller than ``SIZE``
    then the array is zero padded. By default, if not provided, zero is obtained using::

        zero(tp, sum_)

    Forcing the shape of the array to be consistent with ``SIZE``.
    """
    if _sz.SIZE == _m_sz.SIZE:
        return _as_array[SIZE : Size[_sz.SIZE]](tsw)
    else:
        from hgraph import sum_

        return _as_array_with_padding[SIZE : Size[_sz.SIZE]](
            tsw, hgraph.zero(TS[_tp], sum_) if zero is None else zero, sz=_sz.SIZE
        )


@compute_node(all_valid=("tsw",))
def _as_array(tsw: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE]) -> TS[Array[SCALAR, SIZE]]:
    return tsw.value


@compute_node(all_valid=("tsw",))
def _as_array_with_padding(
    tsw: TSW[SCALAR, WINDOW_SIZE, WINDOW_SIZE_MIN], zero: TS[SCALAR], sz: int
) -> TS[Array[SCALAR, SIZE]]:
    v = tsw.value
    if len(v) == sz:
        return v
    else:
        z = zero.value
        zeros = np.full(sz - len(v), z)
        return np.concatenate((v, zeros))


@operator
def get_item(ts: TS[ARRAY], idx: SCALAR) -> OUT:
    """
    Deference an array to return the value associate to the supplied index.

    The index can be an integer or a tuple of integers. This is equivalent to numpy.ndarray[idx].
    This will return either a scalar or a slice of the input array.
    """


def _get_item_resolver(m, s):
    idx = s["idx"]
    if isinstance(idx, int):
        idx = (idx,)
    arr = m[ARRAY].py_type
    scalar = extract_type_from_array(arr)
    dimensions = extract_dimensions_from_array(arr)
    if len(dimensions) < len(idx):
        raise ValueError(f"Index {idx} is too large for the array of dimensions {dimensions}")
    if len(dimensions) == len(idx):
        return TS[scalar]
    else:
        return TS[Array[scalar, *(Size[i] for i in dimensions[len(idx) :])]]


def _get_item_scalar_resolver(m, s):
    idx = s["idx"]
    if isinstance(idx, int):
        idx = (idx,)
    arr = m[ARRAY].py_type
    scalar = extract_type_from_array(arr)
    dimensions = extract_dimensions_from_array(arr)
    if len(dimensions) == len(idx):
        return scalar
    else:
        return None


def _get_item_not_scalar(m, s):
    idx = s["idx"]
    if isinstance(idx, int):
        idx = (idx,)
    arr = m[ARRAY].py_type
    scalar = extract_type_from_array(arr)
    dimensions = extract_dimensions_from_array(arr)
    if len(dimensions) == len(idx):
        return "This has a scalar output type"
    else:
        return True


@compute_node(overloads=get_item, resolvers={OUT: _get_item_resolver, SCALAR: _get_item_scalar_resolver})
def get_item_int(ts: TS[ARRAY], idx: int, s_type: type[SCALAR] = AUTO_RESOLVE) -> OUT:
    v = ts.value[idx]
    return s_type(v)


@compute_node(overloads=get_item, resolvers={OUT: _get_item_resolver}, requires=_get_item_not_scalar)
def get_item_int(ts: TS[ARRAY], idx: int) -> OUT:
    v = ts.value[idx]
    return v


@compute_node(overloads=get_item, resolvers={OUT: _get_item_resolver, SCALAR: _get_item_scalar_resolver})
def get_item_tuple(ts: TS[ARRAY], idx: tuple[int, ...], s_type: type[SCALAR] = AUTO_RESOLVE) -> OUT:
    v = ts.value[idx]
    return s_type(v)


@compute_node(overloads=get_item, resolvers={OUT: _get_item_resolver}, requires=_get_item_not_scalar)
def get_item_tuple(ts: TS[ARRAY], idx: tuple[int, ...]) -> OUT:
    return ts.value[idx]
