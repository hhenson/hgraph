import numpy as np
import pytest

from hgraph import Array, Size, TS, compute_node
from hgraph.test import eval_node


def test_array_type_retains_element_and_dimensions():
    annotation = Array[float, Size[3], Size[2]]
    assert annotation == Array[float, Size[3], Size[2]]
    assert hash(annotation) == hash(Array[float, Size[3], Size[2]])
    assert "Size[-1]" in repr(Array[int, Size[-1]])


def test_fixed_array_capacity_accepts_a_shorter_logical_value():
    @compute_node
    def passthrough(ts: TS[Array[int, Size[3]]]) -> TS[Array[int, Size[3]]]:
        return ts.value

    values = [np.array([1]), np.array([1, 2]), np.array([1, 2, 3])]
    actual = eval_node(passthrough, values)
    assert all(np.array_equal(a, b) for a, b in zip(actual, values))

    with pytest.raises(ValueError, match="at most 3 elements"):
        eval_node(passthrough, [np.array([1, 2, 3, 4])])
