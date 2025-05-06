import numpy as np
import pytest

from hgraph import MIN_ST, MIN_TD, Size, TS, Array
from hgraph.nodes._numpy import np_rolling_window, np_quantile
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ["values", "sz", "expected"],
    [
        [
            [1, 2, 3, 4, 5],
            Size[3],
            [
                None,
                None,
                {"buffer": np.array((1, 2, 3)), "index": np.array((MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD))},
                {
                    "buffer": np.array((2, 3, 4)),
                    "index": np.array((MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)),
                },
                {
                    "buffer": np.array((3, 4, 5)),
                    "index": np.array((MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)),
                },
            ],
        ],
        [
            [1.0, 2.0, 3.0, 4.0, 5.0],
            Size[3],
            [
                None,
                None,
                {
                    "buffer": np.array((1.0, 2.0, 3.0)),
                    "index": np.array((MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD)),
                },
                {
                    "buffer": np.array((2.0, 3.0, 4.0)),
                    "index": np.array((MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD)),
                },
                {
                    "buffer": np.array((3.0, 4.0, 5.0)),
                    "index": np.array((MIN_ST + 2 * MIN_TD, MIN_ST + 3 * MIN_TD, MIN_ST + 4 * MIN_TD)),
                },
            ],
        ],
    ],
)
def test_np_rolling_window(values, sz, expected):
    result = eval_node(np_rolling_window, values, sz)
    assert len(result) == len(expected)
    assert all((r["buffer"] == e["buffer"]).all() for r, e in zip(result, expected) if not (r is None and e is None))


def test_np_quantile():
    assert eval_node(np_quantile, [np.array([1, 2])], 0.8, resolution_dict={"ts": TS[Array[int, Size[2]]]}) == [1.8]
