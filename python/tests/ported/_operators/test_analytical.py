import pytest

from hgraph import TS, average, accumulate, graph
from hgraph.test import eval_node


def test_accumulate():
    @graph
    def app(ts: TS[int]) -> TS[int]:
        return accumulate(ts)

    assert eval_node(app, [1, 2, 3, 4]) == [1, 3, 6, 10]


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [
            [
                1,
                2,
                3,
                4,
            ],
            [1.0, 1.5, 2.0, 2.5],
        ],
        [
            [
                1.0,
                2.0,
                3.0,
                4.0,
            ],
            [1.0, 1.5, 2.0, 2.5],
        ],
    ],
)
def test_average(value, expected):
    tp = value[0].__class__

    @graph
    def app(ts: TS[tp]) -> TS[float]:
        return average(ts)

    assert eval_node(app, value) == expected
