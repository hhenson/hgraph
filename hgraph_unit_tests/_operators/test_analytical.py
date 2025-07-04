import pytest

from hgraph import TS, NodeException, average, accumulate, graph, diff, count, clip, center_of_mass_to_alpha, span_to_alpha
from hgraph.test import eval_node


def test_conversions():
    assert center_of_mass_to_alpha(1.0) == 0.5
    assert span_to_alpha(1.0) == 1.0


@pytest.mark.parametrize(
    ["values", "min", "max", "expected"],
    [
        [[2, 1, 0, -1, -2], -1, 1, [1, 1, 0, -1, -1]],
        [[2.0, 1.0, 0.0, -1.0, -2.0], -1.0, 1.0, [1.0, 1.0, 0.0, -1.0, -1.0]],
    ],
)
def test_clip(values, min, max, expected):
    assert eval_node(clip, values, min, max) == expected


def test_clip_failure():
    with pytest.raises(NodeException):
        assert eval_node(clip, [1.0], 1.0, -1.0) == [1.0]


def test_count():
    assert eval_node(
        count,
        [
            3,
            2,
            1,
        ],
        resolution_dict={"ts": TS[int]},
    ) == [1, 2, 3]


def test_count_reset():
    assert eval_node(
        count,
        [
            3,
            2,
            1,
        ],
        reset=[None, True, None],
        resolution_dict={"ts": TS[int], "reset": TS[bool]},
    ) == [1, 1, 2]
    assert eval_node(
        count,
        [
            3,
            None,
            2,
            1,
        ],
        reset=[None, True, None],
        resolution_dict={"ts": TS[int], "reset": TS[bool]},
    ) == [1, None, 1, 2]


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


def test_diff():
    assert eval_node(
        diff,
        [
            1,
            2,
            3,
            4,
        ],
    ) == [None, 1, 1, 1]
