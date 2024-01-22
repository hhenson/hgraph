import pytest

from hgraph import Size, TS, TSL, MIN_TD
from hgraph.nodes import merge
from hgraph.nodes._analytical import lag, sum_
from hgraph.nodes._tsl_operators import tsl_to_tsd
from hgraph.test import eval_node


def test_merge():
    assert eval_node(merge, [{1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None, {0: 6}],
                     resolution_dict={"tsl": TSL[TS[int], Size[3]]}) == [1, 2, 4, None, 6]


def test_tsl_lag():
    assert eval_node(lag, [{1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None, {0: 6}], MIN_TD,
                     resolution_dict={"ts": TSL[TS[int], Size[3]]}) == [None, {1: 1}, {0: 2, 2: 3}, {1: 4, 2: 5}, None,
                                                                        {0: 6}]


@pytest.mark.parametrize(
    ["inputs", "expected"],
    [
        [[(1, 2), (2, 3)], [3, 5]],
        [[(1.0, 2.0), (2.0, 3.0)], [3.0, 5.0]],
    ]
)
def test_sum(inputs, expected):
    assert eval_node(sum_, inputs, resolution_dict={'ts': TSL[TS[type(inputs[0][0])], Size[2]]}) == expected


def test_tsl_to_tsd():

    assert eval_node(tsl_to_tsd, [(1, 2, 3), {1: 3}], ('a', 'b', 'c'),
                     resolution_dict={'tsl': TSL[TS[int], Size[3]]}) == [{'a': 1, 'b': 2, 'c': 3}, {'b': 3}]
