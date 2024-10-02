from hgraph import compute_node, TSD, TS


@compute_node
def rank(raw_signal: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
    """
    Takes a raw_signal that needs to be evenly normalised over the range [-1.0,1.0].
    """
    sz = len(raw_signal)
    keys = (k for _, k in sorted((v, k) for k, v in raw_signal.value.items()))
    return {k: -1.0 + i * 2.0 / (sz - 1.0) for k, i in zip(keys, range(sz))}


import pytest
from hgraph.test import eval_node
from frozendict import frozendict as fd


@pytest.mark.parametrize(
    ["raw_signal", "expected"],
    [
        [[fd(a=0.1, b=0.3, c=-3.0)], [fd(c=-1.0, a=0.0, b=1.0)]],
    ],
)
def test_rank(raw_signal, expected):
    assert eval_node(rank, raw_signal) == expected
