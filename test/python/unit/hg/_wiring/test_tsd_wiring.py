from hg import compute_node, TS, TSD
from hg.test import eval_node


@compute_node
def make_tsd(k: TS[str], v: TS[int]) -> TSD[str, TS[int]]:
    return {k.value: v.delta_value}


def test_tsd():
    assert eval_node(make_tsd, k=['a', 'b'], v=[1, 2]) == [{'a': 1}, {'b': 2}]
