from hgraph import compute_node, TS, TSD, graph, TSS
from hgraph.test import eval_node


@compute_node
def make_tsd(k: TS[str], v: TS[int]) -> TSD[str, TS[int]]:
    return {k.value: v.delta_value}


def test_tsd():
    assert eval_node(make_tsd, k=['a', 'b'], v=[1, 2]) == [{'a': 1}, {'b': 2}]


def test_tsd_key_set():

    @graph
    def _extract_key_set(tsd: TSD[str, TS[int]]) -> TSS[str]:
        return tsd.key_set

    assert eval_node(_extract_key_set, tsd=[{'a': 1}, {'b': 2}]) == [{'a'}, {'b'}]