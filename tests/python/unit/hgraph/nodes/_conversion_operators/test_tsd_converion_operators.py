from hgraph import TS, combine, TSD, graph
from hgraph.test import eval_node


def test_combine_tsd_from_tuple_and_tsl():
    @graph
    def g(a: TS[int], b: TS[int]) -> TSD[str, TS[int]]:
        return combine[TSD](('a', 'b'), a, b)

    assert eval_node(g, [1, 2], [3, None]) == [{'a': 1, 'b': 3}, {'a': 2}]

    @graph
    def g(a: TS[int], b: TS[int]) -> TSD[str, TS[int]]:
        return combine[TSD[str, TS[int]]](('a', 'b'), a, b)

    assert eval_node(g, [1, 2], [3, None]) == [{'a': 1, 'b': 3}, {'a': 2}]

