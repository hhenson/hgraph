import pytest
from hgraph import JSON, SCHEMA, TS, TS_SCHEMA, TSB, combine, graph, json_decode, json_encode
from hgraph.test import eval_node



def test_json_combine_encode():
    @graph
    def g() -> TS[str]:
        j1 = combine[TS[JSON]](a=1, b="test")
        j2 = combine[TS[JSON]](c=3.14, d=[1, 2, 3])
        j3 = combine[TS[JSON]](x=j1, y=j2)
        return json_encode[str](j3)
    
    assert eval_node(g) == [
        '{"x": {"a": 1, "b": "test"}, "y": {"c": 3.14, "d": [1, 2, 3]}}'
    ]


def test_json_decode():
    @graph
    def g() -> TSB[TS_SCHEMA]:
        j = '{"a": 1, "b": "test", "c": 3.14, "d": [1, 2, 3]}'
        decoded = json_decode(j)
        return combine(a=decoded["a"].int, b=decoded["b"].str, c=decoded["c"].float, d=decoded["d"][0].int)
    
    assert eval_node(g) == [
        {'a': 1, 'b': 'test', 'c': 3.14, 'd': 1}
    ]


def test_combine_json_dereferences_reference_arguments():
    """A JSON object serialises the VALUES it is given.

    ``if_then_else`` publishes a REFERENCE, and ``combine[TS[JSON]]`` packs its
    keyword arguments into a structural bundle. Before the deref rule was
    applied where variadic and keyword arguments are BOUND, the ref token
    reached the encoder and serialised as ``"<ref>"`` instead of the value it
    names - the same defect as the logging one, in a different consumer.
    """
    from hgraph import TS, JSON, combine, graph, if_then_else
    from hgraph.test import eval_node

    @graph
    def g(choose: TS[bool], lhs: TS[int], rhs: TS[int]) -> TS[JSON]:
        return combine[TS[JSON]](v=if_then_else(choose, lhs, rhs))

    assert eval_node(g, [True], [8], [-6]) == [{"v": 8}]
