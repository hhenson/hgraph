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
