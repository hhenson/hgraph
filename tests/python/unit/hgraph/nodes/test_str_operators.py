from hgraph import add_, mul_, contains_, TS, graph
from hgraph.nodes import match_, parse
from hgraph.test import eval_node


def test_match():
    assert eval_node(match_, pattern=["a"], s=["a"]) == [{'is_match': True, 'groups': ()}]
    assert eval_node(match_, pattern=["(a)"], s=["a"]) == [{'is_match': True, 'groups': ('a',)}]
    assert eval_node(match_, pattern=["(a)"], s=["aa"]) == [{'is_match': True, 'groups': ('a',)}]
    assert eval_node(match_, pattern=["(a)"], s=["aa"]) == [{'is_match': True, 'groups': ('a',)}]
    assert eval_node(match_, pattern=["a"], s=["b"]) == [{'is_match': False}]


def test_parse():
    assert eval_node(parse[float], s=["1"]) == [1.0]
    assert eval_node(parse[int], s=["1"]) == [1]


def test_mul_str():
    assert eval_node(mul_, ["abc"], [3]) == ["abcabcabc"]


def test_contains_str():
    @graph
    def app(lhs: TS[str], rhs: TS[str]) -> TS[bool]:
        return contains_(lhs, rhs)

    assert eval_node(app, ["abc", None, ""], ["z", "bc", ""]) == [False, True, True]
