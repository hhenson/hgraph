from hgraph.nodes import match, parse
from hgraph.test import eval_node


def test_match():
    assert eval_node(match, pattern=["a"], s=["a"]) == [{'is_match': True, 'groups': ()}]
    assert eval_node(match, pattern=["(a)"], s=["a"]) == [{'is_match': True, 'groups': ('a',)}]
    assert eval_node(match, pattern=["(a)"], s=["aa"]) == [{'is_match': True, 'groups': ('a',)}]
    assert eval_node(match, pattern=["(a)"], s=["aa"]) == [{'is_match': True, 'groups': ('a',)}]
    assert eval_node(match, pattern=["a"], s=["b"]) == [{'is_match': False}]


def test_parse():
    assert eval_node(parse[float], s=["1"]) == [1.0]
    assert eval_node(parse[int], s=["1"]) == [1]