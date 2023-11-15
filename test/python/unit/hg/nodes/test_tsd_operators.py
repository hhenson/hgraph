from hg.nodes import make_tsd
from hg.test import eval_node


def test_make_tsd():
    assert eval_node(make_tsd, ['a', 'b', 'a'], [1, 2, 3]) == [{'a': 1}, {'b': 2}, {'a': 3}]