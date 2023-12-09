import pytest

from hg import switch_, graph, TS
from hg.nodes import add_, sub_
from hg.test import eval_node


@pytest.mark.xfail(reason="Not implemented", strict=True)
def test_switch():

    @graph
    def switch_test(key: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
        s = switch_({'add': add_, 'sub': sub_}, key, lhs, rhs)
        return s

    eval_node(switch_test, ['add', 'sub'], [1, 2], [3, 4]) == [4, -2]
