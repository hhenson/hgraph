from hgraph import MIN_TD, const, OperatorWiringNodeClass, default  # ported
# deviation: python-implementation internals (object  # deviation: internals) do not exist
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke

@pytest.mark.skip(reason="deviation: python wiring-node signature introspection does not exist")
def test_const_wiring():

    assert type(const) is OperatorWiringNodeClass
    const_: object  # deviation: internals = const
    assert const_.signature.args == (
        "value",
        "tp",
        "delay",
    )


def test_const():
    assert eval_node(const, 1) == [1]


def test_delayed_const():
    assert eval_node(const, 1, delay=MIN_TD * 2) == [None, None, 1]


def test_delayed():
    assert eval_node(default, [None, 2, 3], 1) == [1, 2, 3]
    assert eval_node(default, [2, 3, 4], 1) == [2, 3, 4]
