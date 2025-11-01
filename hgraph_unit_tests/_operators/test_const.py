from hgraph import MIN_TD, const, OperatorWiringNodeClass, default
from hgraph._wiring._wiring_node_class._python_wiring_node_classes import PythonGeneratorWiringNodeClass
from hgraph.test import eval_node


import pytest
pytestmark = pytest.mark.smoke

def test_const_wiring():

    assert type(const) is OperatorWiringNodeClass
    const_: PythonGeneratorWiringNodeClass = const
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
