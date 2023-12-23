from hgraph import PythonGeneratorWiringNodeClass, TS, MIN_TD
from hgraph.nodes import const, default
from hgraph.test import eval_node


def test_const_wiring():

    assert type(const) is PythonGeneratorWiringNodeClass
    const_: PythonGeneratorWiringNodeClass = const
    assert const_.signature.args == ("value", "tp", "delay", "_clock")
    assert const_.signature.input_types['_clock'].is_injectable


def test_const():
    assert eval_node(const, 1) == [1]


def test_delayed_const():
    assert eval_node(const, 1, delay=MIN_TD*2) == [None, None, 1]


def test_delayed():
    assert eval_node(default, [None, 2, 3], 1) == [1, 2, 3]
    assert eval_node(default, [2, 3, 4], 1) == [2, 3, 4]
