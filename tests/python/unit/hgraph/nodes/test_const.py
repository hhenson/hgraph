from hgraph import PythonGeneratorWiringNodeClass, TS, MIN_TD
from hgraph.nodes import const
from hgraph.test import eval_node


def test_const_wiring():

    assert type(const) is PythonGeneratorWiringNodeClass
    const_: PythonGeneratorWiringNodeClass = const
    assert const_.signature.args == ("value", "tp", "delay", "context")
    assert const_.signature.input_types['context'].is_injectable


def test_const():
    assert eval_node(const, 1) == [1]


def test_delayed_const():
    assert eval_node(const, 1, delay=MIN_TD*2) == [None, None, 1]
