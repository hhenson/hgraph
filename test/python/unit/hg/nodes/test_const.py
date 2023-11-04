from hg import PythonGeneratorWiringNodeClass, TS
from hg.nodes import const, empty_ts
from hg.test import eval_node


def test_const_wiring():

    assert type(const) is PythonGeneratorWiringNodeClass
    const_: PythonGeneratorWiringNodeClass = const
    assert const_.signature.args == ("value", "tp", "context")
    assert const_.signature.input_types['context'].is_injectable


def test_const():
    assert eval_node(const, 1) == [1]


def test_empty_ts():
    assert eval_node(empty_ts, TS[int]) is None
