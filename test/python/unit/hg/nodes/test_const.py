from hg import PythonGeneratorWiringNodeClass
from hg.nodes._const import const


def test_const():

    assert type(const) is PythonGeneratorWiringNodeClass
    const_: PythonGeneratorWiringNodeClass = const
    assert const_.signature.args == ("value", "tp", "context")
    assert const_.signature.input_types['context'].is_injectable
