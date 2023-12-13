from hg import PythonWiringNodeClass
from hg.nodes._write import write_str


def test_write_str():

    assert type(write_str) == PythonWiringNodeClass