import pytest

from hgraph import generator, TS, MIN_ST, NodeException
from hgraph.test import eval_node


def test_generator_duplicate_time():

    @generator
    def play_duplicate() -> TS[int]:
        yield MIN_ST, 1
        yield MIN_ST, 2

    with pytest.raises(NodeException):
        print(eval_node(play_duplicate))

