from hgraph import component, TS
from hgraph.test import eval_node


def test_component():

    @component
    def my_component(ts: TS[float]) -> TS[float]:
        return ts + 1.0

    assert eval_node(my_component, [1.0, 2.0, 3.0]) == [2.0, 3.0, 4.0]
