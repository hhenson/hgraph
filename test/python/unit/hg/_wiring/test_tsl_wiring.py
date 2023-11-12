from hg import TS, graph, TSL, Size, SCALAR
from hg.nodes import flatten_tsl_values
from hg.test import eval_node


def test_fixed_tsl():
    @graph
    def my_tsl(ts1: TS[int], ts2: TS[int]) -> TS[tuple[int, ...]]:
        tsl = TSL[TS[float], Size[2]].from_ts(ts1, ts2)
        return flatten_tsl_values[SCALAR: int](tsl)

    assert eval_node(my_tsl, ts1=[1, 2], ts2=[3, 4]) == [(1,3), (2, 4)]
