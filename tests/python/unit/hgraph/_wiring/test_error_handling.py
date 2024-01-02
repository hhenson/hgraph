from frozendict import frozendict

from hgraph import graph, TS, TSB, NodeError, ts_schema, TSD, map_, REF
from hgraph._wiring.error_context import error_context, error_time_series
from hgraph.nodes import div_, debug_print
from hgraph.test import eval_node


def test_error_handling():

    schema = ts_schema(out=TS[float], error=TS[NodeError])

    @graph
    def main(lhs: TS[float], rhs: TS[float]) -> TSB[schema]:
        out = lhs / rhs
        return TSB[schema].from_ts(out=out, error=error_time_series(out))

    result = eval_node(main, [1.0, 2.0, 3.0], [1.0, 2.0, 0.0])
    assert result [0:2] == [{"out": 1.0}, {"out": 1.0}]
    assert result[2].keys() == {"error"}

    print(result[2]["error"])


def test_error_handling_with_map():
    schema = ts_schema(out=TSD[int, REF[TS[float]]], error=TSD[int, TS[NodeError]])

    @graph
    def main(lhs: TSD[int, TS[float]], rhs: TSD[int, TS[float]]) -> TSB[schema]:
        out = map_(div_, lhs, rhs)
        return TSB[schema].from_ts(out=out, error=error_time_series(out))

    result = eval_node(main, [{0: 1.0}, {1: 2.0}, {2: 3.0}], [{0: 1.0}, {1: 2.0}, {2: 0.0}])
    assert result[0:2] == [{"out": frozendict({0: 1.0})}, {"out": frozendict({1: 1.0})}]
    assert "error" in result[2].keys()

   # print(result[2]["error"][2])