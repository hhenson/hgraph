from hgraph import graph, TS, TSB, NodeError, ts_schema
from hgraph._wiring.error_context import error_context, error_time_series
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