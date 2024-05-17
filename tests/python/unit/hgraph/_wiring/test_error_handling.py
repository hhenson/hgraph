from typing import cast

from frozendict import frozendict

from hgraph import TryExceptResult, try_except, TryExceptTsdMapResult, exception_time_series
from hgraph import graph, TS, TSB, NodeError, ts_schema, TSD, map_, REF, sink_node
from hgraph.nodes import div_ts
from hgraph.test import eval_node


def test_error_handling():

    schema = ts_schema(out=TS[float], error=TS[NodeError])

    @graph
    def main(lhs: TS[float], rhs: TS[float]) -> TSB[schema]:
        out = lhs / rhs
        return TSB[schema].from_ts(out=out, error=exception_time_series(out))

    result = eval_node(main, [1.0, 2.0, 3.0], [1.0, 2.0, 0.0])
    assert result [0:2] == [{"out": 1.0}, {"out": 1.0}]
    assert result[2].keys() == {"error"}

    # print(result[2]["exception"])


def test_error_handling_with_map():
    schema = ts_schema(out=TSD[int, REF[TS[float]]], error=TSD[int, TS[NodeError]])

    @graph
    def main(lhs: TSD[int, TS[float]], rhs: TSD[int, TS[float]]) -> TSB[schema]:
        out = map_(div_ts, lhs, rhs)
        return TSB[schema].from_ts(out=out, error=exception_time_series(out))

    result = eval_node(main, [{0: 1.0}, {1: 2.0}, {2: 3.0}], [{0: 1.0}, {1: 2.0}, {2: 0.0}])
    assert result[0:2] == [{"out": frozendict({0: 1.0})}, {"out": frozendict({1: 1.0})}]
    assert "error" in result[2].keys()


def test_error_handling_try_except():
    @graph
    def main(lhs: TS[float], rhs: TS[float]) -> TSB[TryExceptResult[TS[float]]]:
        out = try_except(div_ts, lhs, rhs)
        return out

    result = eval_node(main, [1.0, 2.0, 3.0], [1.0, 2.0, 0.0])
    assert result[0:2] == [{"out": 1.0}, {"out": 1.0}]
    assert result[2].keys() == {"exception"}

    # print(result[2]["exception"])


def test_error_handling_with_map_try_except():
    schema = ts_schema(out=TSD[int, REF[TS[float]]], error=TSD[int, TS[NodeError]])

    @graph
    def main(lhs: TSD[int, TS[float]], rhs: TSD[int, TS[float]]) -> \
            TSB[TryExceptTsdMapResult[int, TSD[int, TS[float]]]]:
        out = try_except(map_, div_ts, lhs, rhs)
        return out

    result = eval_node(main, [{0: 1.0}, {1: 2.0}, {2: 3.0}], [{0: 1.0}, {1: 2.0}, {2: 0.0}])
    assert result[0:2] == [{"out": frozendict({0: 1.0})}, {"out": frozendict({1: 1.0})}]
    assert "exception" in result[2].keys()


def test_try_except():
    @graph
    def error(lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return lhs / rhs

    @graph
    def main(lhs: TS[float], rhs: TS[float]) -> TSB[TryExceptResult[TS[float]]]:
        return try_except(error, lhs, rhs)

    out = eval_node(main, [1.0, 2.0], [2.0, 0.0])
    assert out[0]["out"] == 0.5
    assert out[1]["exception"] is not None


def test_sink_node():
    @sink_node
    def sink(ts: TS[float]):
        if ts.value == 2.0:
            raise RuntimeError("Test error")

    @graph
    def main(ts: TS[float]) -> TS[NodeError]:
        return try_except(sink, ts)

    out = eval_node(main, [1.0, 2.0])
    assert len(out) == 2
    assert out[0] is None
    assert cast(NodeError, out[1]).error_msg == "Test error"
