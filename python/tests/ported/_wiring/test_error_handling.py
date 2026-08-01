from hgraph import (REMOVE, NodeError, TSD, TS, TSB, TryExceptResult,
                    TryExceptTsdMapResult, compute_node, exception_time_series,
                    graph, map_, sink_node, try_except)
from hgraph.test import eval_node


def test_mapped_python_child_errors_are_keyed_and_erased_with_the_child():
    @compute_node
    def divide(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        if rhs.value == 0:
            raise RuntimeError("division by zero")
        return lhs.value // rhs.value

    @graph
    def error_messages(lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]]) -> TSD[int, TS[str]]:
        values = map_(divide, lhs, rhs)
        return map_(lambda error: error.error_msg, exception_time_series(values))

    result = eval_node(
        error_messages,
        lhs=[{0: 10}, {1: 9}, {2: 8}, {1: REMOVE}],
        rhs=[{0: 2}, {1: 0}, {2: 4}, {1: REMOVE}],
    )

    assert result[0] is None
    assert result[1].keys() == {1}
    assert "division by zero" in result[1][1]
    assert result[2] is None
    assert result[3] == {1: REMOVE}


def test_try_except_wraps_python_value_graph():
    @compute_node
    def divide(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        if rhs.value == 0:
            raise RuntimeError("division by zero")
        return lhs.value // rhs.value

    @graph
    def risky(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return divide(lhs, rhs)

    @graph
    def protected(lhs: TS[int], rhs: TS[int]) -> TSB[TryExceptResult[TS[int]]]:
        return try_except(
            risky,
            lhs=lhs,
            rhs=rhs,
            __trace_back_depth__=2,
            __capture_values__=True,
        )

    result = eval_node(protected, lhs=[10, 9, 8], rhs=[2, 0, 4])

    assert result[0] == {"out": 5}
    assert result[1].keys() == {"exception"}
    assert isinstance(result[1]["exception"], NodeError)
    assert "division by zero" in result[1]["exception"].error_msg
    assert result[1]["exception"].additional_context is None
    # issue #247: the back trace names the USER function (upstream parity)
    assert "divide" in result[1]["exception"].activation_back_trace
    assert "*args*: value={_0: 9, _1: 0}" in result[1]["exception"].activation_back_trace
    assert result[2] == {"out": 2}


def test_try_except_uses_native_error_output_for_python_compute_node():
    @compute_node
    def divide(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        if rhs.value == 0:
            raise RuntimeError("division by zero")
        return lhs.value // rhs.value

    @graph
    def protected(lhs: TS[int], rhs: TS[int]) -> TSB[TryExceptResult[TS[int]]]:
        return try_except(
            divide,
            lhs,
            rhs,
            __trace_back_depth__=2,
            __capture_values__=True,
        )

    result = eval_node(protected, lhs=[10, 9, 8], rhs=[2, 0, 4])

    assert result[0] == {"out": 5}
    assert "division by zero" in result[1]["exception"].error_msg
    # issue #247: the back trace names the USER function (upstream parity)
    assert "divide" in result[1]["exception"].activation_back_trace
    assert "*args*: value={_0: 9, _1: 0}" in result[1]["exception"].activation_back_trace
    assert result[2] == {"out": 2}


def test_try_except_wraps_python_sink_graph():
    @sink_node
    def reject_negative(value: TS[int]):
        if value.value < 0:
            raise RuntimeError("negative value")

    @graph
    def protected(value: TS[int]) -> TS[NodeError]:
        return try_except(reject_negative, value)

    result = eval_node(protected, value=[1, -1, 2])

    assert result[0] is None
    assert isinstance(result[1], NodeError)
    assert "negative value" in result[1].error_msg
    assert result[2] is None


def test_try_except_preserves_keyed_map_errors():
    @compute_node
    def divide(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        if rhs.value == 0:
            raise RuntimeError("division by zero")
        return lhs.value // rhs.value

    @graph
    def protected(
        lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]]
    ) -> TSB[TryExceptTsdMapResult[int, TSD[int, TS[int]]]]:
        return try_except(
            map_,
            divide,
            lhs,
            rhs,
            __trace_back_depth__=2,
            __capture_values__=True,
        )

    result = eval_node(
        protected,
        lhs=[{0: 10}, {1: 9}, {2: 8}, {1: REMOVE}],
        rhs=[{0: 2}, {1: 0}, {2: 4}, {1: REMOVE}],
    )

    assert result[0]["out"] == {0: 5}
    assert result[0]["exception"] == {}
    assert "division by zero" in result[1]["exception"][1].error_msg
    # issue #247: the back trace names the USER function (upstream parity)
    assert "divide" in result[1]["exception"][1].activation_back_trace
    assert "value={_0: 9, _1: 0}" in result[1]["exception"][1].activation_back_trace
    assert result[1]["out"] == {}
    assert result[2]["out"] == {2: 2}
    assert result[2]["exception"] == {}
    assert result[3]["exception"] == {1: REMOVE}
    assert result[3]["out"] == {}
