import pytest
from hgraph import (
    TS, TSL, TSD, Size, graph, compute_node, const, passive,
    inspect_wiring_dependencies, wiring_extension_state, WiringDependency, eval_node,
)


@compute_node(active=("clock",))
def sample(clock: TS[int], value: TS[int]) -> TS[int]:
    return value.value


@compute_node
def join(left: TS[int], right: TS[int]) -> TS[int]:
    return left.value + right.value


@pytest.mark.parametrize("native", [False, True])
def test_active_path_wins_and_boundaries_stop_traversal(native):
    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        weak = sample(a, b)
        assert inspect_wiring_dependencies(weak, (a, b)) == (
            WiringDependency(0, False), WiringDependency(1, True))
        combined = weak + b if native else join(weak, b)
        assert inspect_wiring_dependencies(combined, (a, b)) == ((0, False), (1, False))
        assert inspect_wiring_dependencies(weak, (weak, a, b)) == ((0, False),)
        return combined
    assert eval_node(g, [1], [2]) == [4]


def test_native_passive_usage_and_structural_input():
    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        value = a + passive(b)
        assert inspect_wiring_dependencies(value, (a, b)) == ((0, False), (1, True))
        packed = TSL.from_ts(a, b)
        assert inspect_wiring_dependencies(packed[1], (a, b)) == ((1, False),)
        return value
    assert eval_node(g, [1], [2]) == [3]


def test_extension_state_is_shared_inline_and_retired_between_runs():
    states = []
    @graph
    def inner(a: TS[int]) -> TS[int]:
        assert wiring_extension_state("inspection.test", list) is states[-1]
        return a
    @graph
    def g(a: TS[int]) -> TS[int]:
        state = wiring_extension_state("inspection.test", list)
        assert not state
        state.append(1)
        states.append(state)
        return inner(a)
    assert eval_node(g, [1]) == [1]
    assert eval_node(g, [1]) == [1]
    assert states[0] is not states[1]


def test_inspection_requires_live_wiring():
    with pytest.raises(RuntimeError, match="no active wiring"):
        inspect_wiring_dependencies(None, ())


def test_compiled_graph_requires_explicit_boundary():
    from hgraph import map_
    @graph
    def g(a: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        result = map_(sample, a, a)
        with pytest.raises(ValueError, match="compiled child"):
            inspect_wiring_dependencies(result, (a,))
        assert inspect_wiring_dependencies(result, (result, a)) == ((0, False),)
        return result
    assert eval_node(g, [{"a": 1, "b": 2}]) == [{"a": 1, "b": 2}]


def test_bound_delayed_input_and_projected_boundary():
    from hgraph import delayed_binding
    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        delayed = delayed_binding(TSL[TS[int], Size[2]])
        result = delayed()[1]
        assert inspect_wiring_dependencies(result, (delayed(),)) == ((0, False),)
        with pytest.raises(ValueError, match="unbound delayed"):
            inspect_wiring_dependencies(result, (a, b))
        delayed(TSL.from_ts(a, b))
        assert inspect_wiring_dependencies(result, (a, b)) == ((1, False),)
        return result
    assert eval_node(g, [1], [2]) == [2]


def test_python_usage_passivity_and_all_passive_policy():
    @compute_node(active=())
    def dormant(a: TS[int]) -> TS[int]:
        return a.value

    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        result = join(a, passive(b))
        assert inspect_wiring_dependencies(result, (a, b)) == ((0, False), (1, True))
        assert inspect_wiring_dependencies(dormant(a), (a,)) == ((0, True),)
        return result
    assert eval_node(g, [1], [2]) == [3]


def test_error_capture_preserves_packed_input_activity():
    from hgraph import exception_time_series
    @graph
    def g(a: TS[int], b: TS[int]) -> TS[int]:
        result = sample(a, b)
        exception_time_series(result)
        assert inspect_wiring_dependencies(result, (a, b)) == ((0, False), (1, True))
        return result
    assert eval_node(g, [1], [2]) == [2]
