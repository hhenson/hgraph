"""Compatibility gate for Python-authored graphs over the C++ runtime."""

import datetime
import threading
import time
from dataclasses import dataclass, field

import hgraph as hg
from hgraph import Size, TS, TSD, TSL, TSS, TSW, WindowSize, graph, eval_node, run_graph


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def expect_raises(exc_type, fn, message=None):
    try:
        fn()
    except exc_type as exc:
        if message is not None:
            check(message in str(exc), f"expected {message!r} in {exc!r}")
        return exc
    raise AssertionError(f"expected {exc_type.__name__}")


def test_compute_nodes_mix_with_cpp_and_bind_keywords():
    @hg.compute_node
    def notional(price: TS[float], quantity: TS[int], scale: float = 1.0) -> TS[float]:
        return price.value * quantity.value * scale

    @graph
    def app(price: TS[float], quantity: TS[int]) -> TS[float]:
        python_value = notional(quantity=quantity, price=price, scale=2.0)
        return python_value + hg.const(1.0, tp=TS[float])

    check(eval_node(app, [2.5, 4.0], [10, 3]) == [51.0, 25.0], "mixed compute pipeline")


def test_public_wiring_cache_keeps_distinct_input_ports_separate():
    @graph
    def app(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        lhs_shifted = lhs + 1
        rhs_shifted = rhs + 1
        return lhs_shifted * 10 + rhs_shifted

    check(
        eval_node(app, [1, 2], [3, 4]) == [24, 35],
        "structurally similar nodes retain their input-port identity",
    )


def test_repeated_python_native_boundary_runs_preserve_type_and_state_lifetimes():
    @hg.compute_node
    def add_bias(value: TS[int], bias: int) -> TS[int]:
        return value.value + bias

    @hg.compute_node
    def finish(value: TS[int]) -> TS[int]:
        return value.value - 1

    @graph
    def app(value: TS[int]) -> TS[int]:
        python_value = add_bias(value, 3)
        native_value = (python_value + hg.const(2, tp=TS[int])) * hg.const(4, tp=TS[int])
        return finish(native_value)

    for run in range(32):
        inputs = [run, run + 1, run - 2]
        expected = [(value + 5) * 4 - 1 for value in inputs]
        check(eval_node(app, inputs) == expected, f"mixed boundary run {run}")


def test_python_node_type_records_identify_bridge_implementations():
    observed = {}

    @hg.compute_node
    def increment(value: TS[int]) -> TS[int]:
        return value.value + 1

    @hg.compute_node
    def offset(value: TS[int], amount: int) -> TS[int]:
        return value.value + amount

    @hg.compute_node
    def remember(value: TS[int], state: hg.STATE = None) -> TS[int]:
        state.last = value.value
        return state.last

    @hg.generator
    def ticks() -> TS[int]:
        yield hg.MIN_ST, 1

    @graph
    def app(value: TS[int]) -> TS[int]:
        direct = increment(value)
        computed = offset(direct, 1)
        stateful = remember(computed)
        generated = ticks()
        observed["single_compute"] = direct._port.node_type_info
        observed["fast_compute"] = computed._port.node_type_info
        observed["full_compute"] = stateful._port.node_type_info
        observed["generator"] = generated._port.node_type_info
        return stateful + generated

    check(eval_node(app, [2]) == [5], "python record-backed nodes execute")
    check(observed["single_compute"]["implementation_label"] == "hgraph.python.compute.fast",
          f"single compute type record: {observed['single_compute']}")
    check(observed["fast_compute"]["implementation_label"] == "hgraph.python.compute.fast",
          f"fast compute type record: {observed['fast_compute']}")
    check(observed["full_compute"]["implementation_label"] == "hgraph.python.compute",
          f"full compute type record: {observed['full_compute']}")
    check(observed["generator"]["implementation_label"] == "hgraph.python.generator",
          f"generator type record: {observed['generator']}")
    check(observed["single_compute"]["semantic_label"] == "__py_compute",
          "single compute semantic schema label")
    check(observed["fast_compute"]["semantic_label"] == "__py_compute",
          "fast compute semantic schema label")
    check(observed["full_compute"]["semantic_label"] == "__py_compute",
          "full compute semantic schema label")
    check(observed["generator"]["semantic_label"] == "__py_generator", "generator semantic schema label")
    check(observed["single_compute"]["family"] == observed["generator"]["family"],
          "common Node family")
    check(observed["single_compute"]["role"] == observed["generator"]["role"],
          "common Runtime role")


def test_python_generator_stop_hook_receives_state_and_scalars():
    stopped = []

    @hg.generator
    def source(label: str, state: hg.STATE = None) -> TS[int]:
        state.value = 7
        check(isinstance(state, hg.STATE), "generator receives the public STATE wrapper")
        yield hg.MIN_ST, state["value"]

    @source.stop
    def stop(label: str, state: hg.STATE = None):
        stopped.append((label, state["value"]))

    @graph
    def app() -> TS[int]:
        return source("generator")

    check(eval_node(app) == [7], "generator output")
    check(stopped == [("generator", 7)], f"generator stop hook: {stopped}")


def test_naked_state_matches_attribute_dictionary_compatibility_surface():
    lifecycle = []

    @hg.compute_node
    def accumulate(value: TS[int], state: hg.STATE = None) -> TS[int]:
        check(not state.is_updated(), "dirty state is reset between observations")
        state.total = state["total"] + value.value
        check(state.is_updated(), "attribute assignment marks state updated")
        check(state.total == state["total"], "attribute and item reads agree")
        check(list(state.keys()) == ["total"], "state keys view")
        check(list(state.items()) == [("total", state.total)], "state items view")
        check(list(state.values()) == [state.total], "state values view")
        result = state["total"]
        state.reset_updated()
        return result

    @accumulate.start
    def start(state: hg.STATE = None):
        check(isinstance(state, hg.STATE), "naked injection returns STATE")
        check(repr(state) == "SCALAR()", "empty state repr")
        check(state.as_schema == {}, "empty backing dictionary")
        check(not state.is_updated(), "new state is clean")
        expect_raises(AttributeError, lambda: state.missing)
        expect_raises(AttributeError, lambda: state["missing"])
        expect_raises(AttributeError, lambda: state.__setitem__("total", 10))
        state.total = 10
        check(state["total"] == 10, "attribute writes feed item reads")
        check(repr(state) == "SCALAR(total=10)", "populated state repr")
        check(state.as_schema == {"total": 10}, "backing dictionary exposes values")
        check(state.is_updated(), "start mutation marks state updated")
        state.reset_updated()
        lifecycle.append(("start", state["total"]))

    @accumulate.stop
    def stop(state: hg.STATE = None):
        lifecycle.append(("stop", state["total"]))
        del state.total
        check(state.is_updated(), "attribute deletion marks state updated")
        check(list(state.items()) == [], "attribute deletion removes the item")

    check(eval_node(accumulate, [1, 2, 3]) == [11, 13, 16], "naked state mapping")
    check(lifecycle == [("start", 10), ("stop", 16)], f"state lifecycle: {lifecycle}")


def test_python_compute_consumes_and_produces_dynamic_tsl():
    # Size[0] is the current native spelling for the unbounded TSL shape.
    @hg.compute_node
    def increment_modified(values: TSL[TS[int], Size[0]]) -> TSL[TS[int], Size[0]]:
        return {index: child.value + 10 for index, child in enumerate(values.values()) if child.modified}

    @graph
    def app(values: TSL[TS[int], Size[0]]) -> TSL[TS[int], Size[0]]:
        return increment_modified(values)

    result = eval_node(app, [{0: 1}, {1: 2}, {0: 3}])
    check(result == [{0: 11}, {1: 12}, {0: 13}], f"dynamic TSL: {result}")


def test_native_map_lifted_kernel_grows_dynamic_tsl_output():
    @graph
    def app(
        lhs: TSL[TS[int], Size[0]], rhs: TSL[TS[int], Size[0]]
    ) -> TSL[TS[int], Size[0]]:
        return hg.map_("add_", lhs, rhs)

    result = eval_node(
        app,
        [{0: 1}, {1: 2}, {0: 3}],
        [{0: 10}, {1: 20}, {0: 100}],
    )
    check(result == [{0: 11}, {1: 22}, {0: 103}], f"dynamic TSL map: {result}")


def test_native_dynamic_tsl_map_runs_python_child_nodes_by_index():
    @hg.compute_node
    def combine(
        ndx: TS[int], lhs: TS[int], rhs: TS[int], offset: TS[int]
    ) -> TS[int]:
        return ndx.value + lhs.value + rhs.value + offset.value

    @graph
    def app(
        lhs: TSL[TS[int], Size[0]],
        rhs: TSL[TS[int], Size[0]],
        offset: TS[int],
    ) -> TSL[TS[int], Size[0]]:
        return hg.map_(combine, lhs, rhs, offset)

    result = eval_node(
        app,
        [{0: 1, 1: 2}, None, {0: 5}],
        [{0: 10}, {1: 20}, None],
        [100, None, 200],
    )
    check(
        result == [{0: 111}, {1: 123}, {0: 215, 1: 223}],
        f"Python child in native dynamic TSL map: {result}",
    )

    @graph
    def captured_offset(
        values: TSL[TS[int], Size[0]], offset: TS[int]
    ) -> TSL[TS[int], Size[0]]:
        return hg.map_(lambda value: value + offset, values)

    captured_result = eval_node(
        captured_offset,
        [{0: 1}, {1: 2}, {0: 3}],
        [10, None, 100],
    )
    check(
        captured_result == [{0: 11}, {1: 12}, {0: 103, 1: 102}],
        f"captured Python input in native dynamic TSL map: {captured_result}",
    )


def test_native_dynamic_tsl_map_preserves_composed_structural_child_outputs():
    class Pair(hg.TimeSeriesSchema):
        value: TS[int]
        offset: TS[int]

    @graph
    def pair(value: TS[int]) -> hg.TSB[Pair]:
        return hg.combine[hg.TSB[Pair]](value=value, offset=value + 100)

    @graph
    def app(
        values: TSL[TS[int], Size[0]],
    ) -> TSL[hg.TSB[Pair], Size[0]]:
        return hg.map_(pair, values)

    result = eval_node(app, [{0: 1}, {1: 2}, {0: 3}])
    check(
        result
        == [
            {0: {"value": 1, "offset": 101}},
            {1: {"value": 2, "offset": 102}},
            {0: {"value": 3, "offset": 103}},
        ],
        f"structural Python child in native dynamic TSL map: {result}",
    )


def test_python_sink_nodes_work_as_native_dynamic_tsl_map_children():
    seen = []

    @hg.sink_node
    def collect(ndx: TS[int], value: TS[int]):
        seen.append((ndx.value, value.value))

    @graph
    def app(values: TSL[TS[int], Size[0]]) -> TSL[TS[int], Size[0]]:
        check(hg.map_(collect, values) is None, "dynamic TSL sink map wiring result")
        return values

    inputs = [{0: 10}, {1: 20}, {0: 30}]
    check(eval_node(app, inputs) == inputs, "dynamic TSL sink map wrapper output")
    check(seen == [(0, 10), (1, 20), (0, 30)], f"dynamic TSL sink calls: {seen}")


def test_non_associative_reduce_uses_ordered_native_paths():
    @graph
    def reduce_tsl(
        values: TSL[TS[int], Size[4]], zero: TS[int]
    ) -> TS[int]:
        return hg.reduce(
            lambda lhs, rhs: lhs - rhs,
            values,
            zero,
            is_associative=False,
        )

    result = eval_node(
        reduce_tsl,
        [{0: 1}, {1: 2, 2: 3, 3: 4}, None, {0: 10}],
        [100, None, 7, None],
    )
    check(result == [-299, -8, None, 1], f"ordered TSL reduce: {result}")

    @graph
    def reduce_tuple(values: TS[tuple[int, ...]], zero: TS[str]) -> TS[str]:
        return hg.reduce(
            lambda lhs, rhs: hg.format_("{lhs}, {rhs}", lhs=lhs, rhs=rhs),
            values,
            zero,
            is_associative=False,
        )

    result = eval_node(reduce_tuple, [(1, 2), (1,), tuple()], ["a"])
    check(result == ["a, 1, 2", "a, 1", "a"], f"ordered tuple reduce: {result}")


def test_python_compute_produces_tick_and_duration_tsw():
    @hg.compute_node
    def tick_window(value: TS[int]) -> TSW[int, WindowSize[3], WindowSize[1]]:
        return value.value

    @hg.compute_node
    def duration_window(
        value: TS[int],
    ) -> TSW[int, WindowSize[hg.MIN_TD * 3], WindowSize[hg.MIN_TD]]:
        return value.value

    check(eval_node(tick_window, [1, 2, 3, 4]) == [1, 2, 3, 4], "tick TSW output")
    check(eval_node(duration_window, [1, 2, 3, 4]) == [1, 2, 3, 4], "duration TSW output")


def test_compute_validity_optional_inputs_and_no_tick():
    @hg.compute_node(valid=("trigger",))
    def sample(trigger: TS[int], optional: TS[int] = None) -> TS[int]:
        if trigger.value < 0:
            return None
        return trigger.value + (optional.value if optional.valid else 0)

    @graph
    def app(trigger: TS[int], optional: TS[int]) -> TS[int]:
        return sample(trigger, optional)

    check(eval_node(app, [1, -1, 3], [None, 10, None]) == [1, None, 13], "validity gating")

    @graph
    def absent(trigger: TS[int]) -> TS[int]:
        return sample(trigger)

    check(eval_node(absent, [2]) == [2], "optional unwired input")


def test_compute_and_sink_active_inputs_and_scheduler_wakeup():
    compute_calls = []
    sink_calls = []

    @hg.compute_node(active=("trigger",), valid=("trigger",))
    def sample(trigger: TS[int], observed: TS[int]) -> TS[int]:
        compute_calls.append((trigger.value, observed.value))
        return observed.value

    @hg.sink_node(active=("trigger",), valid=("trigger",))
    def collect(trigger: TS[int], observed: TS[int]):
        sink_calls.append((trigger.value, observed.value))

    @graph
    def app(trigger: TS[int], observed: TS[int]) -> TS[int]:
        collect(trigger, observed)
        return sample(trigger, observed)

    check(eval_node(app, [1, None, 2], [10, 20, 30]) == [10, None, 30], "active input output")
    expected = [(1, 10), (2, 30)]
    check(compute_calls == expected, f"compute active calls: {compute_calls}")
    check(sink_calls == expected, f"sink active calls: {sink_calls}")

    @hg.compute_node(active=(), valid=("value",))
    def scheduled(value: TS[int]) -> TS[int]:
        return value.value

    @scheduled.start
    def scheduled_start(scheduler: hg.SCHEDULER = None):
        scheduler.schedule(hg.MIN_TD)

    check(eval_node(scheduled, [7]) == [None, 7], "scheduler wakes an input-passive node")


def test_compute_input_policies_reject_non_time_series_names():
    def decorate_active():
        @hg.compute_node(active=("label",))
        def invalid(value: TS[int], label: str) -> TS[int]:
            return value.value

    expect_raises(TypeError, decorate_active, "non-time-series input(s): label")

    def decorate_valid():
        @hg.sink_node(valid=("missing",))
        def invalid(value: TS[int]):
            pass

    expect_raises(TypeError, decorate_valid, "non-time-series input(s): missing")


def test_compute_state_clock_scheduler_and_output_view():
    @hg.compute_node
    def delayed_delta(
        value: TS[int],
        state: hg.STATE = None,
        clock: hg.CLOCK = None,
        scheduler: hg.SCHEDULER = None,
        _output=None,
    ) -> TS[int]:
        if getattr(state, "pending", None) is not None:
            pending, state.pending = state.pending, None
            return pending
        prior = _output.value if _output.valid else 0
        state.pending = value.value - prior
        state.started_at = getattr(state, "started_at", clock.evaluation_time)
        scheduler.schedule(hg.MIN_TD)
        return value.value

    check(eval_node(delayed_delta, [4]) == [4, 4], "state/scheduler/output injection")


def test_compute_typed_state_constructs_the_declared_state_once():
    @dataclass
    class History:
        values: list[int] = field(default_factory=list)

    @hg.compute_node
    def remember(value: TS[int], state: hg.STATE[History] = None) -> TS[int]:
        state.values.append(value.value)
        return sum(state.values)

    check(eval_node(remember, [1, 2, 3]) == [1, 3, 6], "typed state")


def test_mutable_output_view_set_operations():
    @hg.compute_node
    def mutate(trigger: TS[bool], _output: TSS[int] = None) -> TSS[int]:
        _output.add(1)
        _output.add(2)
        _output.remove(1)

    check(eval_node(mutate, [True]) == [{2}], "mutable TSS output")


def test_mutable_output_view_status_delta_and_invalidation():
    observations = []

    @hg.compute_node
    def mutate(value: TS[int], _output: TS[int] = None) -> TS[int]:
        observations.append((_output.modified, _output.can_apply_result(value.value)))
        _output.value = value.value
        observations.append(
            (_output.modified, _output.delta_value, _output.can_apply_result(value.value + 1))
        )
        if value.value == 2:
            _output.invalidate()

    check(eval_node(mutate, [1, 2, 3]) == [1, None, 3], "mutable output invalidation")
    check(
        observations
        == [
            (False, True),
            (True, 1, False),
            (False, True),
            (True, 2, False),
            (False, True),
            (True, 3, False),
        ],
        "mutable output status/delta",
    )


def test_mutable_output_views_expire_after_evaluation():
    retained = []

    @hg.compute_node
    def mutate(trigger: TS[bool], _output: TSD = None) -> TSD[str, TS[int]]:
        child = _output.get_or_create("a")
        child.value = 1
        retained.extend((_output, child))

    check(eval_node(mutate, [True]) == [{"a": 1}], "mutable TSD output")
    for view in retained:
        expect_raises(RuntimeError, lambda view=view: view.value,
                      "outside its node's evaluation")
        expect_raises(RuntimeError, lambda view=view: setattr(view, "value", None),
                      "outside its node's evaluation")


def test_compute_and_sink_lifecycle_callbacks():
    events = []

    @hg.compute_node
    def tracked(value: TS[int], label: str, state: hg.STATE = None) -> TS[int]:
        events.append(("eval", label, value.value, state.bias))
        return value.value + state.bias

    @tracked.start
    def tracked_start(label: str, state: hg.STATE = None, clock: hg.CLOCK = None):
        state.bias = 10
        events.append(("start", label, clock.evaluation_time))

    @tracked.stop
    def tracked_stop(label: str, state: hg.STATE = None):
        events.append(("stop", label, state.bias))

    seen = []
    sink_events = []

    @hg.sink_node
    def collect(value: TS[int], label: str):
        seen.append((label, value.value))

    @collect.start
    def collect_start(label: str):
        sink_events.append(("start", label))

    @collect.stop
    def collect_stop(label: str):
        sink_events.append(("stop", label))

    @graph
    def app(value: TS[int]) -> TS[int]:
        out = tracked(value, "node")
        collect(out, label="sink")
        return out

    check(eval_node(app, [1, 2]) == [11, 12], "lifecycle output")
    check(seen == [("sink", 11), ("sink", 12)], f"sink calls: {seen}")
    check(sink_events == [("start", "sink"), ("stop", "sink")], f"sink lifecycle: {sink_events}")
    check(events[0][0] == "start" and events[-1][0] == "stop", f"lifecycle order: {events}")


def test_zero_argument_lifecycle_callbacks():
    events = []

    @hg.compute_node
    def passthrough(value: TS[int]) -> TS[int]:
        return value.value

    @passthrough.start
    def start():
        events.append("start")

    @passthrough.stop
    def stop():
        events.append("stop")

    check(eval_node(passthrough, [1]) == [1], "zero-argument lifecycle output")
    check(events == ["start", "stop"], f"zero-argument lifecycle: {events}")


def test_runtime_global_state_and_graph_seed_injection():
    state = hg.GlobalState(offset=5)
    runtime_states = []

    @hg.compute_node
    def apply_offset(value: TS[int], global_state: hg.GlobalState = None) -> TS[int]:
        runtime_states.append(("eval", global_state))
        global_state["calls"] = global_state.get("calls", 0) + 1
        return value.value + global_state["offset"]

    @apply_offset.start
    def start(global_state: hg.GlobalState = None):
        runtime_states.append(("start", global_state))

    @apply_offset.stop
    def stop(global_state: hg.GlobalState = None):
        runtime_states.append(("stop", global_state))

    @graph
    def app(value: TS[int], global_state: hg.GlobalState = None) -> TS[int]:
        check(global_state["offset"] == 5, "graph sees seed")
        return apply_offset(value)

    with hg.GlobalContext(state):
        check(eval_node(app, [1, 2]) == [6, 7], "runtime global state")
    check(state["calls"] == 2, "runtime state copied back")
    check([phase for phase, _ in runtime_states] == ["start", "eval", "eval", "stop"],
          "runtime state spans complete executor phases")
    check(all(runtime_state is runtime_states[0][1]
              for _, runtime_state in runtime_states),
          "runtime state projection is cached by the owning node")
    for _, runtime_state in runtime_states:
        expect_raises(RuntimeError, lambda runtime_state=runtime_state: runtime_state.get("calls"),
                      "outside its node's evaluation")


def test_runtime_global_state_must_be_injected():
    @hg.compute_node
    def invalid(value: TS[int]) -> TS[int]:
        return value.value + hg.GlobalState.instance().get("offset", 0)

    with hg.GlobalContext(hg.GlobalState(offset=5)):
        expect_raises(RuntimeError, lambda: eval_node(invalid, [1]),
                      "declare a GlobalState injectable")


def test_wiring_failure_releases_global_context():
    @graph
    def invalid(lhs: TS[str], rhs: TS[str]) -> TS[str]:
        return lhs - rhs

    expect_raises(hg.WiringError, lambda: eval_node(invalid, ["a"], ["b"]))

    @graph
    def valid(value: TS[int]) -> TS[int]:
        return value + 1

    check(eval_node(valid, [1]) == [2], "wiring context released after failure")


def test_collection_views_and_deltas_cross_both_directions():
    @hg.compute_node(valid=("values",))
    def increment_modified(values: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return {key: child.value + 1 for key, child in values.modified_items()}

    @graph
    def app(values: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_("add_", increment_modified(values), hg.const(10, tp=TS[int]))

    out = eval_node(app, [{"a": 1}, {"b": 2}, {"a": 4}])
    check(out == [{"a": 12}, {"b": 13}, {"a": 15}], f"collection deltas: {out}")

    @hg.compute_node
    def emit_delta(step: TS[int]) -> TSD[str, TS[int]]:
        if step.value == 1:
            return {"a": 1, "b": 2}
        if step.value == 2:
            return {"a": hg.REMOVE}
        return {"b": step.value}

    out = eval_node(emit_delta, [1, 2, 3])
    check(out == [{"a": 1, "b": 2}, {"a": hg.REMOVE}, {"b": 3}],
          f"Python TSD output deltas: {out}")


def test_graph_scalar_return_lifts_to_const():
    # parity issues #48/#52: a plain value returned from a @graph with a
    # time-series return annotation lifts to const of the annotated type,
    # including at the eval_node top level.
    @graph
    def const_int(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return 3

    check(eval_node(const_int, [None], [None]) == [3],
          "int scalar graph return lifts to const")

    @graph
    def const_float(lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return -19.2390193939209

    check(eval_node(const_float, [None], [None]) == [-19.2390193939209],
          "float scalar graph return lifts to const")


def test_sink_only_graph_returns_none():
    seen = []

    @hg.sink_node
    def collect(value: TS[int], prefix: str = "v"):
        seen.append(f"{prefix}:{value.value}")

    @graph
    def app(value: TS[int]) -> None:
        collect(value=value, prefix="item")

    check(eval_node(app, [1, 2]) is None, "sink graph result")
    check(seen == ["item:1", "item:2"], f"sink graph: {seen}")


def test_generators_capture_arguments_are_distinct_and_cleanup():
    cleaned = []

    @hg.generator
    def sequence(start: int, *, count: int) -> TS[int]:
        try:
            for index in range(count):
                yield hg.MIN_ST + index * hg.MIN_TD, start + index
        finally:
            cleaned.append(start)

    @graph
    def app() -> TS[int]:
        return sequence(10, count=2) + sequence(100, count=2)

    check([value for _, value in run_graph(app)] == [110, 112], "distinct generators")
    check(cleaned == [10, 100], f"generator cleanup: {cleaned}")

    @hg.generator
    def empty() -> TS[int]:
        if False:
            yield hg.MIN_ST, 1

    check(run_graph(empty) == [], "empty generator")


def test_generator_injects_engine_api_for_its_full_lifetime():
    @hg.generator
    def sequence(offset: int, *, _api: hg.EvaluationEngineApi) -> TS[datetime.datetime]:
        yield datetime.timedelta(), _api.start_time + offset * hg.MIN_TD
        yield hg.MIN_TD, _api.start_time + (offset + 1) * hg.MIN_TD

    assert eval_node(sequence, offset=2) == [
        hg.MIN_ST + 2 * hg.MIN_TD,
        hg.MIN_ST + 3 * hg.MIN_TD,
    ]


def test_generator_evaluation_clock_advances_with_each_resume():
    @hg.generator
    def sequence(_clock: hg.EvaluationClock = None) -> hg.TS[datetime.datetime]:
        for _ in range(3):
            yield _clock.next_cycle_evaluation_time, _clock.evaluation_time

    assert eval_node(sequence) == [
        None,
        hg.MIN_ST,
        hg.MIN_ST + hg.MIN_TD,
        hg.MIN_ST + 2 * hg.MIN_TD,
    ]


def test_generator_rejects_duplicate_or_retrograde_times():
    @hg.generator
    def duplicate() -> TS[int]:
        yield hg.MIN_ST, 1
        yield hg.MIN_ST, 2

    expect_raises(RuntimeError, lambda: run_graph(duplicate))

    @hg.generator
    def retrograde() -> TS[int]:
        yield hg.MIN_ST + hg.MIN_TD, 1
        yield hg.MIN_ST, 2

    expect_raises(RuntimeError, lambda: run_graph(retrograde))

    @hg.generator
    def broken() -> TS[int]:
        yield hg.MIN_ST, 1
        raise ValueError("generator failed")

    expect_raises(RuntimeError, lambda: run_graph(broken), "generator failed")


def test_python_exception_is_translated_before_leaving_the_gil_scope():
    @hg.compute_node
    def broken(value: TS[int]) -> TS[int]:
        raise ValueError(f"compute failed for {value.value}")

    error = expect_raises(RuntimeError, lambda: eval_node(broken, [7]), "compute failed for 7")
    check("node[" in str(error) and "evaluate failed" in str(error),
          f"missing native node context: {error}")


def test_python_graphs_work_as_native_higher_order_functions():
    @graph
    def transform(value: TS[int]) -> TS[int]:
        return value * 2 + 1

    @graph
    def app(values: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_(transform, values)

    check(eval_node(app, [{"a": 2}, {"b": 3}]) == [{"a": 5}, {"b": 7}], "Python WiredFn")


def test_python_sink_nodes_work_as_native_keyed_map_children():
    seen = []
    lifecycle = []

    @hg.sink_node
    def collect(key: TS[int], value: TS[int]):
        seen.append((key.value, value.value))

    @collect.start
    def collect_start():
        lifecycle.append("start")

    @collect.stop
    def collect_stop():
        lifecycle.append("stop")

    @graph
    def collect_graph(key: TS[int], value: TS[int]) -> None:
        collect(key, value)

    @graph
    def app(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        check(hg.map_(collect_graph, values) is None, "sink map wiring result")
        return values

    inputs = [{1: 10, 2: 20}, {2: 200}, {1: hg.REMOVE}, {1: 7}]
    check(eval_node(app, inputs) == inputs, "sink map wrapper output")
    check(seen == [(1, 10), (2, 20), (2, 200), (1, 7)], f"sink map calls: {seen}")
    check(lifecycle.count("start") == 3, f"sink map starts: {lifecycle}")
    check(lifecycle.count("stop") == 3, f"sink map stops: {lifecycle}")


def test_python_mapped_key_source_preserves_frozenset_scalar_values():
    observed = []

    @hg.compute_node
    def inspect_key(key: TS[frozenset[int]], value: TS[int]) -> TS[bool]:
        mapped_key = key.value
        observed.append(mapped_key)
        return type(mapped_key) is frozenset and hash(mapped_key) == hash(frozenset(mapped_key))

    @graph
    def app(values: TSD[frozenset[int], TS[int]]) -> TSD[frozenset[int], TS[bool]]:
        return hg.map_(inspect_key, values)

    first = frozenset({1, 2})
    second = frozenset({3})
    check(
        eval_node(app, [{first: 10, second: 20}, {first: hg.REMOVE}]) == [
            {first: True, second: True},
            {first: hg.REMOVE},
        ],
        "mapped frozenset key values remain immutable and hashable",
    )
    check(
        len(observed) == 2 and all(type(value) is frozenset for value in observed),
        f"mapped key Python values: {observed}",
    )


def test_python_key_only_sink_map_uses_explicit_keys():
    seen = []

    @hg.sink_node
    def collect(key: TS[int]):
        seen.append(key.value)

    @graph
    def app(keys: TSS[int]) -> TSS[int]:
        check(hg.map_(collect, __keys__=keys) is None, "key-only sink map wiring result")
        return keys

    inputs = [{1, 2}, {hg.Removed(1)}, {3}]
    check(eval_node(app, inputs) == inputs, "key-only sink map wrapper output")
    check(seen == [1, 2, 3], f"key-only sink map calls: {seen}")


def test_reference_service_path_and_scalar_configuration():
    @hg.reference_service
    def configured_value() -> TS[int]: ...

    @hg.service_impl(interfaces=configured_value)
    def configured_value_impl(path: str, base: int) -> TS[int]:
        return hg.const(base + len(path), tp=TS[int])

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("desk", configured_value_impl, base=40)
        return value + hg.passive(configured_value(path="desk"))

    check(eval_node(app, [1]) == [45], "configured reference service")


def test_request_reply_service_path_and_scalar_configuration():
    @hg.request_reply_service
    def adjust(request: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=adjust)
    def adjust_impl(path: str, requests, increment: int) -> TSD[int, TS[int]]:
        return hg.map_(lambda value: value + increment + len(path), requests)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_service("rr", adjust_impl, increment=3)
        return adjust(value, path="rr")

    out = eval_node(app, [5], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [None, 10], f"configured request/reply: {out}")


def test_subscription_service_can_use_a_python_compute_node():
    @hg.subscription_service
    def quote(symbol: TS[str]) -> TS[int]: ...

    @hg.compute_node
    def quote_values(symbols: TSS[str]) -> TSD[str, TS[int]]:
        return {symbol: len(symbol) for symbol in symbols.added()}

    implementation = hg.service_impl(quote_values, interfaces=quote)

    @graph
    def app(symbol: TS[str]) -> TS[int]:
        hg.register_service("quotes", implementation)
        return quote(symbol, path="quotes")

    out = eval_node(app, ["EURUSD"], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    check(out == [None, 6], f"subscription compute implementation: {out}")


def test_adaptor_path_and_scalar_configuration():
    @hg.adaptor
    def loopback(value: TS[int]) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=(loopback,))
    def loopback_impl(path: str, factor: int):
        incoming = hg.from_graph(loopback, path=path)
        hg.to_graph(loopback, incoming * factor, path=path)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_adaptor("io", loopback_impl, factor=3)
        return loopback(value, path="io")

    check(eval_node(app, [2, 4]) == [6, 12], "configured adaptor")


def test_realtime_push_source_and_python_sink():
    seen = []
    threads = []

    @hg.push_queue(TS[int])
    def source(sender, values: tuple):
        def feed():
            time.sleep(0.05)
            for value in values:
                sender(value)
                time.sleep(0.01)

        thread = threading.Thread(target=feed)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def collect(value: TS[int]):
        seen.append(value.value)

    @graph
    def app() -> None:
        collect(source(values=(1, 2, 3)) + 1)

    end = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(seconds=1)
    run_graph(app, end_time=end, run_mode=hg.EvaluationMode.REAL_TIME)
    for thread in threads:
        thread.join()
    check(seen == [2, 3, 4], f"push source: {seen}")


def main():
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} Python authoring tests passed")


if __name__ == "__main__":
    main()
