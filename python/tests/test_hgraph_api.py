"""The hgraph-shaped Python API over the C++ runtime."""
import datetime
from dataclasses import dataclass

import hgraph as hg
import pytest
from hgraph import (CompoundScalar, TS, TSS, TSD, TSL, TSB, Size,
                    TimeSeriesSchema, graph, run_graph, eval_node)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_graph_with_operator_sugar():
    @graph
    def calc(a: TS[int], b: TS[int]) -> TS[int]:
        return (a + b) * hg.const(2, tp=TS[int])

    check(eval_node(calc, [1, None, 3], [10, 20, None]) == [22, 42, 46], "sugar")


def test_global_state_copy_in_and_copy_back():
    state = hg.GlobalState(seed=7)

    @graph
    def app() -> TS[int]:
        return hg.const(3, tp=TS[int])

    with hg.GlobalContext(state) as active:
        check(active is state and hg.GlobalState.instance() is state, "selected state identity")
        try:
            with hg.GlobalContext():
                pass
            check(False, "expected nested GlobalContext to fail")
        except RuntimeError:
            pass

        check(run_graph(app) == [(hg.MIN_ST, 3)], "contextualized run")
        check(state["seed"] == 7, "seed copied into and back from the graph")
        check("__run_graph__" in state, "final graph state copied back")

    shorthand = hg.GlobalState(value=11)
    with shorthand as active:
        check(active is shorthand, "GlobalState context shorthand")


def test_comparisons_and_unary():
    @graph
    def logic(a: TS[int], b: TS[int]) -> TS[bool]:
        return (a > b) | (a == b)

    check(eval_node(logic, [1, 5, 3], [2, 4, 3]) == [False, True, True], "cmp")

    @graph
    def negate(a: TS[int]) -> TS[int]:
        return -a

    check(eval_node(negate, [4, -2]) == [-4, 2], "neg")


def test_named_operators_via_module_getattr():
    @graph
    def fmt(a: TS[int]) -> TS[str]:
        return hg.format_("value={}", a)

    check(eval_node(fmt, [7]) == ["value=7"], "format_")

    @graph
    def clipped(a: TS[float]) -> TS[float]:
        return hg.clip(a, 0.0, 10.0)

    check(eval_node(clipped, [-5.0, 5.0, 15.0]) == [0.0, 5.0, 10.0], "clip")


def test_nested_graphs_inline():
    @graph
    def double(a: TS[int]) -> TS[int]:
        return a + a

    @graph
    def quad(a: TS[int]) -> TS[int]:
        return double(double(a))

    check(eval_node(quad, [1, 2]) == [4, 8], "nested")


def test_run_graph_returns_time_value_pairs():
    @graph
    def top() -> TS[int]:
        return hg.const(5, tp=TS[int]) + hg.const(6, tp=TS[int])

    result = run_graph(top)
    check(result == [(hg.MIN_ST, 11)], f"run_graph: {result}")


def test_tss_and_filtering():
    @graph
    def evens(a: TS[int]) -> TS[int]:
        return hg.filter_(a % hg.const(2, tp=TS[int]) == hg.const(0, tp=TS[int]), a)

    out = eval_node(evens, [1, 2, 3, 4])
    check(out == [None, 2, None, 4], f"filter_: {out}")


def test_tsb_bundle():
    class Pair(TimeSeriesSchema):
        x: TS[int]
        y: TS[int]

    check(TSB[Pair] is not None, "TSB construction")
    check(TSD[str, TS[int]] is not None and TSL[TS[int], Size[2]] is not None, "container types")


def test_scalars_in_operators():
    @graph
    def lagged(a: TS[int]) -> TS[int]:
        return hg.lag(a, 1)

    out = eval_node(lagged, [1, 2, 3])
    check(out == [None, 1, 2], f"lag: {out}")


def test_wire_does_not_retry_with_blanket_auto_const():
    from hgraph.test import use_wiring

    class FakeWiring:
        def __init__(self):
            self.calls = 0

        def wire(self, name, args, kwargs, output_type=None):
            self.calls += 1
            raise RuntimeError("first failure")

    fake = FakeWiring()
    with use_wiring(fake):
        try:
            hg.wire("needs_mixed_args", 1, False)
            check(False, "expected WiringError")
        except hg.WiringError:
            pass

    check(fake.calls == 1, f"wire retried {fake.calls} times")


def test_wire_does_not_promote_positional_types_generically():
    from hgraph.test import use_wiring

    seen = {}

    class FakeWiring:
        def wire(self, name, args, kwargs, output_type=None):
            seen["name"] = name
            seen["args"] = args
            seen["output_type"] = output_type
            return None

    with use_wiring(FakeWiring()):
        hg.wire("custom", TS[int])

    check(seen["name"] == "custom", f"unexpected operator: {seen}")
    check(len(seen["args"]) == 1, f"positional type was stripped: {seen}")
    check(seen["output_type"] is None, f"positional type became output_type: {seen}")


def test_const_positional_output_type_compatibility():
    @graph
    def source() -> TS[int]:
        return hg.const(5, TS[int])

    check(eval_node(source) == [5], "const(value, TS[int])")


def test_eval_node_scalar_inputs_follow_ts_annotations():
    @graph
    def total(a: TS[float], b: TS[float], c: TS[float]) -> TS[float]:
        return hg.sum_(a, b, c)

    check(eval_node(total, 4.0, 5.0, 6.0) == [15.0], "scalar eval_node inputs")


def test_eval_node_resolution_dict_validates_decorated_targets():
    @graph
    def passthrough(tsd: TS[int], scale: int = 1) -> TS[int]:
        return tsd * scale

    @hg.compute_node
    def python_passthrough(tsd: TS[int]) -> TS[int]:
        return tsd.value

    @hg.compute_node
    def packed_passthrough(*values: TSL[TS[int], Size[2]]) -> TS[int]:
        return sum(value.value for value in values)

    check(
        eval_node(passthrough, [1, 2], resolution_dict={"tsd": TS[int]}) == [1, 2],
        "valid graph resolution",
    )
    check(
        eval_node(python_passthrough, [1, 2], resolution_dict={"tsd": TS[int]}) == [1, 2],
        "valid node resolution",
    )
    check(
        eval_node(
            packed_passthrough,
            [1, 3],
            [2, 4],
            resolution_dict={"values": TS[int]},
        ) == [3, 7],
        "valid packed vararg resolution",
    )

    for target in (passthrough, python_passthrough):
        with pytest.raises(
            hg.WiringError,
            match=(
                r"resolution_dict contains unknown parameter\(s\): 'other', 'ts'; "
                r"valid time-series parameters: 'tsd'"
            ),
        ):
            eval_node(
                target,
                [1],
                resolution_dict={"ts": TS[str], "other": TS[int]},
            )

    with pytest.raises(
        hg.WiringError,
        match=r"unknown parameter\(s\): 'scale'.*valid time-series parameters: 'tsd'",
    ):
        eval_node(passthrough, [1], resolution_dict={"scale": TS[int]})

    @graph
    def generic_passthrough(tsd: hg.TIME_SERIES_TYPE) -> hg.TIME_SERIES_TYPE:
        return tsd

    specialized = generic_passthrough[hg.TIME_SERIES_TYPE: TS[int]]
    check(
        eval_node(specialized, [1], resolution_dict={"tsd": TS[int]}) == [1],
        "valid subscripted graph resolution",
    )
    with pytest.raises(
        hg.WiringError,
        match=r"unknown parameter\(s\): 'typo'.*valid time-series parameters: 'tsd'",
    ):
        eval_node(specialized, [1], resolution_dict={"typo": TS[int]})


def test_eval_node_resolution_dict_accepts_expanded_variadic_keyword_keys():
    @graph
    def expanded_graph(**bundle: TSB[hg.TS_SCHEMA]) -> TS[int]:
        return bundle["a"]

    @hg.compute_node
    def expanded_node(**bundle: TSB[hg.TS_SCHEMA]) -> TS[int]:
        return bundle["a"].value

    for target in (expanded_graph, expanded_node):
        check(
            eval_node(target, a=[None], resolution_dict={"a": TS[int]}) is None,
            "expanded all-None series resolution",
        )
        with pytest.raises(
            hg.WiringError,
            match=r"unknown parameter\(s\): 'typo'.*valid time-series parameters: 'a'",
        ):
            eval_node(target, a=[None], resolution_dict={"typo": TS[int]})


def test_map_and_reduce_over_tsd():
    @graph
    def doubled(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_("add_", d, d)

    out = eval_node(doubled, [{"a": 1}, {"b": 2}, {"a": 5}])
    check(out == [{"a": 2}, {"b": 4}, {"a": 10}], f"map_: {out}")

    @graph
    def summed(d: TSD[str, TS[int]]) -> TS[int]:
        return hg.reduce("add_", d, 0)

    check(eval_node(summed, [{"a": 1}, {"b": 2}, {"a": 5}]) == [1, 3, 7], "reduce")


def test_tsd_key_removal():
    @graph
    def keys(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_("add_", d, d)

    # None = nothing ticked for the key (ruling 2026-07-28); removals are
    # the explicit sentinels.
    out = eval_node(keys, [{"a": 1, "b": 2}, {"a": hg.REMOVE_IF_EXISTS}])
    check(out == [{"a": 2, "b": 4}, {"a": hg.REMOVE}], f"removal: {out}")
    out = eval_node(keys, [{"a": 1, "b": 2}, {"a": None}])
    check(out == [{"a": 2, "b": 4}, None], f"None no-op: {out}")


def test_tss_deltas():
    # Removals use hgraph's forms (set_delta / Removed markers) — a dict fed
    # to a TSS is NOT a removal directive (upstream parity: it iterates as
    # its keys; see test_tsd_removal_semantics.py).
    @graph
    def sized(s: TSS[int]) -> TS[int]:
        return hg.len_(s)

    out = eval_node(sized, [{1, 2}, {3}, hg.set_delta(removed={1}, tp=int)])
    check(out == [2, 3, 2], f"tss: {out}")


def test_switch_over_named_branches():
    @graph
    def routed(k: TS[str], a: TS[int], b: TS[int]) -> TS[int]:
        return hg.switch_(k, {"plus": "add_", "minus": "sub_"}, a, b)

    out = eval_node(routed, ["plus", None, "minus"], [10, 20, 30], [1, 2, 3])
    check(out == [11, 22, 27], f"switch_: {out}")


def test_feedback_accumulator():
    # The fb() read is consumed PASSIVELY (hgraph's default idiom), so the
    # adder only fires on live ticks and the graph quiesces naturally - no
    # end-time bound needed.
    @graph
    def accum(a: TS[int]) -> TS[int]:
        fb = hg.feedback(TS[int], 0)
        total = a + hg.passive(fb())
        fb(total)
        return total

    out = eval_node(accum, [1, 2, 3])
    check(out == [1, 3, 6], f"feedback: {out}")


def test_feedback_active_consumption_with_explicit_end():
    # ACTIVE consumption re-ticks every cycle; such a graph never quiesces
    # and must be explicitly bounded. We run ONE TICK past the inputs to
    # validate the run-on behaviour: with the inputs exhausted the loop
    # still fires, re-adding the held a=3 to the fed-back total (6 -> 9).
    @graph
    def accum(a: TS[int]) -> TS[int]:
        fb = hg.feedback(TS[int], 0)
        total = a + fb()
        fb(total)
        return total

    out = eval_node(accum, [1, 2, 3], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [1, 3, 6, 9], f"feedback: {out}")


def test_python_graph_fns_in_higher_order_operators():
    # Python @graph callables erase into WiredFn values (the type-erased
    # context+ops backend): map_/switch_ COMPILE them as C++ sub-graphs,
    # reduce builds its combiner tree from them.
    @graph
    def double_plus_one(x: TS[int]) -> TS[int]:
        return x + x + hg.const(1, tp=TS[int])

    @graph
    def mapped(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_(double_plus_one, d)

    out = eval_node(mapped, [{"a": 1}, {"b": 2}, {"a": 5}])
    check(out == [{"a": 3}, {"b": 5}, {"a": 11}], f"map_ python: {out}")

    @graph
    def routed(k: TS[str], x: TS[int]) -> TS[int]:
        return hg.switch_(k, {"dbl": double_plus_one, "neg": "neg_"}, x)

    out = eval_node(routed, ["dbl", None, "neg"], [10, 20, 30])
    check(out == [21, 41, -30], f"switch_ python: {out}")

    @graph
    def summed(d: TSD[str, TS[int]]) -> TS[int]:
        return hg.reduce(lambda a, b: a + b, d, 0)

    check(eval_node(summed, [{"a": 1}, {"b": 2}, {"a": 5}]) == [1, 3, 7], "reduce lambda")


def test_python_user_nodes():
    # @compute_node / @sink_node / @generator: python functions as runtime
    # nodes - graph-thread only, one GIL scope per complete executor phase,
    # values across the boundary.
    @hg.compute_node
    def fizzbuzz(n: TS[int]) -> TS[str]:
        n = n.value
        return "fizzbuzz" if n % 15 == 0 else ("fizz" if n % 3 == 0 else ("buzz" if n % 5 == 0 else str(n)))

    @graph
    def game(n: TS[int]) -> TS[str]:
        return fizzbuzz(n)

    out = eval_node(game, [1, 3, 5, 15])
    check(out == ["1", "fizz", "buzz", "fizzbuzz"], f"compute_node: {out}")

    seen = []

    @hg.sink_node
    def collect(v: TS[str]) -> None:
        seen.append(v.value)

    @graph
    def watched(n: TS[int]) -> TS[str]:
        result = fizzbuzz(n)
        collect(result)
        return result

    eval_node(watched, [3, 5])
    check(seen == ["fizz", "buzz"], f"sink_node: {seen}")


def test_python_generator():
    @hg.generator
    def ticks(count: int) -> TS[int]:
        for i in range(count):
            yield (hg.MIN_ST + i * hg.MIN_TD, i * 10)

    @graph
    def src() -> TS[int]:
        return ticks(3)

    out = run_graph(src)
    check([v for _, v in out] == [0, 10, 20], f"generator: {out}")
    check(out[0][0] == hg.MIN_ST, "generator times")


def test_compute_node_any_arity():
    # One bundle-based operator serves ANY arity (no per-arity stubs).
    @hg.compute_node
    def weighted(price: TS[float], qty: TS[int]) -> TS[float]:
        return price.value * qty.value

    @graph
    def notional(p: TS[float], q: TS[int]) -> TS[float]:
        return weighted(p, q)

    out = eval_node(notional, [2.5, 4.0], [10, 20])
    check(out == [25.0, 80.0], f"two inputs: {out}")

    @hg.compute_node
    def combine(a: TS[int], b: TS[int], c: TS[int], d: TS[int], e: TS[int]) -> TS[int]:
        return a.value + b.value + c.value + d.value + e.value

    @graph
    def wide(a: TS[int], b: TS[int], c: TS[int], d: TS[int], e: TS[int]) -> TS[int]:
        return combine(a, b, c, d, e)

    check(eval_node(wide, [1], [2], [3], [4], [5]) == [15], "five inputs")


def test_two_input_fast_compute_does_not_repoint_retained_views():
    retained = []

    @hg.compute_node
    def add(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        retained.append((lhs, rhs))
        return lhs.value + rhs.value

    check(eval_node(add, [1, 2], [10, 20]) == [11, 22], "two-input fast compute")
    check(len(retained) == 2, "two-input fast compute invocation count")
    for lhs, rhs in retained:
        with pytest.raises(RuntimeError, match="outside its node's evaluation"):
            _ = lhs.value
        with pytest.raises(RuntimeError, match="outside its node's evaluation"):
            _ = rhs.value


def test_user_node_scalars_and_injectables():
    # Wiring-time scalars ride the node identity; STATE/CLOCK/SCHEDULER
    # annotated parameters are injected (not supplied by the caller).
    @hg.compute_node
    def ema(x: TS[float], alpha: float, state: hg.STATE = None, clock: hg.CLOCK = None) -> TS[float]:
        prev = getattr(state, "value", None)
        state.value = x.value if prev is None else alpha * x.value + (1 - alpha) * prev
        check(clock.evaluation_time is not None, "clock injected")
        return state.value

    @graph
    def smooth(x: TS[float]) -> TS[float]:
        return ema(x, 0.5)

    out = eval_node(smooth, [1.0, 2.0, 3.0])
    check(out == [1.0, 1.5, 2.25], f"ema: {out}")


def test_user_node_scheduler():
    @hg.compute_node
    def defer(x: TS[int], state: hg.STATE = None, sched: hg.SCHEDULER = None) -> TS[int]:
        if getattr(state, "pending", None) is not None:
            value, state.pending = state.pending, None
            return value
        state.pending = x.value + 100
        sched.schedule_delta(hg.MIN_TD)
        return x.value

    @graph
    def deferred(x: TS[int]) -> TS[int]:
        return defer(x)

    out = eval_node(deferred, [1])
    check(out == [1, 101], f"scheduler: {out}")


def test_component_record_replay_modes():
    hg.set_record_replay_config(hg.DATA_FRAME)
    M = hg.RecordReplayEnum

    @hg.component
    def calc(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    @graph
    def recording(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.RECORD):
            return calc(a, b)

    out = eval_node(recording, [1, None, 3], [10, 20, None])
    check(out == [11, 21, 23], f"record: {out}")
    for key in ("calc.lhs", "calc.rhs", "calc.__out__"):
        check(hg.frame_store_contains(key), f"missing frame {key}")

    @graph
    def replaying(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.REPLAY):
            return calc(a, b)

    # The recordings win over garbage live inputs.
    out = eval_node(replaying, [100, 100, 100], [100, 100, 100])
    check(out == [11, 21, 23], f"replay: {out}")

    @graph
    def comparing(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.COMPARE):
            return calc(a, b)

    eval_node(comparing, [100, 100, 100], [100, 100, 100])
    check(hg.comparison_summary("calc.__compare__") == (3, 0), "compare clean")

    @graph
    def recovering(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.RECOVER):
            return calc(a, b)

    # Seeded from the recordings at start (1+10), live overrides (100+10).
    out = eval_node(recovering, [None, 100], [None, None])
    check(out == [11, 110], f"recover: {out}")

    hg.set_record_replay_config(hg.IN_MEMORY)


def test_realtime_push_queue():
    # hgraph's @push_queue: the wrapped fn is the START hook, receiving the
    # thread-safe sender callable; it spawns a feeder thread while the main
    # thread blocks in run (GIL released). A python sink collects results.
    import threading
    import time

    collected = []
    threads = []

    @hg.push_queue(TS[int])
    def ticks(sender, values: tuple = (1, 2, 3)):
        def feed():
            time.sleep(0.15)
            for value in values:
                sender(value)
                time.sleep(0.02)

        thread = threading.Thread(target=feed)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def collect(v: TS[int]) -> None:
        collected.append(v.value)

    @graph
    def live() -> None:
        port = ticks()
        collect(port + port)

    end = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(seconds=1)
    run_graph(live, end_time=end, run_mode=hg.EvaluationMode.REAL_TIME)
    for thread in threads:
        thread.join()
    check(collected == [2, 4, 6], f"realtime push: {collected}")


def test_frame_pyarrow_round_trip():
    # Frames cross the boundary as pyarrow.Tables (the Arrow C stream
    # protocol - zero copy): store reads return Tables, and Tables convert
    # back to Frame values.
    import pyarrow as pa

    hg.set_record_replay_config(hg.DATA_FRAME)

    @hg.component
    def snap(x: TS[int]) -> TS[int]:
        return x + x

    @graph
    def recording(x: TS[int]) -> TS[int]:
        with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
            return snap(x)

    eval_node(recording, [1, 2, 3])
    table = hg.frame_store_read("snap.__out__")
    check(isinstance(table, pa.Table), f"expected a pyarrow.Table, got {type(table)}")
    check(table.column("value").to_pylist() == [2, 4, 6], f"values: {table.to_pydict()}")
    check(table.num_columns == 3, "bitemporal columns present")
    hg.set_record_replay_config(hg.IN_MEMORY)


def test_context_publish_and_get():
    # with hg.context("name", port): publish for the wiring scope within;
    # nested graphs consume by name (same-wiring, the design record).
    @graph
    def inner() -> TS[int]:
        check(hg.context.has("rate"), "context visible")
        return hg.context.get("rate") + hg.const(1, tp=TS[int])

    @graph
    def outer(r: TS[int]) -> TS[int]:
        with hg.context("rate", r):
            return inner()

    out = eval_node(outer, [10, 20])
    check(out == [11, 21], f"context: {out}")

    @graph
    def unpublished(r: TS[int]) -> TS[int]:
        check(not hg.context.has("rate"), "context not leaked")
        return r

    eval_node(unpublished, [1])


def test_context_imports_into_compiled_higher_order_graphs():
    @graph
    def add_price(value: TS[int]) -> TS[int]:
        return value + hg.context.get("price")

    @graph
    def subtract_price(value: TS[int]) -> TS[int]:
        return value - hg.context.get("price")

    @graph
    def mapped(values: TSD[str, TS[int]], price: TS[int]) -> TSD[str, TS[int]]:
        with hg.context("price", price):
            return hg.map_(add_price, values)

    assert eval_node(
        mapped,
        [{"a": 1, "b": 2}, None, {"a": 5}],
        [10, 20, None],
    ) == [
        {"a": 11, "b": 12},
        {"a": 21, "b": 22},
        {"a": 25},
    ]

    @graph
    def switched(key: TS[str], value: TS[int], price: TS[int]) -> TS[int]:
        with hg.context("price", price):
            return hg.switch_(key, {"add": add_price, "sub": subtract_price}, value)

    assert eval_node(
        switched,
        ["add", None, None, "sub", None],
        [1, 2, None, None, 3],
        [10, None, 20, None, None],
    ) == [11, 12, 22, -18, -17]


class _TestContext:
    # Transcribed from ext/main hgraph_unit_tests/_wiring/test_context.py.
    __instance__ = None

    def __init__(self, msg="non-default"):
        self.msg = msg

    @classmethod
    def instance(cls):
        if _TestContext.__instance__ is None:
            return _TestContext("default")
        return _TestContext.__instance__

    def __enter__(self):
        _TestContext.__instance__ = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _TestContext.__instance__ = None

    def __eq__(self, other):
        return isinstance(other, _TestContext) and other.msg == self.msg

    def __hash__(self):
        return hash(self.msg)


def test_hgraph_context_compat():
    # The existing hgraph context API: `with port:` publishes; CONTEXT[...]
    # params resolve by type; the context VALUE is entered around eval.
    from hgraph import CONTEXT, REQUIRED

    @hg.compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = None) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with hg.const(_TestContext("Hello"), tp=TS[_TestContext]):
            return use_context(ts)

    out = eval_node(g, [True, None, False])
    check(out == ["Hello True", None, "Hello False"], f"context: {out}")

    @graph
    def g_no_context(ts: TS[bool]) -> TS[str]:
        return use_context(ts)

    out = eval_node(g_no_context, [True])
    check(out == ["default True"], f"no context: {out}")

    @dataclass(frozen=True)
    class ContextStruct(CompoundScalar, _TestContext):
        value: int
        msg: str = "bundle"

    @graph
    def g_subclass_context(ts: TS[bool]) -> TS[str]:
        with hg.const(ContextStruct(1), tp=TS[ContextStruct]):
            return use_context(ts)

    out = eval_node(g_subclass_context, [True, None, False])
    check(out == ["bundle True", None, "bundle False"],
          f"subclass context: {out}")

    @graph
    def g_structural_subclass_context(ts: TS[bool]) -> TS[str]:
        with TSB[ContextStruct].from_ts(value=1, msg="structural"):
            return use_context(ts)

    out = eval_node(g_structural_subclass_context, [True, None, False])
    check(out == ["structural True", None, "structural False"],
          f"structural subclass context: {out}")

    @hg.compute_node
    def needs_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED) -> TS[str]:
        return "x"

    @graph
    def g_required(ts: TS[bool]) -> TS[str]:
        return needs_context(ts)

    try:
        eval_node(g_required, [True])
        check(False, "expected WiringError")
    except hg.WiringError:
        pass


def test_hgraph_context_named():
    from hgraph import CONTEXT, REQUIRED

    @hg.compute_node
    def named(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED["a"]) -> TS[str]:
        return context.value.msg

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with hg.const(_TestContext("Hello_A")) as a:
            with hg.const(_TestContext("Hello_Z")) as z:
                return hg.format_("{} {}", named(ts), named(ts, context="z"))

    out = eval_node(g, [True])
    check(out == ["Hello_A Hello_Z"], f"named contexts: {out}")


def test_arbitrary_object_scalars():
    class Order:
        def __init__(self, qty):
            self.qty = qty

        def __eq__(self, other):
            return isinstance(other, Order) and other.qty == self.qty

        def __hash__(self):
            return hash(self.qty)

    @hg.compute_node
    def total(o: TS[Order]) -> TS[int]:
        return o.value.qty * 2

    @graph
    def g(o: TS[Order]) -> TS[int]:
        return total(o)

    out = eval_node(g, [Order(3), Order(5)])
    check(out == [6, 10], f"object scalars: {out}")


def test_nested_compound_scalar_tsb_values_keep_their_python_types():
    construction_calls = []

    @dataclass(frozen=True)
    class Amount(CompoundScalar):
        value: int

        def __post_init__(self):
            construction_calls.append(self.value)

    class Snapshot(TimeSeriesSchema):
        amount: TSB[Amount]

    observations = []

    @hg.compute_node
    def observe(ts: TSB[Snapshot], _output: hg.TSB_OUT[Snapshot] = None) -> TSB[Snapshot]:
        prior = _output.amount.value if _output.amount.valid else None
        observations.append((ts.value["amount"], prior))
        return ts.delta_value

    out = eval_node(observe, [{"amount": {"value": 1}}, {"amount": {"value": 2}}])
    check(out == [{"amount": {"value": 1}}, {"amount": {"value": 2}}], f"output: {out}")
    check(isinstance(observations[0][0], Amount), f"input value: {observations[0]}")
    check(isinstance(observations[1][1], Amount), f"output value: {observations[1]}")
    check(construction_calls == [], f"value reads called __post_init__: {construction_calls}")


def test_tsd_of_compound_scalar_tsb_accepts_snapshot_objects():
    @dataclass(frozen=True)
    class Price(CompoundScalar):
        qty: float
        unit: str

    @graph
    def selected_qty(prices: TSD[str, TSB[Price]]) -> TS[float]:
        return prices["selected"].qty

    out = eval_node(
        selected_qty,
        [
            {"selected": Price(qty=12.5, unit="USD")},
            {"selected": {"qty": 13.0}},
        ],
    )

    check(out == [12.5, 13.0], f"compound scalar TSD snapshots: {out}")


def test_time_series_view_api():
    # The full view surface: value/delta_value/modified/last_modified_time
    # plus the TSD conveniences (hgraph parity).
    observations = []

    @hg.compute_node
    def observe(d: TSD[str, TS[int]]) -> TS[int]:
        observations.append(
            (dict(d.value), d.delta_value, d.modified, sorted(d.modified_keys()), d.removed_keys())
        )
        return len(d.value)

    @graph
    def g(d: TSD[str, TS[int]]) -> TS[int]:
        return observe(d)

    out = eval_node(g, [{"a": 1, "b": 2}, {"a": hg.REMOVE_IF_EXISTS}])
    check(out == [2, 1], f"sizes: {out}")
    check(observations[0][0] == {"a": 1, "b": 2}, f"value: {observations[0]}")
    check(observations[0][3] == ["a", "b"] and observations[0][4] == [], "first delta keys")
    check(observations[1][0] == {"b": 2}, f"post-removal value: {observations[1]}")
    check(observations[1][4] == ["a"], f"removed key: {observations[1]}")
    check(all(entry[2] for entry in observations), "modified flags")
    check(observations[0][1] == {"a": 1, "b": 2}, f"delta_value: {observations[0][1]}")


def test_time_series_view_lifetime_guard():
    # A view is only usable during its node's evaluation: storing it and
    # touching it later raises rather than dangling.
    stashed = []
    cross_evaluation_errors = []

    @hg.compute_node
    def stash(x: TS[int]) -> TS[int]:
        if stashed:
            try:
                _ = stashed[-1].value
                cross_evaluation_errors.append("view became live again")
            except RuntimeError as exc:
                cross_evaluation_errors.append(str(exc))
        stashed.append(x)
        return x.value

    @graph
    def g(x: TS[int]) -> TS[int]:
        return stash(x)

    eval_node(g, [1, 2])
    check(len(cross_evaluation_errors) == 1, "cross-evaluation lifetime check")
    check("outside its node's evaluation" in cross_evaluation_errors[0],
          f"unexpected cross-evaluation error: {cross_evaluation_errors[0]}")
    for view in stashed:
        try:
            _ = view.value
            check(False, "expected a lifetime error")
        except RuntimeError as e:
            check("outside its node's evaluation" in str(e), f"unexpected error: {e}")


def test_services_from_python():
    # All three flavours: python stubs + python impls + python clients over
    # the erased runtime-identity core (services.rst rulings 2026-07-05).
    @hg.reference_service
    def base_rate() -> TS[int]: ...

    @hg.service_impl(interfaces=base_rate)
    def base_rate_impl() -> TS[int]:
        return hg.const(100, tp=TS[int])

    @graph
    def ref_graph(x: TS[int]) -> TS[int]:
        hg.register_service("main", base_rate_impl)
        return x + hg.passive(base_rate(path="main"))

    check(eval_node(ref_graph, [1, 2]) == [101, 102], "reference service")

    @hg.subscription_service
    def quotes(symbol: TS[str]) -> TS[int]: ...

    @hg.compute_node
    def price_impl(keys: TSS[str]) -> TSD[str, TS[int]]:
        return {k: len(k) * 10 for k in keys.value}

    quotes_impl = hg.service_impl(price_impl, interfaces=quotes)

    @graph
    def sub_graph(sym: TS[str]) -> TS[int]:
        hg.register_service("live", quotes_impl)
        return quotes(sym, path="live")

    # Subscription keys forward NEXT cycle by design (the sanctioned stub).
    out = eval_node(sub_graph, ["fx", None, "rates"], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)
    check(out == [None, 20, None, 50], f"subscription service: {out}")

    @hg.request_reply_service
    def doubler(request: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=doubler)
    def doubler_impl(reqs) -> TSD[int, TS[int]]:
        return hg.map_("add_", reqs, reqs)

    @graph
    def rr_graph(x: TS[int]) -> TS[int]:
        hg.register_service("dbl", doubler_impl)
        return doubler(x, path="dbl")

    # Requests and responses each cross their next-cycle transport boundary.
    out = eval_node(rr_graph, [5, 7], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [None, None, 10, 14], f"request/reply service: {out}")

    # @service_impl validates: wrong signature shape for the flavour...
    try:
        @hg.service_impl(interfaces=base_rate)
        def bad_impl(extra) -> TS[int]:
            return hg.const(1, tp=TS[int])
        check(False, "expected a shape validation error")
    except TypeError:
        pass

    # ...and register_service refuses undecorated implementations.
    @graph
    def unvalidated(x: TS[int]) -> TS[int]:
        hg.register_service("p", lambda: hg.const(1, tp=TS[int]))
        return x

    try:
        eval_node(unvalidated, [1])
        check(False, "expected WiringError")
    except hg.WiringError:
        pass


def test_mesh_from_python():
    # mesh_: per-key instances read each other via mesh_(fn)[key], created on
    # demand and evaluated in dependency order (the C++ ChainFn topology).
    @graph
    def dep(key: TS[int], link: TS[int]) -> TS[int]:
        return key + hg.default(hg.mesh_(dep)[link], hg.const(0, tp=TS[int]))

    @graph
    def chain(links: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.mesh_(dep, links)

    # 3 -> 2 -> 1; instance 1 is created ON DEMAND (no link -> base 0).
    out = eval_node(chain, [{2: 1, 3: 2}], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    check(out == [{2: 3, 3: 6, 1: 1}], f"mesh chain: {out}")

    # A genuine dependency cycle is detected and reported.
    @graph
    def cyclic(links: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.mesh_(dep, links)

    try:
        eval_node(cyclic, [{1: 2, 2: 1}], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
        check(False, "expected a cycle error")
    except RuntimeError as e:
        check("cycle" in str(e), f"unexpected: {e}")

    # A plain named function is NOT wirable - it must be tagged @graph
    # (bare lambdas remain the anonymous convenience).
    def untagged(key: TS[int], link: TS[int]) -> TS[int]:
        return key

    @graph
    def rejected(links: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.mesh_(untagged, links)

    try:
        eval_node(rejected, [{1: 0}], __end_time__=hg.MIN_ST + 2 * hg.MIN_TD)
        check(False, "expected a decoration error")
    except TypeError as e:
        check("@graph" in str(e), f"unexpected: {e}")


def test_mesh_python_reference_surface():
    class Pair(hg.TimeSeriesSchema):
        value: TS[int]
        other: TS[int]

    @graph
    def has_previous(key: TS[int]) -> TS[bool]:
        mesh = hg.mesh_("numbers")
        assert isinstance(mesh, hg.MeshWiringPort)
        assert mesh is not None
        assert mesh
        return hg.contains_(mesh, key - 1)

    @graph
    def contains_previous(keys: hg.TSS[int]) -> TSD[int, TS[bool]]:
        return hg.mesh_(has_previous, __keys__=keys, __name__="numbers")

    assert eval_node(contains_previous, [{1}, {2}, {4}, {3}]) == [
        {1: False}, {2: True}, {4: False}, {3: True, 4: True}
    ]

    @graph
    def has_previous_by_function(key: TS[int]) -> TS[bool]:
        mesh = hg.get_mesh(has_previous_by_function)
        assert isinstance(mesh, hg.MeshWiringPort)
        return hg.contains_(mesh, key - 1)

    @graph
    def contains_previous_by_function(keys: hg.TSS[int]) -> TSD[int, TS[bool]]:
        return hg.mesh_(has_previous_by_function, __keys__=keys)

    assert eval_node(contains_previous_by_function, [{1}, {2}]) == [
        {1: False}, {2: True}
    ]

    @graph
    def value_from_zero(key: TS[int]) -> TS[int]:
        return hg.switch_(key, {
            0: lambda _: hg.const(10),
            hg.DEFAULT: lambda _: hg.mesh_("constant-key")[0],
        }, key)

    @graph
    def lookup_constant_key(keys: hg.TSS[int]) -> TSD[int, TS[int]]:
        return hg.mesh_(value_from_zero, __keys__=keys, __name__="constant-key")

    assert eval_node(lookup_constant_key, [{1}])[0][1] == 10

    @graph
    def pair_from_zero(key: TS[int]) -> hg.TSB[Pair]:
        return hg.switch_(key, {
            0: lambda _: hg.combine[hg.TSB[Pair]](value=10, other=20),
            hg.DEFAULT: lambda _: hg.combine[hg.TSB[Pair]](
                value=hg.mesh_("structured-key")[0].value,
                other=hg.mesh_("structured-key")[0].other,
            ),
        }, key)

    @graph
    def lookup_structured_key(keys: hg.TSS[int]) -> hg.TSD[int, hg.TSB[Pair]]:
        return hg.mesh_(pair_from_zero, __keys__=keys, __name__="structured-key")

    assert eval_node(lookup_structured_key, [{1}]) == [
        {0: {"value": 10, "other": 20}, 1: {"value": 10, "other": 20}}
    ]
    assert hg.get_mesh(has_previous) is None
    assert not hasattr(hg, "mesh_ref")


def test_service_implementations_materialize_only_when_requested():
    compositions = []

    @hg.reference_service
    def lazy_value() -> TS[int]: ...

    @hg.service_impl(interfaces=lazy_value)
    def lazy_value_impl() -> TS[int]:
        compositions.append("built")
        return hg.const(7, tp=TS[int])

    @graph
    def unused() -> TS[int]:
        hg.register_service("lazy", lazy_value_impl)
        return hg.const(1, tp=TS[int])

    assert eval_node(unused) == [1]
    assert compositions == []

    @graph
    def requested() -> TS[int]:
        hg.register_service("lazy", lazy_value_impl)
        return lazy_value(path="lazy")

    assert eval_node(requested) == [7]
    assert compositions == ["built"]


def test_explicit_service_build_reenters_registered_contexts():
    active = []
    observed = []

    class BuildContext:
        def __enter__(self):
            active.append(True)
            return self

        def __exit__(self, *_):
            active.pop()

    @hg.reference_service
    def contextual_value() -> TS[int]: ...

    @hg.service_impl(interfaces=contextual_value)
    def contextual_value_impl() -> TS[int]:
        observed.append(bool(active))
        return hg.const(9, tp=TS[int])

    @graph
    def requested() -> TS[int]:
        hg.register_service("contextual", contextual_value_impl)
        value = contextual_value(path="contextual")
        hg.WiringGraphContext.instance().add_service_build_context(
            BuildContext(), "build_context")
        hg.WiringGraphContext.instance().build_services()
        return value

    assert eval_node(requested) == [9]
    assert observed == [True]


def test_private_service_transport_helpers_are_not_public():
    import hgraph.nodes as nodes

    private_transport = (
        "capture_output_node_to_global_state",
        "capture_output_to_global_state",
        "get_shared_reference_output",
        "mesh_subscribe_node",
        "write_service_replies",
        "write_service_request",
        "write_subscription_key",
    )
    assert all(not hasattr(nodes, name) for name in private_transport)

    public_surface = (
        "reference_service",
        "subscription_service",
        "request_reply_service",
        "adaptor",
        "service_adaptor",
        "mesh_",
        "get_mesh",
    )
    assert all(hasattr(hg, name) for name in public_surface)


def test_adaptor_from_python():
    # A duplex adaptor: the client input reaches the impl via from_graph,
    # the impl publishes via to_graph, the client reads it back same-cycle.
    @hg.adaptor
    def loopback(ts: TS[int]) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=(loopback,))
    def loopback_impl():
        incoming = hg.from_graph(loopback, path="io")
        hg.to_graph(loopback, incoming + incoming, path="io")

    @graph
    def g(x: TS[int]) -> TS[int]:
        hg.register_adaptor("io", loopback_impl)
        return loopback(x, path="io")

    out = eval_node(g, [3, 5], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    check(out == [6, 10], f"adaptor: {out}")

    @hg.adaptor_impl(interfaces=loopback)
    def direct_loopback_impl(ts: TS[int]) -> TS[int]:
        return ts + ts

    with pytest.raises(TypeError, match="automatic adaptor inputs"):
        @hg.adaptor_impl(interfaces=loopback)
        def missing_automatic_input() -> TS[int]: ...

    with pytest.raises(TypeError, match="output does not match"):
        @hg.adaptor_impl(interfaces=loopback)
        def wrong_automatic_output(ts: TS[int]) -> TS[str]: ...

    @graph
    def direct(x: TS[int]) -> TS[int]:
        hg.register_adaptor("direct-io", direct_loopback_impl)
        return loopback(x, path="direct-io")

    out = eval_node(direct, [3, 5], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    check(out == [6, 10], f"direct adaptor implementation: {out}")

    @hg.adaptor
    def source(path: str = "source") -> TS[int]: ...

    @hg.adaptor_impl(interfaces=source)
    def source_impl() -> TS[int]:
        return hg.const(7, tp=TS[int])

    @hg.adaptor
    def sink(ts: TS[int], path: str = "sink"): ...

    captured = []

    @hg.sink_node
    def capture_for_sink(ts: TS[int]):
        captured.append(ts.value)

    @hg.adaptor_impl(interfaces=sink)
    def sink_impl(ts: TS[int]):
        capture_for_sink(ts)

    @graph
    def source_and_sink(x: TS[int]) -> TS[int]:
        hg.register_adaptor("source", source_impl)
        hg.register_adaptor("sink", sink_impl)
        sink(x, path="sink")
        return source(path="source")

    check(eval_node(source_and_sink, [3]) == [7], "automatic source adaptor")
    check(captured == [3], f"automatic sink adaptor: {captured}")

    observed_paths = []

    @hg.adaptor
    def fallback_source(path: str = "fallback") -> TS[str]: ...

    @hg.adaptor_impl(interfaces=fallback_source)
    def fallback_source_impl(path: str = "fallback") -> TS[str]:
        observed_paths.append(path)
        return hg.const(path)

    @graph
    def custom_source() -> TS[str]:
        hg.register_adaptor(None, fallback_source_impl)
        return fallback_source(path="custom-source")

    check(eval_node(custom_source) == ["custom-source"], "default adaptor at custom path")
    check(observed_paths == ["custom-source"], f"adaptor paths: {observed_paths}")

    @hg.adaptor_impl(interfaces=(loopback,))
    def configured_manual(extra: TS[int]):
        incoming = hg.from_graph(loopback, path="configured")
        hg.to_graph(loopback, incoming + extra, path="configured")

    @graph
    def configured(x: TS[int]) -> TS[int]:
        hg.register_adaptor(
            "configured", configured_manual, extra=hg.const(10, tp=TS[int]))
        return loopback(x, path="configured")

    check(eval_node(configured, [3, 5]) == [13, 15], "manual adaptor TS configuration")

    @hg.adaptor
    def left_io(ts: TS[int]) -> TS[int]: ...

    @hg.adaptor
    def right_io(ts: TS[int]) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=(left_io, right_io))
    def paired_impl(extra: TS[int]):
        left_value = hg.from_graph(left_io, path="paired")
        right_value = hg.from_graph(right_io, path="paired")
        hg.to_graph(left_io, right_value + extra, path="paired")
        hg.to_graph(right_io, left_value + extra, path="paired")

    @graph
    def paired(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        hg.register_adaptor("paired", paired_impl, extra=hg.const(1, tp=TS[int]))
        return left_io(lhs, path="paired") + right_io(rhs, path="paired")

    check(eval_node(paired, [1, 2], [10, 20]) == [13, 24], "manual multi-adaptor")

    @hg.adaptor
    def arithmetic_io(lhs: TS[int], rhs: TS[int]) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=arithmetic_io)
    def arithmetic_io_impl(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    @graph
    def arithmetic_graph(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        hg.register_adaptor("arithmetic-io", arithmetic_io_impl)
        return arithmetic_io(lhs, rhs, path="arithmetic-io")

    check(eval_node(arithmetic_graph, [1, 2], [10, 20]) == [11, 22],
          "automatic multi-input adaptor")

    unbound_values = []

    @hg.sink_node
    def capture_unbound(value: TS[int]):
        unbound_values.append(value.value)

    @hg.adaptor_impl(interfaces=())
    def unbound_impl(value: TS[int]):
        capture_unbound(value)

    @graph
    def unbound_graph(value: TS[int]) -> TS[int]:
        hg.register_adaptor("unbound", unbound_impl, value=value)
        return value

    check(eval_node(unbound_graph, [4, 5]) == [4, 5], "unbound manual adaptor")
    check(unbound_values == [4, 5], f"unbound manual adaptor values: {unbound_values}")


def test_service_adaptor_from_python():
    @hg.service_adaptor
    def echo(request: TS[int]) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=echo)
    def echo_impl(requests: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return requests

    @graph
    def two_clients(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        hg.register_adaptor("echo", echo_impl)
        return echo(lhs, path="echo") + echo(rhs, path="echo")

    out = eval_node(two_clients, [1, None, 2], [10, None, 20])
    check(out == [None, 11, None, 22], f"service adaptor clients: {out}")

    @hg.service_adaptor
    def routed(path: str, request: TS[int]) -> TS[int]: ...

    @graph
    def add_offset(value: TS[int], offset: TS[int]) -> TS[int]:
        return value + offset

    @hg.service_adaptor_impl(interfaces=routed)
    def routed_impl(requests: TSD[int, TS[int]], offset: int) -> TSD[int, TS[int]]:
        return hg.map_(add_offset, requests, hg.const(offset, tp=TS[int]))

    @graph
    def separated_paths(value: TS[int]) -> TS[int]:
        hg.register_adaptor("small", routed_impl, offset=1)
        hg.register_adaptor("large", routed_impl, offset=10)
        return routed("small", request=value) + routed(value, path="large")

    out = eval_node(separated_paths, [1, None, 2])
    check(out == [None, 13, None, 15], f"service adaptor paths: {out}")

    @hg.service_adaptor
    def left(request: TS[int]) -> TS[int]: ...

    @hg.service_adaptor
    def right(request: TS[int]) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=(left, right))
    def both_impl(path: str):
        left_requests = hg.impl_input(left, path)
        right_requests = hg.impl_input(right, path)
        hg.impl_output(left, left_requests, path)
        hg.impl_output(right, right_requests, path)

    @graph
    def two_interfaces(value: TS[int]) -> TS[int]:
        hg.register_adaptor("both", both_impl)
        return left(value, path="both") + right(value, path="both")

    out = eval_node(two_interfaces, [3, None, 4])
    check(out == [None, 6, None, 8], f"service adaptor interfaces: {out}")

    class ArithmeticResult(hg.TimeSeriesSchema):
        total: TS[int]
        difference: TS[int]

    @hg.service_adaptor
    def arithmetic(lhs: TS[int], rhs: TS[int]) -> TSB[ArithmeticResult]: ...

    @hg.graph
    def arithmetic_for_client(lhs: TS[int], rhs: TS[int]) -> TSB[ArithmeticResult]:
        return hg.combine[TSB[ArithmeticResult]](
            total=lhs + rhs,
            difference=lhs - rhs,
        )

    @hg.service_adaptor_impl(interfaces=arithmetic)
    def arithmetic_impl(
        lhs: TSD[int, TS[int]],
        rhs: TSD[int, TS[int]],
    ) -> TSD[int, TSB[ArithmeticResult]]:
        return hg.map_(arithmetic_for_client, lhs, rhs)

    @graph
    def arithmetic_client(lhs: TS[int], rhs: TS[int]) -> TSB[ArithmeticResult]:
        hg.register_adaptor("arithmetic", arithmetic_impl)
        return arithmetic(lhs, rhs, path="arithmetic")

    out = eval_node(arithmetic_client, [7], [2])
    check(out == [None, {"total": 9, "difference": 5}], f"multi-field service adaptor: {out}")

    try:
        @hg.service_adaptor_impl(interfaces=echo)
        def invalid_impl(): ...
        check(False, f"expected invalid service adaptor implementation: {invalid_impl}")
    except TypeError as e:
        check("1 time-series" in str(e), f"unexpected implementation error: {e}")

    @graph
    def missing(value: TS[int]) -> TS[int]:
        return echo(value, path="missing")

    try:
        eval_node(missing, [1])
        check(False, "expected missing service-adaptor implementation")
    except (hg.WiringError, ValueError) as e:
        check("missing implementation" in str(e), f"unexpected missing implementation error: {e}")


def test_service_adaptor_explicit_request_id_client_split():
    @hg.service_adaptor
    def echo(request: TS[int]) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=echo)
    def echo_impl(requests: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return requests

    @graph
    def split_client(value: TS[int]) -> TS[int]:
        hg.register_adaptor("echo-split", echo_impl)
        rid = hg.request_id(1)
        echo.from_graph(value, path="echo-split", __request_id__=rid)
        return echo.to_graph(
            path="echo-split", __request_id__=rid, __no_ts_inputs__=True)

    out = eval_node(split_client, [1, None, 2])
    check(out == [None, 1, None, 2], f"split service adaptor client: {out}")


def test_sink_only_service_adaptor_from_python():
    published = []
    key_snapshot = {}
    value_snapshot = {}

    @hg.service_adaptor
    def publish(key: TS[str], value: TS[int]) -> None: ...

    @hg.sink_node
    def capture(
        keys: TSD[int, TS[str]], values: TSD[int, TS[int]]
    ):
        if keys.modified or values.modified:
            key_snapshot.update(
                (request_id, value.value)
                for request_id, value in keys.modified_items())
            value_snapshot.update(
                (request_id, value.value)
                for request_id, value in values.modified_items())
            for request_id in value_snapshot:
                key_snapshot[request_id] = keys[request_id].value
            published.append((dict(key_snapshot), dict(value_snapshot)))

    @hg.service_adaptor_impl(interfaces=publish)
    def publish_impl(
        key: TSD[int, TS[str]], value: TSD[int, TS[int]]
    ) -> None:
        capture(key, value)

    @graph
    def two_publishers(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        hg.register_adaptor("publish", publish_impl)
        publish(hg.const("lhs", tp=TS[str]), lhs, path="publish")
        publish(hg.const("rhs", tp=TS[str]), rhs, path="publish")
        return lhs + rhs

    out = eval_node(two_publishers, [1, 2], [10, 20])
    check(out == [11, 22], f"sink service adaptor passthrough: {out}")
    check(len(published) == 2, f"sink service adaptor cycles: {published}")
    check(all(set(keys.values()) == {"lhs", "rhs"}
              for keys, _ in published),
          f"sink service adaptor static keys: {published}")
    check([sorted(values.values()) for _, values in published]
          == [[1, 10], [2, 20]],
          f"sink service adaptor values: {published}")


def test_adaptor_client_scalar_options_reach_the_registered_implementation():
    observed = []

    @hg.adaptor
    def configured_publish(
        value: TS[int], multiplier: int, label: str = "value",
    ) -> None: ...

    @hg.sink_node
    def capture_configured(value: TS[int], multiplier: int, label: str):
        observed.append((label, value.value * multiplier))

    @hg.adaptor_impl(interfaces=configured_publish)
    def configured_publish_impl(
        value: TS[int], multiplier: int, label: str = "value",
    ) -> None:
        capture_configured(value, multiplier, label)

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_adaptor("configured-publish", configured_publish_impl)
        configured_publish(
            value, multiplier=10, label="scaled",
            path="configured-publish",
        )
        return value

    check(eval_node(app, [2, 3]) == [2, 3], "configured adaptor passthrough")
    check(observed == [("scaled", 20), ("scaled", 30)],
          f"configured adaptor options: {observed}")

    observed.clear()

    @graph
    def default_path(value: TS[int]) -> TS[int]:
        hg.register_adaptor(None, configured_publish_impl)
        configured_publish(value, multiplier=5, label="default")
        return value

    check(eval_node(default_path, [2, 3]) == [2, 3],
          "default-path configured adaptor passthrough")
    check(observed == [("default", 10), ("default", 15)],
          f"default-path configured adaptor options: {observed}")

    @graph
    def conflicting(value: TS[int]) -> TS[int]:
        hg.register_adaptor("conflicting-publish", configured_publish_impl)
        configured_publish(value, multiplier=2, path="conflicting-publish")
        configured_publish(value, multiplier=3, path="conflicting-publish")
        return value

    try:
        eval_node(conflicting, [1])
        check(False, "expected conflicting client options")
    except hg.WiringError as error:
        check("disagree" in str(error), f"unexpected scalar option error: {error}")


def test_adaptor_client_config_follows_cxx_first_wiring_lifetime():
    # C++-first wiring enters Python through a borrowed wrapper without an
    # owning Python Wiring at the bottom of the stack. The scalar config must
    # survive that wrapper and be released with the underlying C++ Wiring.
    import _hgraph
    import gc
    from hgraph._wiring import _wiring_stack
    from hgraph._wiring._services import _ADAPTOR_CLIENT_CONFIGS

    built_config = []

    @hg.service_adaptor
    def configured_service(
        value: TS[int], multiplier: int, label: str = "value",
    ) -> TS[int]: ...

    @hg.service_adaptor_impl(interfaces=configured_service)
    def configured_service_impl(
        value: TSD[int, TS[int]], multiplier: int, label: str = "value",
    ) -> TSD[int, TS[int]]:
        built_config.append((multiplier, label))
        return value

    @hg.operator
    def cxx_first_client(value: TS[int]) -> TS[int]: ...

    @hg.graph(overloads=cxx_first_client)
    def cxx_first_client_impl(value: TS[int]) -> TS[int]:
        return configured_service(
            value, multiplier=6, label="cxx", path="cxx-first-configured")

    wiring = _hgraph.Wiring()
    _wiring_stack.append(wiring)
    try:
        hg.register_adaptor("cxx-first-configured", configured_service_impl)
    finally:
        _wiring_stack.pop()
    source = wiring.wire("nothing", output_type=TS[int].handle)
    client = wiring.wire(cxx_first_client._registry_name, (source,), {})
    wiring_identity = wiring.identity()
    check(isinstance(wiring_identity, int) and wiring_identity > 0,
          f"invalid public Wiring identity: {wiring_identity!r}")
    check(wiring.identity() == wiring_identity,
          "public Wiring identity changed during its lifetime")
    check(any(key[0] == wiring_identity for key in _ADAPTOR_CLIENT_CONFIGS),
          "C++-first adaptor config was not retained")
    wiring.build_services()
    check(built_config == [(6, "cxx")],
          f"C++-first adaptor config: {built_config}")

    del client, source, wiring
    gc.collect()
    check(not any(key[0] == wiring_identity for key in _ADAPTOR_CLIENT_CONFIGS),
          "C++-first adaptor config outlived its Wiring")


def test_generic_adaptor_specializations_from_python():
    from typing import TypeVar

    payload = TypeVar("payload", int, str)

    @hg.adaptor
    def generic_adaptor(value: TS[payload], path: str = "generic_adaptor") -> TS[payload]: ...

    int_adaptor = generic_adaptor[payload:int]

    @hg.adaptor_impl(interfaces=(int_adaptor,))
    def int_adaptor_impl(path: str):
        value = hg.from_graph(int_adaptor, path=path)
        hg.to_graph(int_adaptor, value + 1, path=path)

    @hg.adaptor_impl(interfaces=generic_adaptor)
    def automatic_generic_impl(value: TS[payload]) -> TS[payload]:
        return value

    @graph
    def automatic_generic(value: TS[int]) -> TS[int]:
        hg.register_adaptor("automatic-generic", automatic_generic_impl)
        return generic_adaptor(value, path="automatic-generic")

    check(eval_node(automatic_generic, [1, 2]) == [1, 2],
          "automatic generic adaptor")

    @hg.service_adaptor
    def generic_service_adaptor(
        request: TS[payload], path: str = "generic_service_adaptor"
    ) -> TS[payload]: ...

    int_service_adaptor = generic_service_adaptor[payload:int]

    @hg.service_adaptor_impl(interfaces=generic_service_adaptor)
    def int_service_adaptor_impl(
        requests: TSD[int, TS[payload]],
    ) -> TSD[int, TS[payload]]:
        return requests

    @graph
    def app(value: TS[int]) -> TS[int]:
        hg.register_adaptor("generic", int_adaptor_impl)
        hg.register_adaptor("generic_service", int_service_adaptor_impl)
        adapted = generic_adaptor(value, path="generic")
        return generic_service_adaptor(adapted, path="generic_service")

    out = eval_node(app, [2, None, 4])
    check(out == [None, 3, None, 5], f"generic adaptors: {out}")

    try:
        generic_adaptor[payload:float]
        check(False, "expected generic adaptor constraint error")
    except TypeError as e:
        check("must be one of" in str(e), f"unexpected generic adaptor error: {e}")


def test_multi_interface_service_impl():
    # ONE implementation serving TWO interfaces (register_services +
    # impl_input/impl_output, erased): a reference rate and a request/reply
    # boost that adds the shared rate (broadcast into the map_ child).
    @hg.reference_service
    def rate() -> TS[int]: ...

    @hg.request_reply_service
    def boost(request: TS[int]) -> TS[int]: ...

    @graph
    def add_rate(r: TS[int], rate_ts: TS[int]) -> TS[int]:
        return r + rate_ts

    # hgraph's exact multi-service shape: the registered path is INJECTED,
    # inputs read via get_service_inputs(path, stub).ts, outputs published
    # via set_service_output(path, stub, out).
    @hg.service_impl(interfaces=(rate, boost))
    def combined_impl(path: str):
        the_rate = hg.const(100, tp=TS[int])
        hg.set_service_output(path, rate, the_rate)
        requests = hg.get_service_inputs(path, boost).ts
        hg.set_service_output(path, boost, hg.map_(add_rate, requests, the_rate))

    @graph
    def g(x: TS[int]) -> TS[int]:
        hg.register_service("svc", combined_impl)
        return boost(x, path="svc") + hg.passive(rate(path="svc"))

    out = eval_node(g, [5, 7], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [None, None, 205, 207], f"multi-interface: {out}")

    # The stub-method spellings work too (hgraph parity).
    check(hasattr(boost, "wire_impl_inputs_stub") and hasattr(rate, "wire_impl_out_stub"),
          "stub impl methods")

    # A multi-interface impl must take no wired inputs (path is injected).
    try:
        @hg.service_impl(interfaces=(rate, boost))
        def bad(extra: TS[int]):
            pass
        check(False, "expected a shape error")
    except TypeError:
        pass



def test_opaque_references():
    """Howard's REF ruling: references are opaque values - store/emit/pass
    (ref.value), never dereference (.output); plain ports promote to REF
    at REF-annotated params; non-REF params on REF sources deref."""
    from hgraph import REF, TimeSeriesReference

    @hg.compute_node
    def pick(sel: TS[int], ref: REF[TS[int]], ref2: REF[TS[int]]) -> REF[TS[int]]:
        if sel.value == 0:
            return TimeSeriesReference.make()   # EMPTY: consumers go invalid
        if sel.value == -1:
            return ref2.value
        return ref.value

    @hg.graph
    def app(sel: TS[int], a: TS[int], b: TS[int]) -> TS[int]:
        return pick(sel, a, b)

    out = hg.eval_node(app, [1, None, -1, 0, 1], [10, 11], [20, None, None, 21])
    # cycle1: a ticks through the emitted reference; cycle4: the sampled
    # retarget serves a's current value (11).
    check(out == [10, 11, 20, None, 11], f"opaque refs: {out}")

def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} hgraph-api tests passed")


if __name__ == "__main__":
    main()
