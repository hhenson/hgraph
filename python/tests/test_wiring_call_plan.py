"""The Python node decorator's per-definition wiring plan.

The plan caches what the signature fixes; the facts it borrows from the
registry (parameter roles) must follow the registry, not the process.
"""

import _hgraph
import hgraph as hg
from hgraph import TS, compute_node, eval_node, operator


def test_registration_generation_moves_when_an_overload_is_registered():
    before = _hgraph._operator_registration_generation()

    @operator
    def _call_plan_probe(x: TS[int]) -> TS[int]: ...

    @compute_node(overloads=_call_plan_probe)
    def _call_plan_probe_int(x: TS[int]) -> TS[int]:
        return x.value + 1

    assert _hgraph._operator_registration_generation() > before
    assert eval_node(_call_plan_probe, [1, 2]) == [2, 3]


def test_optional_time_series_input_with_none_default_is_a_scalar_when_unwired():
    # An unwired optional input lands in the scalar slot of the layout and a
    # wired one in an input slot; both shapes of the same definition must
    # wire correctly in one process (the layout cache is per layout string).
    @compute_node(valid=("x",))
    def _add_or_pass(x: TS[int], y: TS[int] = None) -> TS[int]:
        return x.value + (y.value if y is not None and y.valid else 0)

    assert eval_node(_add_or_pass, [1, 2]) == [1, 2]
    assert eval_node(_add_or_pass, [1, 2], [10, None]) == [11, 12]


def test_signed_zero_scalar_arguments_keep_their_sign_in_either_wiring_order():
    from math import copysign, inf

    @compute_node
    def scalar_sign(value: TS[float], divisor: float) -> TS[float]:
        return copysign(inf, divisor)

    @compute_node
    def tuple_sign(value: TS[float], divisors: tuple[float, float]) -> TS[float]:
        return copysign(inf, divisors[0])

    @compute_node
    def observe(a: TS[float], b: TS[float], c: TS[float], d: TS[float],
                e: TS[float], f: TS[float], equal: TS[bool]) -> TS[tuple[float, ...]]:
        assert equal.value
        return (copysign(1., a.value), copysign(1., b.value), c.value, d.value, e.value, f.value)

    @hg.graph
    def configured(value: TS[float], reverse: bool) -> TS[tuple[float, ...]]:
        first = -0.0 if reverse else 0.0
        second = -first
        a, b = hg.const(first), hg.const(second)
        c, d = scalar_sign(value, first), scalar_sign(value, second)
        e, f = tuple_sign(value, (first, second)), tuple_sign(value, (second, first))
        return observe(a, b, c, d, e, f, hg.eq_(a, b))

    for reverse in (False, True):
        sign = -1.0 if reverse else 1.0
        tick = (sign, -sign, sign * inf, -sign * inf, sign * inf, -sign * inf)
        assert eval_node(configured, [1.0, None, 1.0], reverse=reverse) == [tick, None, tick]


def test_computed_python_fields_with_distinct_private_state_evaluate_independently():
    getter_reads = []
    evaluations = []

    @hg.register_python_object_type(fields={"divisor": float})
    class Rate:
        def __init__(self):
            self.private_cache = object()

        def __eq__(self, other):
            return type(other) is Rate

        def __hash__(self):
            return 0

        @property
        def divisor(self):
            getter_reads.append(2.5)
            return 2.5

    @compute_node
    def read(tick: TS[int], rate: Rate) -> TS[float]:
        evaluations.append(tick.value)
        return rate.divisor

    @compute_node
    def pair(a: TS[float], b: TS[float]) -> TS[tuple[float, float]]:
        return a.value, b.value

    @hg.graph
    def graph(tick: TS[int]) -> TS[tuple[float, float]]:
        return pair(read(tick, Rate()), read(tick, Rate()))

    assert eval_node(graph, [1, None, 3]) == [(2.5, 2.5), None, (2.5, 2.5)]
    assert evaluations == [1, 1, 3, 3]
    assert getter_reads == [2.5] * 4
