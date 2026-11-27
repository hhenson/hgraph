"""Non-identity reduce ``zero`` semantics (documented upstream deviation).

``reduce`` assumes ``zero`` is the identity of the combiner. When it is not,
released hgraph's node-tree implementation applies ``zero`` a
capacity-history-dependent number of times: freed and padding tree leaves are
re-bound to ``zero``, so the result depends on how large the internal tree
previously grew (e.g. an emptied TSD publishes ``2*zero`` or ``4*zero``
depending on the prior key count). Upstream's own suite only ever exercises
identity zeros, so that behaviour is an implementation artifact, not a
contract.

hg_cpp instead applies ``zero`` deterministically:

- no live values  -> the result is ``zero``
- one live value  -> the result is ``func(value, zero)``
- two or more     -> ``zero`` is not an operand

When ``zero`` is OMITTED the two implementations differ only on empty input:
released hgraph always applies an inferred identity zero (an empty TSD ticks
``0`` for int add), while hg_cpp never infers one — with no live values the
output is INVALID: it never becomes valid if the collection never ticks, and
it is invalidated (not merely left un-ticked) when the collection empties.
For any non-empty input both produce identical results.

With an identity ``zero`` the implementations agree once the same values are
valid. During mapped phantom-slot startup, hg_cpp reduces the currently-valid
subset (which can be empty and therefore publishes the explicit zero), while
released hgraph may wait; that accepted #95 timing deviation is documented in
``nested_graphs.rst``. These tests pin the hg_cpp behaviour for both arities.
See issue #44 and the documented reduce deviations in
``docs/source/developer_guide/parity_matrix.rst``.
"""

import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, graph
from hgraph.test import eval_node


def _map_reduce(increment: int, zero: int):
    @graph
    def map_reduce(values: TSD[str, TS[int]]) -> TS[int]:
        mapped = hg.map_(lambda value: value + increment, values)
        return hg.reduce(lambda lhs, rhs: lhs + rhs, mapped, zero)

    return map_reduce


def test_issue_44_singleton_then_empty():
    # The issue #44 minimized recipe. Released hgraph publishes [-8, -6]
    # (empty == 2*zero, from a capacity-2 tree); hg_cpp publishes zero itself.
    out = eval_node(_map_reduce(increment=-3, zero=-3), [{"a": -2}, {"a": hg.REMOVE}])
    assert out == [-8, -3]


def test_three_live_values_do_not_include_zero():
    # Released hgraph pads the capacity-4 tree with one zero leaf (result 3);
    # hg_cpp folds only the live values.
    out = eval_node(_map_reduce(increment=0, zero=-3), [{"a": 1, "b": 2, "c": 3}])
    assert out == [6]


def test_emptied_result_is_zero_regardless_of_history():
    # Released hgraph publishes 4*zero (-12) here because the tree grew to
    # four leaves; hg_cpp's empty result is always exactly zero.
    out = eval_node(
        _map_reduce(increment=0, zero=-3),
        [
            {"a": 1, "b": 2, "c": 3, "d": 4},
            {"a": hg.REMOVE, "b": hg.REMOVE, "c": hg.REMOVE, "d": hg.REMOVE},
        ],
    )
    assert out == [10, -3]


def test_never_ticking_tsd_publishes_zero():
    # Issue #46 family: a TSD that never becomes valid still publishes zero
    # once. Released hgraph publishes 2*zero (both leaves of its initial
    # capacity-2 tree are bound to zero).
    out = eval_node(_map_reduce(increment=3, zero=-2), [None, None, None])
    assert out == [-2, None, None]


def test_final_list_removals_and_zero_publications_remain_distinct():
    list_type = hg.TSL[TS[int], hg.Size[-1]]

    @graph
    def total(values: list_type, zero: TS[int]) -> TS[int]:
        return hg.reduce(hg.add_, values, zero)

    @graph
    def without_zero(values: list_type) -> TS[int]:
        return hg.reduce(hg.add_, values)

    edits = [{0: 3, 1: 4}, {0: hg.REMOVE, 1: hg.REMOVE}, {0: hg.REMOVE}, None, {0: 8}, None]
    assert eval_node(total, edits, [0, 5, None, 5, None, 5]) == [7, 5, None, 5, 13, 13]
    assert eval_node(total, edits, [0, None, None, 5, None, 5]) == [7, 0, None, 5, 13, 13]
    assert eval_node(without_zero, edits) == [7, None, None, None, 8, None]


@pytest.mark.parametrize(("zero_kind", "zeros", "publications"), [
    ("constant", [None] * 7, [7, 5, None, None, None, 13, None]),
    ("dynamic", [0, None, None, 5, 5, None, 5], [7, 0, None, 5, 5, 13, 13]),
    ("dynamic", [None, None, None, 5, 5, None, None], [7, None, None, 5, 5, 13, None]),
    ("dynamic", [None] * 7, [7, None, None, None, None, None, None]),
    ("omitted", [None] * 7, [7, None, None, None, None, 8, None]),
])
def test_withdrawing_the_list_restores_the_initial_zero(zero_kind, zeros, publications):
    list_type = hg.TSL[TS[int], hg.Size[-1]]
    observations = []

    @hg.compute_node
    def list_owner(step: TS[int], _output: list_type = None) -> list_type:
        if step.value == 0:
            return (3, 4)
        if step.value in (1, 2):
            _output.invalidate()
        if step.value == 5:
            return {0: 8}

    @hg.compute_node
    def cold_owner(step: TS[int]) -> list_type:
        if step.value == 5:
            return {0: 8}

    @hg.compute_node(active=("step",), valid=("step",))
    def inspect(step: TS[int], result: TS[int], cold: TS[int]) -> TS[int]:
        observations.append(tuple((ts.valid, ts.modified, ts.value if ts.valid else None)
                                  for ts in (result, cold)))
        return step.value

    @graph
    def total(step: TS[int], zero: TS[int]) -> TS[int]:
        zero_args = () if zero_kind == "omitted" else (5 if zero_kind == "constant" else zero,)
        warm = hg.reduce(hg.add_, list_owner(step), *zero_args)
        cold = hg.reduce(hg.add_, cold_owner(step), *zero_args)
        return inspect(step, warm, cold)

    eval_node(total, list(range(7)), zeros)
    assert [value if valid and modified else None for (valid, modified, value), _ in observations] == publications
    # At withdrawal the warm result can change to the already-held cold value.
    warm, cold = observations[1]
    assert (warm[0], warm[2]) == (cold[0], cold[2])
    assert all(warm == cold for warm, cold in observations[2:])


def test_identity_zero_matches_upstream():
    # With the combiner's identity, both implementations agree on every tick.
    out = eval_node(
        _map_reduce(increment=0, zero=0),
        [{"a": 1, "b": 2, "c": 3}, {"a": hg.REMOVE, "b": hg.REMOVE, "c": hg.REMOVE}],
    )
    assert out == [6, 0]


def test_explicit_zero_tracks_valid_values_through_forwarded_mesh_slots():
    @graph
    def double(value: TS[int]) -> TS[int]:
        return value * 2

    @graph
    def negate(value: TS[int]) -> TS[int]:
        return value * -1

    @graph
    def switched_child(key: TS[str], selector: TS[str]) -> TS[int]:
        return hg.switch_(
            selector,
            {"double": double, "negate": negate},
            hg.len_(key),
        )

    @graph
    def mesh_reduce(keys: TSS[str], selector: TS[str]) -> TS[int]:
        meshed = hg.mesh_(
            switched_child,
            selector,
            __keys__=keys,
            __key_arg__="key",
        )
        return hg.reduce(lambda lhs, rhs: lhs + rhs, meshed, 0)

    # A live mesh slot whose forwarded switch target is still invalid is not a
    # live reduction value. The explicit zero therefore publishes first; the
    # child joins once selected and removing its key returns to zero.
    out = eval_node(
        mesh_reduce,
        [
            hg.set_delta(added={"k"}, tp=str),
            None,
            None,
            hg.set_delta(removed={"k"}, tp=str),
        ],
        [None, "double", "negate", None],
    )
    assert out == [0, 2, -1, 0]


def test_reduce_mapped_bundle_collection_projection_dereferences_key_set_source():
    class NestedBundle(hg.TimeSeriesSchema):
        values: TSD[str, TS[int]]

    @graph
    def wrap(values: TSD[str, TS[int]]) -> hg.TSB[NestedBundle]:
        return hg.combine[hg.TSB[NestedBundle]](values=values)

    @graph
    def reduce_projected(
        values: TSD[str, TSD[str, TS[int]]],
    ) -> TSD[str, TS[int]]:
        projected = hg.map_(wrap, values).values
        return projected.reduce(
            lambda lhs, rhs: hg.map_(lambda x, y: x + y, lhs, rhs)
        )

    # The projected values are REF[TSD]. Two outer keys instantiate the
    # combiner, whose inner map binds each operand's key-set projection.
    assert eval_node(
        reduce_projected,
        [{"a": {"x": 1}, "b": {"x": 2}}],
        __elide__=True,
    ) == [{"x": 3}]


def _map_reduce_no_zero(increment: int):
    @graph
    def map_reduce(values: TSD[str, TS[int]]) -> TS[int]:
        mapped = hg.map_(lambda value: value + increment, values)
        return hg.reduce(lambda lhs, rhs: lhs + rhs, mapped)

    return map_reduce


def test_omitted_zero_never_ticking_tsd_stays_invalid():
    # Documented deviation (empty input only): released hgraph infers an
    # identity zero and ticks 0 immediately; hg_cpp never infers a zero, so
    # with no live values the output remains invalid and never ticks.
    out = eval_node(_map_reduce_no_zero(increment=3), [None, None, None])
    assert out is None


def test_omitted_zero_emptied_tsd_becomes_invalid():
    # Documented deviation (empty input only): on emptying, released hgraph
    # ticks the inferred zero (0); hg_cpp invalidates the output instead (no
    # tick appears in the trace, and the value is gone, not retained).
    out = eval_node(
        _map_reduce_no_zero(increment=0),
        [{"a": 1}, {"b": 2}, {"a": hg.REMOVE, "b": hg.REMOVE}],
    )
    assert out == [1, 3, None]

    # Pin the invalidation itself, not just the missing tick.
    @hg.compute_node(valid=("trigger",), active=("trigger",))
    def probe(trigger: TS[int], value: TS[int]) -> TS[str]:
        return f"valid={value.valid}" + (
            f" value={value.value}" if value.valid else ""
        )

    @graph
    def probed(values: TSD[str, TS[int]], trigger: TS[int]) -> TS[str]:
        reduced = hg.reduce(lambda lhs, rhs: lhs + rhs, values)
        return probe(trigger, reduced)

    out = eval_node(
        probed,
        [{"a": 1}, {"b": 2}, {"a": hg.REMOVE, "b": hg.REMOVE}, None],
        [1, 2, 3, 4],
    )
    assert out == [
        "valid=True value=1",
        "valid=True value=3",
        "valid=False",
        "valid=False",
    ]


def test_omitted_zero_matches_upstream_while_values_are_live():
    # Outside the empty case the omitted-zero arity matches released hgraph
    # exactly: live values fold with no zero operand.
    out = eval_node(
        _map_reduce_no_zero(increment=0),
        [{"a": 1, "b": 2, "c": 3}, {"a": 5}, {"c": hg.REMOVE}],
    )
    assert out == [6, 10, 7]


@hg.generator
def _ordered_delayed_source() -> hg.TS[int]:
    yield 5 * hg.MIN_TD, 42
    yield 3 * hg.MIN_TD, 43


@hg.graph
def _ordered_delayed_combiner(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return _ordered_delayed_source()


@hg.graph
def _ordered_delayed_reduce(values: hg.TSD[int, hg.TS[int]], zero: hg.TS[int]) -> hg.TS[int]:
    return hg.reduce(_ordered_delayed_combiner, values, zero, is_associative=False)


def test_ordered_reduce_retains_delayed_source_schedules():
    assert eval_node(_ordered_delayed_reduce, [{0: 1}] + [None] * 9, [0] + [None] * 9) == [
        None, None, None, None, None, 42, None, None, 43, None
    ]


def test_ordered_reduce_samples_a_dictionary_first_selected_after_publication():
    book = TSD[int, TS[int]]

    @hg.compute_node
    def decimal_fold(accumulator: TS[int], member: TS[int]) -> TS[int]:
        assert member.modified
        assert member.delta_value == member.value
        return 10 * accumulator.value + member.value

    @graph
    def fold(left: book, right: book, choose_right: TS[bool], zero: TS[int]) -> TS[int]:
        return hg.reduce(decimal_fold, hg.if_then_else(choose_right, right, left), zero, is_associative=False)

    assert eval_node(fold, [{0: 1, 1: 2}], [{0: 10, 1: 20}], [None, False, True], [None, 100]) == [
        None, 10012, 10120,
    ]


def test_ordered_reduce_retarget_keeps_a_deduplicated_output_quiet():
    book = TSD[int, TS[int]]

    @graph
    def combine(accumulator: TS[int], member: TS[int]) -> TS[int]:
        return hg.dedup(10 * accumulator + member)

    @graph
    def fold(left: book, right: book, choose_right: TS[bool], zero: TS[int]) -> TS[int]:
        return hg.reduce(combine, hg.if_then_else(choose_right, right, left), zero, is_associative=False)

    assert eval_node(fold, [{0: 1, 1: 2}], [{0: 1, 1: 2}, None, {1: 3}], [False, True], [100]) == [
        10012, None, 10013,
    ]


@pytest.mark.parametrize("members", [{}, {0: 1}])
def test_ordered_reduce_retarget_samples_each_quiet_field_of_a_passed_through_zero(members):
    class Seed(hg.TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    book = TSD[int, TS[int]]
    seed = hg.TSB[Seed]

    @graph
    def pass_zero(accumulator: seed, member: TS[int]) -> seed:
        return accumulator

    @graph
    def fold(left: book, right: book, choose_right: TS[bool], zero: seed) -> seed:
        return hg.reduce(pass_zero, hg.if_then_else(choose_right, right, left), zero, is_associative=False)

    assert eval_node(fold, [members], [members], [False, True], [{"a": 10, "b": 20}, {"a": 11}]) == [
        {"a": 10, "b": 20}, {"a": 11, "b": 20},
    ]


@pytest.mark.parametrize("members", [{}, {0: 1}])
def test_ordered_reduce_forwards_dictionary_zero_removals(members):
    book = TSD[int, TS[int]]

    @hg.compute_node(valid=("choice",), active=("choice",))
    def select(choice: TS[int], left: hg.REF[book], right: hg.REF[book]) -> hg.REF[book]:
        return (left.value, right.value, hg.TimeSeriesReference.make())[choice.value]

    @graph
    def pass_zero(accumulator: book, member: TS[int]) -> book:
        return accumulator

    @graph
    def fold(values: book, choice: TS[int], left: book, right: book) -> book:
        return hg.reduce(pass_zero, values, select(choice, left, right), is_associative=False, output_type=book)

    assert eval_node(fold, [members], [0, 1, 2, 0], [{1: 10, 2: 20}], [{2: 200, 3: 300}]) == [
        {1: 10, 2: 20}, {1: hg.REMOVE, 2: 200, 3: 300}, {2: hg.REMOVE, 3: hg.REMOVE}, {1: 10, 2: 20},
    ]


def test_ordered_reduce_exposes_scalar_clear_through_a_reference_zero():
    class Seed(hg.TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    book = TSD[int, TS[int]]
    seed = hg.TSB[Seed]

    @hg.compute_node
    def source(step: TS[int], _output: hg.TS_OUT[seed] = None) -> seed:
        if step.value == 0:
            return {"a": 10, "b": 20}
        _output["a" if step.value == 1 else "b"].invalidate()

    @hg.compute_node
    def select(zero: hg.REF[seed]) -> hg.REF[seed]:
        return zero.value

    @graph
    def pass_zero(accumulator: seed, member: TS[int]) -> seed:
        return accumulator

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: TS[int], zero: seed) -> TS[tuple[bool, bool, bool]]:
        child = zero["a" if step.value < 2 else "b"]
        return child.valid, child.modified, child.invalidated

    @graph
    def fold(values: book, step: TS[int]) -> TS[tuple[bool, bool, bool]]:
        zero = hg.reduce(pass_zero, values, select(source(step)), is_associative=False, output_type=seed)
        return observe(step, zero)

    assert eval_node(fold, [{0: 1}], [0, 1, 2]) == [
        (True, True, False), (False, False, True), (False, False, True),
    ]
