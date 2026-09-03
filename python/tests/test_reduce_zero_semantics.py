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
