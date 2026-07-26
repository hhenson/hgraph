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
output simply does not tick (stays invalid, or keeps its last value once it
has ticked). For any non-empty input both produce identical results.

With an identity ``zero`` the two implementations agree everywhere. These
tests pin the hg_cpp behaviour for both arities. See issue #44 and the
documented reduce deviation in ``docs/source/developer_guide/parity_matrix.rst``.
"""

import hgraph as hg
from hgraph import TS, TSD, graph
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


def test_omitted_zero_emptied_tsd_does_not_tick():
    # Documented deviation (empty input only): on emptying, released hgraph
    # ticks the inferred zero (0); hg_cpp emits no tick and the output keeps
    # its last value.
    out = eval_node(
        _map_reduce_no_zero(increment=0),
        [{"a": 1}, {"b": 2}, {"a": hg.REMOVE, "b": hg.REMOVE}],
    )
    assert out == [1, 3, None]


def test_omitted_zero_matches_upstream_while_values_are_live():
    # Outside the empty case the omitted-zero arity matches released hgraph
    # exactly: live values fold with no zero operand.
    out = eval_node(
        _map_reduce_no_zero(increment=0),
        [{"a": 1, "b": 2, "c": 3}, {"a": 5}, {"c": hg.REMOVE}],
    )
    assert out == [6, 10, 7]
