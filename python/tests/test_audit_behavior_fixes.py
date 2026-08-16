"""Behavior fixes from the 2026-08-15 std-operator audit (phase 3).

Each test captures the intended semantic that was violated; the fixes land
in the same change. Parity references: release/0.5.
"""

from typing import Tuple

from hgraph import TS, TSD, eq_, eval_node, graph, mean, min_, sum_


@graph
def _eq_tsd(l: TSD[str, TS[int]], r: TSD[str, TS[int]]) -> TS[bool]:
    return eq_(l, r)


def test_eq_tsd_stays_silent_until_a_source_ticks():
    """Two never-ticked dicts must not compare: upstream eq_tsds is a plain
    compute node gated on input validity, so it emits nothing. The C++
    overload's schedule_on_start + Unchecked shape emitted True at start."""
    assert eval_node(_eq_tsd, [None, None], [None, None]) is None


def test_eq_tsd_compares_once_bound():
    assert eval_node(_eq_tsd, [{"a": 1}], [{"a": 1}]) == [True]
    assert eval_node(_eq_tsd, [{"a": 1}], [{"a": 2}]) == [False]


@graph
def _sum_default(ts: TS[Tuple[int, ...]]) -> TS[int]:
    return sum_(ts, 42)


@graph
def _mean_default(ts: TS[Tuple[float, ...]]) -> TS[float]:
    return mean(ts, 42.0)


@graph
def _min_default(ts: TS[Tuple[int, ...]]) -> TS[int]:
    return min_(ts, 42)


def test_container_sum_honours_default_when_empty():
    """The default-bearing sum_/mean overloads accepted default_value to win
    overload resolution and then discarded it — an empty tuple emitted 0/NaN
    regardless. The extremum siblings (min_/max_) honour theirs; sum_/mean
    now match that behaviour (upstream 0.5 has no default-bearing sum_ call
    shape at all, so this is the native extension's own contract)."""
    assert eval_node(_sum_default, [tuple()]) == [42]
    assert eval_node(_min_default, [tuple()]) == [42]


def test_container_mean_honours_default_when_empty():
    assert eval_node(_mean_default, [tuple()]) == [42.0]


def test_container_sum_with_default_still_sums_when_non_empty():
    assert eval_node(_sum_default, [(1, 2, 3)]) == [6]
    assert eval_node(_mean_default, [(2.0, 4.0)]) == [3.0]


def test_passive_read_follows_reference_retarget():
    """Pin for the passive-trust work (access-path ledger, deferred item 1):
    a PASSIVE input reading through an if_-routed reference must follow
    every retarget — whichever resolution path (full walk today, trusted
    handle after the trie maintains passive nodes) serves the read."""
    from hgraph import TS, graph, if_, sample, eval_node

    @graph
    def g(cond: TS[bool], value: TS[int], sig: TS[bool]) -> TS[int]:
        routed = if_(cond, value)
        return sample(sig, routed.true)

    # cond flips the .true branch's binding between samples; the sampled
    # (passive) read must reflect the CURRENT binding each time.
    assert eval_node(
        g,
        [True, None, False, None, True, None],
        [1, 2, 3, 4, 5, 6],
        [None, True, None, True, None, True],
    ) == [None, 2, None, None, None, 6]
