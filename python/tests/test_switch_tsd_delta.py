"""Regression tests for nested TSD deltas crossing structural references.

GitHub issue #38 lost a nested TSD update through switch_/feedback when a
sibling TSD removed a key.

Root cause (two layers, both delta-clock monotonicity violations):

- ``TSDataTracking::record_modified`` accepted an OLDER time than the one
  already recorded, rewinding a link/output delta clock mid-cycle. A
  freshly re-homed child terminal replaying its source's historical
  timestamp (plain ``bind`` semantics) regressed the parent chain and
  erased the sampled structural transition installed by the switch's
  branch-flip rebind.
- The TSD/TSS slot-store ``prepare_delta`` rebased the whole delta window
  on ANY differing time, so the same stale replay record wiped the
  added/modified marks of sibling keys created earlier in the same cycle
  (map_'s freshly instantiated output keys), producing an empty or
  partial delta while the value read correctly.

Delta clocks and delta windows are monotonic: an older record joins the
current window; only a newer one rolls it.

GitHub issue #40 exposed a separate structural-lag bug: TSB and fixed TSL
children bypassed kind-specific lag dispatch, so a nested TSD could lose its
key during an ``if_then_else`` feedback rebind.
"""

from frozendict import frozendict

from hgraph import (
    TS, TSB, TSD, TimeSeriesSchema, combine, compute_node, const, dedup,
    default, feedback, graph, if_then_else, lag, map_, no_key, sample, switch_,
)
from hgraph.test import eval_node


@compute_node
def _observe(tsd: TSD[str, TS[float]]) -> TS[str]:
    mods = tuple(sorted((str(k), v.value) for k, v in tsd.modified_items()))
    rems = tuple(sorted(str(k) for k in tsd.removed_keys()))
    return repr((mods, rems))


def test_switch_flip_to_const_branch_publishes_sampled_delta():
    """Baseline: retargeting onto an already-valid const output presents the
    full sampled delta (new keys modified, vanished keys removed)."""

    @graph
    def probe(roll: TS[bool], current: TSD[str, TS[float]]) -> TS[str]:
        out = switch_(
            roll,
            {True: lambda current: const[TSD[str, TS[float]]](frozendict({"next": 20.0})),
             False: lambda current: current},
            current,
        )
        return _observe(out)

    result = eval_node(probe, roll=[False, True],
                       current=[{"old": 10.0, "next": 10.0}, None])
    assert result == [
        "((('next', 10.0), ('old', 10.0)), ())",
        "((('next', 20.0),), ('old',))",
    ]


def test_switch_flip_to_map_branch_publishes_sampled_delta():
    """Issue #38 kernel: the new branch's terminal is a map_-owned TSD that
    only becomes valid after the sampled rebind. The stale replay record
    from the re-homed per-key terminal must not erase the transition."""

    @graph
    def probe(roll: TS[bool], current: TSD[str, TS[float]],
              prices: TSD[str, TS[float]]) -> TS[str]:
        out = switch_(
            roll,
            {True: lambda current, prices: map_(lambda p: p, prices),
             False: lambda current, prices: current},
            current, prices,
        )
        return _observe(out)

    result = eval_node(probe, roll=[False, True],
                       current=[{"old": 10.0, "next": 10.0}, None],
                       prices=[{"next": 20.0}, None])
    assert result == [
        "((('next', 10.0), ('old', 10.0)), ())",
        "((('next', 20.0),), ('old',))",
    ]


def test_switch_flip_multi_multiplexed_map_branch_delta():
    """Two multiplexed TSDs: the union key created from the sampled boundary
    (with a stale source timestamp) must not wipe the sibling key's marks."""

    @graph
    def probe(roll: TS[bool], units: TSD[str, TS[float]],
              prices: TSD[str, TS[float]]) -> TS[str]:
        out = switch_(
            roll,
            {True: lambda units, prices: map_(lambda u, p: p, units, prices),
             False: lambda units, prices: prices},
            units, prices,
        )
        return _observe(out)

    result = eval_node(probe, roll=[False, True],
                       units=[{"next": 1.0}, None],
                       prices=[{"old": 10.0, "next": 10.0}, {"next": 20.0}])
    assert result == [
        "((('next', 10.0), ('old', 10.0)), ())",
        "((('next', 20.0), ('old', 10.0)), ())",
    ]


class _Pos(TimeSeriesSchema):
    units: TSD[str, TS[float]]
    unit_values: TSD[str, TS[float]]


def test_switch_flip_tsb_materialized_branch_delta():
    """TSB-shaped branches route through the owned-output/materialize path
    (capture_delta over map_'s raw dict marks) rather than the forwarding
    transition wrapper — the raw marks themselves must survive the cycle."""

    @graph
    def branch_true(current: TSB[_Pos], prices: TSD[str, TS[float]]) -> TSB[_Pos]:
        units = const[TSD[str, TS[float]]](frozendict({"next": 1.0}))
        return combine[TSB[_Pos]](
            units=units,
            unit_values=map_(lambda unit, price: price, units, prices))

    @graph
    def probe(roll: TS[bool], prices: TSD[str, TS[float]], current: TSB[_Pos]) -> TS[str]:
        out = switch_(roll, {True: branch_true,
                             False: lambda current, prices: current}, current, prices)
        return _observe(out.unit_values)

    result = eval_node(
        probe,
        roll=[False, True],
        prices=[{"old": 10.0, "next": 10.0}, {"next": 20.0}],
        current=[{"units": frozendict({"old": 0.5, "next": 0.5}),
                  "unit_values": frozendict({"old": 10.0, "next": 10.0})}, None],
    )
    assert result == [
        "((('next', 10.0), ('old', 10.0)), ())",
        "((('next', 20.0), ('old', 10.0)), ())",
    ]


class _FeedbackPosition(TimeSeriesSchema):
    units: TSD[str, TS[float]]
    unit_values: TSD[str, TS[float]]


def test_issue_38_nested_tsd_update_through_switch_feedback():
    """The full issue #38 reproducer: the rolled position's new unit value
    must survive the dedup/feedback round-trip after the switch flip."""

    @graph
    def update_position(
        current: TSB[_FeedbackPosition],
        prices: TSD[str, TS[float]],
    ) -> TSB[_FeedbackPosition]:
        units = const[TSD[str, TS[float]]](frozendict({"next": 1.0}))
        return combine[TSB[_FeedbackPosition]](
            units=units,
            unit_values=map_(lambda unit, price: price, units, prices),
        )

    @graph
    def position_feedback_graph(
        roll: TS[bool],
        prices: TSD[str, TS[float]],
        trigger: TS[int],
    ) -> TS[float]:
        position_feedback = feedback(TSB[_FeedbackPosition])
        initial_position = combine[TSB[_FeedbackPosition]](
            units=const[TSD[str, TS[float]]](frozendict({"old": 0.5, "next": 0.5})),
            unit_values=const[TSD[str, TS[float]]](frozendict({"old": 10.0, "next": 10.0})),
        )
        position = dedup(default(lag(position_feedback(), 1, trigger), initial_position))
        output = switch_(
            roll,
            {
                True: update_position,
                False: lambda current, prices: dedup(current),
            },
            position,
            prices,
        )
        position_feedback(dedup(output))
        return sample(trigger, position.unit_values["next"])

    result = eval_node(
        position_feedback_graph,
        roll=[False, True, False, False],
        prices=[
            {"old": 10.0, "next": 10.0},
            {"next": 20.0},
            None,
            None,
        ],
        trigger=[0, 1, 2, 3],
    )
    assert result == [10.0, 10.0, 10.0, 20.0]


class _RebasedFeedbackState(TimeSeriesSchema):
    unit_values: TSD[str, TS[float]]
    target_units: TSD[str, TS[float]]


def test_issue_40_feedback_preserves_no_key_map_after_if_then_else_rebind():
    """Nested TSD lag keeps mapped keys alive across a reference rebind."""

    @graph
    def rebased_feedback_graph(
        target: TSD[str, TS[float]],
        rebase: TS[bool],
        prices: TSD[str, TS[float]],
        trigger: TS[int],
    ) -> TS[float]:
        state_feedback = feedback(TSB[_RebasedFeedbackState])
        initial_state = combine[TSB[_RebasedFeedbackState]](
            unit_values=const[TSD[str, TS[float]]](frozendict()),
            target_units=const[TSD[str, TS[float]]](frozendict()),
        )
        state = default(
            lag(state_feedback(), 1, trigger),
            initial_state,
        )
        target_units = if_then_else(
            rebase,
            target,
            state.target_units,
        )
        unit_values = map_(
            lambda unit, price: price,
            target_units,
            no_key(prices),
        )
        state_feedback(
            combine[TSB[_RebasedFeedbackState]](
                unit_values=unit_values,
                target_units=target_units,
            )
        )
        return sample(
            trigger,
            default(state.unit_values["next"], -1.0),
        )

    result = eval_node(
        rebased_feedback_graph,
        target=[{"next": 1.0}, None, None, None],
        rebase=[True, False, False, False],
        prices=[{"next": 10.5}, None, None, None],
        trigger=[0, 1, 2, 3],
    )

    assert result == [-1.0, -1.0, 10.5, 10.5]
