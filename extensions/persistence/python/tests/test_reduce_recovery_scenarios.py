"""Durable reduction restarts retain live combiner history and topology."""

import hgraph as hg
import pytest

from .test_component_recovery_scenarios import CUTS, compare_restarts


class CombinerState(hg.TimeSeriesSchema):
    calls: hg.TS[int]


@hg.compute_node
def historical_combine_node(
    lhs: hg.TS[int], rhs: hg.TS[int],
    state: hg.RECORDABLE_STATE[CombinerState] = None,
) -> hg.TS[int]:
    state.calls.value = (state.calls.value if state.calls.valid else 0) + 1
    # Both operand order and the number of real evaluations affect the output.
    return lhs.value * 10 + rhs.value + state.calls.value


@hg.graph
def historical_combine(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return historical_combine_node(lhs, rhs)


ASSOCIATIVE_SHAPES = [
    ("dict-str", hg.TSD[str, hg.TS[int]],
     [None, {"a": 1, "b": 2, "c": 3}, {"b": 7}, {"a": hg.REMOVE},
      {"d": 4, "e": 5}, {"b": hg.REMOVE, "c": hg.REMOVE, "d": hg.REMOVE, "e": hg.REMOVE},
      {"a": 8, "e": 9}, None]),
    ("dict-int", hg.TSD[int, hg.TS[int]],
     [None, {10: 1, -5: 2, 40: 3}, {-5: 7}, {10: hg.REMOVE},
      {80: 4, 90: 5}, {-5: hg.REMOVE, 40: hg.REMOVE, 80: hg.REMOVE, 90: hg.REMOVE},
      {10: 8, 90: 9}, None]),
    ("fixed-list", hg.TSL[hg.TS[int], hg.Size[4]],
     [None, {0: 1, 1: 2}, {2: 3}, {0: 7}, {3: 4}, None, {1: 8, 3: 9}, None]),
    ("dynamic-list", hg.TSL[hg.TS[int], hg.Size[-1]],
     [None, {0: 1, 1: 2, 2: 3}, {1: 7}, {1: hg.REMOVE, 2: hg.REMOVE},
      {1: 4, 2: 5, 3: 6}, {0: hg.REMOVE, 1: hg.REMOVE, 2: hg.REMOVE, 3: hg.REMOVE},
      {0: 8, 1: 9}, None]),
]


@pytest.mark.parametrize("name,schema,events", ASSOCIATIVE_SHAPES,
                         ids=[shape[0] for shape in ASSOCIATIVE_SHAPES])
@pytest.mark.parametrize("stateful", [False, True])
@pytest.mark.parametrize("cuts", CUTS)
def test_reduce_combiner_tree_restart_at_every_boundary(tmp_path, name, schema, events, stateful, cuts):
    function = historical_combine if stateful else hg.add_

    @hg.component
    def scenario(values: schema, zero: hg.TS[int]) -> hg.TS[int]:
        return hg.reduce(function, values, 0 if name == "fixed-list" else zero)

    zeros = [0, None, None, None, None, 5, None, None]
    compare_restarts(tmp_path, scenario, (schema, hg.TS[int]), hg.TS[int], (events, zeros), cuts)


ORDERED_SHAPES = [
    ("dict", hg.TSD[int, hg.TS[int]],
     [None, {0: 1, 1: 2, 2: 3}, {1: 7}, {2: hg.REMOVE}, {2: 4, 3: 5},
      {0: hg.REMOVE, 1: hg.REMOVE, 2: hg.REMOVE, 3: hg.REMOVE}, {0: 8, 1: 9}, None]),
    ("dynamic-list", hg.TSL[hg.TS[int], hg.Size[-1]],
     [None, {0: 1, 1: 2, 2: 3}, {1: 7}, {2: hg.REMOVE}, {2: 4, 3: 5},
      {0: hg.REMOVE, 1: hg.REMOVE, 2: hg.REMOVE, 3: hg.REMOVE}, {0: 8, 1: 9}, None]),
]


@pytest.mark.parametrize("name,schema,events", ORDERED_SHAPES,
                         ids=[shape[0] for shape in ORDERED_SHAPES])
@pytest.mark.parametrize("stateful", [False, True])
@pytest.mark.parametrize("cuts", CUTS)
def test_ordered_reduce_chain_restart_at_every_boundary(tmp_path, name, schema, events, stateful, cuts):
    function = historical_combine if stateful else hg.sub_

    @hg.component
    def scenario(values: schema, zero: hg.TS[int]) -> hg.TS[int]:
        return hg.reduce(function, values, zero, is_associative=False)

    zeros = [10, None, None, None, None, 5, None, None]
    compare_restarts(tmp_path, scenario, (schema, hg.TS[int]), hg.TS[int], (events, zeros), cuts)


@pytest.mark.parametrize("cuts", CUTS)
def test_no_zero_reduce_empty_singleton_and_recreated_combiner_restart(tmp_path, cuts):
    schema = hg.TSD[str, hg.TS[int]]
    events = [{}, None, {"a": 1}, {"b": 2}, {"a": hg.REMOVE},
              {"b": hg.REMOVE}, {"c": 3, "d": 4}, None]

    @hg.component
    def scenario(values: schema) -> hg.TS[int]:
        return hg.reduce(historical_combine, values)

    compare_restarts(tmp_path, scenario, (schema,), hg.TS[int], (events,), cuts)


@pytest.mark.parametrize("stateful", [False, True])
@pytest.mark.parametrize("cuts", CUTS)
def test_ordered_fixed_list_reference_lowering_resumes(tmp_path, stateful, cuts):
    schema = hg.TSL[hg.TS[int], hg.Size[4]]
    function = historical_combine if stateful else hg.sub_

    @hg.component
    def scenario(values: schema, zero: hg.TS[int]) -> hg.TS[int]:
        return hg.reduce(function, values, zero, is_associative=False)

    events = [None, {0: 1, 1: 2}, {2: 3}, None, {0: 7, 3: 4}, {1: 8}, {3: 9}, None]
    zeros = [10, None, None, None, None, 5, None, None]
    compare_restarts(tmp_path, scenario, (schema, hg.TS[int]), hg.TS[int], (events, zeros), cuts)


@hg.compute_node
def _own_output(ts: hg.TS[int]) -> hg.TS[int]:
    return ts.value


# KNOWN DEFECT, found by the recovery campaign (tools/recovery, family
# ``tsd-restored-slot-order``). strict: the day this passes, the xfail comes off and the
# campaign's known-defect family is deleted with it.
@pytest.mark.xfail(strict=True, reason=(
    "A restored keyed input iterates its keys in a different order from the unbroken run when "
    "a slot freed before the cut is still free at it and another removal follows: "
    "(0, 3) unbroken, (3, 0) restored. An order-sensitive reduction shows it as 76 vs 40. "
    "Python-value keyed storage only; the same stream passes from C++ "
    "(tests/cpp/test_reduce_checkpoint.cpp, 'reduce checkpoint keeps leaf order when one "
    "cycle both removes and adds keys')."))
def test_a_restored_input_keeps_its_key_order_with_a_removal_on_each_side_of_the_cut(tmp_path):
    # The minimal stream, reduced from eight nightly failures that shared this shape and
    # nothing else -- one of them with no dmap_ or spawn_ in it at all.
    schema = hg.TSD[int, hg.TS[int]]
    events = [{2: 2}, {2: hg.REMOVE, 1: 4}, {0: 7, 1: hg.REMOVE, 3: 3}]

    @hg.component
    def scenario(ts: schema) -> hg.TS[int]:
        return _own_output(hg.reduce(historical_combine, ts, 0))

    compare_restarts(tmp_path, scenario, (schema,), hg.TS[int], (events,), (2,))


def test_the_same_stream_with_one_removal_restarts_invisibly(tmp_path):
    # The control for the pin above: take away either removal and the restart is invisible,
    # so what fails there is the pair of removals, not the reduction or the cut.
    schema = hg.TSD[int, hg.TS[int]]

    @hg.component
    def scenario(ts: schema) -> hg.TS[int]:
        return _own_output(hg.reduce(historical_combine, ts, 0))

    for events in ([{2: 2}, {2: hg.REMOVE, 1: 4}, {0: 7, 3: 3}],
                   [{2: 2}, {1: 4}, {0: 7, 1: hg.REMOVE, 3: 3}]):
        compare_restarts(tmp_path / str(len(list(tmp_path.iterdir()))), scenario, (schema,), hg.TS[int], (events,), (2,))
