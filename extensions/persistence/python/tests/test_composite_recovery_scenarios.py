"""Composite child plans and whole-target references share the restart contract."""

from collections.abc import Mapping

import hgraph as hg
import pytest

from .test_component_recovery_scenarios import SHAPES, compare_restarts


COMPOSITES = [item for item in SHAPES if item[0] in (
    "signal", "set", "fixed-list", "dynamic-list", "dict", "bundle",
    "dict-bundle", "dict-set", "mixed-bundle")]
# Invalid input, live partial aggregate, retired child, and repeated restoration.
CUTS = [(1,), (3,), (5,), tuple(range(1, 8))]


@pytest.mark.parametrize("name,shape,base", COMPOSITES, ids=[item[0] for item in COMPOSITES])
@pytest.mark.parametrize("mesh", [False, True], ids=["map", "mesh"])
@pytest.mark.parametrize("cuts", CUTS)
def test_composite_children_restore_deltas_and_membership(tmp_path, name, shape, base, mesh, cuts):
    schema = hg.TSD[str, shape]

    @hg.compute_node
    def copy_delta(value: shape) -> shape:
        return value.delta_value

    @hg.component
    def scenario(values: schema) -> schema:
        return hg.mesh_(copy_delta, values) if mesh else hg.map_(copy_delta, values)

    events = [None, {"a": base[1]}, {"a": base[2]}, None, {"a": hg.REMOVE},
              {"a": base[1], "b": base[1]}, {"a": base[2], "b": base[2]}, None]
    compare_restarts(tmp_path, scenario, (schema,), schema, (events,), cuts)


def _stable_value(value):
    """Compare public values without depending on mapping/set iteration order."""
    if isinstance(value, Mapping):
        return tuple(sorted((repr(key), _stable_value(child)) for key, child in value.items()))
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(repr(child) for child in value))
    if isinstance(value, (list, tuple)):
        return tuple(_stable_value(child) for child in value)
    return value


def _select_reference(schema):
    reference = hg.REF[schema]

    @hg.compute_node(active=("pick",), valid=("pick",))
    def select(pick: hg.TS[int], left: reference, right: reference) -> reference:
        if pick.value == 0:
            return hg.TimeSeriesReference.make()
        return left.value if pick.value == 1 else right.value

    return select


@pytest.mark.parametrize("name,schema,events", COMPOSITES, ids=[item[0] for item in COMPOSITES])
@pytest.mark.parametrize("cuts", CUTS)
def test_whole_composite_reference_restores_value_validity_and_clocks(tmp_path, name, schema, events, cuts):
    select = _select_reference(schema)

    @hg.compute_node(active=("sample",), valid=("sample",))
    def observe(sample: hg.SIGNAL, value: schema) -> hg.TS[str]:
        # A sampled snapshot also sees a never-valid or explicitly empty REF;
        # copying a TSD delta into a separate dictionary would change semantics
        # when switching between dictionaries with different memberships.
        return repr((value.valid, value.all_valid, value.modified, value.last_modified_time,
                     _stable_value(value.value) if value.valid else "INVALID",
                     _stable_value(value.delta_value) if value.modified and value.valid else None))

    @hg.component
    def scenario(pick: hg.TS[int], left: schema, right: schema, sample: hg.SIGNAL) -> hg.TS[str]:
        return observe(sample, select(pick, left, right))

    compare_restarts(tmp_path, scenario, (hg.TS[int], schema, schema, hg.SIGNAL), hg.TS[str],
                     ([1, None, None, 2, None, 0, 1, None], events,
                      [None] + events[:-1], [True] * 8), cuts)


@pytest.mark.parametrize("duration", [False, True], ids=["count", "duration"])
@pytest.mark.parametrize("cuts", CUTS)
def test_whole_window_reference_restores_warmup_reset_and_retargeting(tmp_path, duration, cuts):
    period = hg.MIN_TD * 3 if duration else 3
    minimum = hg.MIN_TD if duration else 2
    schema = hg.TSW[int, hg.WindowSize[period], hg.WindowSize[minimum]]
    select = _select_reference(schema)

    @hg.compute_node
    def snapshot(window: schema) -> hg.TS[tuple[int, ...]]:
        return tuple(window.value)

    @hg.component
    def scenario(pick: hg.TS[int], lhs: hg.TS[int], rhs: hg.TS[int], reset: hg.SIGNAL) -> hg.TS[tuple[int, ...]]:
        left = hg.to_window(lhs, period, minimum, reset=reset)
        right = hg.to_window(rhs, period, minimum)
        return snapshot(select(pick, left, right))

    compare_restarts(tmp_path, scenario, (hg.TS[int], hg.TS[int], hg.TS[int], hg.SIGNAL),
                     hg.TS[tuple[int, ...]],
                     ([1, None, 2, None, 1, 0, 2, None],
                      [None, 1, 2, 3, None, 4, 5, 6], [10, 20, None, 30, 40, 50, 60, 70],
                      [None, None, None, True, None, None, None, None]), cuts)
