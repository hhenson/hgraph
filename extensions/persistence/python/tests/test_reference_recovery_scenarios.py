"""Durable internal REF recovery with plain values at component boundaries.

Fixed structural references and their adapters belong to the image. Keyed
interior adapters remain an explicit unsupported case until their proxy
membership and clocks have an equivalent recovery contract.
"""

import hgraph as hg
import hgraph_persistence as persistence
import pytest

from .test_component_recovery_scenarios import CUTS, _run, compare_restarts


@hg.compute_node(active=("pick",), valid=("pick",))
def select_reference(pick: hg.TS[int], lhs: hg.REF[hg.TS[int]],
                     rhs: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
    if pick.value == 0:
        return hg.TimeSeriesReference.make()
    return lhs.value if pick.value == 1 else rhs.value


@hg.compute_node
def copy_scalar(value: hg.TS[int]) -> hg.TS[int]:
    return value.delta_value


@hg.component
def scalar_selection(pick: hg.TS[int], lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return copy_scalar(select_reference(pick, lhs, rhs))


@pytest.mark.parametrize("cuts", CUTS)
def test_selected_reference_retargets_after_every_cut(tmp_path, cuts):
    compare_restarts(
        tmp_path, scalar_selection, (hg.TS[int], hg.TS[int], hg.TS[int]), hg.TS[int],
        ([1, None, None, 2, None, 1, None, None],
         [10, 11, None, None, 12, None, 13, None],
         [20, None, 21, None, 22, None, 23, None]), cuts)


@pytest.mark.parametrize("cuts", CUTS)
def test_empty_and_bound_invalid_reference_survive_every_cut(tmp_path, cuts):
    # A reference may identify lhs while lhs has never produced a value.
    # EMPTY then severs the binding; later lhs changes must not revive it.
    compare_restarts(
        tmp_path, scalar_selection, (hg.TS[int], hg.TS[int], hg.TS[int]), hg.TS[int],
        ([1, None, None, 0, None, 2, 1, None],
         [None, None, 10, None, 11, None, None, 12],
         [20, None, None, None, None, 21, None, None]), cuts)


class Pair(hg.TimeSeriesSchema):
    first: hg.TS[int]
    second: hg.TS[int]


@hg.compute_node(active=("pick",), valid=("pick",))
def select_list_reference(
    pick: hg.TS[int], lhs: hg.REF[hg.TSL[hg.TS[int], hg.Size[2]]],
    rhs: hg.REF[hg.TSL[hg.TS[int], hg.Size[2]]],
) -> hg.REF[hg.TSL[hg.TS[int], hg.Size[2]]]:
    return lhs.value if pick.value == 1 else rhs.value


@hg.compute_node
def copy_list(value: hg.TSL[hg.TS[int], hg.Size[2]]) -> hg.TSL[hg.TS[int], hg.Size[2]]:
    return value.delta_value


@hg.component
def structural_list_selection(pick: hg.TS[int], a: hg.TS[int], b: hg.TS[int],
                              c: hg.TS[int], d: hg.TS[int]) -> hg.TSL[hg.TS[int], hg.Size[2]]:
    lhs = hg.combine[hg.TSL[hg.TS[int], hg.Size[2]]](a, b)
    rhs = hg.combine[hg.TSL[hg.TS[int], hg.Size[2]]](c, d)
    return copy_list(select_list_reference(pick, lhs, rhs))


@hg.compute_node(active=("pick",), valid=("pick",))
def select_bundle_reference(pick: hg.TS[int], lhs: hg.REF[hg.TSB[Pair]],
                            rhs: hg.REF[hg.TSB[Pair]]) -> hg.REF[hg.TSB[Pair]]:
    return lhs.value if pick.value == 1 else rhs.value


@hg.compute_node
def copy_bundle(value: hg.TSB[Pair]) -> hg.TSB[Pair]:
    return value.delta_value


@hg.component
def structural_bundle_selection(pick: hg.TS[int], a: hg.TS[int], b: hg.TS[int],
                                c: hg.TS[int], d: hg.TS[int]) -> hg.TSB[Pair]:
    lhs = hg.combine[hg.TSB[Pair]](first=a, second=b)
    rhs = hg.combine[hg.TSB[Pair]](first=c, second=d)
    return copy_bundle(select_bundle_reference(pick, lhs, rhs))


@pytest.mark.parametrize("cuts", CUTS)
@pytest.mark.parametrize("bundle", [False, True])
def test_non_peered_structural_reference_preserves_child_clocks(tmp_path, cuts, bundle):
    component = structural_bundle_selection if bundle else structural_list_selection
    output = hg.TSB[Pair] if bundle else hg.TSL[hg.TS[int], hg.Size[2]]
    compare_restarts(
        tmp_path, component, (hg.TS[int],) * 5, output,
        ([1, None, None, 2, None, 1, None, None],
         [10, 11, None, None, None, None, 12, None],
         [None, 20, 21, None, None, None, None, 22],
         [30, None, 31, None, 32, None, None, None],
         [40, None, None, None, 41, None, 42, None]), cuts)


class RememberedReference(hg.TimeSeriesSchema):
    selected: hg.REF[hg.TS[int]]


@hg.compute_node(active=("pick", "sample"), valid=("sample",))
def remember_reference(
    pick: hg.TS[int], sample: hg.SIGNAL, lhs: hg.REF[hg.TS[int]], rhs: hg.REF[hg.TS[int]],
    state: hg.RECORDABLE_STATE[RememberedReference] = None,
) -> hg.REF[hg.TS[int]]:
    if pick.modified:
        state.selected.value = lhs.value if pick.value == 1 else rhs.value
    if state.selected.valid:
        return state.selected.value


@hg.component
def remembered_selection(pick: hg.TS[int], sample: hg.SIGNAL,
                         lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return copy_scalar(remember_reference(pick, sample, lhs, rhs))


@pytest.mark.parametrize("cuts", CUTS)
def test_recordable_reference_is_available_to_later_evaluations(tmp_path, cuts):
    compare_restarts(
        tmp_path, remembered_selection, (hg.TS[int], hg.SIGNAL, hg.TS[int], hg.TS[int]), hg.TS[int],
        ([1, None, None, 2, None, None, 1, None],
         [True, None, True, True, None, True, True, None],
         [10, 11, None, None, 12, None, None, 13],
         [20, None, 21, None, 22, None, 23, None]), cuts)


@hg.graph
def select_mapped_reference(pick: hg.TS[int], lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return copy_scalar(select_reference(pick, lhs, rhs))


@hg.component
def mapped_selection(pick: hg.TSD[str, hg.TS[int]], lhs: hg.TSD[str, hg.TS[int]],
                     rhs: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
    return hg.map_(select_mapped_reference, pick, lhs, rhs)


@pytest.mark.parametrize("cuts", CUTS)
def test_references_inside_keyed_children_survive_membership_reuse(tmp_path, cuts):
    schema = hg.TSD[str, hg.TS[int]]
    compare_restarts(
        tmp_path, mapped_selection, (schema,) * 3, schema,
        ([{"a": 1, "b": 2}, None, {"a": 2}, None,
          {"a": hg.REMOVE}, {"a": 1}, {"b": 1}, None],
         [{"a": 10, "b": 20}, {"a": 11}, None, {"b": 21},
          {"a": hg.REMOVE}, {"a": 12}, None, {"a": 13}],
         [{"a": 30, "b": 40}, {"b": 41}, {"a": 31}, None,
          {"a": hg.REMOVE}, {"a": 32}, {"b": 42}, None]), cuts)


@hg.compute_node
def keyed_references(value: hg.REF[hg.TS[int]]) -> hg.TSD[str, hg.REF[hg.TS[int]]]:
    return {"selected": value.value}


@hg.compute_node
def copy_dictionary(value: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
    return value.delta_value


@hg.component
def keyed_interior_adapter(value: hg.TS[int]) -> hg.TSD[str, hg.TS[int]]:
    return copy_dictionary(keyed_references(value))


def test_keyed_interior_reference_adapter_refuses_publication(tmp_path):
    # This graph works normally, but proxy membership recovery is not supported.
    assert _run(keyed_interior_adapter, (hg.TS[int],), hg.TSD[str, hg.TS[int]], ([1, 2],), 0)
    with pytest.raises((RuntimeError, ValueError, hg.WiringError),
                       match="keyed interior REF adapter is unsupported"):
        compare_restarts(tmp_path, keyed_interior_adapter, (hg.TS[int],), hg.TSD[str, hg.TS[int]],
                         ([1, 2],), (1,))
    assert not persistence.ComponentCheckpointStore(tmp_path).contains("cut-1")
