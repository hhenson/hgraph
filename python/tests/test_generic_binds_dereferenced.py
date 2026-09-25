"""Resolving a generic dereferences everything (issue #847).

Design record: ``docs/source/developer_guide/writing_nodes.rst`` ("A generic
time-series parameter binds the DEREFERENCED type"). Owner ruling 2026-09-24:
whenever a generic is resolved, every reference is followed at every depth;
code that depends on a reference expresses it. Before the fix the matcher
stripped only the outer reference, so ``debug_print`` over a ``map_``-shaped
output (``TSD[str, REF[TS[int]]]``) printed each element's reference token.
"""

from hgraph import (
    REF,
    TIME_SERIES_TYPE,
    TS,
    TSD,
    TSL,
    Size,
    compute_node,
    debug_print,
    dereference,
    getattr_,
    graph,
    if_,
    nothing,
)
from hgraph.nodes import tsl_to_tsd
from hgraph.test import eval_node


@compute_node
def _show(ts: TIME_SERIES_TYPE) -> TS[str]:
    return repr(ts.value)


@compute_node
def _is_reference(ts: REF[TS[int]]) -> TS[bool]:
    # The reference token, not the int it targets.
    return ts.value.has_output and not ts.value.is_empty


def test_debug_print_over_reference_elements_prints_the_values(capsys):
    # The issue's reproduction: tsl_to_tsd produces TSD[str, REF[TS[int]]].
    @graph
    def g(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        debug_print("whole", tsl_to_tsd(tsl, keys), print_delta=False)

    eval_node(g, [(1, 2, 3), {1: 30}], ("a", "b", "c"))
    printed = [line for line in capsys.readouterr().out.splitlines() if "whole:" in line]
    assert len(printed) == 2
    assert printed[0].endswith("whole: {'a': 1, 'b': 2, 'c': 3}")
    assert printed[1].endswith("whole: {'a': 1, 'b': 30, 'c': 3}")


def test_a_generic_node_parameter_observes_the_values():
    @graph
    def g(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]) -> TS[str]:
        return _show(tsl_to_tsd(tsl, keys))

    assert eval_node(g, [(1, 2, 3), {1: 30}], ("a", "b", "c")) == [
        "{'a': 1, 'b': 2, 'c': 3}",
        "{'a': 1, 'b': 30, 'c': 3}",
    ]


def test_a_node_that_declares_a_reference_receives_the_reference():
    # The Python node wrapper states its declared inputs, so the REF survives
    # the native generic it is wired through.
    @graph
    def g(value: TS[int]) -> TS[bool]:
        return _is_reference(value)

    assert eval_node(g, [1]) == [True]


def test_a_requested_output_keeps_a_nested_reference():
    @graph
    def g():
        out = nothing[TSD[str, REF[TS[int]]]]()
        assert out.output_type == TSD[str, REF[TS[int]]]

    eval_node(g)


def test_a_bundle_projection_keeps_a_reference_field():
    # tsb["x"], tsb.x and getattr_ are structural: they resolve no generic, so
    # a field declared as a reference stays one, and a consumer still reads
    # the value through it.
    @graph
    def g(condition: TS[bool], value: TS[int]) -> TS[int]:
        fields = dereference(if_(condition, value))
        assert fields["true"].output_type == REF[TS[int]]
        assert fields.true.output_type == REF[TS[int]]
        assert getattr_(fields, "true").output_type == REF[TS[int]]
        return fields["true"]

    assert eval_node(g, [True], [1]) == [1]
