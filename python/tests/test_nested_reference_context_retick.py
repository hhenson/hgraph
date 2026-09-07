"""A nested branch must not re-tick because its owner evaluated.

A nested-graph boundary (``switch_``, ``map_``) re-binds its child inputs on
every evaluation so an upstream reference that re-points is absorbed. The
"is this already bound?" test compared the offered output against what the
child link *observes*, but a ``REF`` source is transparent at an input
boundary, so the link observes the referenced output and the comparison never
matched. The boundary therefore re-bound a reference-sourced input every
cycle, and every re-bind notifies the consumer, so a branch reading such an
input emitted whenever the switch key ticked - even when the key was
unchanged and nothing upstream had moved (issues #769-#779).

Released hgraph 0.5 rebuilds a branch only when the key *value* changes, and
notifies the branch's boundary only then, so these graphs must not tick.
"""

import hgraph as hg
from hgraph.test import eval_node


@hg.compute_node
def _publish_ref(ts: hg.REF[hg.TIME_SERIES_TYPE]) -> hg.TSD[str, hg.REF[hg.TIME_SERIES_TYPE]]:
    """Republish ``ts`` under one key, the shape a TSD item lookup has."""
    return {"k": ts.value}


def _via_reference(value):
    """Route ``value`` through a REF-producing source, as the parity corpus does."""
    return _publish_ref(value)[hg.const("k")]


@hg.graph
def _add_offset(value: hg.TS[int]) -> hg.TS[int]:
    return value + hg.get_context("offset", hg.TS[int])


@hg.graph
def _subtract_offset(value: hg.TS[int]) -> hg.TS[int]:
    return value - hg.get_context("offset", hg.TS[int])


@hg.graph
def _context_switch(selector: hg.TS[str], value: hg.TS[int], offset: hg.TS[int]) -> hg.TS[int]:
    offset = _via_reference(offset)
    with offset:
        return hg.switch_(selector, {"add": _add_offset, "subtract": _subtract_offset}, value)


def test_unchanged_switch_key_does_not_retick_a_reference_sourced_context():
    # The selector re-ticks with the same key on cycles 2 and 3: the branch is
    # not rebuilt and no input changed, so there is no output.
    assert eval_node(
        _context_switch,
        ["add", "add", "add"],
        [0, None, None],
        [0, None, None],
    ) == [0, None, None]


def test_changed_switch_key_still_rebuilds_and_emits():
    # A real key change rebinds the branch, which samples its inputs and emits.
    assert eval_node(
        _context_switch,
        ["add", "subtract"],
        [1, None],
        [10, None],
    ) == [11, -9]


def test_reference_sourced_context_still_propagates_its_own_ticks():
    # The guard must not silence a genuine change to the context.
    assert eval_node(
        _context_switch,
        ["add", None, None],
        [1, None, None],
        [10, 20, None],
    ) == [11, 21, None]


def test_reference_sourced_switch_argument_does_not_retick():
    # The same boundary rule for an ordinary switch argument rather than a
    # context: an unchanged key must not resample it.
    @hg.graph
    def passthrough(value: hg.TS[int]) -> hg.TS[int]:
        return value

    @hg.graph
    def graph(selector: hg.TS[str], value: hg.TS[int]) -> hg.TS[int]:
        return hg.switch_(selector, {"a": passthrough, "b": passthrough}, _via_reference(value))

    assert eval_node(graph, ["a", "a", "a"], [1, None, None]) == [1, None, None]
