"""Pin issue #79: start/stop lifecycle parameters match eval's by NAME only.

Ruling (Howard, 2026-07-27): the eval signature is the signature bearer — a
lifecycle parameter takes the type/injectable identity of the same-named eval
parameter, and annotations/defaults on the start/stop functions themselves are
documentation, never contract. Names eval does not declare keep their own
annotation (an extra injectable such as a clock). The C++ engine is untouched;
the relaxation is Python-matcher-only.
"""

import pytest

import hgraph as hg
from hgraph import TS
from hgraph.test import eval_node


class Counter(hg.CompoundScalar):
    count: int = 0


def _run(node):
    @hg.graph
    def app(value: TS[int]) -> TS[int]:
        return node(value)

    return eval_node(app, [1, 2])


def test_lifecycle_strict_spelling_still_works():
    events = []

    @hg.compute_node
    def node(value: TS[int], _state: hg.STATE = None) -> TS[int]:
        return value.value

    @node.start
    def node_start(_state: hg.STATE = None):
        events.append("start")

    @node.stop
    def node_stop(_state: hg.STATE = None):
        events.append("stop")

    assert _run(node) == [1, 2]
    assert events == ["start", "stop"]


def test_lifecycle_bare_injectable_annotation_without_default():
    # A stop param spelled ``_state: STATE`` (no ``= None``) must match the
    # eval's ``_state`` by name and receive the state object.
    seen = []

    @hg.compute_node
    def node(value: TS[int], _state: hg.STATE = None) -> TS[int]:
        _state.mark = value.value
        return value.value

    @node.stop
    def node_stop(_state: hg.STATE):
        seen.append(_state.mark)

    assert _run(node) == [1, 2]
    assert seen == [2]


def test_lifecycle_unannotated_parameter_matches_by_name():
    # No annotation at all: the name alone binds to eval's ``_state``.
    seen = []

    @hg.compute_node
    def node(value: TS[int], _state: hg.STATE = None) -> TS[int]:
        _state.mark = value.value
        return value.value

    @node.stop
    def node_stop(_state):
        seen.append(_state.mark)

    assert _run(node) == [1, 2]
    assert seen == [2]


def test_lifecycle_unparameterized_state_against_keyed_eval_state():
    # eval declares ``STATE[Counter]``; the stop spelling ``_state: STATE``
    # is name-matched — the eval definition is the signature bearer.
    seen = []

    @hg.compute_node
    def node(value: TS[int], _state: hg.STATE[Counter] = None) -> TS[int]:
        _state.count += 1
        return _state.count

    @node.stop
    def node_stop(_state: hg.STATE):
        seen.append(_state.count)

    assert _run(node) == [1, 2]
    assert seen == [2]


def test_lifecycle_start_and_scalars_match_by_name():
    # Start participates in name-only matching too, scalars included; a
    # name eval does NOT declare keeps its own annotation (the clock) and
    # no longer needs the ``= None`` spelling.
    events = []

    @hg.compute_node
    def node(value: TS[int], label: str, _state: hg.STATE = None) -> TS[int]:
        return value.value + _state.bias

    @node.start
    def node_start(label, _state: hg.STATE, clock: hg.CLOCK):
        _state.bias = 10
        events.append(("start", label, clock.evaluation_time is not None))

    @node.stop
    def node_stop(label, _state):
        events.append(("stop", label, _state.bias))

    @hg.graph
    def app(value: TS[int]) -> TS[int]:
        return node(value, "tagged")

    assert eval_node(app, [1, 2]) == [11, 12]
    assert events == [("start", "tagged", True), ("stop", "tagged", 10)]


def test_lifecycle_stop_reads_input_by_name_only():
    # A stop param named after a ts input reads that input without needing
    # the time-series annotation.
    seen = []

    @hg.compute_node
    def node(value: TS[int]) -> TS[int]:
        return value.value

    @node.stop
    def node_stop(value):
        seen.append(value.value)

    assert _run(node) == [1, 2]
    assert seen == [2]


def test_lifecycle_time_series_in_start_still_rejected():
    # Reading inputs is a stop-only capability regardless of spelling.
    @hg.compute_node
    def node(value: TS[int]) -> TS[int]:
        return value.value

    with pytest.raises(TypeError, match="wiring-time scalars and injectables"):
        @node.start
        def node_start(value):
            pass


def test_lifecycle_stop_reads_the_nodes_output():
    # ``stop`` is given the node's own output to read: what it last published.
    seen = []

    @hg.compute_node
    def node(value: TS[int]) -> TS[int]:
        return value.value * 10

    @node.stop
    def node_stop(_output):
        seen.append((_output.valid, _output.value, _output.last_modified_time))

    assert _run(node) == [10, 20]
    assert seen == [(True, 20, hg.MIN_ST + hg.MIN_TD)]


def test_lifecycle_stop_output_takes_any_spelling_and_sits_beside_an_input():
    # ``_output`` is known by its name, as in eval: annotated or bare, and
    # whether or not eval takes it too.
    seen = []

    @hg.compute_node
    def node(value: TS[int], _output: hg.TS_OUT[int] = None) -> TS[int]:
        prior = _output.value if _output.valid else 0
        return prior + value.value

    @node.stop
    def node_stop(value: TS[int], _output: hg.TS_OUT[int] = None):
        seen.append((value.value, _output.value))

    assert _run(node) == [1, 3]
    assert seen == [(2, 3)]


def test_lifecycle_stop_sees_an_output_that_never_published():
    seen = []

    @hg.compute_node
    def node(value: TS[int]) -> TS[int]:
        return None

    @node.stop
    def node_stop(_output):
        seen.append(_output.valid)

    assert _run(node) is None
    assert seen == [False]


def test_lifecycle_stop_output_is_read_only():
    # A node publishes from eval alone: the view ``stop`` has cannot write.
    refused = []

    @hg.compute_node
    def node(value: TS[int]) -> TS[int]:
        return value.value

    @node.stop
    def node_stop(_output):
        try:
            _output.value = 99
        except AttributeError as error:
            refused.append(type(error).__name__)
        refused.append(hasattr(_output, "invalidate"))

    assert _run(node) == [1, 2]
    assert refused == ["AttributeError", False]


def test_lifecycle_stop_reads_the_output_of_a_recordable_node():
    class Durable(hg.TimeSeriesSchema):
        total: TS[int]

    seen = []

    @hg.compute_node
    def node(value: TS[int],
             state: hg.RECORDABLE_STATE[Durable] = None) -> TS[int]:
        total = (state.total.value if state.total.valid else 0) + value.value
        state.total.value = total
        return total

    @node.stop
    def node_stop(_output, state: hg.RECORDABLE_STATE[Durable] = None):
        seen.append((_output.value, state.total.value))

    assert _run(node) == [1, 3]
    assert seen == [(3, 3)]


def test_lifecycle_output_in_start_still_rejected():
    # The output has published nothing when a node starts.
    @hg.compute_node
    def node(value: TS[int]) -> TS[int]:
        return value.value

    with pytest.raises(TypeError, match="wiring-time scalars and injectables"):
        @node.start
        def node_start(_output):
            pass


def test_lifecycle_output_in_a_sinks_stop_rejected():
    @hg.sink_node
    def node(value: TS[int]):
        pass

    with pytest.raises(TypeError, match="the node has no output"):
        @node.stop
        def node_stop(_output):
            pass


def test_generator_stop_matches_by_name():
    seen = []

    @hg.generator
    def gen(count: int, _state: hg.STATE = None) -> TS[int]:
        _state.last = count
        yield hg.MIN_ST, count

    @gen.stop
    def gen_stop(count, _state: hg.STATE):
        seen.append((count, _state.last))

    @hg.graph
    def app() -> TS[int]:
        return gen(5)

    assert eval_node(app) == [5]
    assert seen == [(5, 5)]
