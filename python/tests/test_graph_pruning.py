import pytest

import hgraph as hg


def unused_nodes(events):
    @hg.generator
    def unused_source() -> hg.TS[int]:
        events.append("source")
        yield hg.MIN_ST, 99

    @hg.compute_node
    def unused_compute(value: hg.TS[int]) -> hg.TS[int]:
        events.append("compute")
        return value.value

    @unused_compute.start
    def start():
        events.append("start")

    @unused_compute.stop
    def stop():
        events.append("stop")

    return unused_source, unused_compute


@pytest.mark.parametrize("mode", ["flat", "map", "switch"])
def test_unused_branches_never_start_evaluate_or_stop(mode):
    events = []
    source, compute = unused_nodes(events)

    @hg.graph
    def child(value: hg.TS[int]) -> hg.TS[int]:
        compute(source())
        compute(value)
        return value + 1

    if mode == "flat":
        assert hg.eval_node(child, [1, 2]) == [2, 3]
    elif mode == "map":
        @hg.graph
        def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(child, values)

        assert hg.eval_node(app, [{"a": 1}, {"a": 2}]) == [{"a": 2}, {"a": 3}]
    else:
        @hg.graph
        def app(key: hg.TS[str], value: hg.TS[int]) -> hg.TS[int]:
            return hg.switch_(key, {"child": child}, value)

        assert hg.eval_node(app, ["child", None], [1, 2]) == [2, 3]

    assert events == []


def test_graph_without_sinks_does_not_run_unused_nodes():
    events = []
    source, compute = unused_nodes(events)

    @hg.graph
    def app() -> None:
        compute(source())

    assert hg.run_graph(app) is None
    assert events == []


def test_all_sinks_and_shared_producers_are_retained():
    events = []
    source, compute = unused_nodes(events)

    @hg.sink_node
    def record(value: hg.TS[int]):
        events.append(("sink", value.value))

    @hg.graph
    def app() -> None:
        value = compute(source())
        record(value)
        record(value)

    hg.run_graph(app)
    assert events.count("source") == 1
    assert events.count("start") == 1
    assert events.count("compute") == 1
    assert events.count("stop") == 1
    assert events.count(("sink", 99)) == 2


def test_nested_return_and_sink_are_both_roots():
    events = []

    @hg.sink_node
    def record(value: hg.TS[int]):
        events.append(value.value)

    @hg.graph
    def child(value: hg.TS[int]) -> hg.TS[int]:
        record(value * 2)
        return value + 1

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(child, values)

    assert hg.eval_node(app, [{"a": 1}, {"a": 2}]) == [{"a": 2}, {"a": 3}]
    assert events == [2, 4]
