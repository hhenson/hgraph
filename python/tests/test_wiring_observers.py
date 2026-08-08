import hgraph as hg
import pytest


def test_wiring_observer_event_api_is_not_exposed_to_python():
    assert not hasattr(hg, "WiringObserver")
    assert not hasattr(hg, "WiringScopeEvent")
    assert not hasattr(hg, "WiringResolutionEvent")


def test_native_wiring_tracer_can_be_supplied_as_an_observer():
    from hgraph.test import WiringTracer

    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(7)

    tracer = WiringTracer(filter="app")
    result = hg.evaluate_graph(
        app,
        hg.GraphConfiguration(wiring_observers=(tracer,)),
    )

    assert [value for _, value in result] == [7]
    assert tracer.lines
    assert any("wiring graph app" in line for line in tracer.lines)
    assert any("Resolved operator const" in line for line in tracer.lines)


def test_eval_node_trace_wiring_uses_native_filter_options(capsys):
    @hg.graph
    def traced_graph(value: hg.TS[int]) -> hg.TS[int]:
        return value + 1

    assert hg.eval_node(
        traced_graph,
        [1],
        __trace_wiring__={"filter": "traced_graph", "node": False},
    ) == [2]

    output = capsys.readouterr().out
    assert "traced_graph" in output
    assert "wiring graph" in output
    assert "wiring node" not in output
    assert "Resolved operator" not in output


def test_python_wiring_observer_is_rejected_and_restores_the_runner():
    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(1)

    with pytest.raises(TypeError, match="native WiringTracer instances only"):
        hg.evaluate_graph(
            app,
            hg.GraphConfiguration(wiring_observers=(object(),)),
        )

    # Rejected configuration must not leave the Python wiring stack or
    # GlobalState context active for the next graph.
    assert [value for _, value in hg.evaluate_graph(app)] == [1]
