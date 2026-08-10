import subprocess
import sys
import textwrap


def test_native_documentation_is_available_at_runtime_and_in_the_stub():
    """Keep module-reset tests from replacing the installed binding under test."""

    check = textwrap.dedent(
        r"""
        from inspect import getdoc
        from pathlib import Path

        import _hgraph
        import hgraph as hg

        types = [
            ("CivilDateTime", "timezone-free calendar date"),
            ("Period", "calendar-relative duration"),
            ("TimeSeriesRef", "runtime reference"),
            ("TimeSeries", "callback-scoped native time-series input view"),
            ("OutputView", "callback-scoped view of a Python node output"),
            ("EvaluationClock", "view of graph evaluation time"),
            ("EvaluationEngineApi", "control view for the running graph executor"),
            ("Scheduler", "scheduler for the current node"),
            ("EvaluationProfiler", "Collect graph and node lifecycle timing"),
            ("GraphDiagnostics", "Collect graph structure"),
            ("WiringTracer", "Capture graph and node wiring events"),
        ]
        for name, summary in types:
            documentation = getdoc(getattr(_hgraph, name))
            assert documentation is not None, name
            assert summary in documentation, name

        methods = [
            ("CivilDateTime", "weekday", "Monday=0"),
            ("TimeSeries", "make_passive", "Stop this input"),
            ("OutputView", "invalidate", "Invalidate the output"),
            ("InstantRange", "touches", "finite upper endpoint equals"),
            ("InstantRange", "adjacent", "exactly one meeting boundary"),
            ("EvaluationEngineApi", "request_engine_stop", "graceful stop"),
            ("EvaluationTrace", "set_print_all_values", "valid input values that did not tick"),
            ("EvaluationProfiler", "snapshot", "immutable snapshot"),
            ("GraphDiagnostics", "reset", "Clear collected diagnostics"),
            ("Scheduler", "reset", "Cancel every outstanding schedule"),
        ]
        for owner, member, summary in methods:
            documentation = getdoc(getattr(getattr(_hgraph, owner), member))
            assert documentation is not None, f"{owner}.{member}"
            assert summary in documentation, f"{owner}.{member}"

        properties = [
            ("TimeSeries", "value", "current Python value"),
            ("TimeSeries", "active", "currently schedule its node"),
            ("OutputView", "value", "Assigning publishes a value"),
            ("RecordableStateView", "value", "current persistent value"),
            ("EvaluationClock", "next_cycle_evaluation_time", "immediately following this evaluation cycle"),
            ("EvaluationEngineApi", "evaluation_mode", "active execution mode"),
            ("Graph", "nodes", "callback-scoped views"),
            ("Node", "node_index", "zero-based index"),
            ("Scheduler", "is_scheduled", "outstanding schedule"),
        ]
        for owner, member, summary in properties:
            documentation = getdoc(getattr(getattr(_hgraph, owner), member))
            assert documentation is not None, f"{owner}.{member}"
            assert summary in documentation, f"{owner}.{member}"

        enum_values = [
            (_hgraph.MonthEndPolicy.PRESERVE_END_OF_MONTH, "source month-end"),
            (_hgraph.AmbiguousTimePolicy.EARLIEST, "earlier of the two"),
            (_hgraph.NonexistentTimePolicy.NEXT_VALID, "after the transition gap"),
            (_hgraph.Boundary.CLOSED, "Include the endpoint"),
        ]
        for value, summary in enum_values:
            documentation = getdoc(value)
            assert documentation is not None, value
            assert summary in documentation, value

        public_types = [
            (hg.TimeSeries, "callback-scoped native time-series input view"),
            (hg.Graph, "callback-scoped view of a running graph"),
            (hg.Node, "callback-scoped view of the currently running node"),
            (hg.GlobalState, "Graph-scoped configuration"),
            (hg.SCHEDULER, "current node's scheduler"),
            (hg.CLOCK, "graph evaluation clock"),
            (hg.TS_OUT, "inspect or mutate its native output"),
            (hg.RECORDABLE_STATE, "Persistent output-backed node state"),
        ]
        for value, summary in public_types:
            documentation = getdoc(value)
            assert documentation is not None, value
            assert summary in documentation, value
        assert "immediately following possible evaluation cycle" in getdoc(hg.CLOCK)

        stub_path = Path(_hgraph.__file__).parent / "_hgraph.pyi"
        stub = stub_path.read_text()
        assert "A calendar-relative duration measured in years" in stub
        assert "A read-only, callback-scoped native time-series input view" in stub
        assert "Collect graph and node lifecycle timing" in stub
        assert "A callback-scoped scheduler for the current node" in stub
        assert "The current Python value, or None when the input is invalid" in stub
        assert "The node's zero-based index within its graph" in stub
        assert "Select the earlier of the two matching instants" in stub
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", check],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
