"""Python-node diagnostic identity (issue #247): trace/profiler/wiring-trace
must name the user's function, not only the erased operator."""

from hgraph import TS, compute_node, graph
from hgraph.test import EvaluationProfiler, eval_node


@compute_node
def alpha(v: TS[int]) -> TS[int]:
    return v.value + 1


@compute_node
def beta(v: TS[int]) -> TS[int]:
    return v.value * 10


@graph
def _chain(t: TS[int]) -> TS[int]:
    return beta(alpha(t))


def test_profiler_entries_carry_user_function_labels():
    profiler = EvaluationProfiler()
    assert eval_node(_chain, [1, 2], __observers__=[profiler]) == [20, 30]
    labels = [entry.label for entry in profiler.snapshot().entries]
    assert any("alpha" in label for label in labels), labels
    assert any("beta" in label for label in labels), labels
    # the two python nodes are DISTINGUISHABLE, not both "__py_compute"
    assert len({l for l in labels if "alpha" in l or "beta" in l}) >= 2


def test_explicit_label_wins_over_function_name():
    @compute_node(label="my_custom")
    def gamma(v: TS[int]) -> TS[int]:
        return v.value

    @graph
    def g(t: TS[int]) -> TS[int]:
        return gamma(t)

    profiler = EvaluationProfiler()
    assert eval_node(g, [7], __observers__=[profiler]) == [7]
    labels = [entry.label for entry in profiler.snapshot().entries]
    assert any("my_custom" in label for label in labels), labels


def test_label_placeholders_resolve_with_wiring_scalars():
    # Review coverage (PR #250): label="prefix {scalar}" resolves against the
    # call's wiring-time scalar values (upstream custom-label parity), and
    # the resolved form is what diagnostics render.
    @compute_node(label="custom_label {i}")
    def delta(v: TS[int], i: str) -> TS[int]:
        return v.value

    @graph
    def g(t: TS[int]) -> TS[int]:
        return delta(t, i="one")

    profiler = EvaluationProfiler()
    assert eval_node(g, [3], __observers__=[profiler]) == [3]
    labels = [entry.label for entry in profiler.snapshot().entries]
    assert any("custom_label one" in label for label in labels), labels
