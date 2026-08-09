"""hgraph.test - the test utilities (hgraph-compatible import path)."""
from contextlib import contextmanager

from _hgraph import EvaluationProfiler, EvaluationProfileEntry, EvaluationProfilePhase, EvaluationProfileSnapshot, EvaluationTrace, GraphDiagnostics, WiringTracer

from .._wiring import eval_node
from ._breakpoint import breakpoint_


@contextmanager
def use_wiring(wiring):
    """Install ``wiring`` as the active wiring context for the duration.

    Test support: lets a test intercept ``hgraph.wire`` calls with a stub
    (the sanctioned route — test code must not reach into ``hgraph._wiring``).
    """
    from .._wiring import _wiring_stack

    _wiring_stack.append(wiring)
    try:
        yield wiring
    finally:
        _wiring_stack.pop()

__all__ = [
    "breakpoint_",
    "eval_node", "EvaluationProfiler", "EvaluationProfileEntry",
    "EvaluationProfilePhase", "EvaluationProfileSnapshot", "EvaluationTrace", "WiringTracer",
    "GraphDiagnostics",
    "use_wiring",
]
