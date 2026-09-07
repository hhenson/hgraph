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


@contextmanager
def wiring_context():
    """Create a compile-only wiring scope for graph wiring tests.

    Unlike :func:`eval_node`, leaving this context does not build services or
    prepare and execute the graph. Wiring errors still surface from calls made
    inside the context.
    """
    import _hgraph

    from .._types import _finalize_compound_scalar_types
    from .._wiring._state import _global_state_scope

    _finalize_compound_scalar_types()
    with _global_state_scope() as state:
        wiring = _hgraph.Wiring(state._impl)
        try:
            with use_wiring(wiring):
                yield wiring
                _finalize_compound_scalar_types()
        finally:
            from .._wiring._services import _SERVICE_BUILD_CONTEXTS

            _SERVICE_BUILD_CONTEXTS.pop(wiring, None)
            wiring._release_seed_context()


__all__ = [
    "breakpoint_",
    "eval_node", "EvaluationProfiler", "EvaluationProfileEntry",
    "EvaluationProfilePhase", "EvaluationProfileSnapshot", "EvaluationTrace", "WiringTracer",
    "GraphDiagnostics",
    "use_wiring", "wiring_context",
]
