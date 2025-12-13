"""
Pytest configuration for time-series behavior tests.

Provides fixtures and hooks for state tracing.
"""
import pytest
import os

# Check if state tracing is enabled via environment variable
TRACE_ENABLED = os.environ.get('HGRAPH_TRACE_STATES', '0') == '1'


@pytest.fixture(autouse=TRACE_ENABLED)
def trace_test(request):
    """Fixture to trace state transitions for each test."""
    if not TRACE_ENABLED:
        yield
        return

    from hgraph_unit_tests.ts_tests._state_tracer import get_tracer

    tracer = get_tracer()
    tracer.enable()
    tracer.set_test(request.node.name)

    yield

    tracer.set_test(None)


def pytest_sessionstart(session):
    """Called at the start of the test session."""
    if TRACE_ENABLED:
        from hgraph_unit_tests.ts_tests._state_tracer import get_tracer
        tracer = get_tracer()
        tracer.clear()
        tracer.enable()


def pytest_sessionfinish(session, exitstatus):
    """Called at the end of the test session."""
    if TRACE_ENABLED:
        from hgraph_unit_tests.ts_tests._state_tracer import get_tracer
        import json

        tracer = get_tracer()
        tracer.disable()

        # Generate summary
        summary = tracer.summary()
        print("\n\n" + "=" * 70)
        print("STATE TRACING SUMMARY")
        print("=" * 70)
        print(f"Total transitions: {summary['total_transitions']}")
        print(f"Unique tests: {summary['unique_tests']}")
        print("\nTransitions by type:")
        for tt, count in sorted(summary['by_type'].items(), key=lambda x: -x[1]):
            print(f"  {tt.name}: {count}")
        print("\nTransitions by TS type:")
        for ts_type, count in sorted(summary['by_ts_type'].items(), key=lambda x: -x[1]):
            print(f"  {ts_type}: {count}")

        # Save detailed trace to file
        trace_file = os.environ.get('HGRAPH_TRACE_FILE', 'state_trace.json')
        trace_data = {
            'summary': {
                'total_transitions': summary['total_transitions'],
                'by_type': {k.name: v for k, v in summary['by_type'].items()},
                'by_ts_type': summary['by_ts_type'],
            },
            'unique_transitions': tracer.unique_transitions(),
            'transitions': [
                {
                    'type': t.transition_type.name,
                    'ts_type': t.ts_type,
                    'class': t.class_name,
                    'method': t.method_name,
                    'changes': t.state_change(),
                    'test': t.test_name,
                }
                for t in tracer.transitions
            ]
        }
        with open(trace_file, 'w') as f:
            json.dump(trace_data, f, indent=2, default=str)
        print(f"\nDetailed trace saved to: {trace_file}")
