from hgraph import TS, compute_node, const
from hgraph.test import wiring_context


def test_wiring_context_builds_without_executing():
    calls = []

    @compute_node
    def record_call(ts: TS[int]) -> TS[int]:
        calls.append(ts.value)
        return ts.value

    with wiring_context():
        record_call(const(1))

    assert calls == []
