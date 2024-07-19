import pytest

from hgraph import component, TS, graph
from hgraph.test import eval_node


def test_component():

    @component
    def my_component(ts: TS[float], key: str) -> TS[float]:
        return ts + 1.0

    assert eval_node(my_component, ts=[1.0, 2.0, 3.0], key="key_1") == [2.0, 3.0, 4.0]


def test_component_error_duplicate_id():
    @component
    def my_component(ts: TS[float], key: str) -> TS[float]:
        return ts + 1.0

    @graph
    def duplicate_wiring(ts: TS[float], key: str) -> TS[float]:
        a = my_component(ts=ts, key=key)
        b = my_component(ts=ts + 1, key=key)
        return a + b

    with pytest.raises(RuntimeError):
        assert eval_node(duplicate_wiring, ts=[1.0, 2.0, 3.0], key="key_1") == [2.0, 3.0, 4.0]


def test_recordable_id_from_ts():

    @component(recordable_id="Test_{key}")
    def my_component(ts: TS[float], key: TS[str]) -> TS[float]:
        return ts + 1.0

    assert eval_node(my_component, ts=[1.0, 2.0, 3.0], key=["key_1"]) == [2.0, 3.0, 4.0]
