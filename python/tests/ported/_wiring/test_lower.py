import pyarrow as pa
import pytest

# The user boundary presents NAIVE UTC datetimes (upstream-verbatim, issue
# #80): version-2 Instant columns strip their UTC timezone on the way out.
from hgraph import (
    GlobalContext,
    GlobalState,
    MIN_ST,
    MIN_TD,
    TS,
    compute_node,
    graph,
    lower,
    sink_node,
)


def _frame(values):
    return pa.table({
        "date": [when for when, _ in values],
        "value": [value for _, value in values],
    })


def test_lower_replays_arrow_frames_through_native_graph():
    @graph
    def add_frames(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    lowered = lower(add_frames)
    result = lowered(
        lhs=_frame(((MIN_ST, 1), (MIN_ST + MIN_TD, 2))),
        rhs=_frame(((MIN_ST, 3), (MIN_ST + MIN_TD * 2, 4))),
    )

    assert result.to_pydict() == {
        "date": [
            MIN_ST,
            MIN_ST + MIN_TD,
            MIN_ST + MIN_TD * 2,
        ],
        "value": [4, 5, 6],
    }


def test_lower_captures_scalar_arguments_and_copies_global_state_back():
    @compute_node
    def remember(ts: TS[int], factor: int) -> TS[int]:
        value = ts.value * factor + GlobalState.instance()["seed"]
        GlobalState.instance()["lower.last"] = value
        return value

    @graph
    def scaled(ts: TS[int], factor: int = 2) -> TS[int]:
        return remember(ts, factor)

    state = GlobalState(seed=7)
    with GlobalContext(state):
        result = lower(scaled)(_frame(((MIN_ST, 4),)), factor=3)

    assert result.column("value").to_pylist() == [19]
    assert state["seed"] == 7
    assert state["lower.last"] == 19


def test_lower_sink_returns_none():
    @sink_node
    def consume(ts: TS[int]):
        _ = ts.value

    assert lower(consume)(_frame(((MIN_ST, 1),))) is None


def test_lower_selects_latest_as_of_version():
    @graph
    def identity(ts: TS[int]) -> TS[int]:
        return ts

    frame = pa.table({
        "date": [MIN_ST, MIN_ST],
        "as_of": [MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2],
        "value": [1, 2],
    })
    result = lower(identity, no_as_of_support=False)(frame)

    assert result.column_names == ["date", "as_of", "value"]
    assert result.column("value").to_pylist() == [2]


def test_lower_preserves_polars_boundary_compatibility():
    pl = pytest.importorskip("polars")

    @graph
    def add_one(ts: TS[int]) -> TS[int]:
        return ts + 1

    frame = pl.DataFrame({"date": [MIN_ST], "value": [1]})
    result = lower(add_one)(frame)

    assert isinstance(result, pl.DataFrame)
    assert result.equals(
        pl.DataFrame({"date": [MIN_ST], "value": [2]})
    )
