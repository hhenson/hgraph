from dataclasses import dataclass, replace
from datetime import datetime

import hgraph as hg
from hgraph import CompoundScalar, TS, TSB, combine, compute_node, eval_node, graph
from hgraph.stream import (
    Data,
    Stream,
    StreamStatus,
    combine_status_messages,
    combine_statuses,
    reduce_status_messages,
    reduce_statuses,
    register_status_message_pattern,
)
from hgraph.reflection import fields


def test_stream_data_schema_flattens_payload_fields():
    stream_type = TSB[Stream[Data[int]]]

    @graph
    def make_stream(value: TS[int]) -> stream_type:
        return combine[stream_type](
            status=StreamStatus.OK,
            status_msg="",
            values=value,
            timestamp=datetime(2026, 1, 1),
        )

    assert eval_node(make_stream, [42]) == [{
        "status": StreamStatus.OK,
        "status_msg": "",
        "values": 42,
        "timestamp": datetime(2026, 1, 1),
    }]


def test_stream_full_value_is_the_associated_scalar():
    @dataclass(frozen=True)
    class Payload(CompoundScalar):
        value: int

    schema = Stream[Payload]
    stream_type = TSB[schema]
    scalar_type = schema.scalar_type()

    @compute_node
    def inspect_stream(value: stream_type) -> TS[int]:
        snapshot = value.value
        assert isinstance(snapshot, scalar_type)
        return replace(snapshot, value=snapshot.value + 1).value

    assert scalar_type.__bundle_type__ is schema
    assert eval_node(inspect_stream, [{
        "status": StreamStatus.OK,
        "status_msg": "",
        "value": 7,
    }]) == [8]


def test_stream_time_series_schema_preserves_nested_fields():
    class Cascade(hg.TimeSeriesSchema):
        weights: hg.TSD[str, hg.TS[float]]

    stream_type = hg.TSB[Stream[Cascade]]
    assert fields(stream_type) == {
        "status": hg.TS[StreamStatus],
        "status_msg": hg.TS[str],
        "weights": hg.TSD[str, hg.TS[float]],
    }

    @graph
    def make_stream(weights: hg.TSD[str, hg.TS[float]]) -> stream_type:
        return combine[stream_type](
            status=StreamStatus.OK,
            status_msg="",
            weights=weights,
        )

    assert eval_node(make_stream, [{"front": 0.6, "back": 0.4}]) == [{
        "status": StreamStatus.OK,
        "status_msg": "",
        "weights": {"front": 0.6, "back": 0.4},
    }]

    scalar_type = Stream[Cascade].scalar_type()

    @compute_node
    def inspect_stream(value: stream_type) -> TS[float]:
        snapshot = value.value
        assert isinstance(snapshot, scalar_type)
        return snapshot.weights["front"]

    assert eval_node(inspect_stream, [{
        "status": StreamStatus.OK,
        "status_msg": "",
        "weights": {"front": 0.6, "back": 0.4},
    }]) == [0.6]


def test_stream_status_operations_use_severity_and_deduplicate_messages():
    assert eval_node(
        combine_statuses,
        [StreamStatus.OK, StreamStatus.ERROR],
        [StreamStatus.STALE, StreamStatus.WAITING],
    ) == [StreamStatus.STALE, StreamStatus.ERROR]

    register_status_message_pattern(r"For (\w+), price is stale")
    assert eval_node(
        combine_status_messages,
        ["For A, price is stale"],
        ["For B, price is stale"],
    ) == ["For A, B, price is stale"]


@graph
def _reduce_status_dict(values: hg.TSD[int, hg.TS[StreamStatus]]) -> hg.TS[StreamStatus]:
    return reduce_statuses(values)


@graph
def _reduce_message_dict(values: hg.TSD[int, hg.TS[str]]) -> hg.TS[str]:
    return reduce_status_messages(values)


def test_reduce_stream_statuses_and_messages_use_current_tsd_state():
    assert eval_node(
        _reduce_status_dict,
        [
            {1: StreamStatus.OK, 2: StreamStatus.ERROR},
            {2: hg.REMOVE},
        ],
    ) == [StreamStatus.ERROR, StreamStatus.OK]
    assert eval_node(
        _reduce_message_dict,
        [{1: "failed a", 2: "failed b"}, {1: hg.REMOVE}],
    ) == ["failed a; failed b", "failed b"]


def test_stream_accepts_python_owned_dataclass_payload():
    # Issue #36: a frozen python-owned dataclass is a structured payload and
    # flattens exactly like a CompoundScalar payload.
    from hgraph.reflection import fields

    @dataclass(frozen=True)
    class Payload:
        value: int

    assert fields(TSB[Stream[Payload]]) == {
        "value": TS[int],
        "status": TS[StreamStatus],
        "status_msg": TS[str],
    }

    stream_type = TSB[Stream[Payload]]

    @graph
    def make_stream(value: TS[int]) -> stream_type:
        return combine[stream_type](
            status=StreamStatus.OK,
            status_msg="",
            value=value,
        )

    assert eval_node(make_stream, [7]) == [{
        "status": StreamStatus.OK,
        "status_msg": "",
        "value": 7,
    }]


def test_stream_still_rejects_unstructured_payloads():
    import pytest

    with pytest.raises(TypeError, match="structured dataclass"):
        Stream[int]


def test_generic_stream_defers_full_value_scalar_materialization():
    schema = Stream[Data[hg.SCALAR]]

    assert schema.scalar_type() is None
    assert repr(TSB[schema]).startswith("TSB[Stream[")
