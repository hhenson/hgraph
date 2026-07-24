from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pytest

from hgraph import (
    CompoundScalar,
    Frame,
    TS,
    frame_metadata,
    has_frame_metadata,
    pass_through,
    without_frame_metadata,
    with_frame_metadata,
)
from hgraph.test import eval_node


@dataclass(frozen=True)
class PriceRow(CompoundScalar):
    instrument: str
    value: float


@dataclass(frozen=True)
class SnapshotContext(CompoundScalar):
    desk: str
    sequence: int


@dataclass(frozen=True)
class SnapshotMetadata(CompoundScalar):
    as_of: datetime
    source: str
    context: SnapshotContext


@dataclass(frozen=True)
class OtherMetadata(CompoundScalar):
    as_of: datetime
    source: str
    context: SnapshotContext


def test_typed_frame_metadata_is_encoded_in_arrow_schema_and_round_trips():
    table = pa.table(
        {"instrument": ["A"], "value": [101.5]},
        metadata={b"external.owner": b"research"},
    )
    metadata = SnapshotMetadata(
        as_of=datetime(2026, 1, 1),
        source='fixture\n"α"',
        context=SnapshotContext(desk="systematic", sequence=3),
    )
    value = with_frame_metadata(table, metadata)

    assert isinstance(value, pa.Table)
    assert value.schema.metadata[b"external.owner"] == b"research"
    assert value.schema.metadata[b"hgraph.metadata.schema"].endswith(
        b"::SnapshotMetadata"
    )
    assert value.schema.metadata[b"hgraph.metadata.version"] == b"1"
    assert value.schema.metadata[b"hgraph.metadata.field.as_of"] == (
        b"2026-01-01T00:00:00Z"
    )
    assert value.schema.metadata[b"hgraph.metadata.field.source"] == (
        'fixture\n"α"'.encode()
    )
    assert value.schema.metadata[b"hgraph.metadata.field.context"] == (
        b'{"desk": "systematic", "sequence": 3}'
    )
    assert frame_metadata(value) == metadata

    markerless_metadata = dict(value.schema.metadata)
    del markerless_metadata[b"hgraph.metadata.schema"]
    markerless = value.replace_schema_metadata(markerless_metadata)
    assert has_frame_metadata(markerless)
    assert frame_metadata(markerless, SnapshotMetadata) == metadata
    with pytest.raises(ValueError, match="provide a metadata schema"):
        frame_metadata(markerless)

    result = eval_node(
        pass_through,
        [value],
        resolution_dict={"ts": TS[Frame[PriceRow, SnapshotMetadata]]},
    )[0]

    assert isinstance(result, pa.Table)
    assert result.equals(value)
    assert frame_metadata(result, SnapshotMetadata) == metadata

    markerless_result = eval_node(
        pass_through,
        [markerless],
        resolution_dict={"ts": TS[Frame[PriceRow, SnapshotMetadata]]},
    )[0]
    assert markerless_result.equals(markerless)
    assert frame_metadata(markerless_result, SnapshotMetadata) == metadata

    row_only = without_frame_metadata(result)
    assert not has_frame_metadata(row_only)
    assert row_only.schema.metadata == {b"external.owner": b"research"}


def test_typed_frame_metadata_contract_is_enforced_at_python_boundary():
    table = pa.table({"instrument": ["A"], "value": [101.5]})
    when = datetime(2026, 1, 1)
    context = SnapshotContext(desk="systematic", sequence=3)

    with pytest.raises(ValueError, match="named Bundle"):
        TS[Frame[PriceRow, int]]

    with pytest.raises(TypeError, match="requires metadata"):
        eval_node(
            pass_through,
            [table],
            resolution_dict={"ts": TS[Frame[PriceRow, SnapshotMetadata]]},
        )

    with pytest.raises(TypeError, match="incompatible"):
        eval_node(
            pass_through,
            [
                with_frame_metadata(
                    table,
                    OtherMetadata(
                        as_of=when,
                        source="fixture",
                        context=context,
                    ),
                )
            ],
            resolution_dict={"ts": TS[Frame[PriceRow, SnapshotMetadata]]},
        )

    typed = with_frame_metadata(
        table,
        SnapshotMetadata(as_of=when, source="fixture", context=context),
    )
    markerless_metadata = dict(typed.schema.metadata)
    del markerless_metadata[b"hgraph.metadata.schema"]
    markerless = typed.replace_schema_metadata(markerless_metadata)

    with pytest.raises(TypeError, match="row-only Frame"):
        eval_node(
            pass_through,
            [markerless],
            resolution_dict={"ts": TS[Frame[PriceRow]]},
        )
