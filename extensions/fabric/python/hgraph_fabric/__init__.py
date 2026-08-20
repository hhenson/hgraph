"""C++-first public contracts for the hgraph versioned dataflow fabric."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final

from hgraph import CompoundScalar, Frame, MIN_DT, TS, operator_function

from . import _hgraph_fabric as _native


DataVersion = int
RevisionId = int
SubscriptionMode = _native.SubscriptionMode
FabricSubscriptionMode = SubscriptionMode


@dataclass(frozen=True)
class DataDependency(CompoundScalar, namespace="hgraph.fabric"):
    """One immediate fabric input version used by a publication."""

    data_id: str
    version: DataVersion


@dataclass(frozen=True)
class DataRevision(CompoundScalar, namespace="hgraph.fabric"):
    """One canonical output-version acknowledgement and its immediate lineage."""

    format_version: int
    data_id: str
    revision: RevisionId
    output_version: DataVersion
    dependencies: tuple[DataDependency, ...]
    self_predecessor: DataVersion | None
    as_of: datetime


@dataclass(frozen=True)
class DependencyHandle:
    """Opaque Python authoring handle completed by dependency planning in checkpoint 3."""

    _token: object


@dataclass(frozen=True)
class DependencySelection:
    """Automatic lineage or an explicit set of subscription handles."""

    dependencies: tuple[DependencyHandle, ...] | None = None

    @property
    def is_automatic(self) -> bool:
        return self.dependencies is None

    @classmethod
    def automatic(cls) -> "DependencySelection":
        return cls()

    @classmethod
    def explicit(
        cls, *dependencies: DependencyHandle
    ) -> "DependencySelection":
        if not dependencies:
            raise ValueError("explicit fabric dependencies must not be empty")
        return cls(tuple(dependencies))


AUTO: Final = DependencySelection.automatic()

REVISION_MEDIA_TYPE: Final[str] = _native.REVISION_MEDIA_TYPE
AS_OF_MEDIA_TYPE: Final[str] = _native.AS_OF_MEDIA_TYPE
LATEST_MEDIA_TYPE: Final[str] = _native.LATEST_MEDIA_TYPE

_subscribe_data = operator_function("hgraph.fabric.subscribe_data")
_publish_data = operator_function("hgraph.fabric.publish_data")


def subscribe_data(
    data_id: str,
    *,
    mode: SubscriptionMode,
    as_of: datetime | None = None,
) -> TS[Frame]:
    """Wire a complete atomic Frame subscription under one stable data id."""

    return _subscribe_data(data_id, mode, MIN_DT if as_of is None else as_of)


def publish_data(
    data_id: str,
    value: TS[Frame],
    *,
    dependencies: DependencySelection = AUTO,
) -> None:
    """Wire a complete atomic Frame publisher.

    Explicit Python dependency handles become executable with the shared C++
    planner in RFC 0026 checkpoint 3. Automatic lineage is fully declared now.
    """

    if not dependencies.is_automatic:
        raise NotImplementedError(
            "explicit Python fabric dependency handles arrive in RFC 0026 checkpoint 3"
        )
    _publish_data(data_id, value)


def _to_epoch_microseconds(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def _from_epoch_microseconds(value: int) -> datetime:
    return (datetime(1970, 1, 1) + timedelta(microseconds=value))


def encode_revision(revision: DataRevision) -> bytes:
    """Encode a semantic revision with the canonical native v1 codec."""

    return _native._encode_revision(
        revision.format_version,
        revision.data_id,
        revision.revision,
        revision.output_version,
        [(item.data_id, item.version) for item in revision.dependencies],
        revision.self_predecessor,
        _to_epoch_microseconds(revision.as_of),
    )


def decode_revision(encoded: bytes) -> DataRevision:
    """Decode and validate a canonical native v1 revision."""

    value = _native._decode_revision(encoded)
    return DataRevision(
        format_version=value["format_version"],
        data_id=value["data_id"],
        revision=value["revision"],
        output_version=value["output_version"],
        dependencies=tuple(DataDependency(*item) for item in value["dependencies"]),
        self_predecessor=value["self_predecessor"],
        as_of=_from_epoch_microseconds(value["as_of_microseconds"]),
    )


def encode_as_of_reference(revision: RevisionId) -> bytes:
    return _native._encode_revision_reference(2, revision)


def decode_as_of_reference(encoded: bytes) -> RevisionId:
    return _native._decode_revision_reference(2, encoded)


def encode_latest_reference(revision: RevisionId) -> bytes:
    return _native._encode_revision_reference(3, revision)


def decode_latest_reference(encoded: bytes) -> RevisionId:
    return _native._decode_revision_reference(3, encoded)


__all__ = [
    "AS_OF_MEDIA_TYPE",
    "AUTO",
    "DataDependency",
    "DataRevision",
    "DataVersion",
    "DependencyHandle",
    "DependencySelection",
    "FabricSubscriptionMode",
    "LATEST_MEDIA_TYPE",
    "REVISION_MEDIA_TYPE",
    "RevisionId",
    "SubscriptionMode",
    "decode_as_of_reference",
    "decode_latest_reference",
    "decode_revision",
    "encode_as_of_reference",
    "encode_latest_reference",
    "encode_revision",
    "publish_data",
    "subscribe_data",
]
