"""C++-first public contracts for the hgraph versioned dataflow fabric."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Final

from hgraph import (
    CompoundScalar,
    Frame,
    MAX_DT,
    TS,
    WiringPort,
    operator_function,
)
from hgraph._wiring._state import _active_global_state

# The native Fabric bridge consumes the persistence SDK.  Loading its declared
# Python dependency first is also required on Windows: the
# persistence bridge loads Parquet from PyArrow before Fabric's module is
# resolved, while macOS and Linux can use their relative runtime paths.
import hgraph_persistence as _hgraph_persistence  # noqa: F401

from . import _hgraph_fabric as _native


DataVersion = int
RevisionId = int
ResolutionStatus = _native.ResolutionStatus
FabricConfig = _native.FabricConfig


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
    """Opaque proof that a dependency came from ``subscribe_data``."""

    _token: WiringPort


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
        if not all(isinstance(item, DependencyHandle) for item in dependencies):
            raise TypeError(
                "explicit fabric dependencies must be DependencyHandle values"
            )
        return cls(tuple(dependencies))


AUTO: Final = DependencySelection.automatic()

_subscribe_data = operator_function("hgraph.fabric.subscribe_data")
_publish_data = operator_function("hgraph.fabric.publish_data")
_publish_data_explicit = operator_function(
    "hgraph.fabric._publish_data_explicit"
)
_register_memory_service = operator_function(
    "hgraph.fabric.register_memory_service"
)
_register_configured_service = operator_function(
    "hgraph.fabric.register_configured_service"
)


def make_memory_fabric_config(
    *, prefix: str = "fabric", notification_request_limit: int = 1024
) -> FabricConfig:
    """Create an owning in-process Fabric configuration.

    ``notification_request_limit`` bounds graph-transport candidates and
    applies publication backpressure while all slots are awaiting delivery.
    """

    return _native._make_memory_fabric_config(prefix, notification_request_limit)


def register_fabric_service(
    config: FabricConfig, *, path: str = "fabric"
) -> None:
    """Register the native graph service using an explicit configuration."""

    state = _active_global_state()
    _native._set_fabric_config(state._impl, path, config)
    _register_configured_service(path)


def register_memory_fabric_service(
    *, prefix: str = "fabric", path: str = "fabric"
) -> None:
    """Register one native Fabric service backed by process-local stores.

    This is the deterministic local/test host. Production hosts install their
    persistence and notification resources in ``GlobalState`` and register the
    same native service contract from C++.
    """

    _register_memory_service(prefix, path)


def subscribe_data(data_id: str, *, path: str = "fabric") -> TS[Frame]:
    """Wire a Frame subscription on one Fabric service path."""

    return _subscribe_data(data_id, path)


def load_data(
    config: FabricConfig,
    data_id: str,
    as_of: datetime = MAX_DT,
):
    """Load the latest stored Frame, optionally at or before ``as_of``.

    This is a synchronous point lookup, not a graph subscription: it does not
    coordinate the selected value with dependency versions. The result is
    ``None`` when no matching value exists. Frame presentation follows hgraph's
    compatibility switch, yielding Polars when enabled and available and
    PyArrow otherwise.
    """

    return _native._load_data(config, data_id, as_of)


def dependency_handle(subscription: TS[Frame]) -> DependencyHandle:
    """Create an explicit-lineage handle from a direct subscription result.

    Native planning validates the direct source and wiring-root identity when
    the handle is consumed by ``publish_data``.
    """

    if not isinstance(subscription, WiringPort):
        raise TypeError("fabric dependency handle requires a WiringPort")
    return DependencyHandle(subscription)


def publish_data(
    data_id: str,
    value: TS[Frame],
    *,
    path: str = "fabric",
    dependencies: DependencySelection = AUTO,
) -> None:
    """Wire a complete atomic Frame publisher on one Fabric service path.

    Explicit handles and automatic discovery use the same native wiring-time
    dependency planner.
    """

    if dependencies.is_automatic:
        _publish_data(data_id, path, value)
        return
    _publish_data_explicit(
        data_id, path, value, *(item._token for item in dependencies.dependencies)
    )


def encode_revision(revision: DataRevision) -> bytes:
    """Encode a semantic revision with the canonical native v1 codec."""

    return _native._encode_revision(
        revision.format_version,
        revision.data_id,
        revision.revision,
        revision.output_version,
        [(item.data_id, item.version) for item in revision.dependencies],
        revision.self_predecessor,
        revision.as_of,
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
        as_of=value["as_of"],
    )


def encode_as_of_reference(revision: RevisionId) -> bytes:
    return _native._encode_revision_reference(2, revision)


def decode_as_of_reference(encoded: bytes) -> RevisionId:
    return _native._decode_revision_reference(2, encoded)


def encode_latest_reference(revision: RevisionId) -> bytes:
    return _native._encode_revision_reference(3, revision)


def decode_latest_reference(encoded: bytes) -> RevisionId:
    return _native._decode_revision_reference(3, encoded)


def _resolve_fixture(
    revisions: tuple[DataRevision, ...],
    roots: tuple[str, ...],
    exposed: tuple[tuple[str, DataVersion], ...] = (),
):
    """Exercise the native resolver over an isolated in-memory history.

    This private compatibility-test seam keeps Python examples on the same
    C++ resolver and persistence contracts as production ingress.
    """

    return _native._resolve_fixture(
        [encode_revision(revision) for revision in revisions], roots, exposed
    )


__all__ = [
    "AUTO",
    "DataDependency",
    "DataRevision",
    "DataVersion",
    "DependencyHandle",
    "DependencySelection",
    "FabricConfig",
    "RevisionId",
    "ResolutionStatus",
    "decode_as_of_reference",
    "decode_latest_reference",
    "decode_revision",
    "dependency_handle",
    "encode_as_of_reference",
    "encode_latest_reference",
    "encode_revision",
    "load_data",
    "make_memory_fabric_config",
    "publish_data",
    "register_fabric_service",
    "register_memory_fabric_service",
    "subscribe_data",
]
