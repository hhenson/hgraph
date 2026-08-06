"""Owned inspection snapshots from the native hgraph runtime.

The observer and all measurement logic are implemented in C++.  This module
only exposes the native records and converts a snapshot into presentation
rows suitable for a table or notebook.
"""

from _hgraph import (
    GraphDiagnosticEntityKind,
    GraphDiagnosticEntry,
    GraphDiagnosticValue,
    GraphDiagnosticsSnapshot,
    GraphDiagnostics,
    NodeStorageMetrics,
    RuntimeRegistrySnapshot,
    runtime_registry_snapshot,
)


def inspector(port: int = 8080, publish_interval: float = 2.5):
    """Wire the optional interactive inspector presentation."""
    try:
        from ._inspector import inspector as implementation
    except ModuleNotFoundError as error:
        if error.name in {"tornado", "perspective"}:
            raise RuntimeError(
                "inspector() requires the 'perspective' extra") from error
        raise
    return implementation(port=port, publish_interval=publish_interval)


def graph_diagnostics_rows(snapshot: GraphDiagnosticsSnapshot) -> list[dict]:
    """Return flat, owned presentation rows for an inspection snapshot."""
    rows = []
    for entry in snapshot.entries:
        rows.append({
            "id": entry.id,
            "parent_id": entry.parent_id,
            "path": entry.path,
            "label": entry.label,
            "kind": entry.kind.name.lower(),
            "node_kind": (
                entry.node_kind.name.lower()
                if entry.kind == GraphDiagnosticEntityKind.NODE
                else None
            ),
            "schema": entry.schema_label,
            "implementation": entry.implementation_label,
            "started": entry.started,
            "stopped": entry.stopped,
            "evaluation_count": entry.evaluation.count,
            "evaluation_time": entry.evaluation.total_time,
            "scheduled_time": entry.scheduled_time,
            "static_bytes": entry.storage.static_bytes,
            "nested_graph_count": entry.storage.nested_graph_count,
            "nested_graph_capacity": entry.storage.nested_graph_capacity,
            "dynamic_live_bytes": entry.storage.dynamic_live_bytes,
            "dynamic_reserved_bytes": entry.storage.dynamic_reserved_bytes,
            "peak_dynamic_reserved_bytes": entry.peak_storage.dynamic_reserved_bytes,
            "input": entry.input.json if entry.input.available else None,
            "output": entry.output.json if entry.output.available else None,
            "scalars": entry.scalars.json if entry.scalars.available else None,
        })
    return rows


__all__ = [
    "GraphDiagnosticEntityKind",
    "GraphDiagnosticEntry",
    "GraphDiagnosticValue",
    "GraphDiagnosticsSnapshot",
    "GraphDiagnostics",
    "NodeStorageMetrics",
    "RuntimeRegistrySnapshot",
    "graph_diagnostics_rows",
    "inspector",
    "runtime_registry_snapshot",
]
