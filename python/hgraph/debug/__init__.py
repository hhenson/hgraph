"""Owned inspection snapshots from the native hgraph runtime.

The observer and all measurement logic are implemented in C++.  This module
only exposes the native records and converts a snapshot into presentation
rows suitable for a table or notebook.
"""

from _hgraph import (
    InspectionEntityKind,
    InspectionEntry,
    InspectionSnapshot,
    Inspector,
    NodeStorageMetrics,
    RuntimeRegistrySnapshot,
    runtime_registry_snapshot,
)


def inspection_rows(snapshot: InspectionSnapshot) -> list[dict]:
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
                if entry.kind == InspectionEntityKind.NODE
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
        })
    return rows


__all__ = [
    "InspectionEntityKind",
    "InspectionEntry",
    "InspectionSnapshot",
    "Inspector",
    "NodeStorageMetrics",
    "RuntimeRegistrySnapshot",
    "inspection_rows",
    "runtime_registry_snapshot",
]
