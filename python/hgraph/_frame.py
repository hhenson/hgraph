"""Field-wise hgraph metadata encoded in an Arrow table schema."""

from __future__ import annotations

from . import _hgraph
from ._types import _value_type


def with_frame_metadata(table, metadata):
    """Return an Arrow table carrying ``metadata`` in its schema metadata."""
    return _hgraph._with_frame_metadata(table, _value_type(type(metadata)), metadata)


def frame_metadata(table, metadata_type=None):
    """Decode frame metadata, using its optional type marker when no type is supplied."""
    if metadata_type is None:
        return _hgraph._frame_metadata_reflective(table)
    return _hgraph._frame_metadata(table, _value_type(metadata_type))


def has_frame_metadata(table):
    """Return whether ``table`` carries reserved hgraph metadata entries."""
    return _hgraph._has_frame_metadata(table)


def without_frame_metadata(table):
    """Return an Arrow table with only the reserved hgraph metadata removed."""
    return _hgraph._without_frame_metadata(table)


def as_arrow_table(frame):
    """The canonical ``pyarrow.Table`` form of a frame value.

    Shipped frame-consuming Python nodes normalize their inputs through
    this so the polars_frames compatibility switch (issue #80) never
    changes their internals: a pyarrow table passes through; a polars
    ``DataFrame`` (or any ``__arrow_c_stream__`` carrier) converts on the
    zero-copy Arrow stream. Polars' native ``string_view``/``binary_view``
    layouts cast to the standard string/binary types — several pyarrow
    compute kernels (e.g. ``filter``) have no view-type kernels."""
    import pyarrow as pa

    if isinstance(frame, pa.Table):
        return frame
    if not hasattr(frame, "__arrow_c_stream__"):
        return frame
    table = pa.table(frame)
    fields = [
        field.with_type(pa.string()) if pa.types.is_string_view(field.type)
        else field.with_type(pa.binary()) if pa.types.is_binary_view(field.type)
        else field
        for field in table.schema
    ]
    target = pa.schema(fields, metadata=table.schema.metadata)
    return table.cast(target) if target != table.schema else table


__all__ = [
    "with_frame_metadata",
    "frame_metadata",
    "has_frame_metadata",
    "without_frame_metadata",
    "as_arrow_table",
]
