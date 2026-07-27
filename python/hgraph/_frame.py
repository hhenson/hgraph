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

    Shipped frame-consuming code (adaptor internals, the record/replay
    machinery) normalizes inputs through this so neither the polars_frames
    compatibility switch (issue #80) nor a user-supplied polars frame ever
    changes their internals: a pyarrow table passes through; a
    ``RecordBatch``/``RecordBatchReader``, a polars ``DataFrame``, or any
    other ``__arrow_c_stream__``/``to_arrow`` carrier converts on the
    zero-copy Arrow stream. Polars' native ``string_view``/``binary_view``
    layouts cast to the standard string/binary types — several pyarrow
    compute kernels (e.g. ``filter``) have no view-type kernels."""
    import pyarrow as pa

    if isinstance(frame, pa.Table):
        return frame
    if isinstance(frame, pa.RecordBatch):
        return pa.Table.from_batches([frame])
    if isinstance(frame, pa.RecordBatchReader):
        return frame.read_all()
    if not hasattr(frame, "__arrow_c_stream__"):
        to_arrow = getattr(frame, "to_arrow", None)
        if to_arrow is None:
            return frame
        return as_arrow_table(to_arrow())
    table = pa.table(frame)
    fields = [
        field.with_type(pa.string()) if pa.types.is_string_view(field.type)
        else field.with_type(pa.binary()) if pa.types.is_binary_view(field.type)
        else field
        for field in table.schema
    ]
    target = pa.schema(fields, metadata=table.schema.metadata)
    return table.cast(target) if target != table.schema else table


def _strip_utc_timestamps(table):
    """Naive-UTC presentation of a table's timestamp columns.

    The v2 table codec stamps engine-time columns ``timestamp[us, UTC]``;
    hgraph's Python convention is NAIVE UTC datetimes throughout (upstream
    parity), so the user boundary presents timezone-free columns. Lossless:
    the instants are UTC either way. The ``hgraph.temporal.version`` marker
    drops with the timezone — the result IS the version-1 form, which the
    codec re-ingests (a naive column under a version-2 marker is rejected).
    Zoned values keep their timezone semantics untouched: ZonedDateTime and
    the range kinds are STRUCT columns whose nested timestamps this
    top-level strip never reaches, and the ``hgraph.tzdb.version`` marker
    their ingest validation requires is preserved. Non-UTC zones are never
    stripped."""
    import pyarrow as pa

    metadata = dict(table.schema.metadata or {})
    fields = [
        field.with_type(pa.timestamp(field.type.unit))
        if pa.types.is_timestamp(field.type) and field.type.tz == "UTC"
        else field
        for field in table.schema
    ]
    target = pa.schema(fields, metadata=table.schema.metadata)
    if target == table.schema:
        return table
    metadata.pop(b"hgraph.temporal.version", None)
    return table.cast(pa.schema(fields, metadata=metadata or None))


def as_user_frame(frame):
    """A frame in the USER-FACING form (the original hgraph API).

    Timestamp columns present naive (tz-free) UTC, and with the
    polars_frames compatibility switch on (issue #80) the result is a
    ``polars.DataFrame``. Every framework surface that hands a frame to
    user code — the bridge conversion boundary, ``DataFrameStorage``
    reads, the frame store — returns through this."""
    table = _strip_utc_timestamps(as_arrow_table(frame))
    if not _hgraph.polars_frames():
        return table
    try:
        import polars
    except ImportError as error:
        raise RuntimeError(
            "the polars_frames feature switch is enabled (HGRAPH_POLARS_FRAMES) "
            "but polars is not installed; install polars or disable the switch"
        ) from error
    return polars.from_arrow(table)


def _present_frame(table):
    """Bridge hook: the outbound frame presentation (called by frame_to_py)."""
    return as_user_frame(table)


def _present_series(array):
    """Bridge hook: the outbound series presentation (called by series_to_py)."""
    if not _hgraph.polars_frames():
        return array
    try:
        import polars
    except ImportError as error:
        raise RuntimeError(
            "the polars_frames feature switch is enabled (HGRAPH_POLARS_FRAMES) "
            "but polars is not installed; install polars or disable the switch"
        ) from error
    return polars.from_arrow(array)


__all__ = [
    "with_frame_metadata",
    "frame_metadata",
    "has_frame_metadata",
    "without_frame_metadata",
    "as_arrow_table",
    "as_user_frame",
]
