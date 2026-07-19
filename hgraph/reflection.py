"""Public, implementation-neutral helpers for inspecting hgraph types.

The Python runtime internally represents wired types with ``Hg*TypeMetaData``
objects.  Those objects are wiring implementation details: callers should not
need their class hierarchy, nor should application code depend on it.  This
module accepts either a public type expression or metadata returned by a wired
signature and returns plain, comparable Python/hgraph types.
"""

from __future__ import annotations

from hgraph._types import (
    CompoundScalar,
    HgCompoundScalarType,
    HgREFTypeMetaData,
    HgTSBTypeMetaData,
    HgTSDTypeMetaData,
    HgTSLTypeMetaData,
    HgTSSTypeMetaData,
    HgTSTypeMetaData,
    HgTypeMetaData,
)

__all__ = (
    "scalar_type",
    "key_type",
    "value_type",
    "element_type",
    "size",
    "fields",
    "dereference",
    "is_reference",
    "is_ts",
    "is_tsd",
    "is_tsl",
    "is_tss",
    "is_bundle",
    "is_compound_scalar",
    "operator_overloads",
)


def _metadata(type_or_metadata) -> HgTypeMetaData:
    if isinstance(type_or_metadata, HgTypeMetaData):
        return type_or_metadata
    try:
        return HgTypeMetaData.parse_type(type_or_metadata)
    except (TypeError, RuntimeError) as error:
        raise TypeError(f"{type_or_metadata!r} is not an hgraph type") from error


def scalar_type(type_or_metadata):
    """Return the scalar payload of a ``TS`` or ``TSS`` type."""
    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, (HgTSTypeMetaData, HgTSSTypeMetaData)):
        return metadata.value_scalar_tp.py_type
    raise TypeError(f"scalar_type expects a TS or TSS type, got {type_or_metadata!r}")


def key_type(type_or_metadata):
    """Return the key scalar type of a ``TSD``."""
    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, HgTSDTypeMetaData):
        return metadata.key_tp.py_type
    raise TypeError(f"key_type expects a TSD type, got {type_or_metadata!r}")


def value_type(type_or_metadata):
    """Return the value time-series type of a ``TSD``."""
    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, HgTSDTypeMetaData):
        return metadata.value_tp.py_type
    raise TypeError(f"value_type expects a TSD type, got {type_or_metadata!r}")


def element_type(type_or_metadata):
    """Return the element time-series type of a ``TSL``."""
    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, HgTSLTypeMetaData):
        return metadata.value_tp.py_type
    raise TypeError(f"element_type expects a TSL type, got {type_or_metadata!r}")


def size(type_or_metadata) -> int:
    """Return the fixed size of a ``TSL``."""
    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, HgTSLTypeMetaData):
        size_type = metadata.size_tp.py_type
        if not size_type.FIXED_SIZE:
            raise TypeError(f"size expects a fixed TSL type, got {type_or_metadata!r}")
        return size_type.SIZE
    raise TypeError(f"size expects a TSL type, got {type_or_metadata!r}")


def fields(type_or_metadata) -> dict[str, object]:
    """Return the ordered fields of a TSB or CompoundScalar schema."""
    if isinstance(type_or_metadata, type) and issubclass(type_or_metadata, CompoundScalar):
        return {
            name: field.py_type
            for name, field in type_or_metadata.__meta_data_schema__.items()
        }

    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, HgTSBTypeMetaData):
        return {
            name: field.py_type
            for name, field in metadata.bundle_schema_tp.meta_data_schema.items()
        }
    if (isinstance(metadata, HgTSTypeMetaData)
            and isinstance(metadata.value_scalar_tp, HgCompoundScalarType)):
        return {
            name: field.py_type
            for name, field in metadata.value_scalar_tp.meta_data_schema.items()
        }
    if isinstance(metadata, HgCompoundScalarType):
        return {
            name: field.py_type
            for name, field in metadata.meta_data_schema.items()
        }
    raise TypeError(
        f"fields expects a TSB or CompoundScalar type, got {type_or_metadata!r}")


def dereference(type_or_metadata):
    """Strip one top-level ``REF`` wrapper, returning a public type."""
    metadata = _metadata(type_or_metadata)
    if isinstance(metadata, HgREFTypeMetaData):
        return metadata.value_tp.py_type
    return metadata.py_type


def is_reference(type_or_metadata) -> bool:
    return isinstance(_metadata(type_or_metadata), HgREFTypeMetaData)


def is_ts(type_or_metadata) -> bool:
    return isinstance(_metadata(type_or_metadata), HgTSTypeMetaData)


def is_tsd(type_or_metadata) -> bool:
    return isinstance(_metadata(type_or_metadata), HgTSDTypeMetaData)


def is_tsl(type_or_metadata) -> bool:
    return isinstance(_metadata(type_or_metadata), HgTSLTypeMetaData)


def is_tss(type_or_metadata) -> bool:
    return isinstance(_metadata(type_or_metadata), HgTSSTypeMetaData)


def is_bundle(type_or_metadata) -> bool:
    return isinstance(_metadata(type_or_metadata), HgTSBTypeMetaData)


def is_compound_scalar(type_or_metadata) -> bool:
    metadata = _metadata(type_or_metadata)
    return (
        isinstance(metadata, HgCompoundScalarType)
        or (
            isinstance(metadata, HgTSTypeMetaData)
            and isinstance(metadata.value_scalar_tp, HgCompoundScalarType)
        )
    )


def operator_overloads(operator):
    """Return the decorated implementations registered for an operator.

    Each returned callable can be inspected with ``inspect.signature``. The
    registry helper and its ranking records remain private implementation
    details.
    """
    helper = getattr(operator, "overload_list", None)
    if helper is None:
        return ()
    return tuple(implementation for implementation, _ in helper.overloads)
