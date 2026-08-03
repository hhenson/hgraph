"""Debugger-independent formatting for hgraph's common type-erasure ABI."""

import struct

SCHEMA_HEADER_MAGIC = 0x48475348
TYPE_RECORD_MAGIC = 0x48475452
SCHEMA_HEADER_ABI_VERSION = 1
TYPE_RECORD_ABI_VERSION = 2
TYPE_KIND_NONE = 0xFF
DEBUG_DESCRIPTOR_MAGIC = 0x48474444
DEBUG_DESCRIPTOR_ABI_VERSION = 3
DEBUG_DYNAMIC_LAYOUT_MAGIC = 0x4847444C
DEBUG_DYNAMIC_LAYOUT_ABI_VERSION = 3

FAMILY_NAMES = {
    0: "Invalid",
    1: "Value",
    2: "TimeSeries",
    3: "Node",
    4: "Graph",
    5: "Executor",
    6: "Clock",
}

ROLE_NAMES = {
    0: "Invalid",
    1: "Instance",
    2: "Data",
    3: "Runtime",
    4: "Input",
    5: "Output",
}

ACCESS_NAMES = {
    0: "read-only",
    1: "writable",
    2: "mutation",
}

CAPABILITY_NAMES = (
    (1 << 0, "constructible"),
    (1 << 1, "destructible"),
    (1 << 2, "copyable"),
    (1 << 3, "movable"),
    (1 << 4, "mutable"),
    (1 << 5, "equatable"),
    (1 << 6, "comparable"),
    (1 << 7, "hashable"),
    (1 << 8, "children"),
    (1 << 9, "viewable"),
)
KNOWN_CAPABILITIES = sum(bit for bit, _ in CAPABILITY_NAMES)

KIND_NAMES = {
    1: {
        0: "Atomic",
        1: "Tuple",
        2: "Bundle",
        3: "List",
        4: "Set",
        5: "Map",
        6: "CyclicBuffer",
        7: "Queue",
        8: "Any",
    },
    2: {
        0: "TS",
        1: "TSS",
        2: "TSD",
        3: "TSL",
        4: "TSW",
        5: "TSB",
        6: "REF",
        7: "SIGNAL",
    },
    3: {
        0: "Compute",
        1: "PushSource",
        2: "PullSource",
        3: "Sink",
        4: "Nested",
    },
    5: {
        0: "Simulation",
        1: "RealTime",
    },
}

ALLOWED_ROLES = {
    1: (1,),
    2: (2, 4, 5),
    3: (3,),
    4: (3,),
    5: (3,),
    6: (3,),
}

DEBUG_LAYOUT_NAMES = {
    0: "Opaque",
    1: "Atomic",
    2: "FixedComposite",
    3: "Sequence",
    4: "KeyedSlots",
    5: "Node",
    6: "Graph",
    7: "TimeSeries",
}

DEBUG_ATOMIC_NAMES = {
    0: "Opaque",
    1: "Boolean",
    2: "SignedInteger",
    3: "UnsignedInteger",
    4: "FloatingPoint",
    5: "String",
}

DEBUG_DESCRIPTOR_HAS_VALIDITY = 1 << 0
KNOWN_DEBUG_DESCRIPTOR_FLAGS = DEBUG_DESCRIPTOR_HAS_VALIDITY
DEBUG_FIELD_EMBEDDED_OWNER = 1 << 1
DEBUG_FIELD_EMBEDDED_POINTER = 1 << 2
DEBUG_FIELD_INDIRECT_EMBEDDED_POINTER = 1 << 3
DEBUG_DYNAMIC_SIZE_CONSTANT = 1 << 0
DEBUG_DYNAMIC_DATA_INDIRECT = 1 << 1
DEBUG_DYNAMIC_KEY_DATA_INDIRECT = 1 << 2
DEBUG_DYNAMIC_DATA_POINTER_TABLE = 1 << 3
DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE = 1 << 4
DEBUG_DYNAMIC_HAS_SLOT_STATE = 1 << 5
DEBUG_DYNAMIC_HAS_HEAD = 1 << 6
DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS = 1 << 7
DEBUG_DYNAMIC_KEYS_ARE_OWNERS = 1 << 8
DEBUG_DYNAMIC_ELEMENTS_ARE_POINTERS = 1 << 9
DEBUG_DYNAMIC_DATA_POINTERS_TAGGED = 1 << 10
DEBUG_DYNAMIC_KEY_POINTERS_TAGGED = 1 << 11
DEBUG_DYNAMIC_SLOT_STATE_TAGGED = 1 << 12
DEBUG_DYNAMIC_SLOT_INDEX_INDIRECT = 1 << 13
DEBUG_DYNAMIC_SLOT_SIZE_INDIRECT = 1 << 14
KNOWN_DEBUG_DYNAMIC_FLAGS = (1 << 15) - 1
DEBUG_OWNER_STATE_MASK = 0x3
DEBUG_OWNER_INLINE_STATE = 1
DEBUG_OWNER_HEAP_STATE = 2

DEBUG_DYNAMIC_KIND_NAMES = {
    1: "Contiguous",
    2: "StableSlots",
}

_MISSING = object()


def pointer_text(address):
    return "0x{:x}".format(address) if address else "null"


def family_text(family):
    return FAMILY_NAMES.get(family, "unknown({})".format(family))


def role_text(role):
    return ROLE_NAMES.get(role, "unknown({})".format(role))


def kind_text(family, kind):
    if kind == TYPE_KIND_NONE:
        return "none"
    name = KIND_NAMES.get(family, {}).get(kind)
    return "{}({})".format(name, kind) if name is not None else str(kind)


def access_text(access):
    return ACCESS_NAMES.get(access, "invalid({})".format(access))


def capabilities_text(capabilities):
    names = [name for bit, name in CAPABILITY_NAMES if capabilities & bit]
    unknown = capabilities & ~KNOWN_CAPABILITIES
    if unknown:
        names.append("unknown=0x{:x}".format(unknown))
    return "|".join(names) if names else "none"


def schema_valid(snapshot):
    return (
        snapshot.get("magic") == SCHEMA_HEADER_MAGIC
        and snapshot.get("abi_version") == SCHEMA_HEADER_ABI_VERSION
        and snapshot.get("family") in ALLOWED_ROLES
        and bool(snapshot.get("label"))
    )


def record_valid(snapshot):
    schema = snapshot.get("schema")
    family = schema.get("family") if schema else 0
    role = snapshot.get("role", 0)
    implementation = snapshot.get("implementation_label")
    return (
        snapshot.get("magic") == TYPE_RECORD_MAGIC
        and snapshot.get("abi_version") == TYPE_RECORD_ABI_VERSION
        and snapshot.get("reserved0") == 0
        and snapshot.get("reserved1") == 0
        and schema is not None
        and schema_valid(schema)
        and role in ALLOWED_ROLES.get(family, ())
        and snapshot.get("ops_abi_version", 0) != 0
        and (snapshot.get("capabilities", 0) & ~KNOWN_CAPABILITIES) == 0
        and snapshot.get("plan", 0) != 0
        and snapshot.get("ops", 0) != 0
        and implementation != ""
    )


def schema_summary(snapshot):
    state = "valid" if schema_valid(snapshot) else "invalid"
    return 'SchemaHeader{{{} family={} kind={} abi={} label="{}"}}'.format(
        state,
        family_text(snapshot.get("family", 0)),
        kind_text(snapshot.get("family", 0), snapshot.get("kind", TYPE_KIND_NONE)),
        snapshot.get("abi_version", 0),
        snapshot.get("label") or "",
    )


def record_summary(snapshot):
    schema = snapshot.get("schema") or {}
    state = "valid" if record_valid(snapshot) else "invalid"
    implementation = snapshot.get("implementation_label") or "<semantic>"
    return (
        'TypeRecord{{{} {}/{} kind={} semantic="{}" implementation="{}" '
        "abi={}/{} capabilities={} plan={} ops={} debug={}}}"
    ).format(
        state,
        family_text(schema.get("family", 0)),
        role_text(snapshot.get("role", 0)),
        kind_text(schema.get("family", 0), schema.get("kind", TYPE_KIND_NONE)),
        schema.get("label") or "",
        implementation,
        snapshot.get("abi_version", 0),
        snapshot.get("ops_abi_version", 0),
        capabilities_text(snapshot.get("capabilities", 0)),
        pointer_text(snapshot.get("plan", 0)),
        pointer_text(snapshot.get("ops", 0)),
        pointer_text(snapshot.get("debug", 0)),
    )


def debug_layout_text(layout):
    return DEBUG_LAYOUT_NAMES.get(layout, "unknown({})".format(layout))


def debug_atomic_text(atomic_kind):
    return DEBUG_ATOMIC_NAMES.get(atomic_kind, "unknown({})".format(atomic_kind))


def debug_descriptor_valid(snapshot):
    flags = snapshot.get("flags", 0)
    field_count = snapshot.get("field_count", 0)
    fields = snapshot.get("fields", 0)
    has_validity = bool(flags & DEBUG_DESCRIPTOR_HAS_VALIDITY)
    return (
        snapshot.get("magic") == DEBUG_DESCRIPTOR_MAGIC
        and snapshot.get("abi_version") == DEBUG_DESCRIPTOR_ABI_VERSION
        and snapshot.get("layout") in DEBUG_LAYOUT_NAMES
        and snapshot.get("atomic_kind") in DEBUG_ATOMIC_NAMES
        and (flags & ~KNOWN_DEBUG_DESCRIPTOR_FLAGS) == 0
        and (field_count == 0) == (fields == 0)
        and snapshot.get("reserved0", 0) == 0
        and not (snapshot.get("layout") in (3, 4) and not snapshot.get("dynamic_layout", 0))
        and not (snapshot.get("layout") not in (3, 4, 5, 6) and snapshot.get("dynamic_layout", 0))
        and not (snapshot.get("dynamic_layout", 0) and not snapshot.get("element_type", 0))
        and ((snapshot.get("layout") == 7) == bool(snapshot.get("time_series_layout", 0)))
        and (
            (
                has_validity
                and snapshot.get("layout") == 2
                and snapshot.get("validity_word_size") == 8
            )
            or (
                not has_validity
                and snapshot.get("validity_offset", 0) == 0
                and snapshot.get("validity_word_size", 0) == 0
            )
        )
    )


def debug_descriptor_summary(snapshot):
    state = "valid" if debug_descriptor_valid(snapshot) else "invalid"
    return (
        "DebugDescriptor{{{} layout={} atomic={} fields={} validity_offset={} "
        "validity_word_size={} key={} element={} dynamic={} time_series={}}}"
    ).format(
        state,
        debug_layout_text(snapshot.get("layout", 0)),
        debug_atomic_text(snapshot.get("atomic_kind", 0)),
        snapshot.get("field_count", 0),
        snapshot.get("validity_offset", 0),
        snapshot.get("validity_word_size", 0),
        pointer_text(snapshot.get("key_type", 0)),
        pointer_text(snapshot.get("element_type", 0)),
        pointer_text(snapshot.get("dynamic_layout", 0)),
        pointer_text(snapshot.get("time_series_layout", 0)),
    )


def debug_dynamic_layout_valid(snapshot):
    flags = snapshot.get("flags", 0)
    kind = snapshot.get("kind", 0)
    return (
        snapshot.get("magic") == DEBUG_DYNAMIC_LAYOUT_MAGIC
        and snapshot.get("abi_version") == DEBUG_DYNAMIC_LAYOUT_ABI_VERSION
        and kind in DEBUG_DYNAMIC_KIND_NAMES
        and snapshot.get("reserved0", 0) == 0
        and (flags & ~KNOWN_DEBUG_DYNAMIC_FLAGS) == 0
        and snapshot.get("stride", 0) > 0
        and bool(flags & DEBUG_DYNAMIC_SIZE_CONSTANT)
        == (snapshot.get("size_offset", 0) == 0)
        and not (kind == 1 and flags & DEBUG_DYNAMIC_DATA_POINTER_TABLE)
        and not (
            kind == 2
            and not (
                flags & DEBUG_DYNAMIC_DATA_POINTER_TABLE
                and flags & DEBUG_DYNAMIC_DATA_INDIRECT
            )
        )
        and not (flags & DEBUG_DYNAMIC_HAS_SLOT_STATE and kind != 2)
        and not (
            flags & DEBUG_DYNAMIC_DATA_POINTERS_TAGGED
            and not flags & DEBUG_DYNAMIC_DATA_POINTER_TABLE
        )
        and not (
            flags & DEBUG_DYNAMIC_KEY_POINTERS_TAGGED
            and not flags & DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE
        )
        and not (
            flags & DEBUG_DYNAMIC_SLOT_STATE_TAGGED
            and (
                not flags & DEBUG_DYNAMIC_HAS_SLOT_STATE
                or snapshot.get("state_offset", 0) == 0
                or not flags
                & (
                    DEBUG_DYNAMIC_DATA_POINTERS_TAGGED
                    | DEBUG_DYNAMIC_KEY_POINTERS_TAGGED
                )
            )
        )
        and not (
            flags & DEBUG_DYNAMIC_SLOT_INDEX_INDIRECT
            and (
                kind != 2
                or not flags & DEBUG_DYNAMIC_DATA_INDIRECT
                or not flags & DEBUG_DYNAMIC_DATA_POINTER_TABLE
                or flags & DEBUG_DYNAMIC_HAS_HEAD
                or (
                    snapshot.get("key_stride", 0)
                    and (
                        not flags & DEBUG_DYNAMIC_KEY_DATA_INDIRECT
                        or not flags & DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE
                    )
                )
            )
        )
        and not (
            flags & DEBUG_DYNAMIC_SLOT_SIZE_INDIRECT
            and (
                not flags & DEBUG_DYNAMIC_SLOT_INDEX_INDIRECT
                or flags & DEBUG_DYNAMIC_SIZE_CONSTANT
            )
        )
        and not (
            snapshot.get("key_auxiliary_offset", 0)
            and (
                not flags & DEBUG_DYNAMIC_SLOT_INDEX_INDIRECT
                or not snapshot.get("key_stride", 0)
            )
        )
        and not (
            flags & DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS
            and flags & DEBUG_DYNAMIC_ELEMENTS_ARE_POINTERS
        )
    )


def debug_dynamic_layout_summary(snapshot):
    state = "valid" if debug_dynamic_layout_valid(snapshot) else "invalid"
    kind = DEBUG_DYNAMIC_KIND_NAMES.get(snapshot.get("kind", 0), "unknown")
    return (
        "DebugDynamicLayout{{{} kind={} flags=0x{:x} size_offset={} size_constant={} "
        "data_offset={} stride={} key_data_offset={} key_stride={} state_offset={} "
        "auxiliary_offset={} key_auxiliary_offset={} entry_offset={}}}"
    ).format(
        state,
        kind,
        snapshot.get("flags", 0),
        snapshot.get("size_offset", 0),
        snapshot.get("size_constant", 0),
        snapshot.get("data_offset", 0),
        snapshot.get("stride", 0),
        snapshot.get("key_data_offset", 0),
        snapshot.get("key_stride", 0),
        snapshot.get("state_offset", 0),
        snapshot.get("auxiliary_offset", 0),
        snapshot.get("key_auxiliary_offset", 0),
        snapshot.get("entry_offset", 0),
    )


def debug_field_summary(snapshot):
    name = snapshot.get("name")
    return 'DebugField{{name="{}" offset={} type={} validity_bit={} flags=0x{:x}}}'.format(
        name if name is not None else "",
        snapshot.get("offset", 0),
        pointer_text(snapshot.get("type", 0)),
        snapshot.get("validity_bit", 0),
        snapshot.get("flags", 0),
    )


def decode_atomic(atomic_kind, payload, byte_order):
    if atomic_kind == 0 or payload is None:
        return _MISSING
    if atomic_kind == 1:
        return bool(payload[0]) if len(payload) == 1 else _MISSING
    if atomic_kind in (2, 3) and len(payload) in (1, 2, 4, 8):
        return int.from_bytes(payload, byteorder=byte_order, signed=atomic_kind == 2)
    if atomic_kind == 4 and len(payload) in (4, 8):
        prefix = "<" if byte_order == "little" else ">"
        return struct.unpack(prefix + ("f" if len(payload) == 4 else "d"), payload)[0]
    return _MISSING


def field_is_set(validity_payload, validity_bit, word_size, byte_order):
    if word_size <= 0:
        return True
    bits_per_word = word_size * 8
    word_index = validity_bit // bits_per_word
    bit_index = validity_bit % bits_per_word
    start = word_index * word_size
    end = start + word_size
    if validity_payload is None or end > len(validity_payload):
        return False
    word = int.from_bytes(validity_payload[start:end], byteorder=byte_order, signed=False)
    return bool(word & (1 << bit_index))


def pointer_summary(type_name, record_address, data_address, access, record=None, atomic_value=_MISSING):
    if record_address == 0 and data_address == 0:
        return "{}{{unbound}}".format(type_name)
    if record_address == 0 or access not in ACCESS_NAMES:
        return "{}{{malformed record={} data={} access={}}}".format(
            type_name, pointer_text(record_address), pointer_text(data_address), access_text(access)
        )
    state = "typed-null" if data_address == 0 else "live"
    if record is None:
        classification = "unknown"
        semantic = ""
        implementation = ""
    else:
        schema = record.get("schema") or {}
        classification = "{}{}/{} kind={}".format(
            "invalid-record " if not record_valid(record) else "",
            family_text(schema.get("family", 0)),
            role_text(record.get("role", 0)),
            kind_text(schema.get("family", 0), schema.get("kind", TYPE_KIND_NONE)),
        )
        semantic = schema.get("label") or ""
        implementation = record.get("implementation_label") or "<semantic>"
    value_text = " value={!r}".format(atomic_value) if atomic_value is not _MISSING else ""
    return '{}{{{} {} access={} semantic="{}" implementation="{}" record={} data={}{}'.format(
        type_name,
        state,
        classification,
        access_text(access),
        semantic,
        implementation,
        pointer_text(record_address),
        pointer_text(data_address),
        value_text,
    ) + "}"


def compact_pointer_summary(type_name, record_address, data_address, access, record=None, atomic_value=_MISSING):
    """User-facing carrier summary; full ABI identity remains on the expanded record."""
    if record_address == 0 and data_address == 0:
        return "{}{{unbound}}".format(type_name)
    if record_address == 0 or access not in ACCESS_NAMES or record is None or not record_valid(record):
        return "{}{{malformed}}".format(type_name)
    semantic = record["schema"].get("label") or "<?>"
    parts = [semantic]
    if data_address == 0:
        parts.append("null")
    elif atomic_value is not _MISSING:
        parts.append("value={!r}".format(atomic_value))
    return "{}{{{}}}".format(type_name, " ".join(parts))


def typed_pointer_name(record):
    if not record or not record.get("schema"):
        return "TypedPtr"
    family = record["schema"].get("family")
    role = record.get("role")
    return {
        (1, 1): "ValuePtr",
        (2, 2): "TSDataPtr",
        (2, 4): "TSInputPtr",
        (2, 5): "TSOutputPtr",
        (3, 3): "NodePtr",
        (4, 3): "GraphPtr",
        (5, 3): "ExecutorPtr",
        (6, 3): "ClockPtr",
    }.get((family, role), "TypedPtr")
