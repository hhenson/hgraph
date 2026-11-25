"""Read scalar endpoint descriptions from the matching SDK's debug descriptors."""

import lldb
import hgraph_lldb as hg

UNAVAILABLE = "<unavailable>"
INVALID = "<invalid>"


def storage(view):
    return (hg.type_ref_record(hg.child(view, "type_")), hg.pointer(hg.child(view, "data_")))


def descriptor(record):
    return hg.descriptor_snapshot(hg.dereference(hg.child(record, "debug")))


def scalar_text(record, address):
    info = descriptor(record)
    if not hg.common.debug_descriptor_valid(info):
        return UNAVAILABLE
    schema = hg.schema_snapshot(hg.dereference(hg.child(record, "schema")))
    if schema["kind"] == 9:  # Optional has one field and a presence bit.
        if info["layout"] != 2 or info["field_count"] != 1 or not info["validity_word_size"]:
            return UNAVAILABLE
        field = next(hg.descriptor_fields(record, info), None)
        if field is None:
            return UNAVAILABLE
        field = field[2]
        bits = hg.read_memory(record, address + info["validity_offset"], info["validity_word_size"])
        if bits is None:
            return UNAVAILABLE
        if not hg.common.field_is_set(bits, field["validity_bit"], len(bits), hg.target_byte_order(record)):
            return "null"
        return scalar_text(hg.record_at(record, field["type"]), address + field["offset"])
    if info["layout"] != 1:
        return UNAVAILABLE
    if info["atomic_kind"] == 5:  # Use the platform's std::string formatter.
        value = hg.value_at_address(record, "std::string", address, "value")
        return value.GetSummary() or UNAVAILABLE
    value = hg.atomic_value(record, address)
    if value is hg.common._MISSING:
        return UNAVAILABLE
    return str(value).lower() if isinstance(value, bool) else str(value)


def resolve(value):
    cursor = hg.child(value, "data_")
    raw_view = hg.child(cursor, "raw_data")
    view = raw_view if hg.valid(raw_view) else cursor
    record_addr, address = storage(hg.child(view, "storage_"))
    return resolve_storage(value, record_addr, address)


def resolve_storage(value, record_addr, address):
    if not record_addr:
        return None
    record = hg.record_at(value, record_addr)
    schema = hg.schema_snapshot(hg.dereference(hg.child(record, "schema")))
    if (schema["family"], schema["kind"]) != (2, 0):
        return None  # The raw view can describe the parent of a collection element.
    seen = set()
    latest = 0
    while (record_addr, address) not in seen and len(seen) < 64:
        seen.add((record_addr, address))
        info = descriptor(record)
        if not hg.common.debug_descriptor_valid(info) or info["layout"] != 7:
            return UNAVAILABLE
        layout = hg.time_series_layout_at(value, info["time_series_layout"])
        offset = hg.child(layout, "target_handle_offset")
        if not hg.valid(offset):
            return UNAVAILABLE
        borrowed = hg.pointer(hg.child(layout, "borrowed_source"))
        if borrowed:
            record_addr = borrowed
        else:
            if not address:
                return INVALID
            timestamp = hg.read_unsigned(value, address + hg.integer(hg.child(layout, "last_modified_offset")), 8)
            if timestamp is None:
                return UNAVAILABLE
            latest = max(latest, timestamp)
            if hg.integer(offset) == (1 << (offset.GetByteSize() * 8)) - 1:
                if not hg.integer(hg.child(layout, "inline_scalar")):
                    return UNAVAILABLE
                if timestamp == 0:
                    return INVALID
                return record_addr, address, layout, latest
            handle = hg.value_at_address(value, "hgraph::TSOutputHandle", address + hg.integer(offset), "target")
            target = hg.child(handle, "data_")
            if not hg.valid(hg.child(target, "type_")):
                return UNAVAILABLE
            record_addr, address = storage(target)
            if not record_addr:
                return INVALID
        record = hg.record_at(value, record_addr)
    return UNAVAILABLE


def display(value, resolved, now):
    if not isinstance(resolved, tuple):
        return resolved or UNAVAILABLE
    _, address, layout, latest = resolved
    record = hg.record_at(value, hg.pointer(hg.child(layout, "value_type")))
    text = scalar_text(record, address + hg.integer(hg.child(layout, "value_offset")))
    return text + (" [modified]" if latest == now and text != UNAVAILABLE else "")


def evaluation_time(value):
    error = lldb.SBError()
    now = value.GetData().GetSignedInt64(error, 0)
    return now if error.Success() else None


def summary(value, _internal_dict, _options=None):
    return display(value, resolve(value), evaluation_time(hg.child(value, "evaluation_time_")))
