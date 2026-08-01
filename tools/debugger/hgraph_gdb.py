"""Read-only GDB printers for hgraph's common type-erasure ABI.

Load with::

    (gdb) source /path/to/hg_cpp/tools/debugger/hgraph_gdb.py

The printers read debug-info fields and inferior memory only. They never call
functions in the stopped process.
"""

import os
import sys

import gdb
import gdb.printing

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import hgraph_debug_common as common


def field(value, name):
    try:
        return value[name]
    except Exception:
        value_type = value.type.strip_typedefs()
        for member in value_type.fields():
            if member.is_base_class:
                try:
                    return field(value.cast(member.type), name)
                except Exception:
                    pass
        raise


def integer(value):
    try:
        return int(value)
    except Exception:
        return 0


def pointer(value):
    return integer(value)


def c_string(value):
    if pointer(value) == 0:
        return None
    try:
        return value.string()
    except Exception:
        return None


def dereference(value):
    if pointer(value) == 0:
        return None
    try:
        return value.dereference()
    except Exception:
        return None


def schema_snapshot(value):
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "family": integer(field(value, "family")),
        "kind": integer(field(value, "kind")),
        "label": c_string(field(value, "label")),
        "introspection": pointer(field(value, "introspection")),
    }


def record_snapshot(value):
    schema_value = dereference(field(value, "schema"))
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "role": integer(field(value, "role")),
        "reserved0": integer(field(value, "reserved0")),
        "ops_abi_version": integer(field(value, "ops_abi_version")),
        "reserved1": integer(field(value, "reserved1")),
        "capabilities": integer(field(value, "capabilities")),
        "implementation_label": c_string(field(value, "implementation_label")),
        "schema": schema_snapshot(schema_value) if schema_value is not None else None,
        "plan": pointer(field(value, "plan")),
        "ops": pointer(field(value, "ops")),
        "debug": pointer(field(value, "debug")),
    }


def record_at(address):
    if address == 0:
        return None
    try:
        record_type = gdb.lookup_type("hgraph::TypeRecord")
        return gdb.Value(address).cast(record_type.pointer()).dereference()
    except Exception:
        return None


def descriptor_at(address):
    if address == 0:
        return None
    try:
        descriptor_type = gdb.lookup_type("hgraph::DebugDescriptor")
        return gdb.Value(address).cast(descriptor_type.pointer()).dereference()
    except Exception:
        return None


def dynamic_layout_at(address):
    if address == 0:
        return None
    try:
        layout_type = gdb.lookup_type("hgraph::DebugDynamicLayout")
        return gdb.Value(address).cast(layout_type.pointer()).dereference()
    except Exception:
        return None


def time_series_layout_at(address):
    if address == 0:
        return None
    try:
        layout_type = gdb.lookup_type("hgraph::DebugTimeSeriesLayout")
        return gdb.Value(address).cast(layout_type.pointer()).dereference()
    except Exception:
        return None


def descriptor_snapshot(value):
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "layout": integer(field(value, "layout")),
        "atomic_kind": integer(field(value, "atomic_kind")),
        "flags": integer(field(value, "flags")),
        "field_count": integer(field(value, "field_count")),
        "fields": pointer(field(value, "fields")),
        "validity_offset": integer(field(value, "validity_offset")),
        "validity_word_size": integer(field(value, "validity_word_size")),
        "reserved0": integer(field(value, "reserved0")),
        "key_type": pointer(field(value, "key_type")),
        "element_type": pointer(field(value, "element_type")),
        "dynamic_layout": pointer(field(value, "dynamic_layout")),
        "time_series_layout": pointer(field(value, "time_series_layout")),
    }


def debug_field_snapshot(value):
    return {
        "name": c_string(field(value, "name")),
        "offset": integer(field(value, "offset")),
        "type": pointer(field(value, "type")),
        "validity_bit": integer(field(value, "validity_bit")),
        "flags": integer(field(value, "flags")),
    }


def dynamic_layout_snapshot(value):
    return {
        "magic": integer(field(value, "magic")),
        "abi_version": integer(field(value, "abi_version")),
        "kind": integer(field(value, "kind")),
        "reserved0": integer(field(value, "reserved0")),
        "flags": integer(field(value, "flags")),
        "reserved1": integer(field(value, "reserved1")),
        "size_offset": integer(field(value, "size_offset")),
        "size_constant": integer(field(value, "size_constant")),
        "data_offset": integer(field(value, "data_offset")),
        "stride": integer(field(value, "stride")),
        "key_data_offset": integer(field(value, "key_data_offset")),
        "key_stride": integer(field(value, "key_stride")),
        "state_offset": integer(field(value, "state_offset")),
        "auxiliary_offset": integer(field(value, "auxiliary_offset")),
        "entry_offset": integer(field(value, "entry_offset")),
    }


def descriptor_fields(value, snapshot):
    fields = field(value, "fields")
    for index in range(snapshot["field_count"]):
        try:
            yield index, fields[index], debug_field_snapshot(fields[index])
        except Exception:
            return


def target_byte_order():
    try:
        output = gdb.execute("show endian", to_string=True).lower()
        return "big" if "big endian" in output else "little"
    except Exception:
        return "little"


def target_pointer_size():
    return int(gdb.lookup_type("void").pointer().sizeof)


def read_memory(address, size):
    if address == 0 or size <= 0:
        return None
    try:
        return bytes(gdb.selected_inferior().read_memory(address, size))
    except Exception:
        return None


def read_unsigned(address, size=None):
    size = target_pointer_size() if size is None else size
    payload = read_memory(address, size)
    return int.from_bytes(payload, target_byte_order()) if payload is not None else None


def record_plan_size(record_value):
    try:
        plan = dereference(field(record_value, "plan"))
        return integer(field(field(plan, "layout"), "size")) if plan is not None else 0
    except Exception:
        return 0


def atomic_value(record_value, data_address):
    try:
        descriptor_value = descriptor_at(pointer(field(record_value, "debug")))
        if descriptor_value is None:
            return common._MISSING
        snapshot = descriptor_snapshot(descriptor_value)
        if not common.debug_descriptor_valid(snapshot) or snapshot["layout"] != 1:
            return common._MISSING
        if snapshot["atomic_kind"] == 5:
            string_value = value_at_address("std::string", data_address)
            string_data = pointer(field(field(string_value, "_M_dataplus"), "_M_p"))
            string_size = integer(field(string_value, "_M_string_length"))
            string_payload = read_memory(string_data, string_size)
            return (
                string_payload.decode("utf-8", errors="replace")
                if string_payload is not None
                else common._MISSING
            )
        payload = read_memory(data_address, record_plan_size(record_value))
        return common.decode_atomic(snapshot["atomic_kind"], payload, target_byte_order())
    except Exception:
        return common._MISSING


def pointer_payload(record_value, data_address):
    payload = atomic_value(record_value, data_address)
    if payload is not common._MISSING:
        return payload, None
    try:
        descriptor = descriptor_at(pointer(field(record_value, "debug")))
        snapshot = descriptor_snapshot(descriptor)
        if snapshot["layout"] != 7:
            return common._MISSING, None
        layout = time_series_layout_at(snapshot["time_series_layout"])
        value_record = record_at(pointer(field(layout, "value_type")))
        value = atomic_value(value_record, data_address + integer(field(layout, "value_offset")))
        raw_time = read_unsigned(data_address + integer(field(layout, "last_modified_offset")), 8)
        if raw_time is not None and raw_time & (1 << 63):
            raw_time -= 1 << 64
        return value, raw_time
    except Exception:
        return common._MISSING, None


def make_any_pointer(record_address, data_address, access):
    try:
        size = target_pointer_size()
        order = target_byte_order()
        payload = (record_address | access).to_bytes(size, order) + data_address.to_bytes(size, order)
        return gdb.Value(payload, gdb.lookup_type("hgraph::AnyPtr"))
    except Exception:
        return None


def make_owner_pointer(owner_address, fallback_record, access):
    size = target_pointer_size()
    record_address = read_unsigned(owner_address)
    state_word = read_unsigned(owner_address + size)
    if record_address is None or state_word is None:
        return None
    if record_address == 0:
        record_address = fallback_record
    state = state_word & common.DEBUG_OWNER_STATE_MASK
    if state == 0:
        data_address = 0
    elif state == common.DEBUG_OWNER_INLINE_STATE:
        data_address = owner_address + 2 * size
    elif state == common.DEBUG_OWNER_HEAP_STATE:
        data_address = read_unsigned(owner_address + 2 * size)
        if data_address is None:
            return None
    else:
        return None
    return make_any_pointer(record_address, data_address, access if data_address else 0)


def make_embedded_pointer(pointer_address, fallback_record=0):
    size = target_pointer_size()
    tagged_record = read_unsigned(pointer_address)
    data_address = read_unsigned(pointer_address + size)
    if tagged_record is None or data_address is None:
        return None
    record_address = tagged_record & ~0x3
    access = tagged_record & 0x3
    if record_address == 0:
        record_address = fallback_record
    return make_any_pointer(record_address, data_address, access if data_address else 0)


def make_indirect_embedded_pointer(pointer_address, fallback_record=0):
    object_address = read_unsigned(pointer_address)
    return make_embedded_pointer(object_address, fallback_record) if object_address else None


def make_ts_parent_pointer(data_address, layout):
    parent_address = data_address + integer(field(layout, "parent_offset"))
    tagged_record = read_unsigned(parent_address)
    parent_data = read_unsigned(parent_address + target_pointer_size())
    if tagged_record is None or parent_data is None:
        return None, None
    kind = tagged_record & 0x7
    if kind not in (1, 4) or parent_data == 0:
        return None, kind
    return make_any_pointer(tagged_record & ~0x7, parent_data, 0), kind


def ts_subscribers(data_address, layout):
    encoded = read_unsigned(data_address + integer(field(layout, "observers_offset")))
    if not encoded:
        return
    pointer_address = encoded & ~0x1
    if (encoded & 0x1) == 0:
        try:
            yield dynamic_pointer(
                gdb.Value(pointer_address).cast(gdb.lookup_type("hgraph::Notifiable").pointer())
            )
        except Exception:
            return
        return
    try:
        observer_list = value_at_address("hgraph::TSDataObserverSet::ObserverList", pointer_address)
        entries = field(observer_list, "entries")
        implementation = field(entries, "_M_impl")
        start = pointer(field(implementation, "_M_start"))
        finish = pointer(field(implementation, "_M_finish"))
        if finish < start or (finish - start) % target_pointer_size() != 0:
            return
        subscriber_type = gdb.lookup_type("hgraph::Notifiable").pointer()
        for address in range(start, finish, target_pointer_size()):
            subscriber = read_unsigned(address)
            if subscriber:
                yield dynamic_pointer(gdb.Value(subscriber).cast(subscriber_type))
    except Exception:
        return


def dynamic_pointer(value):
    try:
        dynamic_type = value.dynamic_type
        if dynamic_type is not None and dynamic_type.code == gdb.TYPE_CODE_PTR:
            return value.cast(dynamic_type)
    except Exception:
        pass
    return value


def dynamic_child_addresses(data_address, layout):
    flags = layout["flags"]
    size = (
        layout["size_constant"]
        if flags & common.DEBUG_DYNAMIC_SIZE_CONSTANT
        else read_unsigned(data_address + layout["size_offset"])
    )
    if size is None or size > (1 << 30):
        return
    visible_size = min(size, 4096)
    head = (
        read_unsigned(data_address + layout["auxiliary_offset"])
        if flags & common.DEBUG_DYNAMIC_HAS_HEAD
        else 0
    )
    if head is None:
        return
    data_base = data_address + layout["data_offset"]
    if flags & common.DEBUG_DYNAMIC_DATA_INDIRECT:
        data_base = read_unsigned(data_base)
    if data_base is None:
        return
    key_base = 0
    if layout["key_stride"]:
        key_base = data_address + layout["key_data_offset"]
        if flags & common.DEBUG_DYNAMIC_KEY_DATA_INDIRECT:
            key_base = read_unsigned(key_base)
        if key_base is None:
            return
    state_words = 0
    state_bits = 0
    if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
        state_words = read_unsigned(data_address + layout["state_offset"])
        state_bits = (
            size
            if flags & common.DEBUG_DYNAMIC_SLOT_STATE_TAGGED
            else read_unsigned(data_address + layout["state_offset"] + target_pointer_size())
        )
        if state_words is None or state_bits is None:
            return
    for logical_index in range(visible_size):
        physical_index = (head + logical_index) % size if size and flags & common.DEBUG_DYNAMIC_HAS_HEAD else logical_index
        if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
            if physical_index >= state_bits:
                continue
            if flags & common.DEBUG_DYNAMIC_SLOT_STATE_TAGGED:
                state = read_unsigned(state_words + physical_index * target_pointer_size())
                if state is None or state & 0x3:
                    continue
            else:
                word = read_unsigned(state_words + (physical_index // 64) * 8, 8)
                if word is None or not (word & (1 << (physical_index % 64))):
                    continue
        if flags & common.DEBUG_DYNAMIC_DATA_POINTER_TABLE:
            element = read_unsigned(data_base + physical_index * target_pointer_size())
            if element is not None and flags & common.DEBUG_DYNAMIC_DATA_POINTERS_TAGGED:
                element &= ~0x3
        else:
            element = data_base + physical_index * layout["stride"]
        if key_base:
            if flags & common.DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE:
                key = read_unsigned(key_base + physical_index * target_pointer_size())
                if key is not None and flags & common.DEBUG_DYNAMIC_KEY_POINTERS_TAGGED:
                    key &= ~0x3
            else:
                key = key_base + physical_index * layout["key_stride"]
        else:
            key = 0
        if element:
            element += layout["entry_offset"]
        yield logical_index, key, element


def pointer_shape(record_value, data_address):
    """Return a compact structured-value size without invoking inferior code."""
    try:
        descriptor = descriptor_at(pointer(field(record_value, "debug")))
        snapshot = descriptor_snapshot(descriptor)
        if not common.debug_descriptor_valid(snapshot):
            return None
        if snapshot["layout"] == 7:
            layout = time_series_layout_at(snapshot["time_series_layout"])
            value_record = record_at(pointer(field(layout, "value_type")))
            return pointer_shape(value_record, data_address + integer(field(layout, "value_offset")))
        if snapshot["layout"] == 2:
            return "fields={}".format(snapshot["field_count"])
        if snapshot["layout"] in (3, 4, 5, 6):
            dynamic = dynamic_layout_at(snapshot["dynamic_layout"])
            layout = dynamic_layout_snapshot(dynamic)
            if common.debug_dynamic_layout_valid(layout):
                count = sum(1 for _ in dynamic_child_addresses(data_address, layout))
                return "size={}".format(count)
    except Exception:
        pass
    return None


def any_pointer_value(value):
    type_name = str(value.type.strip_typedefs())
    if "TypedPtr<" in type_name:
        return field(value, "value_")
    return value


def pointer_state(value):
    erased = any_pointer_value(value)
    tagged = field(erased, "type_")
    bits = integer(field(tagged, "m_bits"))
    return bits & ~0x3, pointer(field(erased, "data_")), bits & 0x3


def display_type_name(value):
    type_name = str(value.type.strip_typedefs())
    if type_name == "hgraph::AnyPtr":
        return "AnyPtr"
    return "TypedPtr"


class SchemaHeaderPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.schema_summary(schema_snapshot(self.value))
        except Exception as exc:
            return "SchemaHeader{<printer error: %s>}" % exc

    def children(self):
        for name in ("magic", "abi_version", "family", "kind", "label", "introspection"):
            try:
                yield name, field(self.value, name)
            except Exception:
                return


class TypeRecordPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.record_summary(record_snapshot(self.value))
        except Exception as exc:
            return "TypeRecord{<printer error: %s>}" % exc

    def children(self):
        try:
            schema_pointer = field(self.value, "schema")
            schema_value = dereference(schema_pointer)
            if schema_value is not None:
                yield "schema", schema_value
            yield "schema_ptr", schema_pointer
            debug_pointer = field(self.value, "debug")
            debug_value = descriptor_at(pointer(debug_pointer))
            if debug_value is not None:
                yield "debug_descriptor", debug_value
            for name in (
                "role",
                "capabilities",
                "implementation_label",
                "plan",
                "ops",
                "debug",
                "magic",
                "abi_version",
                "ops_abi_version",
            ):
                yield name, field(self.value, name)
        except Exception:
            return


class DebugDescriptorPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.debug_descriptor_summary(descriptor_snapshot(self.value))
        except Exception as exc:
            return "DebugDescriptor{<printer error: %s>}" % exc

    def children(self):
        try:
            snapshot = descriptor_snapshot(self.value)
            for index, field_value, field_snapshot in descriptor_fields(self.value, snapshot):
                name = field_snapshot["name"] or "[{}]".format(index)
                yield name, field_value
            dynamic_value = dynamic_layout_at(snapshot["dynamic_layout"])
            if dynamic_value is not None:
                yield "dynamic_layout_value", dynamic_value
            time_series_value = time_series_layout_at(snapshot["time_series_layout"])
            if time_series_value is not None:
                yield "time_series_layout_value", time_series_value
            for name in (
                "layout",
                "atomic_kind",
                "flags",
                "validity_offset",
                "validity_word_size",
                "key_type",
                "element_type",
                "dynamic_layout",
                "time_series_layout",
                "magic",
                "abi_version",
            ):
                yield name, field(self.value, name)
        except Exception:
            return


class DebugDynamicLayoutPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.debug_dynamic_layout_summary(dynamic_layout_snapshot(self.value))
        except Exception as exc:
            return "DebugDynamicLayout{<printer error: %s>}" % exc

    def children(self):
        for name in (
            "kind",
            "flags",
            "size_offset",
            "size_constant",
            "data_offset",
            "stride",
            "key_data_offset",
            "key_stride",
            "state_offset",
            "auxiliary_offset",
            "entry_offset",
            "magic",
            "abi_version",
        ):
            try:
                yield name, field(self.value, name)
            except Exception:
                return


class DebugTimeSeriesLayoutPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        return "DebugTimeSeriesLayout{value_offset=%d tracking_offset=%d}" % (
            integer(field(self.value, "value_offset")),
            integer(field(self.value, "tracking_offset")),
        )

    def children(self):
        for name in ("value_type", "delta_type", "value_offset", "tracking_offset", "last_modified_offset", "parent_offset", "observers_offset", "delta_aliases_value"):
            yield name, field(self.value, name)


class DebugFieldPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            return common.debug_field_summary(debug_field_snapshot(self.value))
        except Exception as exc:
            return "DebugField{<printer error: %s>}" % exc

    def children(self):
        try:
            type_pointer = field(self.value, "type")
            type_value = record_at(pointer(type_pointer))
            if type_value is not None:
                yield "type_record", type_value
            for name in ("name", "offset", "type", "validity_bit", "flags"):
                yield name, field(self.value, name)
        except Exception:
            return


class TypePointerPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        try:
            record_address, data_address, access = pointer_state(self.value)
            record_value = record_at(record_address)
            snapshot = record_snapshot(record_value) if record_value is not None else None
            payload, last_modified = (
                pointer_payload(record_value, data_address)
                if record_value is not None and data_address
                else (common._MISSING, None)
            )
            carrier = display_type_name(self.value)
            if carrier == "TypedPtr":
                carrier = common.typed_pointer_name(snapshot)
            summary = common.compact_pointer_summary(
                carrier, record_address, data_address, access, snapshot, payload
            )
            if last_modified is not None:
                summary = summary[:-1] + " last_modified_us={}".format(last_modified) + "}"
            shape = pointer_shape(record_value, data_address) if record_value is not None and data_address else None
            if shape:
                summary += " {}".format(shape)
            return summary
        except Exception as exc:
            return "TypePointer{<printer error: %s>}" % exc

    def children(self):
        try:
            record_address, data_address, access = pointer_state(self.value)
            record_value = record_at(record_address)
            record_info = record_snapshot(record_value) if record_value is not None else None
            if record_value is not None:
                yield "record", record_value
            erased = any_pointer_value(self.value)
            yield "data", field(erased, "data_")
            yield "access", gdb.Value(access)

            if record_value is None or record_address == 0 or data_address == 0 or access not in common.ACCESS_NAMES:
                return
            descriptor_value = descriptor_at(pointer(field(record_value, "debug")))
            if descriptor_value is None:
                return
            snapshot = descriptor_snapshot(descriptor_value)
            if not common.debug_descriptor_valid(snapshot):
                return
            if (
                record_info
                and record_info.get("schema")
                and record_info["schema"]["family"] == 3
                and record_info["role"] == 3
            ):
                for name, type_name in (("input", "hgraph::TSInput"), ("output", "hgraph::TSOutput")):
                    try:
                        _, address = plan_component(self.value, self.value, name)
                        if address:
                            yield name, value_at_address(type_name, address)
                    except Exception:
                        pass
            if snapshot["layout"] == 7:
                ts_layout = time_series_layout_at(snapshot["time_series_layout"])
                if ts_layout is None:
                    return
                value_type = pointer(field(ts_layout, "value_type"))
                value_offset = integer(field(ts_layout, "value_offset"))
                current = make_any_pointer(value_type, data_address + value_offset, access)
                if current is not None:
                    yield "value", current
                    if snapshot["field_count"] == 0:
                        for child_name, child_value in TypePointerPrinter(current).children():
                            if child_name not in ("record", "data", "access"):
                                yield child_name, child_value
                for index, _, field_snapshot in descriptor_fields(descriptor_value, snapshot):
                    child = make_any_pointer(
                        field_snapshot["type"],
                        data_address + field_snapshot["offset"],
                        access,
                    )
                    if child is not None:
                        yield field_snapshot["name"] or "[{}]".format(index), child
                last_modified_offset = integer(field(ts_layout, "last_modified_offset"))
                try:
                    dt_type = gdb.lookup_type("hgraph::DateTime")
                    last_modified = gdb.Value(data_address + last_modified_offset).cast(dt_type.pointer()).dereference()
                    yield "last_modified", last_modified
                except Exception:
                    pass
                parent, parent_kind = make_ts_parent_pointer(data_address, ts_layout)
                if parent is not None:
                    yield "parent" if parent_kind == 1 else "owner_node", parent
                for index, subscriber in enumerate(ts_subscribers(data_address, ts_layout)):
                    yield "subscriber[{}]".format(index), subscriber
                return
            if snapshot["layout"] in (2, 5, 6):
                validity = None
                if snapshot["flags"] & common.DEBUG_DESCRIPTOR_HAS_VALIDITY:
                    bits_per_word = snapshot["validity_word_size"] * 8
                    word_count = (snapshot["field_count"] + bits_per_word - 1) // bits_per_word
                    validity = read_memory(
                        data_address + snapshot["validity_offset"],
                        word_count * snapshot["validity_word_size"],
                    )
                for index, _, field_snapshot in descriptor_fields(descriptor_value, snapshot):
                    name = field_snapshot["name"] or "[{}]".format(index)
                    is_set = common.field_is_set(
                        validity,
                        field_snapshot["validity_bit"],
                        snapshot["validity_word_size"]
                        if snapshot["flags"] & common.DEBUG_DESCRIPTOR_HAS_VALIDITY
                        else 0,
                        target_byte_order(),
                    )
                    if field_snapshot["flags"] & common.DEBUG_FIELD_EMBEDDED_OWNER:
                        child = make_owner_pointer(
                            data_address + field_snapshot["offset"], field_snapshot["type"], access
                        )
                    elif field_snapshot["flags"] & common.DEBUG_FIELD_EMBEDDED_POINTER:
                        child = make_embedded_pointer(
                            data_address + field_snapshot["offset"], field_snapshot["type"]
                        )
                    elif field_snapshot["flags"] & common.DEBUG_FIELD_INDIRECT_EMBEDDED_POINTER:
                        child = make_indirect_embedded_pointer(
                            data_address + field_snapshot["offset"], field_snapshot["type"]
                        )
                    else:
                        child = make_any_pointer(
                            field_snapshot["type"],
                            data_address + field_snapshot["offset"] if is_set else 0,
                            access if is_set else 0,
                        )
                    if child is not None:
                        yield name, child
                if not snapshot["dynamic_layout"]:
                    return
            if snapshot["layout"] not in (3, 4, 5, 6):
                return
            dynamic_value = dynamic_layout_at(snapshot["dynamic_layout"])
            if dynamic_value is None:
                return
            layout = dynamic_layout_snapshot(dynamic_value)
            if not common.debug_dynamic_layout_valid(layout):
                return
            for slot, key_address, element_address in dynamic_child_addresses(data_address, layout):
                if key_address and snapshot["key_type"]:
                    if layout["flags"] & common.DEBUG_DYNAMIC_KEYS_ARE_OWNERS:
                        key_child = make_owner_pointer(key_address, snapshot["key_type"], access)
                    else:
                        key_child = make_any_pointer(snapshot["key_type"], key_address, access)
                    if key_child is not None:
                        yield "key[{}]".format(slot), key_child
                if element_address:
                    if layout["flags"] & common.DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS:
                        element_child = make_owner_pointer(element_address, snapshot["element_type"], access)
                    elif layout["flags"] & common.DEBUG_DYNAMIC_ELEMENTS_ARE_POINTERS:
                        element_child = make_embedded_pointer(element_address, snapshot["element_type"])
                    else:
                        element_child = make_any_pointer(snapshot["element_type"], element_address, access)
                    if element_child is not None:
                        yield "value[{}]".format(slot) if snapshot["key_type"] else "[{}]".format(slot), element_child
        except Exception:
            return


def type_ref_record(value):
    return pointer(field(value, "record_"))


def ts_storage_pointer(value):
    record_address = type_ref_record(field(value, "type_"))
    data_address = pointer(field(value, "data_"))
    return make_any_pointer(record_address, data_address, 0)


def ts_view_pointer(value):
    return ts_storage_pointer(field(value, "storage_"))


def ts_owner_pointer(value):
    return make_any_pointer(
        type_ref_record(field(value, "type_")), pointer(field(value, "data_")), 0
    )


def value_at_address(type_name, address):
    return gdb.Value(address).cast(gdb.lookup_type(type_name).pointer()).dereference()


def plan_component(value, pointer_value, component_name):
    record_address, data_address, _ = pointer_state(pointer_value)
    record = record_at(record_address)
    plan = dereference(field(record, "plan"))
    state_address = pointer(field(plan, "lifecycle_context"))
    state_type = gdb.lookup_type("hgraph::MemoryUtils::CompositeState")
    component_type = gdb.lookup_type("hgraph::MemoryUtils::CompositeComponent")
    state = value_at_address("hgraph::MemoryUtils::CompositeState", state_address)
    alignment = component_type.alignof
    first = state_address + ((state_type.sizeof + alignment - 1) // alignment) * alignment
    for index in range(integer(field(state, "component_count"))):
        component = gdb.Value(first + index * component_type.sizeof).cast(component_type.pointer()).dereference()
        if c_string(field(component, "name")) == component_name:
            return component, data_address + integer(field(component, "offset"))
    return None, 0


def node_component(value, component_name, type_name):
    _, address = plan_component(value, field(value, "pointer_"), component_name)
    return value_at_address(type_name, address) if address else None


def pointer_printer_summary(value, label):
    if value is None:
        return "{}{{unbound}}".format(label)
    summary = TypePointerPrinter(value).to_string()
    body = summary[summary.find("{"):] if "{" in summary else "{" + summary + "}"
    return label + body


def concise_wrapper_summary(value, label):
    if value is None:
        return "{}{{unbound}}".format(label)
    try:
        record_address, data_address, _ = pointer_state(value)
        record = record_at(record_address)
        snapshot = record_snapshot(record)
        semantic = snapshot["schema"]["label"]
        payload, last_modified = pointer_payload(record, data_address) if data_address else (common._MISSING, None)
        parts = [semantic]
        if payload is not common._MISSING:
            parts.append("value={!r}".format(payload))
        shape = pointer_shape(record, data_address) if data_address else None
        if shape:
            parts.append(shape)
        if last_modified is not None:
            parts.append("modified={}us".format(last_modified))
        return "{}{{{}}}".format(label, " ".join(parts))
    except Exception:
        return pointer_printer_summary(value, label)


def date_time_count(value):
    try:
        return integer(field(field(value, "__d"), "__r"))
    except Exception:
        return integer(value)


def ts_delta_pointer(ts_pointer, evaluation_time):
    record_address, data_address, access = pointer_state(ts_pointer)
    record = record_at(record_address)
    descriptor = descriptor_at(pointer(field(record, "debug")))
    snapshot = descriptor_snapshot(descriptor)
    if snapshot["layout"] != 7:
        return None
    layout = time_series_layout_at(snapshot["time_series_layout"])
    if not bool(field(layout, "delta_aliases_value")):
        return None
    value_type = pointer(field(layout, "delta_type"))
    last_modified = read_unsigned(data_address + integer(field(layout, "last_modified_offset")), 8)
    if last_modified is not None and last_modified & (1 << 63):
        last_modified -= 1 << 64
    live = last_modified == date_time_count(evaluation_time)
    return make_any_pointer(
        value_type,
        data_address + integer(field(layout, "value_offset")) if live else 0,
        access if live else 0,
    )


class TypeRefPrinter:
    def __init__(self, value):
        self.value = value

    def _pointer(self):
        return make_any_pointer(type_ref_record(self.value), 0, 0)

    def to_string(self):
        name = str(self.value.type.strip_typedefs()).split("::")[-1]
        return concise_wrapper_summary(self._pointer(), name)

    def children(self):
        record = record_at(type_ref_record(self.value))
        if record is not None:
            yield "record", record


class RuntimeWrapperPrinter:
    """Small adapters which expose existing objects through the common pointer printer."""

    def __init__(self, value):
        self.value = value
        self.name = str(value.type.strip_typedefs()).split("::")[-1]

    def _primary(self):
        name = self.name
        if name == "Value":
            return make_owner_pointer(pointer(field(self.value, "storage_")), 0, 0)
        if name == "ValueView":
            return field(self.value, "pointer_")
        if name in ("NodeView", "GraphView", "RootGraphView", "NestedGraphView"):
            return field(self.value, "pointer_")
        if name == "TSDataView":
            return ts_view_pointer(self.value)
        if name in ("TSData", "TSDataOwnedStorage"):
            return ts_owner_pointer(self.value if name == "TSDataOwnedStorage" else field(self.value, "storage_"))
        if name in ("TSInput", "TSOutput"):
            return ts_owner_pointer(field(field(self.value, "data_"), "storage_"))
        if name == "TSOutputView":
            return ts_view_pointer(field(self.value, "data_"))
        if name == "TSOutputHandle":
            return ts_storage_pointer(field(self.value, "data_"))
        if name == "TSInputView":
            cursor = field(self.value, "data_")
            return ts_view_pointer(field(cursor, "value_data"))
        return None

    def to_string(self):
        try:
            return concise_wrapper_summary(self._primary(), self.name)
        except Exception as exc:
            return "{}{{<printer error: {}>}}".format(self.name, exc)

    def children(self):
        try:
            primary = self._primary()
            if primary is not None:
                primary_name = {
                    "TSInputView": "source_data",
                    "TSOutputView": "data",
                    "TSDataView": "data",
                }.get(self.name, "pointer")
                yield primary_name, primary
                forwarded = {
                    "TSData": {"value", "parent", "owner_node"},
                    "TSDataView": {"value", "parent", "owner_node"},
                    "TSInput": {"value", "parent", "owner_node"},
                    "TSInputView": {"value", "parent", "owner_node"},
                    "TSOutput": {"value", "parent", "owner_node"},
                    "TSOutputView": {"value", "parent", "owner_node"},
                    "NodeView": {"graph", "state", "scalars"},
                }.get(self.name, set())
                for child_name, child_value in TypePointerPrinter(primary).children():
                    structured_ts_child = self.name in (
                        "TSData", "TSDataView", "TSInput", "TSInputView", "TSOutput", "TSOutputView"
                    ) and child_name not in ("record", "data", "access")
                    if child_name in forwarded or structured_ts_child or child_name.startswith("subscriber[") or (
                        self.name in ("GraphView", "RootGraphView", "NestedGraphView")
                        and child_name.startswith("[")
                    ):
                        if child_name.startswith("subscriber[") and child_value.type.code == gdb.TYPE_CODE_PTR:
                            yield child_name, child_value.dereference()
                        else:
                            yield child_name, child_value
            if self.name == "TSInputView":
                cursor = field(self.value, "data_")
                yield "value_data", ts_view_pointer(field(cursor, "value_data"))
                yield "raw_data", ts_view_pointer(field(cursor, "raw_data"))
                yield "consumer", field(self.value, "input_")
                yield "evaluation_time", field(self.value, "evaluation_time_")
            elif self.name == "TSOutputView":
                yield "owner", field(self.value, "output_")
                yield "evaluation_time", field(self.value, "evaluation_time_")
                delta = ts_delta_pointer(primary, field(self.value, "evaluation_time_"))
                if delta is not None:
                    yield "delta", delta
            elif self.name == "TSOutputHandle":
                yield "owner", field(self.value, "output_")
            elif self.name == "NodeView":
                input_value = node_component(self.value, "input", "hgraph::TSInput")
                output_value = node_component(self.value, "output", "hgraph::TSOutput")
                if input_value is not None:
                    yield "input", input_value
                if output_value is not None:
                    yield "output", output_value
            elif self.name in ("GraphView", "RootGraphView", "NestedGraphView"):
                component, address = plan_component(self.value, primary, "schedule")
                if component is not None:
                    schedule_plan = dereference(field(component, "plan"))
                    array_state = value_at_address(
                        "hgraph::MemoryUtils::ArrayState",
                        pointer(field(schedule_plan, "lifecycle_context")),
                    )
                    count = integer(field(array_state, "element_count"))
                    stride = integer(field(array_state, "element_stride"))
                    for index in range(count):
                        yield "schedule[{}]".format(index), value_at_address(
                            "hgraph::DateTime", address + index * stride
                        )
        except Exception:
            return


class NotificationTargetPrinter:
    """Expose semantic notification links in ordinary GDB/IDE variable trees."""

    def __init__(self, value):
        self.value = value

    def to_string(self):
        address = self.value if self.value.type.code == gdb.TYPE_CODE_PTR else self.value.address
        return "{} @ {}".format(self.value.type, address)

    def children(self):
        for name, child in concrete_object_children(self.value).items():
            if name == "notifies" and child.type.code == gdb.TYPE_CODE_PTR and pointer(child):
                yield name, child.dereference()
            else:
                yield name, child


def build_pretty_printer():
    printer = gdb.printing.RegexpCollectionPrettyPrinter("hgraph")
    printer.add_printer("hgraph::SchemaHeader", r"^hgraph::SchemaHeader$", SchemaHeaderPrinter)
    printer.add_printer("hgraph::TypeRecord", r"^hgraph::TypeRecord$", TypeRecordPrinter)
    printer.add_printer("hgraph::DebugDescriptor", r"^hgraph::DebugDescriptor$", DebugDescriptorPrinter)
    printer.add_printer("hgraph::DebugDynamicLayout", r"^hgraph::DebugDynamicLayout$", DebugDynamicLayoutPrinter)
    printer.add_printer("hgraph::DebugTimeSeriesLayout", r"^hgraph::DebugTimeSeriesLayout$", DebugTimeSeriesLayoutPrinter)
    printer.add_printer("hgraph::DebugField", r"^hgraph::DebugField$", DebugFieldPrinter)
    printer.add_printer("hgraph::AnyPtr", r"^hgraph::AnyPtr$", TypePointerPrinter)
    printer.add_printer("hgraph::TypedPtr", r"^hgraph::TypedPtr<.*>$", TypePointerPrinter)
    printer.add_printer(
        "hgraph notification targets",
        r"^hgraph::.*(?:TSInputTargetLinkState|SchedulingNotifier|NodeRuntimeStorage)$",
        NotificationTargetPrinter,
    )
    printer.add_printer("hgraph type refs", r"^hgraph::(?:ValueTypeRef|NodeTypeRef|GraphTypeRef|ExecutorTypeRef|ClockTypeRef|TSRoleTypeRef|BasicTSTypeRef<.*>)$", TypeRefPrinter)
    printer.add_printer("hgraph runtime wrappers", r"^hgraph::(?:Value|ValueView|TSData|TSDataOwnedStorage|TSDataView|TSInput|TSOutput|TSInputView|TSOutputView|TSOutputHandle|NodeView|GraphView|RootGraphView|NestedGraphView)$", RuntimeWrapperPrinter)
    return printer


_PATH_ALIASES = {
    "owning_node": "owner_node",
    "node": "owner_node",
    "source": "source_data",
    "notification_target": "notifies",
}
_HIDDEN_NAVIGATION_CHILDREN = {"record", "access", "schema_ptr", "debug_descriptor"}


def visualized_value(value):
    printer = gdb.default_visualizer(value)
    if printer is not None:
        return value, printer
    try:
        value_type = value.type.strip_typedefs()
        if value_type.code in (gdb.TYPE_CODE_REF, gdb.TYPE_CODE_RVALUE_REF):
            return visualized_value(value.referenced_value())
        for member in value_type.fields():
            if member.is_base_class:
                base_value, base_printer = visualized_value(value.cast(member.type))
                if base_printer is not None:
                    return base_value, base_printer
    except Exception:
        pass
    return value, None


def printer_children(value):
    value, printer = visualized_value(value)
    if printer is None or not hasattr(printer, "children"):
        return {}
    try:
        return {name: child for name, child in printer.children()}
    except Exception:
        return {}


def node_view_value(value):
    try:
        record_address, data_address, access = pointer_state(value)
        record = record_at(record_address)
        if record is None or record_snapshot(record)["schema"]["family"] != 3:
            return None
        size = target_pointer_size()
        order = target_byte_order()
        payload = (record_address | access).to_bytes(size, order) + data_address.to_bytes(size, order)
        return gdb.Value(payload, gdb.lookup_type("hgraph::NodeView"))
    except Exception:
        return None


def visualizer_children(value):
    children = printer_children(value)
    if not children:
        children = concrete_object_children(value)
    node_view = node_view_value(value)
    if node_view is not None:
        for name, child in printer_children(node_view).items():
            if name not in ("pointer", "graph"):
                children.setdefault(name, child)
    return children


def concrete_object_children(value):
    try:
        if value.type.code == gdb.TYPE_CODE_PTR:
            if pointer(value) == 0:
                return {}
            concrete = dynamic_pointer(value).dereference()
        else:
            concrete = value
        result = {}
        for member in concrete.type.strip_typedefs().fields():
            if member.is_base_class:
                result["<{}>".format(member.type)] = concrete.cast(member.type)
            elif member.name:
                result[member.name] = concrete[member.name]
        type_name = str(concrete.type.strip_typedefs())
        if type_name.endswith("TSInputTargetLinkState"):
            target = field(field(concrete, "scheduling_notifier"), "target_")
            if pointer(target):
                result["notifies"] = dynamic_pointer(target)
        elif type_name.endswith("TSInputTargetLinkState::SchedulingNotifier") or type_name.endswith(
            "TSInputSchedulingNotifier"
        ):
            target_name = "target_" if type_name.endswith("::SchedulingNotifier") else "target"
            target = field(concrete, target_name)
            if pointer(target):
                result["notifies"] = dynamic_pointer(target)
        elif type_name.endswith("NodeRuntimeStorage"):
            graph_value = field(concrete, "graph")
            if pointer(graph_value):
                graph = field(graph_value.dereference(), "pointer_")
                result["graph_value"] = graph_value
                result["graph"] = graph
                node = printer_children(graph).get("[{}]".format(integer(field(concrete, "node_index"))))
                if node is not None:
                    result["node"] = node
        return result
    except Exception:
        return {}


def shallow_summary(value):
    value, printer = visualized_value(value)
    if printer is not None and hasattr(printer, "to_string"):
        try:
            return str(printer.to_string())
        except Exception:
            pass
    try:
        if value.type.code == gdb.TYPE_CODE_PTR:
            return "{} {}".format(value.type, value)
        return value.format_string(raw=True, max_elements=4, max_depth=1)
    except Exception:
        return "<{}>".format(value.type)


def resolve_navigation(value, path):
    index = 0
    while index < len(path):
        token = path[index]
        index += 1
        indexed_token = _PATH_ALIASES.get(token, token)
        if indexed_token in ("nodes", "node_at", "schedule"):
            if index >= len(path):
                raise gdb.GdbError("{} requires an index".format(indexed_token))
            try:
                item = int(path[index], 0)
            except ValueError:
                raise gdb.GdbError("{} index must be an integer".format(indexed_token))
            index += 1
            token = "schedule[{}]".format(item) if indexed_token == "schedule" else "[{}]".format(item)
        children = visualizer_children(value)
        if token not in children:
            token = _PATH_ALIASES.get(token, token)
        if token not in children:
            available = [name for name in children if name not in _HIDDEN_NAVIGATION_CHILDREN]
            raise gdb.GdbError(
                "navigation child {!r} is unavailable; choices: {}".format(
                    token, ", ".join(available) or "<none>"
                )
            )
        value = children[token]
    return value


def parse_navigation(argument, accepts_variable=False):
    arguments = gdb.string_to_argv(argument)
    if not arguments:
        raise gdb.GdbError("expected an expression")
    variable = "hg"
    if accepts_variable and arguments[0].startswith("$"):
        variable = arguments.pop(0)[1:]
        if not variable:
            raise gdb.GdbError("convenience variable name cannot be empty")
    if not arguments:
        raise gdb.GdbError("expected an expression")
    value = gdb.parse_and_eval(arguments.pop(0))
    return variable, resolve_navigation(value, arguments)


class HGraphPrintCommand(gdb.Command):
    """Print one hgraph object and its immediate navigation links: hg-p EXPR [PATH ...]."""

    def __init__(self):
        super().__init__("hg-p", gdb.COMMAND_DATA)

    def invoke(self, argument, _from_tty):
        _, value = parse_navigation(argument)
        gdb.write(shallow_summary(value) + "\n")
        children = visualizer_children(value)
        if not children:
            children = concrete_object_children(value)
        for name, child in children.items():
            if name in _HIDDEN_NAVIGATION_CHILDREN:
                continue
            gdb.write("  {}: {}\n".format(name, shallow_summary(child)))


class HGraphVariableCommand(gdb.Command):
    """Store a navigated hgraph object: hg-v [$NAME] EXPR [PATH ...]."""

    def __init__(self):
        super().__init__("hg-v", gdb.COMMAND_DATA)

    def invoke(self, argument, _from_tty):
        variable, value = parse_navigation(argument, accepts_variable=True)
        gdb.set_convenience_variable(variable, value)
        gdb.write("${} = {}\n".format(variable, shallow_summary(value)))


def register_hgraph_printers(obj=None):
    if obj is None:
        obj = gdb.current_objfile()
    gdb.printing.register_pretty_printer(obj, build_pretty_printer(), replace=True)


register_hgraph_printers()
HGraphPrintCommand()
HGraphVariableCommand()
