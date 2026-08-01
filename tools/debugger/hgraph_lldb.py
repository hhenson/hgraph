"""Read-only LLDB printers for hgraph's common type-erasure ABI.

Load with::

    (lldb) command script import /path/to/hg_cpp/tools/debugger/hgraph_lldb.py

The printers read debug-info fields and inferior memory only. They never call
functions in the stopped process.
"""

import os
import sys

import lldb

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import hgraph_debug_common as common


def valid(value):
    return value is not None and value.IsValid()


def raw(value):
    if not valid(value):
        return lldb.SBValue()
    result = value.GetNonSyntheticValue()
    return result if valid(result) else value


def child(value, name):
    if not valid(value):
        return lldb.SBValue()
    value = raw(value)
    result = value.GetChildMemberWithName(name)
    if valid(result):
        return result
    value_type = value.GetType()
    for index in range(value_type.GetNumberOfDirectBaseClasses()):
        base = value.GetChildAtIndex(index)
        result = child(base, name)
        if valid(result):
            return result
    return lldb.SBValue()


def integer(value):
    return value.GetValueAsUnsigned(0) if valid(value) else 0


def pointer(value):
    return integer(value)


def c_string(value):
    address = pointer(value)
    if address == 0 or not valid(value):
        return None
    error = lldb.SBError()
    text = value.GetProcess().ReadCStringFromMemory(address, 256, error)
    return text if error.Success() else None


def dereference(value):
    if pointer(value) == 0:
        return lldb.SBValue()
    result = value.Dereference()
    return result if valid(result) else lldb.SBValue()


def schema_snapshot(value):
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "family": integer(child(value, "family")),
        "kind": integer(child(value, "kind")),
        "label": c_string(child(value, "label")),
        "introspection": pointer(child(value, "introspection")),
    }


def record_snapshot(value):
    schema_value = dereference(child(value, "schema"))
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "role": integer(child(value, "role")),
        "reserved0": integer(child(value, "reserved0")),
        "ops_abi_version": integer(child(value, "ops_abi_version")),
        "reserved1": integer(child(value, "reserved1")),
        "capabilities": integer(child(value, "capabilities")),
        "implementation_label": c_string(child(value, "implementation_label")),
        "schema": schema_snapshot(schema_value) if valid(schema_value) else None,
        "plan": pointer(child(value, "plan")),
        "ops": pointer(child(value, "ops")),
        "debug": pointer(child(value, "debug")),
    }


def record_at(value, address):
    if address == 0 or not valid(value):
        return lldb.SBValue()
    target = value.GetTarget()
    record_type = target.FindFirstType("hgraph::TypeRecord")
    if not record_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress("record", target.ResolveLoadAddress(address), record_type)


def descriptor_at(value, address):
    if address == 0 or not valid(value):
        return lldb.SBValue()
    target = value.GetTarget()
    descriptor_type = target.FindFirstType("hgraph::DebugDescriptor")
    if not descriptor_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress(
        "debug_descriptor", target.ResolveLoadAddress(address), descriptor_type
    )


def dynamic_layout_at(value, address):
    if address == 0 or not valid(value):
        return lldb.SBValue()
    target = value.GetTarget()
    layout_type = target.FindFirstType("hgraph::DebugDynamicLayout")
    if not layout_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress(
        "dynamic_layout", target.ResolveLoadAddress(address), layout_type
    )


def time_series_layout_at(value, address):
    if address == 0:
        return lldb.SBValue()
    target = raw(value).GetTarget()
    layout_type = target.FindFirstType("hgraph::DebugTimeSeriesLayout")
    if not layout_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress(
        "time_series_layout", target.ResolveLoadAddress(address), layout_type
    )


def descriptor_snapshot(value):
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "layout": integer(child(value, "layout")),
        "atomic_kind": integer(child(value, "atomic_kind")),
        "flags": integer(child(value, "flags")),
        "field_count": integer(child(value, "field_count")),
        "fields": pointer(child(value, "fields")),
        "validity_offset": integer(child(value, "validity_offset")),
        "validity_word_size": integer(child(value, "validity_word_size")),
        "reserved0": integer(child(value, "reserved0")),
        "key_type": pointer(child(value, "key_type")),
        "element_type": pointer(child(value, "element_type")),
        "dynamic_layout": pointer(child(value, "dynamic_layout")),
        "time_series_layout": pointer(child(value, "time_series_layout")),
    }


def debug_field_snapshot(value):
    return {
        "name": c_string(child(value, "name")),
        "offset": integer(child(value, "offset")),
        "type": pointer(child(value, "type")),
        "validity_bit": integer(child(value, "validity_bit")),
        "flags": integer(child(value, "flags")),
    }


def dynamic_layout_snapshot(value):
    return {
        "magic": integer(child(value, "magic")),
        "abi_version": integer(child(value, "abi_version")),
        "kind": integer(child(value, "kind")),
        "reserved0": integer(child(value, "reserved0")),
        "flags": integer(child(value, "flags")),
        "key_auxiliary_offset": integer(child(value, "key_auxiliary_offset")),
        "size_offset": integer(child(value, "size_offset")),
        "size_constant": integer(child(value, "size_constant")),
        "data_offset": integer(child(value, "data_offset")),
        "stride": integer(child(value, "stride")),
        "key_data_offset": integer(child(value, "key_data_offset")),
        "key_stride": integer(child(value, "key_stride")),
        "state_offset": integer(child(value, "state_offset")),
        "auxiliary_offset": integer(child(value, "auxiliary_offset")),
        "entry_offset": integer(child(value, "entry_offset")),
    }


def descriptor_fields(value, snapshot):
    target = value.GetTarget()
    field_type = target.FindFirstType("hgraph::DebugField")
    if not field_type.IsValid():
        return
    base = snapshot["fields"]
    for index in range(snapshot["field_count"]):
        field_value = target.CreateValueFromAddress(
            "field",
            target.ResolveLoadAddress(base + index * field_type.GetByteSize()),
            field_type,
        )
        if not valid(field_value):
            return
        yield index, field_value, debug_field_snapshot(field_value)


def target_byte_order(value):
    return "big" if value.GetTarget().GetByteOrder() == lldb.eByteOrderBig else "little"


def read_memory(value, address, size):
    if not valid(value) or address == 0 or size <= 0:
        return None
    error = lldb.SBError()
    payload = value.GetProcess().ReadMemory(address, size, error)
    return payload if error.Success() else None


def read_unsigned(value, address, size=None):
    if not valid(value) or address == 0:
        return None
    size = value.GetTarget().GetAddressByteSize() if size is None else size
    error = lldb.SBError()
    result = value.GetProcess().ReadUnsignedFromMemory(address, size, error)
    return result if error.Success() else None


def record_plan_size(record_value):
    plan = dereference(child(record_value, "plan"))
    return integer(child(child(plan, "layout"), "size")) if valid(plan) else 0


def atomic_value(record_value, data_address):
    try:
        descriptor_value = descriptor_at(record_value, pointer(child(record_value, "debug")))
        if not valid(descriptor_value):
            return common._MISSING
        snapshot = descriptor_snapshot(descriptor_value)
        if not common.debug_descriptor_valid(snapshot) or snapshot["layout"] != 1:
            return common._MISSING
        payload = read_memory(record_value, data_address, record_plan_size(record_value))
        return common.decode_atomic(snapshot["atomic_kind"], payload, target_byte_order(record_value))
    except Exception:
        return common._MISSING


def pointer_payload(record_value, data_address):
    payload = atomic_value(record_value, data_address)
    if payload is not common._MISSING:
        return payload, None
    try:
        descriptor = descriptor_at(record_value, pointer(child(record_value, "debug")))
        snapshot = descriptor_snapshot(descriptor)
        if snapshot["layout"] != 7:
            return common._MISSING, None
        layout = time_series_layout_at(record_value, snapshot["time_series_layout"])
        value_record = record_at(record_value, pointer(child(layout, "value_type")))
        value = atomic_value(
            value_record, data_address + integer(child(layout, "value_offset"))
        )
        raw_time = read_unsigned(
            record_value, data_address + integer(child(layout, "last_modified_offset")), 8
        )
        if raw_time is not None and raw_time & (1 << 63):
            raw_time -= 1 << 64
        return value, raw_time
    except Exception:
        return common._MISSING, None


def make_any_pointer(value, name, record_address, data_address, access):
    target = value.GetTarget()
    pointer_size = target.GetAddressByteSize()
    byte_order = target.GetByteOrder()
    words = [record_address | access, data_address]
    if pointer_size == 8:
        data = lldb.SBData.CreateDataFromUInt64Array(byte_order, pointer_size, words)
    elif pointer_size == 4:
        data = lldb.SBData.CreateDataFromUInt32Array(byte_order, pointer_size, words)
    else:
        return lldb.SBValue()
    pointer_type = target.FindFirstType("hgraph::AnyPtr")
    return target.CreateValueFromData(name, data, pointer_type) if pointer_type.IsValid() else lldb.SBValue()


def make_owner_pointer(value, name, owner_address, fallback_record, access):
    size = value.GetTarget().GetAddressByteSize()
    record_address = read_unsigned(value, owner_address)
    state_word = read_unsigned(value, owner_address + size)
    if record_address is None or state_word is None:
        return lldb.SBValue()
    if record_address == 0:
        record_address = fallback_record
    state = state_word & common.DEBUG_OWNER_STATE_MASK
    if state == 0:
        data_address = 0
    elif state == common.DEBUG_OWNER_INLINE_STATE:
        data_address = owner_address + 2 * size
    elif state == common.DEBUG_OWNER_HEAP_STATE:
        data_address = read_unsigned(value, owner_address + 2 * size)
        if data_address is None:
            return lldb.SBValue()
    else:
        return lldb.SBValue()
    return make_any_pointer(value, name, record_address, data_address, access if data_address else 0)


def make_embedded_pointer(value, name, pointer_address, fallback_record=0):
    size = value.GetTarget().GetAddressByteSize()
    tagged_record = read_unsigned(value, pointer_address)
    data_address = read_unsigned(value, pointer_address + size)
    if tagged_record is None or data_address is None:
        return lldb.SBValue()
    record_address = tagged_record & ~0x3
    access = tagged_record & 0x3
    if record_address == 0:
        record_address = fallback_record
    return make_any_pointer(value, name, record_address, data_address, access if data_address else 0)


def make_indirect_embedded_pointer(value, name, pointer_address, fallback_record=0):
    object_address = read_unsigned(value, pointer_address)
    if not object_address:
        return lldb.SBValue()
    return make_embedded_pointer(value, name, object_address, fallback_record)


def make_ts_parent_pointer(value, data_address, layout, name):
    parent_address = data_address + integer(child(layout, "parent_offset"))
    tagged_record = read_unsigned(value, parent_address)
    parent_data = read_unsigned(value, parent_address + raw(value).GetTarget().GetAddressByteSize())
    if tagged_record is None or parent_data is None:
        return lldb.SBValue(), None
    kind = tagged_record & 0x7
    if kind not in (1, 4) or parent_data == 0:
        return lldb.SBValue(), kind
    return make_any_pointer(value, name, tagged_record & ~0x7, parent_data, 0), kind


def dynamic_child_addresses(value, data_address, layout):
    flags = layout["flags"]
    slot_index_indirect = bool(flags & common.DEBUG_DYNAMIC_SLOT_INDEX_INDIRECT)
    data_implementation = 0
    key_implementation = 0
    if slot_index_indirect:
        data_implementation = read_unsigned(value, data_address + layout["data_offset"])
        if data_implementation is None:
            return
        data_implementation &= ~0x3
        if not data_implementation:
            return
        if layout["key_stride"]:
            key_implementation = read_unsigned(value, data_address + layout["key_data_offset"])
            if key_implementation is None:
                return
            key_implementation &= ~0x3
            if not key_implementation:
                return
        else:
            key_implementation = data_implementation
    size_base = (
        key_implementation
        if flags & common.DEBUG_DYNAMIC_SLOT_SIZE_INDIRECT
        else data_address
    )
    size = (
        layout["size_constant"]
        if flags & common.DEBUG_DYNAMIC_SIZE_CONSTANT
        else read_unsigned(value, size_base + layout["size_offset"])
    )
    if size is None or size > (1 << 30):
        return
    visible_size = min(size, 4096)
    head = (
        read_unsigned(value, data_address + layout["auxiliary_offset"])
        if flags & common.DEBUG_DYNAMIC_HAS_HEAD
        else 0
    )
    if head is None:
        return
    data_base = (
        data_implementation + layout["auxiliary_offset"]
        if slot_index_indirect
        else data_address + layout["data_offset"]
    )
    if flags & common.DEBUG_DYNAMIC_DATA_INDIRECT:
        data_base = read_unsigned(value, data_base)
    if data_base is None:
        return
    key_base = 0
    if layout["key_stride"]:
        key_base = (
            key_implementation + layout["key_auxiliary_offset"]
            if slot_index_indirect
            else data_address + layout["key_data_offset"]
        )
        if flags & common.DEBUG_DYNAMIC_KEY_DATA_INDIRECT:
            key_base = read_unsigned(value, key_base)
        if key_base is None:
            return
    state_words = 0
    state_bits = 0
    if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
        state_base = key_implementation if slot_index_indirect else data_address
        state_words = read_unsigned(value, state_base + layout["state_offset"])
        state_bits = (
            size
            if flags & common.DEBUG_DYNAMIC_SLOT_STATE_TAGGED
            else read_unsigned(
                value,
                state_base + layout["state_offset"] + value.GetTarget().GetAddressByteSize(),
            )
        )
        if state_words is None or state_bits is None:
            return
    pointer_size = value.GetTarget().GetAddressByteSize()
    for logical_index in range(visible_size):
        physical_index = (
            (head + logical_index) % size
            if size and flags & common.DEBUG_DYNAMIC_HAS_HEAD
            else logical_index
        )
        if flags & common.DEBUG_DYNAMIC_HAS_SLOT_STATE:
            if physical_index >= state_bits:
                continue
            if flags & common.DEBUG_DYNAMIC_SLOT_STATE_TAGGED:
                state = read_unsigned(value, state_words + physical_index * pointer_size)
                if state is None or state & 0x3:
                    continue
            else:
                word = read_unsigned(value, state_words + (physical_index // 64) * 8, 8)
                if word is None or not (word & (1 << (physical_index % 64))):
                    continue
        if flags & common.DEBUG_DYNAMIC_DATA_POINTER_TABLE:
            element = read_unsigned(value, data_base + physical_index * pointer_size)
            if element is not None and flags & common.DEBUG_DYNAMIC_DATA_POINTERS_TAGGED:
                element &= ~0x3
        else:
            element = data_base + physical_index * layout["stride"]
        if key_base:
            if flags & common.DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE:
                key = read_unsigned(value, key_base + physical_index * pointer_size)
                if key is not None and flags & common.DEBUG_DYNAMIC_KEY_POINTERS_TAGGED:
                    key &= ~0x3
            else:
                key = key_base + physical_index * layout["key_stride"]
        else:
            key = 0
        if element:
            element += layout["entry_offset"]
        yield logical_index, key, element


def any_pointer_value(value):
    type_name = raw(value).GetType().GetCanonicalType().GetName() or ""
    if "TypedPtr<" in type_name:
        return child(value, "value_")
    return raw(value)


def pointer_state(value):
    erased = any_pointer_value(value)
    bits = integer(child(child(erased, "type_"), "m_bits"))
    return bits & ~0x3, pointer(child(erased, "data_")), bits & 0x3


def display_type_name(value):
    return "AnyPtr" if (value.GetTypeName() or "") == "hgraph::AnyPtr" else "TypedPtr"


def schema_header_summary(value, _internal_dict, _options):
    try:
        return common.schema_summary(schema_snapshot(value))
    except Exception as exc:
        return "SchemaHeader{<printer error: %s>}" % exc


def type_record_summary(value, _internal_dict, _options):
    try:
        return common.record_summary(record_snapshot(value))
    except Exception as exc:
        return "TypeRecord{<printer error: %s>}" % exc


def debug_descriptor_summary(value, _internal_dict, _options):
    try:
        return common.debug_descriptor_summary(descriptor_snapshot(value))
    except Exception as exc:
        return "DebugDescriptor{<printer error: %s>}" % exc


def debug_field_summary(value, _internal_dict, _options):
    try:
        return common.debug_field_summary(debug_field_snapshot(value))
    except Exception as exc:
        return "DebugField{<printer error: %s>}" % exc


def debug_dynamic_layout_summary(value, _internal_dict, _options):
    try:
        return common.debug_dynamic_layout_summary(dynamic_layout_snapshot(value))
    except Exception as exc:
        return "DebugDynamicLayout{<printer error: %s>}" % exc


def debug_time_series_layout_summary(value, _internal_dict, _options):
    return "DebugTimeSeriesLayout{{value_offset={} tracking_offset={}}}".format(
        integer(child(value, "value_offset")), integer(child(value, "tracking_offset"))
    )


def type_pointer_summary(value, _internal_dict, _options):
    try:
        record_address, data_address, access = pointer_state(value)
        record_value = record_at(value, record_address)
        snapshot = record_snapshot(record_value) if valid(record_value) else None
        payload, last_modified = (
            pointer_payload(record_value, data_address)
            if valid(record_value) and data_address
            else (common._MISSING, None)
        )
        carrier = display_type_name(value)
        if carrier == "TypedPtr":
            carrier = common.typed_pointer_name(snapshot)
        summary = common.compact_pointer_summary(
            carrier, record_address, data_address, access, snapshot, payload
        )
        if last_modified is not None:
            summary = summary[:-1] + " last_modified_us={}".format(last_modified) + "}"
        return summary
    except Exception as exc:
        return "TypePointer{<printer error: %s>}" % exc


def type_ref_record(value):
    return pointer(child(value, "record_"))


def ts_storage_pointer(value, name="pointer"):
    return make_any_pointer(
        value, name, type_ref_record(child(value, "type_")), pointer(child(value, "data_")), 0
    )


def ts_view_pointer(value, name="pointer"):
    return ts_storage_pointer(child(value, "storage_"), name)


def ts_owner_pointer(value, name="pointer"):
    return make_any_pointer(
        value, name, type_ref_record(child(value, "type_")), pointer(child(value, "data_")), 0
    )


def value_at_address(value, type_name, address, name):
    target = raw(value).GetTarget()
    value_type = target.FindFirstType(type_name)
    if not value_type.IsValid():
        return lldb.SBValue()
    return target.CreateValueFromAddress(name, target.ResolveLoadAddress(address), value_type)


def plan_component(value, pointer_value, component_name):
    record_address, data_address, _ = pointer_state(pointer_value)
    record = record_at(value, record_address)
    plan = dereference(child(record, "plan"))
    state_address = pointer(child(plan, "lifecycle_context"))
    target = raw(value).GetTarget()
    state_type = target.FindFirstType("hgraph::MemoryUtils::CompositeState")
    component_type = target.FindFirstType("hgraph::MemoryUtils::CompositeComponent")
    state = value_at_address(value, "hgraph::MemoryUtils::CompositeState", state_address, "state")
    alignment = component_type.GetByteAlign()
    first = state_address + ((state_type.GetByteSize() + alignment - 1) // alignment) * alignment
    for index in range(integer(child(state, "component_count"))):
        component = value_at_address(
            value,
            "hgraph::MemoryUtils::CompositeComponent",
            first + index * component_type.GetByteSize(),
            "component",
        )
        if c_string(child(component, "name")) == component_name:
            return component, data_address + integer(child(component, "offset"))
    return lldb.SBValue(), 0


def node_component(value, component_name, type_name):
    _, address = plan_component(value, child(value, "pointer_"), component_name)
    return value_at_address(value, type_name, address, component_name) if address else lldb.SBValue()


def wrapper_name(value):
    type_name = raw(value).GetType().GetCanonicalType().GetName() or value.GetTypeName() or "hgraph"
    return type_name.split("::")[-1]


def runtime_wrapper_pointer(value, name="pointer"):
    kind = wrapper_name(value)
    if kind == "Value":
        return make_owner_pointer(value, name, pointer(child(value, "storage_")), 0, 0)
    if kind == "ValueView":
        return child(value, "pointer_").Clone(name)
    if kind in ("NodeView", "GraphView", "RootGraphView", "NestedGraphView"):
        return child(value, "pointer_").Clone(name)
    if kind == "TSDataView":
        return ts_view_pointer(value, name)
    if kind in ("TSData", "TSDataOwnedStorage"):
        return ts_owner_pointer(value if kind == "TSDataOwnedStorage" else child(value, "storage_"), name)
    if kind in ("TSInput", "TSOutput"):
        return ts_owner_pointer(child(child(value, "data_"), "storage_"), name)
    if kind == "TSOutputView":
        return ts_view_pointer(child(value, "data_"), name)
    if kind == "TSOutputHandle":
        return ts_storage_pointer(child(value, "data_"), name)
    if kind == "TSInputView":
        return ts_view_pointer(child(child(value, "data_"), "value_data"), name)
    return lldb.SBValue()


def date_time_count(value):
    duration = child(value, "__d")
    return integer(child(duration, "__r")) if valid(duration) else integer(value)


def ts_delta_pointer(value, ts_pointer, evaluation_time, name="delta"):
    record_address, data_address, access = pointer_state(ts_pointer)
    record = record_at(value, record_address)
    descriptor = descriptor_at(value, pointer(child(record, "debug")))
    snapshot = descriptor_snapshot(descriptor)
    if snapshot["layout"] != 7:
        return lldb.SBValue()
    layout = time_series_layout_at(value, snapshot["time_series_layout"])
    if not integer(child(layout, "delta_aliases_value")):
        return lldb.SBValue()
    raw_time = read_unsigned(
        value, data_address + integer(child(layout, "last_modified_offset")), 8
    )
    if raw_time is not None and raw_time & (1 << 63):
        raw_time -= 1 << 64
    live = raw_time == date_time_count(evaluation_time)
    return make_any_pointer(
        value,
        name,
        pointer(child(layout, "delta_type")),
        data_address + integer(child(layout, "value_offset")) if live else 0,
        access if live else 0,
    )


def relabel_pointer_summary(summary, label):
    opening = summary.find("{")
    return label + summary[opening:] if opening >= 0 else label + "{" + summary + "}"


def concise_wrapper_summary(value, label):
    try:
        record_address, data_address, _ = pointer_state(value)
        record = record_at(value, record_address)
        snapshot = record_snapshot(record)
        semantic = snapshot["schema"]["label"]
        payload, last_modified = (
            pointer_payload(record, data_address)
            if data_address
            else (common._MISSING, None)
        )
        parts = [semantic]
        if payload is not common._MISSING:
            parts.append("value={!r}".format(payload))
        if last_modified is not None:
            parts.append("modified={}us".format(last_modified))
        return "{}{{{}}}".format(label, " ".join(parts))
    except Exception:
        return relabel_pointer_summary(type_pointer_summary(value, {}, {}), label)


def type_ref_summary(value, _internal_dict, _options):
    pointer_value = make_any_pointer(value, "pointer", type_ref_record(value), 0, 0)
    return concise_wrapper_summary(pointer_value, wrapper_name(value))


def runtime_wrapper_summary(value, _internal_dict, _options):
    try:
        return concise_wrapper_summary(runtime_wrapper_pointer(value), wrapper_name(value))
    except Exception as exc:
        return "{}{{<printer error: {}>}}".format(wrapper_name(value), exc)


# Providers must build children LAZILY: recent lldbs (Xcode 26.5) instantiate
# a child's synthetic provider as soon as ``SBValue.Clone`` materialises it, so
# an eager ``__init__ -> update -> build_children -> add -> Clone`` chain
# recurses across the (cyclic) type-record/descriptor/field graph until the
# Python stack overflows and lldb aborts. Children are therefore built on
# first access only, with a defensive depth cap for any formatter that walks
# the synthetic tree eagerly.
_MAX_BUILD_DEPTH = 8
_build_depth = 0


class SyntheticProvider:
    def __init__(self, value, _internal_dict):
        self.value = value
        self.children = None  # built lazily on first access

    def update(self):
        # Invalidate only; building here would recurse (see note above).
        self.children = None
        return False

    def _ensure_children(self):
        global _build_depth
        if self.children is None:
            if _build_depth >= _MAX_BUILD_DEPTH:
                return []
            self.children = []  # breaks re-entry on this same provider
            _build_depth += 1
            try:
                self.children = self.build_children()
            finally:
                _build_depth -= 1
        return self.children

    def num_children(self):
        return len(self._ensure_children())

    def get_child_at_index(self, index):
        children = self._ensure_children()
        if index < 0 or index >= len(children):
            return lldb.SBValue()
        return children[index][1]

    def get_child_index(self, name):
        for index, (child_name, _) in enumerate(self._ensure_children()):
            if child_name == name:
                return index
        return -1

    def has_children(self):
        # Deliberately cheap: answering this must not force a build.
        return True

    def build_children(self):
        return []

    @staticmethod
    def add(values, name, value):
        if valid(value):
            renamed = value.Clone(name)
            if valid(renamed):
                value = renamed
            values.append((name, value))


class TypeRecordSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        schema_pointer = child(self.value, "schema")
        self.add(values, "schema", dereference(schema_pointer))
        self.add(values, "schema_ptr", schema_pointer)
        self.add(
            values,
            "debug_descriptor",
            descriptor_at(self.value, pointer(child(self.value, "debug"))),
        )
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
            self.add(values, name, child(self.value, name))
        return values


class DebugDescriptorSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        snapshot = descriptor_snapshot(self.value)
        for index, field_value, field_snapshot in descriptor_fields(self.value, snapshot):
            self.add(values, field_snapshot["name"] or "[{}]".format(index), field_value)
        self.add(
            values,
            "dynamic_layout_value",
            dynamic_layout_at(self.value, snapshot["dynamic_layout"]),
        )
        self.add(
            values,
            "time_series_layout_value",
            time_series_layout_at(self.value, snapshot["time_series_layout"]),
        )
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
            self.add(values, name, child(self.value, name))
        return values


class DebugDynamicLayoutSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
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
            self.add(values, name, child(self.value, name))
        return values


class DebugFieldSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "type_record", record_at(self.value, pointer(child(self.value, "type"))))
        for name in ("name", "offset", "type", "validity_bit", "flags"):
            self.add(values, name, child(self.value, name))
        return values


class DebugTimeSeriesLayoutSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        for name in ("value_type", "delta_type", "value_offset", "tracking_offset", "last_modified_offset", "parent_offset", "observers_offset", "delta_aliases_value"):
            self.add(values, name, child(self.value, name))
        return values


class TypePointerSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        try:
            record_address, data_address, access = pointer_state(self.value)
            record_value = record_at(self.value, record_address)
            self.add(values, "record", record_value)
            self.add(values, "data", child(any_pointer_value(self.value), "data_"))
            if (
                not valid(record_value)
                or record_address == 0
                or data_address == 0
                or access not in common.ACCESS_NAMES
            ):
                return values

            descriptor_value = descriptor_at(record_value, pointer(child(record_value, "debug")))
            if not valid(descriptor_value):
                return values
            snapshot = descriptor_snapshot(descriptor_value)
            if not common.debug_descriptor_valid(snapshot):
                return values
            if snapshot["layout"] == 7:
                ts_layout = time_series_layout_at(self.value, snapshot["time_series_layout"])
                if not valid(ts_layout):
                    return values
                self.add(
                    values,
                    "value",
                    make_any_pointer(
                        self.value,
                        "value",
                        pointer(child(ts_layout, "value_type")),
                        data_address + integer(child(ts_layout, "value_offset")),
                        access,
                    ),
                )
                dt_type = raw(self.value).GetTarget().FindFirstType("hgraph::DateTime")
                if dt_type.IsValid():
                    self.add(
                        values,
                        "last_modified",
                        raw(self.value).GetTarget().CreateValueFromAddress(
                            "last_modified",
                            raw(self.value).GetTarget().ResolveLoadAddress(
                                data_address + integer(child(ts_layout, "last_modified_offset"))
                            ),
                            dt_type,
                        ),
                    )
                parent, parent_kind = make_ts_parent_pointer(
                    self.value, data_address, ts_layout, "parent"
                )
                self.add(
                    values,
                    "owner_node" if parent_kind == 4 else "parent",
                    parent,
                )
                return values
            if snapshot["layout"] in (2, 5, 6):
                validity = None
                if snapshot["flags"] & common.DEBUG_DESCRIPTOR_HAS_VALIDITY:
                    bits_per_word = snapshot["validity_word_size"] * 8
                    word_count = (snapshot["field_count"] + bits_per_word - 1) // bits_per_word
                    validity = read_memory(
                        self.value,
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
                        target_byte_order(self.value),
                    )
                    if field_snapshot["flags"] & common.DEBUG_FIELD_EMBEDDED_OWNER:
                        pointer_value = make_owner_pointer(
                            self.value,
                            name,
                            data_address + field_snapshot["offset"],
                            field_snapshot["type"],
                            access,
                        )
                    elif field_snapshot["flags"] & common.DEBUG_FIELD_EMBEDDED_POINTER:
                        pointer_value = make_embedded_pointer(
                            self.value,
                            name,
                            data_address + field_snapshot["offset"],
                            field_snapshot["type"],
                        )
                    elif field_snapshot["flags"] & common.DEBUG_FIELD_INDIRECT_EMBEDDED_POINTER:
                        pointer_value = make_indirect_embedded_pointer(
                            self.value,
                            name,
                            data_address + field_snapshot["offset"],
                            field_snapshot["type"],
                        )
                    else:
                        pointer_value = make_any_pointer(
                            self.value,
                            name,
                            field_snapshot["type"],
                            data_address + field_snapshot["offset"] if is_set else 0,
                            access if is_set else 0,
                        )
                    self.add(values, name, pointer_value)
                if not snapshot["dynamic_layout"]:
                    return values
            if snapshot["layout"] not in (3, 4, 5, 6):
                return values
            dynamic_value = dynamic_layout_at(self.value, snapshot["dynamic_layout"])
            if not valid(dynamic_value):
                return values
            layout = dynamic_layout_snapshot(dynamic_value)
            if not common.debug_dynamic_layout_valid(layout):
                return values
            for slot, key_address, element_address in dynamic_child_addresses(self.value, data_address, layout):
                if key_address and snapshot["key_type"]:
                    if layout["flags"] & common.DEBUG_DYNAMIC_KEYS_ARE_OWNERS:
                        key_value = make_owner_pointer(
                            self.value, "key", key_address, snapshot["key_type"], access
                        )
                    else:
                        key_value = make_any_pointer(
                            self.value, "key", snapshot["key_type"], key_address, access
                        )
                    self.add(
                        values,
                        "key[{}]".format(slot),
                        key_value,
                    )
                if element_address:
                    name = "value[{}]".format(slot) if snapshot["key_type"] else "[{}]".format(slot)
                    if layout["flags"] & common.DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS:
                        pointer_value = make_owner_pointer(
                            self.value, name, element_address, snapshot["element_type"], access
                        )
                    elif layout["flags"] & common.DEBUG_DYNAMIC_ELEMENTS_ARE_POINTERS:
                        pointer_value = make_embedded_pointer(
                            self.value, name, element_address, snapshot["element_type"]
                        )
                    else:
                        pointer_value = make_any_pointer(
                            self.value, name, snapshot["element_type"], element_address, access
                        )
                    self.add(values, name, pointer_value)
        except Exception:
            pass
        return values


class TypeRefSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        self.add(values, "record", record_at(self.value, type_ref_record(self.value)))
        return values


class RuntimeWrapperSyntheticProvider(SyntheticProvider):
    def build_children(self):
        values = []
        try:
            kind = wrapper_name(self.value)
            primary = runtime_wrapper_pointer(self.value)
            primary_name = {
                "TSInputView": "source_data",
                "TSOutputView": "data",
                "TSDataView": "data",
            }.get(kind, "pointer")
            self.add(values, primary_name, primary)
            forwarded = {
                "TSData": {"value", "parent", "owner_node"},
                "TSDataView": {"value", "parent", "owner_node"},
                "TSInput": {"value", "parent", "owner_node"},
                "TSInputView": {"value", "parent", "owner_node"},
                "TSOutput": {"value", "parent", "owner_node"},
                "TSOutputView": {"value", "parent", "owner_node"},
                "NodeView": {"graph", "state", "scalars"},
            }.get(kind, set())
            pointer_provider = TypePointerSyntheticProvider(primary, {})
            for child_name, child_value in pointer_provider._ensure_children():
                if child_name in forwarded or (
                    kind in ("GraphView", "RootGraphView", "NestedGraphView")
                    and child_name.startswith("[")
                ):
                    self.add(values, child_name, child_value)
            if kind == "TSInputView":
                cursor = child(self.value, "data_")
                self.add(values, "value_data", ts_view_pointer(child(cursor, "value_data"), "value_data"))
                self.add(values, "raw_data", ts_view_pointer(child(cursor, "raw_data"), "raw_data"))
                self.add(values, "consumer", child(self.value, "input_"))
                self.add(values, "evaluation_time", child(self.value, "evaluation_time_"))
            elif kind == "TSOutputView":
                self.add(values, "owner", child(self.value, "output_"))
                self.add(values, "evaluation_time", child(self.value, "evaluation_time_"))
                self.add(
                    values,
                    "delta",
                    ts_delta_pointer(
                        self.value,
                        runtime_wrapper_pointer(self.value),
                        child(self.value, "evaluation_time_"),
                    ),
                )
            elif kind == "TSOutputHandle":
                self.add(values, "owner", child(self.value, "output_"))
            elif kind == "NodeView":
                self.add(values, "input", node_component(self.value, "input", "hgraph::TSInput"))
                self.add(values, "output", node_component(self.value, "output", "hgraph::TSOutput"))
            elif kind in ("GraphView", "RootGraphView", "NestedGraphView"):
                component, address = plan_component(
                    self.value, runtime_wrapper_pointer(self.value), "schedule"
                )
                if valid(component):
                    schedule_plan = dereference(child(component, "plan"))
                    array_state = value_at_address(
                        self.value,
                        "hgraph::MemoryUtils::ArrayState",
                        pointer(child(schedule_plan, "lifecycle_context")),
                        "array_state",
                    )
                    count = integer(child(array_state, "element_count"))
                    stride = integer(child(array_state, "element_stride"))
                    for index in range(count):
                        self.add(
                            values,
                            "schedule[{}]".format(index),
                            value_at_address(
                                self.value,
                                "hgraph::DateTime",
                                address + index * stride,
                                "schedule[{}]".format(index),
                            ),
                        )
        except Exception:
            pass
        return values


def __lldb_init_module(debugger, _internal_dict):
    summaries = (
        (r"^hgraph::SchemaHeader$", "hgraph_lldb.schema_header_summary"),
        (r"^hgraph::TypeRecord$", "hgraph_lldb.type_record_summary"),
        (r"^hgraph::DebugDescriptor$", "hgraph_lldb.debug_descriptor_summary"),
        (r"^hgraph::DebugField$", "hgraph_lldb.debug_field_summary"),
        (r"^hgraph::DebugDynamicLayout$", "hgraph_lldb.debug_dynamic_layout_summary"),
        (r"^hgraph::DebugTimeSeriesLayout$", "hgraph_lldb.debug_time_series_layout_summary"),
        (r"^hgraph::AnyPtr$", "hgraph_lldb.type_pointer_summary"),
        (r"^hgraph::TypedPtr<.*>$", "hgraph_lldb.type_pointer_summary"),
        (r"^hgraph::(ValueTypeRef|NodeTypeRef|GraphTypeRef|ExecutorTypeRef|ClockTypeRef|TSRoleTypeRef|BasicTSTypeRef<.*>)$", "hgraph_lldb.type_ref_summary"),
        (r"^hgraph::(Value|ValueView|TSData|TSDataOwnedStorage|TSDataView|TSInput|TSOutput|TSInputView|TSOutputView|TSOutputHandle|NodeView|GraphView|RootGraphView|NestedGraphView)$", "hgraph_lldb.runtime_wrapper_summary"),
    )
    synthetics = (
        (r"^hgraph::TypeRecord$", "hgraph_lldb.TypeRecordSyntheticProvider"),
        (r"^hgraph::DebugDescriptor$", "hgraph_lldb.DebugDescriptorSyntheticProvider"),
        (r"^hgraph::DebugField$", "hgraph_lldb.DebugFieldSyntheticProvider"),
        (r"^hgraph::DebugDynamicLayout$", "hgraph_lldb.DebugDynamicLayoutSyntheticProvider"),
        (r"^hgraph::DebugTimeSeriesLayout$", "hgraph_lldb.DebugTimeSeriesLayoutSyntheticProvider"),
        (r"^hgraph::AnyPtr$", "hgraph_lldb.TypePointerSyntheticProvider"),
        (r"^hgraph::TypedPtr<.*>$", "hgraph_lldb.TypePointerSyntheticProvider"),
        (r"^hgraph::(ValueTypeRef|NodeTypeRef|GraphTypeRef|ExecutorTypeRef|ClockTypeRef|TSRoleTypeRef|BasicTSTypeRef<.*>)$", "hgraph_lldb.TypeRefSyntheticProvider"),
        (r"^hgraph::(Value|ValueView|TSData|TSDataOwnedStorage|TSDataView|TSInput|TSOutput|TSInputView|TSOutputView|TSOutputHandle|NodeView|GraphView|RootGraphView|NestedGraphView)$", "hgraph_lldb.RuntimeWrapperSyntheticProvider"),
    )
    for pattern, function in summaries:
        debugger.HandleCommand(
            'type summary add --category hgraph --regex "{}" --python-function {}'.format(pattern, function)
        )
    for pattern, provider in synthetics:
        debugger.HandleCommand(
            'type synthetic add --category hgraph --regex "{}" --python-class {}'.format(pattern, provider)
        )
    debugger.HandleCommand("type category enable hgraph")
    print("hgraph common type-erasure printers loaded")
