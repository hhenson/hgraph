/**
 * @file ts_type_registry.cpp
 * @brief Implementation of the TSTypeRegistry singleton.
 */

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/type_registry.h>

#include <cstring>
#include <functional>

namespace hgraph {

namespace {

void ts_reference_construct(void* dst, const value::TypeMeta*) {
    new (dst) TimeSeriesReference(TimeSeriesReference::make());
}

void ts_reference_destroy(void* obj, const value::TypeMeta*) {
    static_cast<TimeSeriesReference*>(obj)->~TimeSeriesReference();
}

void ts_reference_copy(void* dst, const void* src, const value::TypeMeta*) {
    *static_cast<TimeSeriesReference*>(dst) = *static_cast<const TimeSeriesReference*>(src);
}

void ts_reference_move(void* dst, void* src, const value::TypeMeta*) {
    *static_cast<TimeSeriesReference*>(dst) = std::move(*static_cast<TimeSeriesReference*>(src));
}

void ts_reference_move_construct(void* dst, void* src, const value::TypeMeta*) {
    new (dst) TimeSeriesReference(std::move(*static_cast<TimeSeriesReference*>(src)));
}

bool ts_reference_equals(const void* a, const void* b, const value::TypeMeta*) {
    return *static_cast<const TimeSeriesReference*>(a) == *static_cast<const TimeSeriesReference*>(b);
}

size_t ts_reference_hash(const void* obj, const value::TypeMeta*) {
    return std::hash<std::string>{}(static_cast<const TimeSeriesReference*>(obj)->to_string());
}

bool ts_reference_less_than(const void* a, const void* b, const value::TypeMeta*) {
    return static_cast<const TimeSeriesReference*>(a)->to_string() <
           static_cast<const TimeSeriesReference*>(b)->to_string();
}

std::string ts_reference_to_string(const void* obj, const value::TypeMeta*) {
    return static_cast<const TimeSeriesReference*>(obj)->to_string();
}

nb::object ts_reference_to_python(const void* obj, const value::TypeMeta*) {
    return nb::cast(*static_cast<const TimeSeriesReference*>(obj));
}

void ts_reference_from_python(void* dst, const nb::object& src, const value::TypeMeta*) {
    *static_cast<TimeSeriesReference*>(dst) = nb::cast<TimeSeriesReference>(src);
}

value::type_ops make_ts_reference_ops() {
    value::type_ops ops{};
    ops.construct = &ts_reference_construct;
    ops.destroy = &ts_reference_destroy;
    ops.copy = &ts_reference_copy;
    ops.move = &ts_reference_move;
    ops.move_construct = &ts_reference_move_construct;
    ops.equals = &ts_reference_equals;
    ops.hash = &ts_reference_hash;
    ops.to_string = &ts_reference_to_string;
    ops.to_python = &ts_reference_to_python;
    ops.from_python = &ts_reference_from_python;
    ops.kind = value::TypeKind::Atomic;
    ops.specific.atomic = {&ts_reference_less_than};
    return ops;
}

const value::TypeMeta* ts_reference_type_meta() {
    static const value::TypeMeta* meta = value::TypeRegistry::instance()
        .register_type<TimeSeriesReference>("TimeSeriesReference", make_ts_reference_ops());
    return meta;
}

} // namespace

// ============================================================================
// Singleton Instance
// ============================================================================

TSTypeRegistry& TSTypeRegistry::instance() {
    static TSTypeRegistry instance;
    return instance;
}

// ============================================================================
// Helper Methods
// ============================================================================

const char* TSTypeRegistry::intern_string(const std::string& s) {
    auto buf = std::make_unique<char[]>(s.size() + 1);
    std::memcpy(buf.get(), s.c_str(), s.size() + 1);
    const char* ptr = buf.get();
    field_names_.push_back(std::move(buf));
    return ptr;
}

TSMeta* TSTypeRegistry::create_schema() {
    auto meta = std::make_unique<TSMeta>();
    TSMeta* ptr = meta.get();
    schemas_.push_back(std::move(meta));
    return ptr;
}

// ============================================================================
// TS[T] - Scalar Time-Series
// ============================================================================

const TSMeta* TSTypeRegistry::ts(const value::TypeMeta* value_type) {
    auto it = ts_cache_.find(value_type);
    if (it != ts_cache_.end()) {
        return it->second;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSValue;
    meta->value_type = value_type;

    ts_cache_[value_type] = meta;
    return meta;
}

// ============================================================================
// TSS[T] - Time-Series Set
// ============================================================================

const TSMeta* TSTypeRegistry::tss(const value::TypeMeta* element_type) {
    auto it = tss_cache_.find(element_type);
    if (it != tss_cache_.end()) {
        return it->second;
    }

    // Build the set value schema from element type
    const value::TypeMeta* value_schema = nullptr;
    if (element_type) {
        value_schema = value::TypeRegistry::instance()
            .set(element_type)
            .build();
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSS;
    meta->value_type = value_schema;

    tss_cache_[element_type] = meta;
    return meta;
}

// ============================================================================
// TSD[K, V] - Time-Series Dict
// ============================================================================

const TSMeta* TSTypeRegistry::tsd(const value::TypeMeta* key_type, const TSMeta* value_ts) {
    TSDKey cache_key{key_type, value_ts};
    auto it = tsd_cache_.find(cache_key);
    if (it != tsd_cache_.end()) {
        return it->second;
    }

    // Build the map value schema from key and value's value type
    const value::TypeMeta* value_schema = nullptr;
    if (key_type && value_ts && value_ts->value_type) {
        value_schema = value::TypeRegistry::instance()
            .map(key_type, value_ts->value_type)
            .build();
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSD;
    meta->key_type = key_type;
    meta->element_ts = value_ts;
    meta->value_type = value_schema;

    tsd_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSL[TS, Size] - Time-Series List
// ============================================================================

const TSMeta* TSTypeRegistry::tsl(const TSMeta* element_ts, size_t fixed_size) {
    TSLKey cache_key{element_ts, fixed_size};
    auto it = tsl_cache_.find(cache_key);
    if (it != tsl_cache_.end()) {
        return it->second;
    }

    // Build the list value schema from element's value type
    const value::TypeMeta* value_schema = nullptr;
    if (element_ts && element_ts->value_type) {
        value_schema = value::TypeRegistry::instance()
            .fixed_list(element_ts->value_type, fixed_size)
            .build();
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSL;
    meta->element_ts = element_ts;
    meta->fixed_size = fixed_size;
    meta->value_type = value_schema;

    tsl_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSW[T, period, min_period] - Tick-Based Window
// ============================================================================

const TSMeta* TSTypeRegistry::tsw(const value::TypeMeta* value_type,
                                   size_t period, size_t min_period) {
    TSWKey cache_key{
        value_type,
        false,
        static_cast<int64_t>(period),
        static_cast<int64_t>(min_period)
    };
    auto it = tsw_cache_.find(cache_key);
    if (it != tsw_cache_.end()) {
        return it->second;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSW;
    meta->value_type = value_type;
    meta->is_duration_based = false;
    meta->window.tick.period = period;
    meta->window.tick.min_period = min_period;

    tsw_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSW[T, time_range, min_time_range] - Duration-Based Window
// ============================================================================

const TSMeta* TSTypeRegistry::tsw_duration(const value::TypeMeta* value_type,
                                            engine_time_delta_t time_range,
                                            engine_time_delta_t min_time_range) {
    TSWKey cache_key{
        value_type,
        true,
        time_range.count(),
        min_time_range.count()
    };
    auto it = tsw_cache_.find(cache_key);
    if (it != tsw_cache_.end()) {
        return it->second;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::TSW;
    meta->value_type = value_type;
    meta->is_duration_based = true;
    meta->window.duration.time_range = time_range;
    meta->window.duration.min_time_range = min_time_range;

    tsw_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// TSB[Schema] - Time-Series Bundle
// ============================================================================

const TSMeta* TSTypeRegistry::tsb(
    const std::vector<std::pair<std::string, const TSMeta*>>& fields,
    const std::string& name,
    nb::object python_type)
{
    TSBKey cache_key{fields, name};
    auto it = tsb_cache_.find(cache_key);
    if (it != tsb_cache_.end()) {
        return it->second;
    }

    const size_t field_count = fields.size();
    auto field_array = std::make_unique<TSBFieldInfo[]>(field_count);

    for (size_t i = 0; i < field_count; ++i) {
        field_array[i].name = intern_string(fields[i].first);
        field_array[i].index = i;
        field_array[i].ts_type = fields[i].second;
    }

    // Build the bundle value schema from fields
    auto builder = value::TypeRegistry::instance().bundle(name);
    for (size_t i = 0; i < field_count; ++i) {
        const auto& f = fields[i];
        if (f.second && f.second->value_type) {
            builder.field(f.first.c_str(), f.second->value_type);
        }
    }
    const value::TypeMeta* value_schema = builder.build();

    auto* meta = create_schema();
    meta->kind = TSKind::TSB;
    meta->fields = field_array.get();
    meta->field_count = field_count;
    meta->bundle_name = intern_string(name);
    meta->python_type = std::move(python_type);
    meta->value_type = value_schema;

    field_arrays_.push_back(std::move(field_array));

    tsb_cache_[cache_key] = meta;
    return meta;
}

// ============================================================================
// REF[TS] - Time-Series Reference
// ============================================================================

const TSMeta* TSTypeRegistry::ref(const TSMeta* referenced_ts) {
    auto it = ref_cache_.find(referenced_ts);
    if (it != ref_cache_.end()) {
        return it->second;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::REF;
    meta->element_ts = referenced_ts;
    meta->value_type = ts_reference_type_meta();

    ref_cache_[referenced_ts] = meta;
    return meta;
}

// ============================================================================
// SIGNAL - Singleton
// ============================================================================

const TSMeta* TSTypeRegistry::signal() {
    if (signal_singleton_) {
        return signal_singleton_;
    }

    auto* meta = create_schema();
    meta->kind = TSKind::SIGNAL;
    meta->value_type = value::scalar_type_meta<bool>();
    signal_singleton_ = meta;
    return meta;
}

// ============================================================================
// Schema Dereferencing
// ============================================================================

bool TSTypeRegistry::contains_ref(const TSMeta* meta) {
    if (!meta) return false;

    switch (meta->kind) {
        case TSKind::REF:
            return true;
        case TSKind::TSB:
            for (size_t i = 0; i < meta->field_count; ++i) {
                if (contains_ref(meta->fields[i].ts_type)) return true;
            }
            return false;
        case TSKind::TSL:
        case TSKind::TSD:
            return contains_ref(meta->element_ts);
        default:
            return false;
    }
}

const TSMeta* TSTypeRegistry::dereference(const TSMeta* source) {
    if (!source) return nullptr;

    auto it = deref_cache_.find(source);
    if (it != deref_cache_.end()) {
        return it->second;
    }

    const TSMeta* result = nullptr;

    switch (source->kind) {
        case TSKind::REF:
            result = dereference(source->element_ts);
            break;

        case TSKind::TSB: {
            bool has_ref = false;
            for (size_t i = 0; i < source->field_count; ++i) {
                if (contains_ref(source->fields[i].ts_type)) {
                    has_ref = true;
                    break;
                }
            }

            if (!has_ref) {
                result = source;
            } else {
                std::vector<std::pair<std::string, const TSMeta*>> deref_fields;
                deref_fields.reserve(source->field_count);
                for (size_t i = 0; i < source->field_count; ++i) {
                    deref_fields.emplace_back(
                        source->fields[i].name,
                        dereference(source->fields[i].ts_type)
                    );
                }
                std::string deref_name = source->bundle_name ? source->bundle_name : "";
                if (!deref_name.empty()) deref_name += "_deref";
                result = tsb(deref_fields, deref_name, source->python_type);
            }
            break;
        }

        case TSKind::TSL: {
            const TSMeta* deref_element = dereference(source->element_ts);
            result = (deref_element == source->element_ts)
                ? source
                : tsl(deref_element, source->fixed_size);
            break;
        }

        case TSKind::TSD: {
            const TSMeta* deref_value = dereference(source->element_ts);
            result = (deref_value == source->element_ts)
                ? source
                : tsd(source->key_type, deref_value);
            break;
        }

        default:
            result = source;
            break;
    }

    deref_cache_[source] = result;
    return result;
}

} // namespace hgraph
