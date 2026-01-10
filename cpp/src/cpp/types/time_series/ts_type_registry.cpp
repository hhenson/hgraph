//
// Created by Claude on 05/01/2025.
//

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/python/chrono.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/pair.h>

namespace hgraph {

    // ============================================================================
    // TSBKey implementation
    // ============================================================================

    bool TSTypeRegistry::TSBKey::operator==(const TSBKey& other) const {
        if (name != other.name) return false;
        if (fields.size() != other.fields.size()) return false;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].first != other.fields[i].first) return false;
            if (fields[i].second != other.fields[i].second) return false;
        }
        return true;
    }

    size_t TSTypeRegistry::TSBKeyHash::operator()(const TSBKey& key) const {
        size_t h = std::hash<std::string>{}(key.name);
        for (const auto& [name, type] : key.fields) {
            h ^= std::hash<std::string>{}(name) << 1;
            h ^= std::hash<const void*>{}(type) << 2;
        }
        return h;
    }

    // ============================================================================
    // TSTypeRegistry implementation
    // ============================================================================

    TSTypeRegistry& TSTypeRegistry::instance() {
        static TSTypeRegistry registry;
        return registry;
    }

    const TSValueMeta* TSTypeRegistry::ts(const value::TypeMeta* scalar_schema) {
        std::lock_guard<std::mutex> lock(_mutex);

        auto it = _ts_cache.find(scalar_schema);
        if (it != _ts_cache.end()) {
            return it->second;
        }

        auto meta = std::make_unique<TSValueMeta>(scalar_schema);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _ts_cache[scalar_schema] = ptr;
        return ptr;
    }

    const TSBTypeMeta* TSTypeRegistry::tsb(
        const std::vector<std::pair<std::string, const TSMeta*>>& fields,
        const std::string& name,
        nb::object python_type)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        TSBKey key{fields, name};
        auto it = _tsb_cache.find(key);
        if (it != _tsb_cache.end()) {
            return it->second;
        }

        // Build TSBFieldInfo vector
        std::vector<TSBFieldInfo> field_infos;
        field_infos.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            field_infos.push_back(TSBFieldInfo{
                fields[i].first,  // name
                i,                // index
                fields[i].second  // type
            });
        }

        // Construct the bundle value schema from field value schemas
        auto& type_registry = value::TypeRegistry::instance();
        value::BundleTypeBuilder builder = type_registry.bundle(name);
        for (const auto& field : fields) {
            const value::TypeMeta* field_value_schema = field.second->value_schema();
            builder.field(field.first.c_str(), field_value_schema);
        }
        // Set Python type if provided
        if (python_type.is_valid() && !python_type.is_none()) {
            builder.with_python_type(std::move(python_type));
        }
        const value::TypeMeta* bundle_schema = builder.build();

        auto meta = std::make_unique<TSBTypeMeta>(
            std::move(field_infos), bundle_schema, name);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _tsb_cache[key] = ptr;
        return ptr;
    }

    const TSLTypeMeta* TSTypeRegistry::tsl(
        const TSMeta* element_type,
        size_t fixed_size)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        TSLKey key{element_type, fixed_size};
        auto it = _tsl_cache.find(key);
        if (it != _tsl_cache.end()) {
            return it->second;
        }

        // Construct the list value schema from element value schema
        auto& type_registry = value::TypeRegistry::instance();
        const value::TypeMeta* element_value_schema = element_type->value_schema();
        const value::TypeMeta* list_schema = (fixed_size > 0)
            ? type_registry.fixed_list(element_value_schema, fixed_size).build()
            : type_registry.list(element_value_schema).build();

        auto meta = std::make_unique<TSLTypeMeta>(element_type, fixed_size, list_schema);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _tsl_cache[key] = ptr;
        return ptr;
    }

    const TSDTypeMeta* TSTypeRegistry::tsd(
        const value::TypeMeta* key_type,
        const TSMeta* value_type)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        TSDKey key{key_type, value_type};
        auto it = _tsd_cache.find(key);
        if (it != _tsd_cache.end()) {
            return it->second;
        }

        // Construct the dict value schema from key type and value's value schema
        auto& type_registry = value::TypeRegistry::instance();
        const value::TypeMeta* value_schema = value_type->value_schema();
        const value::TypeMeta* dict_schema = type_registry.map(key_type, value_schema).build();

        auto meta = std::make_unique<TSDTypeMeta>(key_type, value_type, dict_schema);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _tsd_cache[key] = ptr;
        return ptr;
    }

    const TSSTypeMeta* TSTypeRegistry::tss(
        const value::TypeMeta* element_type)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        auto it = _tss_cache.find(element_type);
        if (it != _tss_cache.end()) {
            return it->second;
        }

        // Construct the set value schema from element type
        auto& type_registry = value::TypeRegistry::instance();
        const value::TypeMeta* set_schema = type_registry.set(element_type).build();

        auto meta = std::make_unique<TSSTypeMeta>(element_type, set_schema);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _tss_cache[element_type] = ptr;
        return ptr;
    }

    const TSWTypeMeta* TSTypeRegistry::tsw(
        const value::TypeMeta* value_type,
        size_t size,
        size_t min_size)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        TSWKey key{value_type, false, static_cast<int64_t>(size), static_cast<int64_t>(min_size)};
        auto it = _tsw_cache.find(key);
        if (it != _tsw_cache.end()) {
            return it->second;
        }

        // Construct the window value schema using Window storage (dual cyclic buffers for values + times)
        auto& type_registry = value::TypeRegistry::instance();
        const value::TypeMeta* window_schema = type_registry.window(value_type, size, min_size).build();

        auto meta = std::make_unique<TSWTypeMeta>(value_type, size, min_size, window_schema);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _tsw_cache[key] = ptr;
        return ptr;
    }

    const TSWTypeMeta* TSTypeRegistry::tsw_duration(
        const value::TypeMeta* value_type,
        engine_time_delta_t time_range,
        engine_time_delta_t min_time_range)
    {
        std::lock_guard<std::mutex> lock(_mutex);

        TSWKey key{value_type, true, time_range.count(), min_time_range.count()};
        auto it = _tsw_cache.find(key);
        if (it != _tsw_cache.end()) {
            return it->second;
        }

        // Construct the window value schema as a dynamic list of the value type
        // Duration-based windows have variable size, so use dynamic list
        auto& type_registry = value::TypeRegistry::instance();
        const value::TypeMeta* window_schema = type_registry.list(value_type).build();

        // Use the time-based constructor (with tag parameter)
        auto meta = std::make_unique<TSWTypeMeta>(value_type, time_range, min_time_range, window_schema, true);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _tsw_cache[key] = ptr;
        return ptr;
    }

    const REFTypeMeta* TSTypeRegistry::ref(const TSMeta* referenced_type) {
        std::lock_guard<std::mutex> lock(_mutex);

        auto it = _ref_cache.find(referenced_type);
        if (it != _ref_cache.end()) {
            return it->second;
        }

        auto meta = std::make_unique<REFTypeMeta>(referenced_type);
        auto* ptr = meta.get();
        _owned.push_back(std::move(meta));
        _ref_cache[referenced_type] = ptr;
        return ptr;
    }

    const SignalTypeMeta* TSTypeRegistry::signal() {
        std::lock_guard<std::mutex> lock(_mutex);

        if (!_signal) {
            _signal = std::make_unique<SignalTypeMeta>();
        }
        return _signal.get();
    }

    // ============================================================================
    // Python Bindings
    // ============================================================================

    void register_ts_type_registry_with_nanobind(nb::module_& m) {
        nb::class_<TSTypeRegistry>(m, "TSTypeRegistry")
            .def_static("instance", &TSTypeRegistry::instance,
                        nb::rv_policy::reference)
            .def("ts", &TSTypeRegistry::ts,
                 nb::rv_policy::reference,
                 "Get or create a TSValueMeta for the given scalar type")
            .def("tsb", &TSTypeRegistry::tsb,
                 nb::arg("fields"),
                 nb::arg("name") = "",
                 nb::arg("python_type") = nb::none(),
                 nb::rv_policy::reference,
                 "Get or create a TSBTypeMeta for the given fields.\n\n"
                 "The value schema is constructed internally from field value schemas.\n"
                 "Pass python_type (e.g., CompoundScalar class) to have to_python() return that type.")
            .def("tsl", &TSTypeRegistry::tsl,
                 nb::arg("element_type"),
                 nb::arg("fixed_size"),
                 nb::rv_policy::reference,
                 "Get or create a TSLTypeMeta for the given element type.\n\n"
                 "The value schema is constructed internally from element_type->value_schema().")
            .def("tsd", &TSTypeRegistry::tsd,
                 nb::arg("key_type"),
                 nb::arg("value_type"),
                 nb::rv_policy::reference,
                 "Get or create a TSDTypeMeta for the given key and value types.\n\n"
                 "The value schema is constructed internally from key_type and value_type->value_schema().")
            .def("tss", &TSTypeRegistry::tss,
                 nb::arg("element_type"),
                 nb::rv_policy::reference,
                 "Get or create a TSSTypeMeta for the given element type.\n\n"
                 "The value schema is constructed internally as Set[element_type].")
            .def("tsw", &TSTypeRegistry::tsw,
                 nb::arg("value_type"),
                 nb::arg("size"),
                 nb::arg("min_size"),
                 nb::rv_policy::reference,
                 "Get or create a size-based TSWTypeMeta (tick count window).\n\n"
                 "The value schema is constructed internally as FixedList[value_type, size].")
            .def("tsw_duration", &TSTypeRegistry::tsw_duration,
                 nb::arg("value_type"),
                 nb::arg("time_range"),
                 nb::arg("min_time_range"),
                 nb::rv_policy::reference,
                 "Get or create a duration-based TSWTypeMeta (time window).\n\n"
                 "The value schema is constructed internally as List[value_type].\n"
                 "Accepts Python timedelta objects directly.")
            .def("ref", &TSTypeRegistry::ref,
                 nb::rv_policy::reference,
                 "Get or create a REFTypeMeta for the given referenced type")
            .def("signal", &TSTypeRegistry::signal,
                 nb::rv_policy::reference,
                 "Get the singleton SignalTypeMeta");
    }

} // namespace hgraph
