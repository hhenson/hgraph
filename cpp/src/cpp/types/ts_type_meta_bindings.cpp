//
// Created by Claude on 15/12/2025.
//
// Python bindings for TimeSeriesTypeMeta and related types.
//

#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_meta.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {

namespace {
    // Hash seeds for different time-series types
    constexpr size_t TS_SEED = 0x54530000;    // "TS\0\0"
    constexpr size_t TSS_SEED = 0x545353;     // "TSS"
    constexpr size_t TSD_SEED = 0x545344;     // "TSD"
    constexpr size_t TSL_SEED = 0x54534C;     // "TSL"
    constexpr size_t TSB_SEED = 0x545342;     // "TSB"
    constexpr size_t TSW_SEED = 0x545357;     // "TSW"
    constexpr size_t REF_SEED = 0x524546;     // "REF"
}

void register_ts_type_meta_with_nanobind(nb::module_ &m) {
    // ========================================================================
    // TimeSeriesKind enum
    // ========================================================================
    nb::enum_<TimeSeriesKind>(m, "TimeSeriesKind")
        .value("TS", TimeSeriesKind::TS)
        .value("TSS", TimeSeriesKind::TSS)
        .value("TSD", TimeSeriesKind::TSD)
        .value("TSL", TimeSeriesKind::TSL)
        .value("TSB", TimeSeriesKind::TSB)
        .value("TSW", TimeSeriesKind::TSW)
        .value("REF", TimeSeriesKind::REF)
        .export_values();

    // ========================================================================
    // TimeSeriesTypeMeta base class
    // ========================================================================
    nb::class_<TimeSeriesTypeMeta>(m, "TimeSeriesTypeMeta")
        .def_prop_ro("ts_kind", [](const TimeSeriesTypeMeta& meta) { return meta.ts_kind; })
        .def_prop_ro("name", [](const TimeSeriesTypeMeta& meta) { return meta.name ? meta.name : ""; })
        .def("type_name_str", &TimeSeriesTypeMeta::type_name_str)
        .def("__repr__", [](const TimeSeriesTypeMeta& meta) {
            return "TimeSeriesTypeMeta(" + meta.type_name_str() + ")";
        });

    // ========================================================================
    // Factory Functions
    // ========================================================================

    // Factory: get_ts_type_meta(scalar_meta) -> TSTypeMeta*
    // Creates a TS[T] type metadata where T is the scalar type
    m.def("get_ts_type_meta", [](const value::TypeMeta* scalar_type) -> const TimeSeriesTypeMeta* {
        size_t key = ts_hash_combine(TS_SEED, reinterpret_cast<size_t>(scalar_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TS;
        meta->scalar_type = scalar_type;

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "scalar_type"_a,
       "Get or create a TS[T] TypeMeta for the given scalar type.");

    // Factory: get_tss_type_meta(element_meta) -> TSSTypeMeta*
    // Creates a TSS[T] type metadata where T is the element type
    m.def("get_tss_type_meta", [](const value::TypeMeta* element_type) -> const TimeSeriesTypeMeta* {
        size_t key = ts_hash_combine(TSS_SEED, reinterpret_cast<size_t>(element_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSSTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSS;
        meta->element_type = element_type;

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "element_type"_a,
       "Get or create a TSS[T] TypeMeta for the given element type.");

    // Factory: get_tsd_type_meta(key_meta, value_ts_meta) -> TSDTypeMeta*
    // Creates a TSD[K, V] type metadata where K is a scalar key and V is a time-series
    m.def("get_tsd_type_meta", [](const value::TypeMeta* key_type,
                                   const TimeSeriesTypeMeta* value_ts_type) -> const TimeSeriesTypeMeta* {
        size_t key = ts_hash_combine(TSD_SEED, reinterpret_cast<size_t>(key_type));
        key = ts_hash_combine(key, reinterpret_cast<size_t>(value_ts_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSDTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSD;
        meta->key_type = key_type;
        meta->value_ts_type = value_ts_type;

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "key_type"_a, "value_ts_type"_a,
       "Get or create a TSD[K, V] TypeMeta for the given key and value time-series types.");

    // Factory: get_tsl_type_meta(element_ts_meta, size) -> TSLTypeMeta*
    // Creates a TSL[V, Size] type metadata where V is a time-series element type
    m.def("get_tsl_type_meta", [](const TimeSeriesTypeMeta* element_ts_type,
                                   int64_t size) -> const TimeSeriesTypeMeta* {
        size_t key = ts_hash_combine(TSL_SEED, reinterpret_cast<size_t>(element_ts_type));
        // +1 to differentiate size=-1 from other values in the hash
        key = ts_hash_combine(key, static_cast<size_t>(size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSLTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSL;
        meta->element_ts_type = element_ts_type;
        meta->size = size;

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "element_ts_type"_a, "size"_a,
       "Get or create a TSL[V, Size] TypeMeta. Use size=-1 for dynamic/unresolved size.");

    // Factory: get_tsb_type_meta(fields, type_name) -> TSBTypeMeta*
    // Creates a TSB[Schema] type metadata from a list of (name, type) tuples
    m.def("get_tsb_type_meta", [](nb::list fields, nb::object type_name) -> const TimeSeriesTypeMeta* {
        // Build cache key from field names and types
        size_t key = TSB_SEED;
        std::vector<TSBTypeMeta::Field> field_vec;

        for (auto item : fields) {
            auto tuple = nb::cast<nb::tuple>(item);
            auto name = nb::cast<std::string>(tuple[0]);
            auto* field_type = nb::cast<const TimeSeriesTypeMeta*>(tuple[1]);
            key = ts_hash_combine(key, std::hash<std::string>{}(name));
            key = ts_hash_combine(key, reinterpret_cast<size_t>(field_type));
            field_vec.push_back({name, field_type});
        }

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSBTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSB;
        meta->fields = std::move(field_vec);

        // Handle type_name - need to store the string persistently
        static std::vector<std::string> stored_names;
        if (!type_name.is_none()) {
            stored_names.push_back(nb::cast<std::string>(type_name));
            meta->name = stored_names.back().c_str();
        }

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "fields"_a, "type_name"_a = nb::none(),
       "Get or create a TSB[Schema] TypeMeta from field definitions. "
       "Fields should be a list of (name, TimeSeriesTypeMeta) tuples.");

    // Factory: get_tsw_type_meta(scalar_meta, size, min_size) -> TSWTypeMeta*
    // Creates a TSW[T, Size] type metadata for time-series windows
    m.def("get_tsw_type_meta", [](const value::TypeMeta* scalar_type,
                                   int64_t size, int64_t min_size) -> const TimeSeriesTypeMeta* {
        size_t key = ts_hash_combine(TSW_SEED, reinterpret_cast<size_t>(scalar_type));
        key = ts_hash_combine(key, static_cast<size_t>(size + 1));
        key = ts_hash_combine(key, static_cast<size_t>(min_size + 1));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<TSWTypeMeta>();
        meta->ts_kind = TimeSeriesKind::TSW;
        meta->scalar_type = scalar_type;
        meta->size = size;
        meta->min_size = min_size;

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "scalar_type"_a, "size"_a, "min_size"_a,
       "Get or create a TSW[T, Size] TypeMeta for time-series windows.");

    // Factory: get_ref_type_meta(value_ts_meta) -> REFTypeMeta*
    // Creates a REF[TS_TYPE] type metadata for time-series references
    m.def("get_ref_type_meta", [](const TimeSeriesTypeMeta* value_ts_type) -> const TimeSeriesTypeMeta* {
        size_t key = ts_hash_combine(REF_SEED, reinterpret_cast<size_t>(value_ts_type));

        auto& registry = TimeSeriesTypeRegistry::global();
        if (auto* existing = registry.lookup_by_key(key)) {
            return existing;
        }

        auto meta = std::make_unique<REFTypeMeta>();
        meta->ts_kind = TimeSeriesKind::REF;
        meta->value_ts_type = value_ts_type;

        return registry.register_by_key(key, std::move(meta));
    }, nb::rv_policy::reference, "value_ts_type"_a,
       "Get or create a REF[TS_TYPE] TypeMeta for the given time-series type.");
}

} // namespace hgraph
