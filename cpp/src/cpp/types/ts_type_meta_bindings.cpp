//
// Created by Claude on 15/12/2025.
//
// Python bindings for TSMeta and related types.
//
// This file provides nanobind bindings for the type API defined in type_api.h.
// It uses the runtime_python:: namespace functions for Python-aware type construction.
//

// Enable Python-aware type construction functions
#define HGRAPH_TYPE_API_WITH_PYTHON
#include <hgraph/types/type_api.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {

using namespace types;

void register_ts_type_meta_with_nanobind(nb::module_ &m) {
    // ========================================================================
    // TSKind enum
    // ========================================================================
    nb::enum_<TSKind>(m, "TSKind")
        .value("TS", TSKind::TS)
        .value("TSS", TSKind::TSS)
        .value("TSD", TSKind::TSD)
        .value("TSL", TSKind::TSL)
        .value("TSB", TSKind::TSB)
        .value("TSW", TSKind::TSW)
        .value("REF", TSKind::REF)
        .export_values();

    // ========================================================================
    // TSMeta base class
    // ========================================================================
    nb::class_<TSMeta>(m, "TSMeta")
        .def_prop_ro("ts_kind", [](const TSMeta& meta) { return meta.ts_kind; })
        .def_prop_ro("name", [](const TSMeta& meta) { return meta.name ? meta.name : ""; })
        .def("type_name_str", &TSMeta::type_name_str)
        .def("__repr__", [](const TSMeta& meta) {
            return "TSMeta(" + meta.type_name_str() + ")";
        });

    // ========================================================================
    // Factory Functions
    // ========================================================================

    // Factory: get_ts_type_meta(scalar_meta) -> TSValueMeta*
    // Creates a TS[T] type metadata where T is the scalar type
    m.def("get_ts_type_meta", [](const value::TypeMeta* scalar_type) -> const TSMeta* {
        return runtime_python::ts(scalar_type);
    }, nb::rv_policy::reference, "scalar_type"_a,
       "Get or create a TS[T] TypeMeta for the given scalar type.");

    // Factory: get_tss_type_meta(element_meta) -> TSSTypeMeta*
    // Creates a TSS[T] type metadata where T is the element type
    m.def("get_tss_type_meta", [](const value::TypeMeta* element_type) -> const TSMeta* {
        return runtime_python::tss(element_type);
    }, nb::rv_policy::reference, "element_type"_a,
       "Get or create a TSS[T] TypeMeta for the given element type.");

    // Factory: get_tsd_type_meta(key_meta, value_ts_meta) -> TSDTypeMeta*
    // Creates a TSD[K, V] type metadata where K is a scalar key and V is a time-series
    m.def("get_tsd_type_meta", [](const value::TypeMeta* key_type,
                                   const TSMeta* value_ts_type) -> const TSMeta* {
        return runtime_python::tsd(key_type, value_ts_type);
    }, nb::rv_policy::reference, "key_type"_a, "value_ts_type"_a,
       "Get or create a TSD[K, V] TypeMeta for the given key and value time-series types.");

    // Factory: get_tsl_type_meta(element_ts_meta, size) -> TSLTypeMeta*
    // Creates a TSL[V, Size] type metadata where V is a time-series element type
    m.def("get_tsl_type_meta", [](const TSMeta* element_ts_type,
                                   int64_t size) -> const TSMeta* {
        return runtime_python::tsl(element_ts_type, size);
    }, nb::rv_policy::reference, "element_ts_type"_a, "size"_a,
       "Get or create a TSL[V, Size] TypeMeta. Use size=-1 for dynamic/unresolved size.");

    // Factory: get_tsb_type_meta(fields, type_name) -> TSBTypeMeta*
    // Creates a TSB[Schema] type metadata from a list of (name, type) tuples
    m.def("get_tsb_type_meta", [](nb::list fields, nb::object type_name) -> const TSMeta* {
        // Convert Python list to C++ vector
        std::vector<std::pair<std::string, const TSMeta*>> field_vec;
        for (auto item : fields) {
            auto tuple = nb::cast<nb::tuple>(item);
            auto name = nb::cast<std::string>(tuple[0]);
            auto* field_type = nb::cast<const TSMeta*>(tuple[1]);
            field_vec.emplace_back(name, field_type);
        }

        const char* name_cstr = nullptr;
        std::string name_str;
        if (!type_name.is_none()) {
            name_str = nb::cast<std::string>(type_name);
            name_cstr = name_str.c_str();
        }

        return runtime_python::tsb(field_vec, name_cstr);
    }, nb::rv_policy::reference, "fields"_a, "type_name"_a = nb::none(),
       "Get or create a TSB[Schema] TypeMeta from field definitions. "
       "Fields should be a list of (name, TSMeta) tuples.");

    // Factory: get_tsw_type_meta(scalar_meta, size, min_size) -> TSWTypeMeta*
    // Creates a TSW[T, Size] type metadata for time-series windows (count-based)
    m.def("get_tsw_type_meta", [](const value::TypeMeta* scalar_type,
                                   int64_t size, int64_t min_size) -> const TSMeta* {
        return runtime_python::tsw(scalar_type, size, min_size);
    }, nb::rv_policy::reference, "scalar_type"_a, "size"_a, "min_size"_a,
       "Get or create a TSW[T, Size] TypeMeta for count-based time-series windows.");

    // Factory: get_tsw_time_type_meta(scalar_meta, duration_us, min_size) -> TSWTypeMeta*
    // Creates a time-based TSW type metadata
    m.def("get_tsw_time_type_meta", [](const value::TypeMeta* scalar_type,
                                        int64_t duration_us, int64_t min_size) -> const TSMeta* {
        return runtime_python::tsw_time(scalar_type, duration_us, min_size);
    }, nb::rv_policy::reference, "scalar_type"_a, "duration_us"_a, "min_size"_a = 0,
       "Get or create a time-based TSW TypeMeta. Duration is in microseconds.");

    // Factory: get_ref_type_meta(value_ts_meta) -> REFTypeMeta*
    // Creates a REF[TS_TYPE] type metadata for time-series references
    m.def("get_ref_type_meta", [](const TSMeta* value_ts_type) -> const TSMeta* {
        return runtime_python::ref(value_ts_type);
    }, nb::rv_policy::reference, "value_ts_type"_a,
       "Get or create a REF[TS_TYPE] TypeMeta for the given time-series type.");
}

} // namespace hgraph
