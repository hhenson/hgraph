/**
 * @file py_ts_type_registry.cpp
 * @brief Python bindings for TSTypeRegistry and related types.
 *
 * This file implements nanobind bindings for:
 * - TSKind enum
 * - TSBFieldInfo class
 * - TSMeta class
 * - TSTypeRegistry class
 */

#include <hgraph/api/python/py_ts_type_registry.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/pair.h>

namespace hgraph {

using namespace nanobind::literals;

// ============================================================================
// TSKind Enum Binding
// ============================================================================

static void register_ts_kind(nb::module_& m) {
    nb::enum_<TSKind>(m, "TSKind", "Categories of time-series types")
        .value("TSValue", TSKind::TSValue, "TS[T] - scalar time-series")
        .value("TSS", TSKind::TSS, "TSS[T] - time-series set")
        .value("TSD", TSKind::TSD, "TSD[K, V] - time-series dict")
        .value("TSL", TSKind::TSL, "TSL[TS, Size] - time-series list")
        .value("TSW", TSKind::TSW, "TSW[T, size, min_size] - time-series window")
        .value("TSB", TSKind::TSB, "TSB[Schema] - time-series bundle")
        .value("REF", TSKind::REF, "REF[TS] - reference to time-series")
        .value("SIGNAL", TSKind::SIGNAL, "SIGNAL - presence/absence marker");
}

// ============================================================================
// TSBFieldInfo Binding
// ============================================================================

static void register_tsb_field_info(nb::module_& m) {
    nb::class_<TSBFieldInfo>(m, "TSBFieldInfo",
        "Metadata for a single field in a TSB schema")
        .def_prop_ro("name", [](const TSBFieldInfo& self) {
            return self.name ? std::string(self.name) : std::string();
        }, "Get the field name")
        .def_ro("index", &TSBFieldInfo::index, "Get the field index (0-based)")
        .def_prop_ro("ts_type", [](const TSBFieldInfo& self) {
            return self.ts_type;
        }, nb::rv_policy::reference, "Get the field's time-series schema")
        .def("__repr__", [](const TSBFieldInfo& self) {
            return "TSBFieldInfo(name='" + (self.name ? std::string(self.name) : "<null>") +
                   "', index=" + std::to_string(self.index) + ")";
        });
}

// ============================================================================
// TSMeta Binding
// ============================================================================

static void register_ts_meta(nb::module_& m) {
    nb::class_<TSMeta>(m, "TSMeta", "Time-series type metadata")
        // Kind
        .def_ro("kind", &TSMeta::kind, "Time-series category")

        // Value/key types
        .def_prop_ro("value_type", [](const TSMeta& self) {
            return self.value_type;
        }, nb::rv_policy::reference, "Value type (for TS, TSS, TSW)")
        .def_prop_ro("key_type", [](const TSMeta& self) {
            return self.key_type;
        }, nb::rv_policy::reference, "Key type (for TSD)")
        .def_prop_ro("element_ts", [](const TSMeta& self) {
            return self.element_ts;
        }, nb::rv_policy::reference, "Element TS schema (for TSD value, TSL, REF)")

        // Size
        .def_ro("fixed_size", &TSMeta::fixed_size, "Fixed size (for TSL, 0=dynamic)")

        // Window properties
        .def_ro("is_duration_based", &TSMeta::is_duration_based,
            "True if duration-based window (for TSW)")
        .def_prop_ro("period", [](const TSMeta& self) -> size_t {
            return self.is_duration_based ? 0 : self.window.tick.period;
        }, "Tick period (for tick-based TSW)")
        .def_prop_ro("min_period", [](const TSMeta& self) -> size_t {
            return self.is_duration_based ? 0 : self.window.tick.min_period;
        }, "Minimum tick period (for tick-based TSW)")
        .def_prop_ro("time_range", [](const TSMeta& self) {
            return self.is_duration_based ? self.window.duration.time_range
                                          : engine_time_delta_t{0};
        }, "Time range (for duration-based TSW)")
        .def_prop_ro("min_time_range", [](const TSMeta& self) {
            return self.is_duration_based ? self.window.duration.min_time_range
                                          : engine_time_delta_t{0};
        }, "Minimum time range (for duration-based TSW)")

        // Bundle properties
        .def_ro("field_count", &TSMeta::field_count, "Number of fields (for TSB)")
        .def_prop_ro("bundle_name", [](const TSMeta& self) {
            return self.bundle_name ? std::string(self.bundle_name) : std::string();
        }, "Bundle schema name (for TSB)")
        .def_prop_ro("fields", [](const TSMeta& self) {
            std::vector<const TSBFieldInfo*> result;
            if (self.fields && self.field_count > 0) {
                result.reserve(self.field_count);
                for (size_t i = 0; i < self.field_count; ++i) {
                    result.push_back(&self.fields[i]);
                }
            }
            return result;
        }, nb::rv_policy::reference_internal, "Field metadata (for TSB)")
        .def_prop_ro("python_type", [](const TSMeta& self) {
            return self.python_type;
        }, "Python type for reconstruction (for TSB)")

        // Helper methods
        .def("is_collection", &TSMeta::is_collection,
            "True if TSS, TSD, TSL, or TSB")
        .def("is_scalar_ts", &TSMeta::is_scalar_ts,
            "True if TS, TSW, or SIGNAL")

        // Repr
        .def("__repr__", [](const TSMeta& self) {
            std::string kind_str;
            switch (self.kind) {
                case TSKind::TSValue: kind_str = "TSValue"; break;
                case TSKind::TSS: kind_str = "TSS"; break;
                case TSKind::TSD: kind_str = "TSD"; break;
                case TSKind::TSL: kind_str = "TSL"; break;
                case TSKind::TSW: kind_str = "TSW"; break;
                case TSKind::TSB: kind_str = "TSB"; break;
                case TSKind::REF: kind_str = "REF"; break;
                case TSKind::SIGNAL: kind_str = "SIGNAL"; break;
            }
            return "TSMeta(kind=" + kind_str + ")";
        });
}

// ============================================================================
// TSTypeRegistry Binding
// ============================================================================

static void register_ts_type_registry(nb::module_& m) {
    nb::class_<TSTypeRegistry>(m, "TSTypeRegistry",
        "Registry for time-series type schemas (singleton)")

        // Singleton accessor
        .def_static("instance", &TSTypeRegistry::instance,
            nb::rv_policy::reference,
            "Get the singleton instance")

        // TS[T]
        .def("ts", &TSTypeRegistry::ts,
            nb::rv_policy::reference,
            "value_type"_a,
            "Create TS[T] schema for scalar time-series")

        // TSS[T]
        .def("tss", &TSTypeRegistry::tss,
            nb::rv_policy::reference,
            "element_type"_a,
            "Create TSS[T] schema for time-series set")

        // TSD[K, V]
        .def("tsd", &TSTypeRegistry::tsd,
            nb::rv_policy::reference,
            "key_type"_a, "value_ts"_a,
            "Create TSD[K, V] schema for time-series dict")

        // TSL[TS, Size]
        .def("tsl", &TSTypeRegistry::tsl,
            nb::rv_policy::reference,
            "element_ts"_a, "fixed_size"_a = 0,
            "Create TSL[TS, Size] schema for time-series list")

        // TSW tick-based
        .def("tsw", &TSTypeRegistry::tsw,
            nb::rv_policy::reference,
            "value_type"_a, "period"_a, "min_period"_a = 0,
            "Create TSW[T] schema for tick-based window")

        // TSW duration-based
        .def("tsw_duration", &TSTypeRegistry::tsw_duration,
            nb::rv_policy::reference,
            "value_type"_a, "time_range"_a,
            "min_time_range"_a = engine_time_delta_t{0},
            "Create TSW[T] schema for duration-based window")

        // TSB
        .def("tsb", &TSTypeRegistry::tsb,
            nb::rv_policy::reference,
            "fields"_a, "name"_a, "python_type"_a = nb::none(),
            "Create TSB[Schema] schema for time-series bundle")

        // REF
        .def("ref", &TSTypeRegistry::ref,
            nb::rv_policy::reference,
            "referenced_ts"_a,
            "Create REF[TS] schema for time-series reference")

        // SIGNAL
        .def("signal", &TSTypeRegistry::signal,
            nb::rv_policy::reference,
            "Get the SIGNAL schema singleton");
}

// ============================================================================
// Module Registration
// ============================================================================

void ts_type_registry_register_with_nanobind(nb::module_& m) {
    register_ts_kind(m);
    register_tsb_field_info(m);
    register_ts_meta(m);
    register_ts_type_registry(m);
}

} // namespace hgraph
