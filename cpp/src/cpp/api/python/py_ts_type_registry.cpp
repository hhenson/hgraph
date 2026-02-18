/**
 * @file py_ts_type_registry.cpp
 * @brief Python bindings for TSTypeRegistry and related types.
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
        .def_ro("kind", &TSMeta::kind, "Time-series category")

        .def_prop_ro("value_type", [](const TSMeta& self) {
            return self.value_type;
        }, nb::rv_policy::reference, "Value type (for TS, TSS, TSW)")
        .def_prop_ro("key_type", [](const TSMeta& self) {
            return self.key_type();
        }, nb::rv_policy::reference, "Key type (for TSD)")
        .def_prop_ro("element_ts", [](const TSMeta& self) {
            return self.element_ts();
        }, nb::rv_policy::reference, "Element TS schema (for TSD value, TSL, REF)")

        .def_prop_ro("fixed_size", [](const TSMeta& self) -> size_t {
            return self.fixed_size();
        }, "Fixed size (for TSL, 0=dynamic)")

        .def_prop_ro("is_duration_based", [](const TSMeta& self) -> bool {
            return self.is_duration_based();
        }, "True if duration-based window (for TSW)")
        .def_prop_ro("period", [](const TSMeta& self) -> size_t {
            return self.period();
        }, "Tick period (for tick-based TSW)")
        .def_prop_ro("min_period", [](const TSMeta& self) -> size_t {
            return self.min_period();
        }, "Minimum tick period (for tick-based TSW)")
        .def_prop_ro("time_range", [](const TSMeta& self) {
            return self.time_range();
        }, "Time range (for duration-based TSW)")
        .def_prop_ro("min_time_range", [](const TSMeta& self) {
            return self.min_time_range();
        }, "Minimum time range (for duration-based TSW)")

        .def_prop_ro("field_count", [](const TSMeta& self) -> size_t {
            return self.field_count();
        }, "Number of fields (for TSB)")
        .def_prop_ro("bundle_name", [](const TSMeta& self) {
            const char* name = self.bundle_name();
            return name ? std::string(name) : std::string();
        }, "Bundle schema name (for TSB)")
        .def_prop_ro("fields", [](const TSMeta& self) {
            std::vector<const TSBFieldInfo*> result;
            const TSBFieldInfo* f = self.fields();
            size_t count = self.field_count();
            if (f && count > 0) {
                result.reserve(count);
                for (size_t i = 0; i < count; ++i) {
                    result.push_back(&f[i]);
                }
            }
            return result;
        }, nb::rv_policy::reference_internal, "Field metadata (for TSB)")
        .def_prop_ro("python_type", [](const TSMeta& self) {
            return self.python_type();
        }, "Python type for reconstruction (for TSB)")

        .def("is_collection", &TSMeta::is_collection, "True if TSS, TSD, TSL, or TSB")
        .def("is_scalar_ts", &TSMeta::is_scalar_ts, "True if TSValue, TSW, or SIGNAL")

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

        .def_static("instance", &TSTypeRegistry::instance,
            nb::rv_policy::reference, "Get the singleton instance")

        .def("ts", &TSTypeRegistry::ts,
            nb::rv_policy::reference, "value_type"_a,
            "Create TS[T] schema for scalar time-series")

        .def("tss", &TSTypeRegistry::tss,
            nb::rv_policy::reference, "element_type"_a,
            "Create TSS[T] schema for time-series set")

        .def("tsd", &TSTypeRegistry::tsd,
            nb::rv_policy::reference, "key_type"_a, "value_ts"_a,
            "Create TSD[K, V] schema for time-series dict")

        .def("tsl", &TSTypeRegistry::tsl,
            nb::rv_policy::reference, "element_ts"_a, "fixed_size"_a = 0,
            "Create TSL[TS, Size] schema for time-series list")

        .def("tsw", &TSTypeRegistry::tsw,
            nb::rv_policy::reference,
            "value_type"_a, "period"_a, "min_period"_a = 0,
            "Create TSW[T] schema for tick-based window")

        .def("tsw_duration", &TSTypeRegistry::tsw_duration,
            nb::rv_policy::reference,
            "value_type"_a, "time_range"_a,
            "min_time_range"_a = engine_time_delta_t{0},
            "Create TSW[T] schema for duration-based window")

        .def("tsb", &TSTypeRegistry::tsb,
            nb::rv_policy::reference,
            "fields"_a, "name"_a, "python_type"_a = nb::none(),
            "Create TSB[Schema] schema for time-series bundle")

        .def("ref", &TSTypeRegistry::ref,
            nb::rv_policy::reference, "referenced_ts"_a,
            "Create REF[TS] schema for time-series reference")

        .def("signal", &TSTypeRegistry::signal,
            nb::rv_policy::reference, "Get the SIGNAL schema singleton")

        .def("dereference", &TSTypeRegistry::dereference,
            nb::rv_policy::reference, "source"_a,
            "Recursively transforms REF[T] -> T throughout the schema tree")

        .def_static("contains_ref", &TSTypeRegistry::contains_ref,
            "meta"_a, "Check if a schema contains any REF types");
}

// ============================================================================
// TS Builder Bindings
// ============================================================================

static void register_ts_builders(nb::module_& m) {
    nb::class_<TSBuilder>(m, "TSBuilder", "Builder for TS[T] schemas")
        .def(nb::init<>())
        .def("set_value_type", &TSBuilder::set_value_type, "type"_a,
            nb::rv_policy::reference, "Set the value type")
        .def("build", &TSBuilder::build, nb::rv_policy::reference,
            "Build the TS schema");

    nb::class_<TSBBuilder>(m, "TSBBuilder", "Builder for TSB[Schema] schemas")
        .def(nb::init<>())
        .def("set_name", &TSBBuilder::set_name, "name"_a,
            nb::rv_policy::reference, "Set the bundle name")
        .def("add_field", &TSBBuilder::add_field, "name"_a, "ts"_a,
            nb::rv_policy::reference, "Add a field")
        .def("set_python_type", &TSBBuilder::set_python_type, "py_type"_a,
            nb::rv_policy::reference, "Set the Python type for reconstruction")
        .def("build", &TSBBuilder::build, nb::rv_policy::reference,
            "Build the TSB schema");

    nb::class_<TSLBuilder>(m, "TSLBuilder", "Builder for TSL[TS, Size] schemas")
        .def(nb::init<>())
        .def("set_element_ts", &TSLBuilder::set_element_ts, "ts"_a,
            nb::rv_policy::reference, "Set the element TS schema")
        .def("set_size", &TSLBuilder::set_size, "size"_a,
            nb::rv_policy::reference, "Set the fixed size (0 = dynamic)")
        .def("build", &TSLBuilder::build, nb::rv_policy::reference,
            "Build the TSL schema");

    nb::class_<TSDBuilder>(m, "TSDBuilder", "Builder for TSD[K, V] schemas")
        .def(nb::init<>())
        .def("set_key_type", &TSDBuilder::set_key_type, "type"_a,
            nb::rv_policy::reference, "Set the key type")
        .def("set_value_ts", &TSDBuilder::set_value_ts, "ts"_a,
            nb::rv_policy::reference, "Set the value TS schema")
        .def("build", &TSDBuilder::build, nb::rv_policy::reference,
            "Build the TSD schema");

    nb::class_<TSSBuilder>(m, "TSSBuilder", "Builder for TSS[T] schemas")
        .def(nb::init<>())
        .def("set_element_type", &TSSBuilder::set_element_type, "type"_a,
            nb::rv_policy::reference, "Set the element type")
        .def("build", &TSSBuilder::build, nb::rv_policy::reference,
            "Build the TSS schema");

    nb::class_<TSWBuilder>(m, "TSWBuilder", "Builder for TSW[T] schemas")
        .def(nb::init<>())
        .def("set_element_type", &TSWBuilder::set_element_type, "type"_a,
            nb::rv_policy::reference, "Set the value type")
        .def("set_period", &TSWBuilder::set_period, "period"_a,
            nb::rv_policy::reference, "Set the tick period")
        .def("set_min_period", &TSWBuilder::set_min_period, "min_period"_a,
            nb::rv_policy::reference, "Set the minimum tick period")
        .def("set_time_range", &TSWBuilder::set_time_range, "time_range"_a,
            nb::rv_policy::reference, "Set the duration time range")
        .def("set_min_window_period", &TSWBuilder::set_min_window_period, "min_period"_a,
            nb::rv_policy::reference, "Set the minimum duration window period")
        .def("build", &TSWBuilder::build, nb::rv_policy::reference,
            "Build the TSW schema");

    nb::class_<REFBuilder>(m, "REFBuilder", "Builder for REF[TS] schemas")
        .def(nb::init<>())
        .def("set_target_ts", &REFBuilder::set_target_ts, "ts"_a,
            nb::rv_policy::reference, "Set the target TS schema")
        .def("build", &REFBuilder::build, nb::rv_policy::reference,
            "Build the REF schema");
}

// ============================================================================
// Module Registration
// ============================================================================

void ts_type_registry_register_with_nanobind(nb::module_& m) {
    register_ts_kind(m);
    register_tsb_field_info(m);
    register_ts_meta(m);
    register_ts_type_registry(m);
    register_ts_builders(m);
}

} // namespace hgraph
