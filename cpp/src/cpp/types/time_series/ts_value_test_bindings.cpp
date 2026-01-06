//
// ts_value_test_bindings.cpp - Lightweight Python bindings for testing TSValue infrastructure
//
// These bindings allow testing TSValue, TSView, and related classes without requiring
// the full Node infrastructure. They are intended for unit testing the type-erased
// time-series system.
//

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/type_meta.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>

namespace nb = nanobind;

namespace hgraph {

/**
 * @brief Test wrapper for TSValue - allows Python testing without Node dependency.
 *
 * This is a simplified wrapper that exposes TSValue functionality for testing.
 * It does NOT use Node ownership - values are standalone.
 */
class TSValueTestWrapper {
public:
    /**
     * @brief Create a test wrapper from a TSMeta schema.
     */
    explicit TSValueTestWrapper(const TSMeta* ts_meta)
        : _ts_value(ts_meta, nullptr, OUTPUT_MAIN) {}

    /**
     * @brief Check if the TSValue is valid.
     */
    [[nodiscard]] bool valid() const { return _ts_value.valid(); }

    /**
     * @brief Get the TSMeta schema.
     */
    [[nodiscard]] const TSMeta* ts_meta() const { return _ts_value.ts_meta(); }

    /**
     * @brief Get the value as a Python object.
     */
    [[nodiscard]] nb::object to_python() const {
        if (!valid()) {
            return nb::none();
        }
        return _ts_value.value().to_python();
    }

    /**
     * @brief Set the value from a Python object.
     */
    void from_python(const nb::object& src) {
        if (!valid()) {
            throw std::runtime_error("Cannot set value on invalid TSValue");
        }
        _ts_value.value().from_python(src);
    }

    /**
     * @brief Get the time-series kind as a string.
     */
    [[nodiscard]] std::string kind_str() const {
        if (!_ts_value.ts_meta()) {
            return "invalid";
        }
        switch (_ts_value.ts_meta()->kind()) {
            case TSTypeKind::TS: return "TS";
            case TSTypeKind::TSB: return "TSB";
            case TSTypeKind::TSL: return "TSL";
            case TSTypeKind::TSD: return "TSD";
            case TSTypeKind::TSS: return "TSS";
            case TSTypeKind::TSW: return "TSW";
            case TSTypeKind::REF: return "REF";
            case TSTypeKind::SIGNAL: return "SIGNAL";
            default: return "unknown";
        }
    }

    /**
     * @brief Get the schema string representation.
     */
    [[nodiscard]] std::string schema_str() const {
        if (!_ts_value.ts_meta()) {
            return "invalid";
        }
        return _ts_value.ts_meta()->to_string();
    }

    // ========== Bundle-specific methods ==========

    /**
     * @brief Check if this is a bundle type.
     */
    [[nodiscard]] bool is_bundle() const {
        return _ts_value.ts_meta() && _ts_value.ts_meta()->is_bundle();
    }

    /**
     * @brief Get a field value by name (for bundles).
     */
    [[nodiscard]] nb::object get_field(const std::string& name) const {
        if (!is_bundle()) {
            throw std::runtime_error("get_field() only valid for bundle types");
        }
        TSBView bundle = _ts_value.bundle_view();
        TSView field = bundle.field(name);
        return field.to_python();
    }

    /**
     * @brief Set a field value by name (for bundles).
     */
    void set_field(const std::string& name, const nb::object& value) {
        if (!is_bundle()) {
            throw std::runtime_error("set_field() only valid for bundle types");
        }
        // For now, we need to go through the underlying value
        // This is a simplified implementation for testing
        auto bundle_view = _ts_value.value().as_bundle();
        bundle_view.at(name).from_python(value);
    }

    /**
     * @brief Get the number of fields (for bundles).
     */
    [[nodiscard]] size_t field_count() const {
        if (!is_bundle()) {
            return 0;
        }
        return _ts_value.bundle_view().field_count();
    }

private:
    TSValue _ts_value;
};

/**
 * @brief Register test bindings for TSValue infrastructure.
 */
void register_ts_value_test_bindings(nb::module_& m) {
    // Create a submodule for test bindings
    auto test_m = m.def_submodule("_ts_test", "Test bindings for TSValue infrastructure");

    nb::class_<TSValueTestWrapper>(test_m, "TSValueTestWrapper")
        .def(nb::init<const TSMeta*>(), nb::arg("ts_meta"))
        .def_prop_ro("valid", &TSValueTestWrapper::valid)
        .def_prop_ro("ts_meta", &TSValueTestWrapper::ts_meta, nb::rv_policy::reference)
        .def_prop_ro("kind", &TSValueTestWrapper::kind_str)
        .def_prop_ro("schema", &TSValueTestWrapper::schema_str)
        .def_prop_ro("value", &TSValueTestWrapper::to_python)
        .def("set_value", &TSValueTestWrapper::from_python, nb::arg("value"))
        .def("to_python", &TSValueTestWrapper::to_python)
        .def("from_python", &TSValueTestWrapper::from_python, nb::arg("src"))
        // Bundle methods
        .def_prop_ro("is_bundle", &TSValueTestWrapper::is_bundle)
        .def("get_field", &TSValueTestWrapper::get_field, nb::arg("name"))
        .def("set_field", &TSValueTestWrapper::set_field, nb::arg("name"), nb::arg("value"))
        .def_prop_ro("field_count", &TSValueTestWrapper::field_count)
        .def("__repr__", [](const TSValueTestWrapper& self) {
            return "TSValueTestWrapper[" + self.schema_str() + "]";
        });

    // Also expose the make_ts_value utility function for creating standalone TSValues
    test_m.def("make_ts_value", [](const TSMeta* ts_meta) {
        return TSValueTestWrapper(ts_meta);
    }, nb::arg("ts_meta"), "Create a TSValue test wrapper from a TSMeta schema");
}

} // namespace hgraph
