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
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>
#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/optional.h>
#include <hgraph/python/chrono.h>

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

    // ========== Overlay/Time-series state methods ==========

    /**
     * @brief Check if the time-series value is valid (has been set).
     */
    [[nodiscard]] bool ts_valid() const {
        return _ts_value.ts_valid();
    }

    /**
     * @brief Get the last modification time.
     * @return The engine time when this value was last modified
     */
    [[nodiscard]] engine_time_t last_modified_time() const {
        return _ts_value.last_modified_time();
    }

    /**
     * @brief Check if modified at specific time.
     * @param time The time to check against
     * @return true if the value was modified at the given time
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _ts_value.modified_at(time);
    }

    /**
     * @brief Mark the value as modified at the given time.
     * @param time The engine time of modification
     */
    void mark_modified(engine_time_t time) {
        _ts_value.notify_modified(time);
    }

    /**
     * @brief Invalidate the time-series value.
     */
    void invalidate() {
        _ts_value.invalidate_ts();
    }

    /**
     * @brief Check if this TSValue has an overlay.
     */
    [[nodiscard]] bool has_overlay() const {
        return _ts_value.overlay() != nullptr;
    }

    /**
     * @brief Get overlay kind as a string.
     * Uses TSMeta kind to determine the overlay type (schema guarantees match).
     */
    [[nodiscard]] std::string overlay_kind() const {
        if (!_ts_value.overlay()) return "none";
        if (!_ts_value.ts_meta()) return "unknown";
        // Use schema to determine overlay type
        switch (_ts_value.ts_meta()->kind()) {
            case TSTypeKind::TS:
            case TSTypeKind::REF:
            case TSTypeKind::SIGNAL:
                return "Scalar";
            case TSTypeKind::TSB:
                return "Composite";
            case TSTypeKind::TSL:
            case TSTypeKind::TSW:
                return "List";
            case TSTypeKind::TSS:
                return "Set";
            case TSTypeKind::TSD:
                return "Map";
            default:
                return "unknown";
        }
    }

    /**
     * @brief Get the overlay's last modified time directly.
     */
    [[nodiscard]] engine_time_t overlay_last_modified_time() const {
        auto* overlay = _ts_value.overlay();
        if (!overlay) return MIN_DT;
        return overlay->last_modified_time();
    }

    /**
     * @brief Mark modified via overlay.
     */
    void overlay_mark_modified(engine_time_t time) {
        auto* overlay = _ts_value.overlay();
        if (overlay) {
            overlay->mark_modified(time);
        }
    }

    // ========== Path tracking methods ==========

    /**
     * @brief Check if the root view has path tracking.
     */
    [[nodiscard]] bool has_path() const {
        return _ts_value.view().has_path();
    }

    /**
     * @brief Get the path string for the root view.
     */
    [[nodiscard]] std::string path_string() const {
        return _ts_value.view().path_string();
    }

    /**
     * @brief Get a field view and return its path string (for bundles).
     */
    [[nodiscard]] std::string get_field_path(const std::string& name) const {
        if (!is_bundle()) {
            throw std::runtime_error("get_field_path() only valid for bundle types");
        }
        TSBView bundle = _ts_value.bundle_view();
        TSView field = bundle.field(name);
        return field.path_string();
    }

    /**
     * @brief Check if a field view has path tracking (for bundles).
     */
    [[nodiscard]] bool field_has_path(const std::string& name) const {
        if (!is_bundle()) {
            throw std::runtime_error("field_has_path() only valid for bundle types");
        }
        TSBView bundle = _ts_value.bundle_view();
        TSView field = bundle.field(name);
        return field.has_path();
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
        // Overlay/time-series state methods
        .def_prop_ro("ts_valid", &TSValueTestWrapper::ts_valid)
        .def_prop_ro("last_modified_time", &TSValueTestWrapper::last_modified_time)
        .def("modified_at", &TSValueTestWrapper::modified_at, nb::arg("time"))
        .def("mark_modified", &TSValueTestWrapper::mark_modified, nb::arg("time"))
        .def("invalidate", &TSValueTestWrapper::invalidate)
        .def_prop_ro("has_overlay", &TSValueTestWrapper::has_overlay)
        .def_prop_ro("overlay_kind", &TSValueTestWrapper::overlay_kind)
        .def_prop_ro("overlay_last_modified_time", &TSValueTestWrapper::overlay_last_modified_time)
        .def("overlay_mark_modified", &TSValueTestWrapper::overlay_mark_modified, nb::arg("time"))
        // Path tracking methods
        .def_prop_ro("has_path", &TSValueTestWrapper::has_path)
        .def_prop_ro("path_string", &TSValueTestWrapper::path_string)
        .def("get_field_path", &TSValueTestWrapper::get_field_path, nb::arg("name"))
        .def("field_has_path", &TSValueTestWrapper::field_has_path, nb::arg("name"))
        .def("__repr__", [](const TSValueTestWrapper& self) {
            return "TSValueTestWrapper[" + self.schema_str() + "]";
        });

    // Also expose the make_ts_value utility function for creating standalone TSValues
    test_m.def("make_ts_value", [](const TSMeta* ts_meta) {
        return TSValueTestWrapper(ts_meta);
    }, nb::arg("ts_meta"), "Create a TSValue test wrapper from a TSMeta schema");
}

} // namespace hgraph
