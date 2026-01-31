/**
 * @file py_ts_input_output.cpp
 * @brief Python bindings for TSInput, TSOutput, TSInputView, TSOutputView.
 *
 * This file implements nanobind bindings for:
 * - TSOutput class (producer of time-series values)
 * - TSOutputView class (view wrapper for TSOutput with mutation support)
 * - TSInput class (consumer of time-series values)
 * - TSInputView class (view wrapper for TSInput with binding support)
 */

#include <hgraph/api/python/py_ts_input_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/node.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/string.h>
#include <unordered_map>
#include <memory>

namespace hgraph {

using namespace nanobind::literals;

// ============================================================================
// PyNotifiable - Wrapper to allow Python objects to be used as Notifiable
// ============================================================================

// Forward declare hash struct before use
struct PyNotifiableKeyHash {
    std::size_t operator()(const std::pair<void*, std::uintptr_t>& key) const {
        return std::hash<void*>{}(key.first) ^ (std::hash<std::uintptr_t>{}(key.second) << 1);
    }
};

/**
 * @brief Wrapper that allows Python objects with a notify(et) method to be
 *        used as C++ Notifiable observers.
 *
 * This class holds a reference to a Python object and calls its notify method
 * when the C++ notify() is invoked.
 */
class PyNotifiable : public Notifiable {
public:
    explicit PyNotifiable(nb::object py_observer)
        : py_observer_(std::move(py_observer)) {}

    void notify(engine_time_t et) override {
        // Acquire GIL and call the Python method
        nb::gil_scoped_acquire gil;
        try {
            py_observer_.attr("notify")(et);
        } catch (const nb::python_error& e) {
            // Log or handle Python exception
            // For now, just let it propagate
            throw;
        }
    }

    // Get the underlying Python object for identity comparison
    nb::object py_object() const { return py_observer_; }

private:
    nb::object py_observer_;
};

// Registry to manage PyNotifiable lifetimes
// Maps (TSOutput*, Python object id) -> PyNotifiable
// Using heap-allocated registry that's intentionally never freed to avoid
// shutdown ordering issues with Python GIL
using PyNotifiableRegistry = std::unordered_map<std::pair<void*, std::uintptr_t>,
                                                 std::unique_ptr<PyNotifiable>,
                                                 PyNotifiableKeyHash>;

static PyNotifiableRegistry& get_py_notifiable_registry() {
    // Leak the registry at shutdown - this is intentional to avoid
    // GIL issues during Python interpreter finalization
    static PyNotifiableRegistry* registry = new PyNotifiableRegistry();
    return *registry;
}

// Helper to get Python object identity
static std::uintptr_t py_object_id(const nb::object& obj) {
    return reinterpret_cast<std::uintptr_t>(obj.ptr());
}


void ts_input_output_register_with_nanobind(nb::module_& m) {
    // Note: We intentionally don't register cleanup at exit because:
    // 1. The GIL may not be in a valid state during Py_AtExit
    // 2. nb::object destructors require the GIL
    // 3. The registry uses a heap pointer that's never freed - this is intentional
    // 4. The OS reclaims all memory at process exit anyway

    // ========================================================================
    // TSOutput - Producer of time-series values
    // ========================================================================
    nb::class_<TSOutput>(m, "TSOutput",
        "Producer of time-series values.\n\n"
        "TSOutput owns the native time-series value and manages cast alternatives\n"
        "for consumers that need different schemas.")

        .def(nb::init<const TSMeta*, node_ptr, size_t>(),
            "ts_meta"_a, "owner"_a = nullptr, "port_index"_a = 0,
            "Construct TSOutput with schema and optional owning node.")

        .def("view", [](TSOutput& self, engine_time_t current_time) {
                return self.view(current_time);
            },
            "current_time"_a,
            "Get view for this output at current time using native schema.")

        .def("view", [](TSOutput& self, engine_time_t current_time, const TSMeta* schema) {
                return self.view(current_time, schema);
            },
            "current_time"_a, "schema"_a,
            "Get view for this output at current time with specific schema.")

        .def_prop_ro("ts_meta", &TSOutput::ts_meta,
            nb::rv_policy::reference,
            "Get the native schema.")

        .def_prop_ro("port_index", &TSOutput::port_index,
            "Get the port index on the owning node.")

        .def("valid", &TSOutput::valid,
            "Check if valid (has schema).")

        .def("native_value", [](TSOutput& self) -> TSValue& {
                return self.native_value();
            },
            nb::rv_policy::reference,
            "Get mutable reference to native value.");

    // ========================================================================
    // TSOutputView - View wrapper for TSOutput
    // ========================================================================
    nb::class_<TSOutputView>(m, "TSOutputView",
        "View wrapper for TSOutput with mutation support.\n\n"
        "Provides value mutation, observer subscription management, and navigation.")

        .def(nb::init<TSView, TSOutput*>(),
            "ts_view"_a, "output"_a = nullptr,
            "Construct from TSView and owning output.")

        .def("value", &TSOutputView::value,
            "Get the value as a View.")

        .def("delta_value", &TSOutputView::delta_value,
            "Get the delta value as a View.")

        .def("modified", &TSOutputView::modified,
            "Check if modified at current time.")

        .def("valid", &TSOutputView::valid,
            "Check if the value has ever been set.")

        .def("current_time", &TSOutputView::current_time,
            "Get the current engine time.")

        .def_prop_ro("ts_meta", &TSOutputView::ts_meta,
            nb::rv_policy::reference,
            "Get the time-series metadata.")

        .def("set_value", &TSOutputView::set_value,
            "v"_a,
            "Set the value at this position.")

        .def("apply_delta", &TSOutputView::apply_delta,
            "dv"_a,
            "Apply delta at this position.")

        .def("invalidate", &TSOutputView::invalidate,
            "Invalidate the value.")

        .def("from_python", &TSOutputView::from_python,
            "src"_a,
            "Set the value from a Python object.")

        .def("to_python", &TSOutputView::to_python,
            "Convert the value to a Python object.")

        .def("subscribe", [](TSOutputView& self, nb::object observer) {
                // Get output pointer for registry key
                void* output_ptr = self.output();
                auto obj_id = py_object_id(observer);
                auto key = std::make_pair(output_ptr, obj_id);

                // Check if already subscribed
                auto it = get_py_notifiable_registry().find(key);
                if (it != get_py_notifiable_registry().end()) {
                    return;  // Already subscribed
                }

                // Create PyNotifiable wrapper
                auto py_notifiable = std::make_unique<PyNotifiable>(observer);
                Notifiable* raw_ptr = py_notifiable.get();

                // Store in registry
                get_py_notifiable_registry()[key] = std::move(py_notifiable);

                // Subscribe the C++ Notifiable
                self.subscribe(raw_ptr);
            },
            "observer"_a,
            "Subscribe observer for notifications.\n\n"
            "The observer must have a notify(et) method that will be called\n"
            "when the output value changes.")

        .def("unsubscribe", [](TSOutputView& self, nb::object observer) {
                // Get output pointer for registry key
                void* output_ptr = self.output();
                auto obj_id = py_object_id(observer);
                auto key = std::make_pair(output_ptr, obj_id);

                // Find in registry
                auto it = get_py_notifiable_registry().find(key);
                if (it == get_py_notifiable_registry().end()) {
                    return;  // Not subscribed
                }

                // Unsubscribe the C++ Notifiable
                self.unsubscribe(it->second.get());

                // Remove from registry (destroys PyNotifiable)
                get_py_notifiable_registry().erase(it);
            },
            "observer"_a,
            "Unsubscribe observer.")

        .def("field", &TSOutputView::field,
            "name"_a,
            "Navigate to field by name (for TSB types).")

        .def("__getitem__", &TSOutputView::operator[],
            "index"_a,
            "Navigate to child by index.")

        .def("size", &TSOutputView::size,
            "Get the number of children.")

        .def("ts_view", [](TSOutputView& self) -> TSView& {
                return self.ts_view();
            },
            nb::rv_policy::reference,
            "Get the underlying TSView.")

        .def("__bool__", [](const TSOutputView& self) {
                return static_cast<bool>(self);
            },
            "Check if this view is valid.");

    // ========================================================================
    // TSInput - Consumer of time-series values
    // ========================================================================
    nb::class_<TSInput>(m, "TSInput",
        "Consumer of time-series values.\n\n"
        "TSInput subscribes to TSOutput(s) and provides access to linked values.\n"
        "Owns a TSValue containing Links at its leaves that point to bound output values.")

        .def(nb::init<const TSMeta*, node_ptr>(),
            "ts_meta"_a, "owner"_a = nullptr,
            "Construct TSInput with schema and optional owning node.")

        .def("view", [](TSInput& self, engine_time_t current_time) {
                return self.view(current_time);
            },
            "current_time"_a,
            "Get view for this input at current time.")

        .def("view", [](TSInput& self, engine_time_t current_time, const TSMeta* schema) {
                return self.view(current_time, schema);
            },
            "current_time"_a, "schema"_a,
            "Get view for this input at current time with specific schema.")

        .def("set_active", nb::overload_cast<bool>(&TSInput::set_active),
            "active"_a,
            "Set active/passive state for entire input.")

        .def("set_active", nb::overload_cast<const std::string&, bool>(&TSInput::set_active),
            "field"_a, "active"_a,
            "Set active/passive state for specific field (TSB).")

        .def("active", &TSInput::active,
            "Check if this input (root level) is active.")

        .def_prop_ro("meta", &TSInput::meta,
            nb::rv_policy::reference,
            "Get the input schema.")

        .def("valid", &TSInput::valid,
            "Check if valid (has schema).")

        .def("value", [](TSInput& self) -> TSValue& {
                return self.value();
            },
            nb::rv_policy::reference,
            "Get mutable reference to the value (contains links).");

    // ========================================================================
    // TSInputView - View wrapper for TSInput
    // ========================================================================
    nb::class_<TSInputView>(m, "TSInputView",
        "View wrapper for TSInput with binding support.\n\n"
        "Provides binding to TSOutputView, active/passive subscription control,\n"
        "and navigation that returns TSInputView.")

        .def(nb::init<TSView, TSInput*>(),
            "ts_view"_a, "input"_a = nullptr,
            "Construct from TSView and owning input.")

        .def("ts_view", [](TSInputView& self) -> TSView& {
                return self.ts_view();
            },
            nb::rv_policy::reference,
            "Get the underlying TSView.")

        .def("value", &TSInputView::value,
            "Get value view at this position.")

        .def("delta_value", &TSInputView::delta_value,
            "Get the delta value as a View.")

        .def("modified", &TSInputView::modified,
            "Check if modified at current time.")

        .def("valid", &TSInputView::valid,
            "Check if the value has ever been set.")

        .def("current_time", &TSInputView::current_time,
            "Get the current engine time.")

        .def_prop_ro("ts_meta", &TSInputView::ts_meta,
            nb::rv_policy::reference,
            "Get the time-series metadata.")

        .def("to_python", &TSInputView::to_python,
            "Convert the value to a Python object.")

        .def("bind", &TSInputView::bind,
            "output"_a,
            "Bind this input position to an output.")

        .def("unbind", &TSInputView::unbind,
            "Unbind from current source.")

        .def("is_bound", &TSInputView::is_bound,
            "Check if bound to an output.")

        .def("make_active", &TSInputView::make_active,
            "Make this position active (subscribe to notifications).")

        .def("make_passive", &TSInputView::make_passive,
            "Make this position passive (unsubscribe from notifications).")

        .def("active", &TSInputView::active,
            "Check if this position is active.")

        .def("field", &TSInputView::field,
            "name"_a,
            "Navigate to field by name (for TSB types).")

        .def("__getitem__", &TSInputView::operator[],
            "index"_a,
            "Navigate to child by index.")

        .def("size", &TSInputView::size,
            "Get the number of children.")

        .def("input", [](TSInputView& self) -> TSInput* {
                return self.input();
            },
            nb::rv_policy::reference,
            "Get the owning TSInput.")

        .def("__bool__", [](const TSInputView& self) {
                return static_cast<bool>(self);
            },
            "Check if this view is valid.");
}

} // namespace hgraph
