/**
 * @file py_signal_view.cpp
 * @brief Python bindings for SignalView.
 */

#include <hgraph/types/time_series/signal_view.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {

void register_signal_view(nb::module_& m) {
    // SignalView is non-copyable (uses unique_ptr for child reference stability)
    nb::class_<SignalView>(m, "SignalView",
        "View for SIGNAL time-series with presence-only semantics.\n\n"
        "SignalView tracks modification state (\"did something tick?\") without\n"
        "carrying actual value data. Key behaviors:\n"
        "- value() returns modification state (bool)\n"
        "- Child signals aggregate modified/valid state\n"
        "- Reference dereferencing: binds to actual data sources, not REF wrappers")

        // Default constructor
        .def(nb::init<>(), "Create an unbound SignalView")

        // Core signal methods
        .def("modified", &SignalView::modified,
            "Check if the signal ticked at current time.\n\n"
            "If child signals exist, returns True if ANY child is modified.")

        .def("valid", &SignalView::valid,
            "Check if the signal has ever ticked.\n\n"
            "If child signals exist, returns True if ANY child is valid.")

        .def("last_modified_time", &SignalView::last_modified_time,
            "Get the last modification time.\n\n"
            "If child signals exist, returns the MAX of all children.")

        .def("current_time", &SignalView::current_time,
            "Get the current engine time.")

        // Value access (uniform API)
        .def("value", &SignalView::value,
            "Get the signal's value (modification state as bool).\n\n"
            "Returns True if modified, False otherwise.")

        .def("delta_value", &SignalView::delta_value,
            "Get the delta value (same as value for SIGNAL).")

        // Child access
        .def("__getitem__", [](SignalView& self, size_t index) -> SignalView& {
            return self[index];
        }, nb::rv_policy::reference_internal,
            "Access child signal by index (lazily creates if needed).")

        .def("at", &SignalView::at,
            nb::rv_policy::reference_internal,
            "index"_a,
            "Access child signal by index (const, returns invalid if not found).")

        .def("field", [](SignalView& self, const std::string& name) -> SignalView& {
            return self.field(name);
        }, nb::rv_policy::reference_internal,
            "name"_a,
            "Access child signal by field name (for TSB sources).")

        .def("has_children", &SignalView::has_children,
            "Check if any child signals have been created.")

        .def("child_count", &SignalView::child_count,
            "Get the number of child signals.")

        // Binding
        .def("bound", &SignalView::bound,
            "Check if bound to a source or has children.")

        .def("bind", &SignalView::bind,
            "source"_a,
            "Bind to a source TSView (dereferences REF types).")

        .def("unbind", &SignalView::unbind,
            "Unbind from current source and clear children.")

        // Active/passive
        .def("active", &SignalView::active,
            "Check if signal is active (subscribed to notifications).")

        .def("make_active", &SignalView::make_active,
            "Make signal active (also activates all children).")

        .def("make_passive", &SignalView::make_passive,
            "Make signal passive (also deactivates all children).")

        // Output operations
        .def("tick", &SignalView::tick,
            "Tick the signal (for outputs). Updates modification time and notifies observers.")

        // Metadata
        .def("ts_meta", &SignalView::ts_meta, nb::rv_policy::reference,
            "Get the SIGNAL metadata singleton.")

        .def("source_meta", &SignalView::source_meta, nb::rv_policy::reference,
            "Get the dereferenced source schema (None if unbound).")

        // Boolean conversion
        .def("__bool__", [](const SignalView& self) {
            return static_cast<bool>(self);
        }, "True if bound or has children.");
}

} // namespace hgraph
