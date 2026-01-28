/**
 * @file py_ts_value.cpp
 * @brief Python bindings for TSValue and TSView.
 *
 * This file implements nanobind bindings for:
 * - TSValue class (owning time-series storage)
 * - TSView class (non-owning time-series view)
 * - Kind-specific view accessors (scalar, bundle, list, set, dict)
 */

#include <hgraph/api/python/py_ts_value.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_scalar_view.h>
#include <hgraph/types/time_series/ts_bundle_view.h>
#include <hgraph/types/time_series/ts_list_view.h>
#include <hgraph/types/time_series/ts_set_view.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/ts_view_range.h>
#include <hgraph/python/chrono.h>

#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/make_iterator.h>

namespace hgraph {

using namespace nanobind::literals;

// ============================================================================
// Iterator Bindings
// ============================================================================

static void register_ts_view_iterators(nb::module_& m) {
    // TSViewIterator - basic index-based iterator
    nb::class_<TSViewIterator>(m, "TSViewIterator",
        "Iterator for a sequence of TSViews.\n\n"
        "Use index() to get the current position.")

        .def("index", &TSViewIterator::index,
            "Get the current index")

        .def("__next__", [](TSViewIterator& self, TSViewIterator& end) -> TSView {
            if (self == end) {
                throw nb::stop_iteration();
            }
            TSView result = *self;
            ++self;
            return result;
        });

    // TSViewRange - iterable range of TSViews
    nb::class_<TSViewRange>(m, "TSViewRange",
        "Range for iterating over TSViews.\n\n"
        "Use in a for loop: for view in range: ...")

        .def("__iter__", [](TSViewRange& self) {
            return std::make_pair(self.begin(), self.end());
        })

        .def("__len__", &TSViewRange::size)

        .def("empty", &TSViewRange::empty,
            "Check if the range is empty");

    // TSFieldIterator - bundle field iterator with names
    nb::class_<TSFieldIterator>(m, "TSFieldIterator",
        "Iterator for bundle fields with names.\n\n"
        "Use index() for field index, name() for field name.")

        .def("index", &TSFieldIterator::index,
            "Get the current field index")

        .def("name", &TSFieldIterator::name,
            "Get the current field name")

        .def("__next__", [](TSFieldIterator& self, TSFieldIterator& end) -> TSView {
            if (self == end) {
                throw nb::stop_iteration();
            }
            TSView result = *self;
            ++self;
            return result;
        });

    // TSFieldRange - iterable range of bundle fields
    nb::class_<TSFieldRange>(m, "TSFieldRange",
        "Range for iterating over bundle fields.\n\n"
        "Use in a for loop. Each iteration yields a TSView.\n"
        "Use the iterator's name() method to get field names.")

        .def("__iter__", [](TSFieldRange& self) {
            return std::make_pair(self.begin(), self.end());
        })

        .def("__len__", &TSFieldRange::size)

        .def("empty", &TSFieldRange::empty,
            "Check if the range is empty");

    // TSDictIterator - dict entry iterator with key access
    nb::class_<TSDictIterator>(m, "TSDictIterator",
        "Iterator for dict entries with key access.\n\n"
        "Use index() for slot index, key() for key as value::View.")

        .def("index", &TSDictIterator::index,
            "Get the current slot index")

        .def("key", &TSDictIterator::key,
            "Get the key at the current slot as a value View")

        .def("__next__", [](TSDictIterator& self, TSDictIterator& end) -> TSView {
            if (self == end) {
                throw nb::stop_iteration();
            }
            TSView result = *self;
            ++self;
            return result;
        });

    // TSDictRange - iterable range of dict entries with key access
    nb::class_<TSDictRange>(m, "TSDictRange",
        "Range for iterating over dict entries.\n\n"
        "Use in a for loop. Each iteration yields a TSView.\n"
        "Use the iterator's key() method to get keys.")

        .def("__iter__", [](TSDictRange& self) {
            return std::make_pair(self.begin(), self.end());
        })

        .def("__len__", &TSDictRange::size)

        .def("empty", &TSDictRange::empty,
            "Check if the range is empty");

    // TSDictSlotIterator - dict entry iterator for filtered iteration (added/modified items)
    nb::class_<TSDictSlotIterator>(m, "TSDictSlotIterator",
        "Iterator for dict entries at specific slots.\n\n"
        "Use slot() for storage slot index, key() for key as value::View.")

        .def("slot", &TSDictSlotIterator::slot,
            "Get the current storage slot index")

        .def("key", &TSDictSlotIterator::key,
            "Get the key at the current slot as a value View")

        .def("__next__", [](TSDictSlotIterator& self, TSDictSlotIterator& end) -> TSView {
            if (self == end) {
                throw nb::stop_iteration();
            }
            TSView result = *self;
            ++self;
            return result;
        });

    // TSDictSlotRange - iterable range of dict entries at specific slots
    nb::class_<TSDictSlotRange>(m, "TSDictSlotRange",
        "Range for iterating over dict entries at specific slots.\n\n"
        "Used for filtered iteration (added_items, modified_items).\n"
        "Use the iterator's key() method to get keys.")

        .def("__iter__", [](TSDictSlotRange& self) {
            return std::make_pair(self.begin(), self.end());
        })

        .def("__len__", &TSDictSlotRange::size)

        .def("empty", &TSDictSlotRange::empty,
            "Check if the range is empty");
}

// ============================================================================
// TSValue Binding
// ============================================================================

static void register_ts_value(nb::module_& m) {
    nb::class_<TSValue>(m, "TSValue",
        "Owning time-series value storage with four parallel Values.\n\n"
        "TSValue owns storage for:\n"
        "- value_: User-visible data\n"
        "- time_: Modification timestamps\n"
        "- observer_: Observer lists\n"
        "- delta_value_: Delta tracking data")

        // Construction
        .def(nb::init<const TSMeta*>(),
            "meta"_a,
            "Construct from TSMeta.\n\n"
            "Allocates storage for all four parallel Values based on the\n"
            "TSMeta's generated schemas.\n\n"
            "Args:\n"
            "    meta: The time-series metadata")

        // Metadata
        .def_prop_ro("meta", [](const TSValue& self) {
            return self.meta();
        }, nb::rv_policy::reference, "Get the time-series metadata")

        // View access (returns value::View which should already be bound)
        .def("value_view", static_cast<value::View (TSValue::*)()>(&TSValue::value_view),
            "Get a mutable view of the value data")

        .def("time_view", static_cast<value::View (TSValue::*)()>(&TSValue::time_view),
            "Get a mutable view of the time data")

        .def("observer_view", static_cast<value::View (TSValue::*)()>(&TSValue::observer_view),
            "Get a mutable view of the observer data")

        .def("delta_value_view",
            static_cast<value::View (TSValue::*)(engine_time_t)>(&TSValue::delta_value_view),
            "current_time"_a,
            "Get a view of the delta value data with lazy clearing.\n\n"
            "If current_time > last_delta_clear_time_, the delta is cleared\n"
            "before returning the view.")

        // Time-series semantics
        .def("last_modified_time", &TSValue::last_modified_time,
            "Get the last modification time.\n\n"
            "For atomic TS types, this is the direct timestamp.\n"
            "For composite types (TSB/TSL/TSD), this is the container's timestamp.")

        .def("modified", &TSValue::modified,
            "current_time"_a,
            "Check if modified at or after current_time.\n\n"
            "Uses >= comparison: something is modified at current_time if\n"
            "last_modified_time >= current_time.")

        .def("valid", &TSValue::valid,
            "Check if the value has ever been set.\n\n"
            "A value is valid if last_modified_time != MIN_ST.")

        .def("has_delta", &TSValue::has_delta,
            "Check if this time-series type has delta tracking.")

        // TSView access
        .def("ts_view", &TSValue::ts_view,
            "current_time"_a,
            "Get a TSView for coordinated access.");
}

// ============================================================================
// TSView Binding
// ============================================================================

static void register_ts_view(nb::module_& m) {
    nb::class_<TSView>(m, "TSView",
        "Non-owning view of a time-series value.\n\n"
        "Provides access to the four parallel Value structures:\n"
        "- value: The user-visible data\n"
        "- time: Modification timestamps\n"
        "- observer: Observer lists\n"
        "- delta_value: Delta tracking data")

        // Metadata
        .def_prop_ro("meta", [](const TSView& self) {
            return self.ts_meta();
        }, nb::rv_policy::reference, "Get the time-series metadata")

        .def_prop_ro("current_time", &TSView::current_time,
            "Get the current engine time")

        // View access
        .def("value", static_cast<value::View (TSView::*)() const>(&TSView::value),
            "Get a view of the value data")

        .def("observer", &TSView::observer,
            "Get a view of the observer data")

        .def("delta_value", &TSView::delta_value,
            "Get a view of the delta value data")

        // Time-series semantics
        .def("last_modified_time", &TSView::last_modified_time,
            "Get the last modification time")

        .def("modified", &TSView::modified,
            "Check if modified at or after current_time")

        .def("valid", &TSView::valid,
            "Check if the value has ever been set")

        .def("all_valid", &TSView::all_valid,
            "Check if this and all children are valid")

        .def("sampled", &TSView::sampled,
            "Check if view was obtained through a modified REF")

        .def("has_delta", &TSView::has_delta,
            "Check if this time-series type has delta tracking")

        // Navigation
        .def("__getitem__", &TSView::operator[],
            "index"_a,
            "Access child by index")

        .def("field", &TSView::field,
            "name"_a,
            "Access field by name (for bundles)")

        .def("size", &TSView::size,
            "Get the number of children")

        // Path access
        .def("short_path", [](const TSView& self) {
            return self.short_path().to_string();
        }, "Get the graph-aware path as a string")

        .def("fq_path", &TSView::fq_path,
            "Get the fully-qualified path as a string")

        // Python interop
        .def("to_python", &TSView::to_python,
            "Convert the value to a Python object")

        .def("delta_to_python", &TSView::delta_to_python,
            "Convert the delta to a Python object")

        // Kind-specific views
        .def("as_bundle", &TSView::as_bundle,
            "Get as a bundle view (for TSB)")

        .def("as_list", &TSView::as_list,
            "Get as a list view (for TSL)")

        .def("as_set", &TSView::as_set,
            "Get as a set view (for TSS)")

        .def("as_dict", &TSView::as_dict,
            "Get as a dict view (for TSD)");

    // Note: as_scalar<T>() requires template specialization, which is tricky
    // in Python. Users can access the value directly via view.value().as<T>()
}

// ============================================================================
// TSBView Binding
// ============================================================================

static void register_tsb_view(nb::module_& m) {
    nb::class_<TSBView>(m, "TSBView",
        "View for time-series bundle (TSB) types.\n\n"
        "Provides field-based access with modification bubbling.")

        // Container-level
        .def("last_modified_time", &TSBView::last_modified_time,
            "Get the container's last modification time")

        .def("modified", &TSBView::modified,
            "Check if any field was modified")

        .def("valid", &TSBView::valid,
            "Check if the bundle has ever been set")

        .def("field_count", &TSBView::field_count,
            "Get the number of fields")

        // TSView navigation
        .def("field", static_cast<TSView (TSBView::*)(size_t) const>(&TSBView::field),
            "index"_a,
            "Get a field as a TSView by index")

        .def("field_by_name", static_cast<TSView (TSBView::*)(std::string_view) const>(&TSBView::field),
            "name"_a,
            "Get a field as a TSView by name")

        // Iteration
        .def("items", &TSBView::items,
            "Iterate over all fields.\n\n"
            "Returns a TSFieldRange. Use the iterator's name() for field names.")

        .def("valid_items", &TSBView::valid_items,
            "Iterate over valid fields only")

        .def("modified_items", &TSBView::modified_items,
            "Iterate over modified fields only");
}

// ============================================================================
// TSLView Binding
// ============================================================================

static void register_tsl_view(nb::module_& m) {
    nb::class_<TSLView>(m, "TSLView",
        "View for time-series list (TSL) types.\n\n"
        "Provides element-based access with modification bubbling.")

        // Container-level
        .def("last_modified_time", &TSLView::last_modified_time,
            "Get the container's last modification time")

        .def("modified", &TSLView::modified,
            "Check if any element was modified")

        .def("valid", &TSLView::valid,
            "Check if the list has ever been set")

        .def("size", &TSLView::size,
            "Get the number of elements")

        // TSView navigation
        .def("at", &TSLView::at,
            "index"_a,
            "Get an element as a TSView by index")

        // Iteration
        .def("values", &TSLView::values,
            "Iterate over all elements as TSViews.\n\n"
            "Returns a TSViewRange. Use the iterator's index() for element index.")

        .def("valid_values", &TSLView::valid_values,
            "Iterate over valid elements only")

        .def("modified_values", &TSLView::modified_values,
            "Iterate over modified elements only")

        .def("items", &TSLView::items,
            "Iterate over all elements with index access")

        .def("valid_items", &TSLView::valid_items,
            "Iterate over valid items only")

        .def("modified_items", &TSLView::modified_items,
            "Iterate over modified items only");
}

// ============================================================================
// TSSView Binding
// ============================================================================

static void register_tss_view(nb::module_& m) {
    nb::class_<TSSView>(m, "TSSView",
        "View for time-series set (TSS) types.\n\n"
        "Provides set operations with delta tracking.")

        // Time-series semantics
        .def("last_modified_time", &TSSView::last_modified_time,
            "Get the last modification time")

        .def("modified", &TSSView::modified,
            "Check if modified")

        .def("valid", &TSSView::valid,
            "Check if the set has ever been set")

        // Set operations (read)
        .def("size", &TSSView::size,
            "Get the set size")

        // Iteration
        .def("values", &TSSView::values,
            "Iterate over all current values")

        // Delta access
        .def("added_slots", &TSSView::added_slots,
            nb::rv_policy::reference_internal,
            "Get the added slot indices")

        .def("removed_slots", &TSSView::removed_slots,
            nb::rv_policy::reference_internal,
            "Get the removed slot indices")

        .def("was_added", &TSSView::was_added,
            "elem"_a,
            "Check if a specific element was added this tick")

        .def("was_removed", &TSSView::was_removed,
            "elem"_a,
            "Check if a specific element was removed this tick");
}

// ============================================================================
// TSDView Binding
// ============================================================================

static void register_tsd_view(nb::module_& m) {
    nb::class_<TSDView>(m, "TSDView",
        "View for time-series dict (TSD) types.\n\n"
        "Provides key-based access with delta tracking and modification bubbling.")

        // Container-level
        .def("last_modified_time", &TSDView::last_modified_time,
            "Get the container's last modification time")

        .def("modified", &TSDView::modified,
            "Check if container is modified")

        .def("valid", &TSDView::valid,
            "Check if the dict has ever been set")

        .def("size", &TSDView::size,
            "Get the dict size")

        // TSView navigation
        .def("at", &TSDView::at,
            "key"_a,
            "Get a value as a TSView by key")

        // Key iteration
        .def("keys", &TSDView::keys,
            "Iterate over all keys")

        // Items iteration
        .def("items", &TSDView::items,
            "Iterate over all entries with key access.\n\n"
            "Returns a TSDictRange. Use the iterator's key() to get the key.")

        .def("valid_items", &TSDView::valid_items,
            "Iterate over entries with valid values")

        .def("added_items", &TSDView::added_items,
            "Iterate over entries added this tick")

        .def("modified_items", &TSDView::modified_items,
            "Iterate over entries with modified values this tick")

        // Delta access
        .def("added_slots", &TSDView::added_slots,
            nb::rv_policy::reference_internal,
            "Get the added slot indices")

        .def("removed_slots", &TSDView::removed_slots,
            nb::rv_policy::reference_internal,
            "Get the removed slot indices")

        .def("updated_slots", &TSDView::updated_slots,
            nb::rv_policy::reference_internal,
            "Get the updated slot indices");
}

// ============================================================================
// Module Registration
// ============================================================================

void ts_value_register_with_nanobind(nb::module_& m) {
    register_ts_view_iterators(m);
    register_ts_value(m);
    register_ts_view(m);
    register_tsb_view(m);
    register_tsl_view(m);
    register_tss_view(m);
    register_tsd_view(m);
}

} // namespace hgraph
