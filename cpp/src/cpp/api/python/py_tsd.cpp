#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/constants.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{

    // Helper to convert Python key to value::Value using TSMeta
    static value::Value<> key_from_python_with_meta(const nb::object &key, const TSMeta* meta) {
        const auto *key_schema = meta->key_type;
        value::Value<> key_val(key_schema);
        key_schema->ops->from_python(key_val.data(), key, key_schema);
        return key_val;
    }

    // Helper to convert value::View key to Python
    static nb::object key_view_to_python(const value::View& key, const TSMeta* meta) {
        return meta->key_type->ops->to_python(key.data(), meta->key_type);
    }

    // ===== PyTimeSeriesDictOutput Implementation =====

    value::Value<> PyTimeSeriesDictOutput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, view().ts_meta());
    }

    size_t PyTimeSeriesDictOutput::size() const {
        auto dict_view = view().as_dict();
        return dict_view.size();
    }

    nb::object PyTimeSeriesDictOutput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) { return key_set(); }
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(item);
        TSView elem_view = dict_view.at(key_val.const_view());
        return wrap_output_view(TSOutputView(elem_view, nullptr));
    }

    nb::object PyTimeSeriesDictOutput::get(const nb::object &item, const nb::object &default_value) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(item);
        if (dict_view.contains(key_val.const_view())) {
            TSView elem_view = dict_view.at(key_val.const_view());
            return wrap_output_view(TSOutputView(elem_view, nullptr));
        }
        return default_value;
    }

    nb::object PyTimeSeriesDictOutput::get_or_create(const nb::object &key) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        TSView elem_view = dict_view.get_or_create(key_val.const_view());
        return wrap_output_view(TSOutputView(elem_view, nullptr));
    }

    void PyTimeSeriesDictOutput::create(const nb::object &item) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(item);
        dict_view.create(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictOutput::contains(const nb::object &item) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(item);
        return dict_view.contains(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::key_set() const {
        auto dict_view = view().as_dict();
        TSSView tss_view = dict_view.key_set();
        // Wrap the TSSView as a TSOutputView (TSS is output-like for reading)
        // Use the parent view's current_time since TSSView doesn't expose it
        return wrap_output_view(TSOutputView(TSView(tss_view.view_data(), view().current_time()), nullptr));
    }

    nb::object PyTimeSeriesDictOutput::keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::values() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.modified_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_values() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::was_modified(const nb::object &key) const {
        auto dict_view = view().as_dict();
        // Check if key is in modified slots
        auto key_val = key_from_python(key);
        // Check if key exists and if it's modified
        if (!dict_view.contains(key_val.const_view())) return false;
        // Get the value and check if it's modified
        TSView elem = dict_view.at(key_val.const_view());
        return elem.modified();
    }

    nb::object PyTimeSeriesDictOutput::valid_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.added_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_values() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_added() const {
        auto dict_view = view().as_dict();
        return !dict_view.added_slots().empty();
    }

    bool PyTimeSeriesDictOutput::was_added(const nb::object &key) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(key);
        return dict_view.was_added(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::removed_keys() const {
        auto dict_view = view().as_dict();
        nb::list result;
        for (auto key : dict_view.removed_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_values() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_output_view(TSOutputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictOutput::has_removed() const {
        auto dict_view = view().as_dict();
        return !dict_view.removed_slots().empty();
    }

    bool PyTimeSeriesDictOutput::was_removed(const nb::object &key) const {
        auto dict_view = view().as_dict();
        auto key_val = key_from_python(key);
        return dict_view.was_removed(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::key_from_value(const nb::object &value) const {
        auto dict_view = view().as_dict();
        // Extract the underlying view from the Python wrapper
        auto& ts_type = nb::cast<PyTimeSeriesType&>(value);
        auto target_data = ts_type.view().view_data().value_data;
        // Search through dict entries to find matching value_data pointer
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView elem = *it;
            if (elem.view_data().value_data == target_data) {
                return key_view_to_python(it.key(), dict_view.meta());
            }
        }
        throw std::runtime_error("key_from_value: value not found in TSD");
    }

    nb::str PyTimeSeriesDictOutput::py_str() const {
        auto dict_view = view().as_dict();
        auto str = fmt::format("TimeSeriesDictOutput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&dict_view), dict_view.size(), dict_view.valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictOutput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictOutput::set_item(const nb::object &key, const nb::object &value) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);

        // Get element value type from TSMeta and convert Python value
        const TSMeta* meta = dict_view.meta();
        const value::TypeMeta* elem_value_type = meta->element_ts->value_type;
        value::Value<> value_val(elem_value_type);
        elem_value_type->ops->from_python(value_val.data(), value, elem_value_type);

        dict_view.set(key_val.const_view(), value_val.const_view());
    }

    void PyTimeSeriesDictOutput::del_item(const nb::object &key) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        dict_view.remove(key_val.const_view());
    }

    nb::object PyTimeSeriesDictOutput::pop(const nb::object &key, const nb::object &default_value) {
        auto dict_view = output_view().ts_view().as_dict();
        auto key_val = key_from_python(key);
        if (!dict_view.contains(key_val.const_view())) {
            return default_value;
        }
        // Get value wrapper before removal
        TSView elem_view = dict_view.at(key_val.const_view());
        nb::object result = wrap_output_view(TSOutputView(elem_view, nullptr));
        // Remove the key
        dict_view.remove(key_val.const_view());
        return result;
    }

    nb::object PyTimeSeriesDictOutput::get_ref(const nb::object &key, const nb::object &requester) {
        // Find the owning TSOutput — either from the output_view directly,
        // or by navigating from the view's ShortPath to the owning node
        TSOutput* ts_output = output_view().output();
        if (!ts_output) {
            // Navigate from the view's path to find the root TSOutput
            const ShortPath& path = view().short_path();
            if (path.valid() && path.node()) {
                ts_output = path.node()->ts_output();
            }
            if (!ts_output) {
                throw std::runtime_error("get_ref: cannot find owning TSOutput");
            }
        }

        // Build alternative schema: TSD[K, REF[elem_ts]]
        const TSMeta* tsd_meta = output_view().ts_meta();
        auto& registry = TSTypeRegistry::instance();
        const TSMeta* ref_elem_meta = registry.ref(tsd_meta->element_ts);
        const TSMeta* ref_tsd_meta = registry.tsd(tsd_meta->key_type, ref_elem_meta);

        // Get evaluation time from the owning node's graph (not from the view,
        // which may have stale current_time from construction)
        auto time = ts_output->owning_node()->graph()->evaluation_time();

        // Get or create the alternative TSD[K, REF[elem_ts]]
        TSOutputView alt_view = ts_output->view(time, ref_tsd_meta);

        // Navigate to element at key
        auto key_val = key_from_python(key);
        TSDView alt_dict = alt_view.ts_view().as_dict();

        // Create element in alternative if it doesn't exist yet
        if (!alt_dict.contains(key_val.const_view())) {
            alt_dict.create(key_val.const_view());
        }

        // Always update the TSReference (matches Python behavior where
        // FeatureOutputExtension.create_or_increment calls value_getter every time).
        // Build TSReference: peered if key exists in native, empty if not.
        TSView alt_elem = alt_dict.at(key_val.const_view());
        TSReference ref;
        TSDView native_dict = this->view().as_dict();
        if (native_dict.contains(key_val.const_view())) {
            TSView native_elem = native_dict.at(key_val.const_view());
            if (native_elem) {
                ref = TSReference::peered(native_elem.short_path());
            }
        }

        if (alt_elem) {
            // Write TSReference directly to the alternative's LOCAL value storage.
            // Do NOT use alt_elem.value() — it may delegate through an active
            // REFLink and return the native's value storage, causing overflow.
            void* value_data = alt_elem.view_data().value_data;
            if (value_data) {
                auto* ref_ptr = static_cast<TSReference*>(value_data);
                *ref_ptr = std::move(ref);
            }
            // Mark element as modified at current time so consumer sees the change
            auto* elem_time = static_cast<engine_time_t*>(alt_elem.view_data().time_data);
            if (elem_time) {
                *elem_time = time;
            }
        }

        TSView elem_view = alt_elem;

        if (!elem_view) {
            throw std::runtime_error("get_ref: key not found in alternative TSD");
        }

        // Wrap as REF output
        return wrap_output_view(TSOutputView(elem_view, nullptr));
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        // No-op: alternative TSD manages element lifecycle automatically
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    // Helper: get a resolved TSDictView for the input.
    // Input's own ViewData has empty MapStorage. The actual data lives in the
    // bound output, accessible through the LinkTarget. resolve_through_link()
    // returns a ViewData pointing to the output's storage if a LinkTarget is active.
    //
    // When the input's element type differs from the native output's (e.g., input is
    // TSD[K, REF[TS[V]]] bound to output TSD[K, TS[V]]), we need the alternative view
    // from the output which has matching element types (REF wrapping). The alternative's
    // time/delta only track structural changes, so we use native's for modification tracking.
    static TSDView resolved_dict_view(const TSInputView& iv) {
        ViewData vd = resolve_through_link(iv.ts_view().view_data());

        // Check if the input's schema differs from the resolved (native) view's schema.
        // This happens when the input expects REF-wrapped elements but the output is native.
        const TSMeta* input_meta = iv.ts_meta();
        if (input_meta && vd.meta && input_meta != vd.meta) {
            // Input schema differs from native — need the alternative view.
            // Recover TSOutput from the LinkTarget's target_path → node → ts_output.
            TSOutput* bound = iv.bound_output();
            if (!bound) {
                // bound_output_ not set on TSInputView (happens when the TSD input
                // is a child field of a TSB and Python's do_bind_output doesn't call
                // the C++ container-level bind). Recover from LinkTarget.
                const auto& ivd = iv.ts_view().view_data();
                if (ivd.uses_link_target && ivd.link_data) {
                    auto* lt = static_cast<const LinkTarget*>(ivd.link_data);
                    if (lt->is_linked && lt->target_path.node()) {
                        bound = lt->target_path.node()->ts_output();
                    }
                }
            }

            if (bound) {
                engine_time_t time = iv.current_time();
                TSOutputView alt_view = bound->view(time, input_meta);
                ViewData alt_vd = alt_view.ts_view().view_data();

                // Use alternative's value_data (has correct element types) + meta/ops,
                // but native's time/delta/observer for modification tracking.
                ViewData native_vd = bound->native_value().make_view_data();
                vd.value_data = alt_vd.value_data;
                vd.meta = alt_vd.meta;
                vd.ops = alt_vd.ops;
                vd.link_data = alt_vd.link_data;
                vd.time_data = native_vd.time_data;
                vd.delta_data = native_vd.delta_data;
                vd.observer_data = native_vd.observer_data;
            }
        }

        return TSView(vd, iv.current_time()).as_dict();
    }

    value::Value<> PyTimeSeriesDictInput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, input_view().ts_meta());
    }

    size_t PyTimeSeriesDictInput::size() const {
        auto dict_view = resolved_dict_view(input_view());
        return dict_view.size();
    }

    nb::object PyTimeSeriesDictInput::get_item(const nb::object &item) const {
        if (get_key_set_id().is(item)) { return key_set(); }
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(item);
        TSView elem_view = dict_view.at(key_val.const_view());
        return wrap_input_view(TSInputView(elem_view, nullptr));
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(item);
        if (dict_view.contains(key_val.const_view())) {
            TSView elem_view = dict_view.at(key_val.const_view());
            return wrap_input_view(TSInputView(elem_view, nullptr));
        }
        return default_value;
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        TSView elem_view = dict_view.get_or_create(key_val.const_view());
        return wrap_input_view(TSInputView(elem_view, nullptr));
    }

    void PyTimeSeriesDictInput::create(const nb::object &item) {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(item);
        dict_view.create(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::iter() const {
        return nb::iter(keys());
    }

    bool PyTimeSeriesDictInput::contains(const nb::object &item) const {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(item);
        return dict_view.contains(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::key_set() const {
        auto dict_view = resolved_dict_view(input_view());
        TSSView tss_view = dict_view.key_set();
        // Wrap the TSSView as a TSInputView (TSS is input-like for reading)
        // Use the parent view's current_time since TSSView doesn't expose it
        return wrap_input_view(TSInputView(TSView(tss_view.view_data(), input_view().current_time()), nullptr));
    }

    nb::object PyTimeSeriesDictInput::keys() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto key : dict_view.keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        if (dict_view.view_data().delta_data) {
            for (auto key : dict_view.modified_keys()) {
                result.append(key_view_to_python(key, dict_view.meta()));
            }
        } else if (dict_view.modified()) {
            // No delta tracking — scan all items
            for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
                TSView ts_view = *it;
                if (ts_view.modified()) {
                    result.append(key_view_to_python(it.key(), dict_view.meta()));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        if (dict_view.view_data().delta_data) {
            for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
                TSView ts_view = *it;
                result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
            }
        } else if (dict_view.modified()) {
            // No delta tracking — scan all items
            for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
                TSView ts_view = *it;
                if (ts_view.modified()) {
                    result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        auto dict_view = resolved_dict_view(input_view());

        nb::list result;
        if (dict_view.view_data().delta_data) {
            // Delta tracking available — use efficient delta-based iteration
            for (auto it = dict_view.modified_items().begin(); it != dict_view.modified_items().end(); ++it) {
                TSView ts_view = *it;
                auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
                result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
            }
        } else if (dict_view.modified()) {
            // No delta tracking (e.g., TSD field within TSB input) — scan all items
            // and check each element's modified flag individually.
            for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
                TSView ts_view = *it;
                if (ts_view.modified()) {
                    auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
                    result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
                }
            }
        }
        return result;
    }

    bool PyTimeSeriesDictInput::was_modified(const nb::object &key) const {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        if (!dict_view.contains(key_val.const_view())) return false;
        TSView elem = dict_view.at(key_val.const_view());
        return elem.modified();
    }

    nb::object PyTimeSeriesDictInput::valid_keys() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.valid_items().begin(); it != dict_view.valid_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_keys() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto key : dict_view.added_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.added_items().begin(); it != dict_view.added_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_added() const {
        auto dict_view = resolved_dict_view(input_view());
        return !dict_view.added_slots().empty();
    }

    bool PyTimeSeriesDictInput::was_added(const nb::object &key) const {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        return dict_view.was_added(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::removed_keys() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto key : dict_view.removed_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_input_view(TSInputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto it = dict_view.removed_items().begin(); it != dict_view.removed_items().end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_input_view(TSInputView(ts_view, nullptr));
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    bool PyTimeSeriesDictInput::has_removed() const {
        auto dict_view = resolved_dict_view(input_view());
        return !dict_view.removed_slots().empty();
    }

    bool PyTimeSeriesDictInput::was_removed(const nb::object &key) const {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        return dict_view.was_removed(key_val.const_view());
    }

    nb::object PyTimeSeriesDictInput::key_from_value(const nb::object &value) const {
        auto dict_view = resolved_dict_view(input_view());
        // Extract the underlying view from the Python wrapper
        auto& ts_type = nb::cast<PyTimeSeriesType&>(value);
        auto target_data = ts_type.view().view_data().value_data;
        // Search through dict entries to find matching value_data pointer
        for (auto it = dict_view.items().begin(); it != dict_view.items().end(); ++it) {
            TSView elem = *it;
            if (elem.view_data().value_data == target_data) {
                return key_view_to_python(it.key(), dict_view.meta());
            }
        }
        throw std::runtime_error("key_from_value: value not found in TSD");
    }

    nb::str PyTimeSeriesDictInput::py_str() const {
        auto dict_view = resolved_dict_view(input_view());
        auto str = fmt::format("TimeSeriesDictInput@{:p}[size={}, valid={}]",
                               static_cast<const void *>(&dict_view), dict_view.size(), dict_view.valid());
        return nb::str(str.c_str());
    }

    nb::str PyTimeSeriesDictInput::py_repr() const {
        return py_str();
    }

    void PyTimeSeriesDictInput::on_key_added(const nb::object &key) {
        // No-op: C++ view-based input sees new keys through bound output automatically
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        // No-op: C++ view-based input reflects key removal through bound output automatically
    }

    // ===== Collection-specific copy operations =====

    void PyTimeSeriesDictOutput::copy_from_input(const PyTimeSeriesInput &input) {
        // Build a combined dict: {k: v for k,v in input.items()} + {k: REMOVE for stale keys}
        // Then use from_python which correctly handles both SET and REMOVE entries.
        nb::object input_value = input.value();
        if (input_value.is_none()) {
            input_value = nb::dict();
        }

        // Get REMOVE sentinel
        nb::module_ tsd_mod = nb::module_::import_("hgraph._types._tsd_type");
        nb::object remove_sentinel = tsd_mod.attr("REMOVE");

        // Build combined dict from input value + REMOVE for stale keys
        nb::dict combined;

        // Add input entries (value is a frozendict, iterate it)
        for (auto item : input_value.attr("items")()) {
            auto kv = nb::cast<nb::tuple>(item);
            combined[kv[0]] = kv[1];
        }

        // Add REMOVE for keys in output but not in input
        nb::set input_keys_set;
        for (auto item : input_value.attr("keys")()) {
            input_keys_set.add(item);
        }

        nb::list output_keys_list = nb::cast<nb::list>(this->keys());
        for (auto key : output_keys_list) {
            if (!input_keys_set.contains(key)) {
                combined[key] = remove_sentinel;
            }
        }

        // Use from_python which handles both SET and REMOVE properly
        output_view().from_python(nb::cast<nb::object>(combined));
    }

    void PyTimeSeriesDictOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        // Same approach: build combined dict from source value + REMOVEs for stale keys
        nb::object src_value = output.value();
        if (src_value.is_none()) {
            src_value = nb::dict();
        }

        nb::module_ tsd_mod = nb::module_::import_("hgraph._types._tsd_type");
        nb::object remove_sentinel = tsd_mod.attr("REMOVE");

        nb::dict combined;
        for (auto item : src_value.attr("items")()) {
            auto kv = nb::cast<nb::tuple>(item);
            combined[kv[0]] = kv[1];
        }

        nb::set src_keys_set;
        for (auto item : src_value.attr("keys")()) {
            src_keys_set.add(item);
        }

        nb::list self_keys_list = nb::cast<nb::list>(this->keys());
        for (auto key : self_keys_list) {
            if (!src_keys_set.contains(key)) {
                combined[key] = remove_sentinel;
            }
        }

        output_view().from_python(nb::cast<nb::object>(combined));
    }

    // ===== Nanobind Registration =====

    void tsd_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesDictOutput, PyTimeSeriesOutput>(m, "TimeSeriesDictOutput")
            .def("copy_from_input", &PyTimeSeriesDictOutput::copy_from_input)
            .def("copy_from_output", &PyTimeSeriesDictOutput::copy_from_output)
            .def("__contains__", &PyTimeSeriesDictOutput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictOutput::get_item, "key"_a)
            .def("__setitem__", &PyTimeSeriesDictOutput::set_item, "key"_a, "value"_a)
            .def("__delitem__", &PyTimeSeriesDictOutput::del_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictOutput::size)
            .def("pop", &PyTimeSeriesDictOutput::pop, "key"_a, "default"_a = nb::none())
            .def("get", &PyTimeSeriesDictOutput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictOutput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictOutput::create, "key"_a)
            .def("clear", &PyTimeSeriesDictOutput::clear)
            .def("__iter__", &PyTimeSeriesDictOutput::iter)
            .def("keys", &PyTimeSeriesDictOutput::keys)
            .def("values", &PyTimeSeriesDictOutput::values)
            .def("items", &PyTimeSeriesDictOutput::items)
            .def("valid_keys", &PyTimeSeriesDictOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictOutput::valid_values)
            .def("valid_items", &PyTimeSeriesDictOutput::valid_items)
            .def("added_keys", &PyTimeSeriesDictOutput::added_keys)
            .def("added_values", &PyTimeSeriesDictOutput::added_values)
            .def("added_items", &PyTimeSeriesDictOutput::added_items)
            .def("was_added", &PyTimeSeriesDictOutput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictOutput::has_added)
            .def("modified_keys", &PyTimeSeriesDictOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictOutput::modified_values)
            .def("modified_items", &PyTimeSeriesDictOutput::modified_items)
            .def("was_modified", &PyTimeSeriesDictOutput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictOutput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictOutput::removed_values)
            .def("removed_items", &PyTimeSeriesDictOutput::removed_items)
            .def("was_removed", &PyTimeSeriesDictOutput::was_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictOutput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictOutput::has_removed)
            .def("get_ref", &PyTimeSeriesDictOutput::get_ref, "key"_a, "requester"_a)
            .def("release_ref", &PyTimeSeriesDictOutput::release_ref, "key"_a, "requester"_a)
            .def_prop_ro("key_set", &PyTimeSeriesDictOutput::key_set)
            .def("__str__", &PyTimeSeriesDictOutput::py_str)
            .def("__repr__", &PyTimeSeriesDictOutput::py_repr);

        nb::class_<PyTimeSeriesDictInput, PyTimeSeriesInput>(m, "TimeSeriesDictInput")
            .def("__contains__", &PyTimeSeriesDictInput::contains, "key"_a)
            .def("__getitem__", &PyTimeSeriesDictInput::get_item, "key"_a)
            .def("__len__", &PyTimeSeriesDictInput::size)
            .def("__iter__", &PyTimeSeriesDictInput::iter)
            .def("get", &PyTimeSeriesDictInput::get, "key"_a, "default"_a = nb::none())
            .def("get_or_create", &PyTimeSeriesDictInput::get_or_create, "key"_a)
            .def("create", &PyTimeSeriesDictInput::create, "key"_a)
            .def("keys", &PyTimeSeriesDictInput::keys)
            .def("values", &PyTimeSeriesDictInput::values)
            .def("items", &PyTimeSeriesDictInput::items)
            .def("valid_keys", &PyTimeSeriesDictInput::valid_keys)
            .def("valid_values", &PyTimeSeriesDictInput::valid_values)
            .def("valid_items", &PyTimeSeriesDictInput::valid_items)
            .def("added_keys", &PyTimeSeriesDictInput::added_keys)
            .def("added_values", &PyTimeSeriesDictInput::added_values)
            .def("added_items", &PyTimeSeriesDictInput::added_items)
            .def("was_added", &PyTimeSeriesDictInput::was_added, "key"_a)
            .def_prop_ro("has_added", &PyTimeSeriesDictInput::has_added)
            .def("modified_keys", &PyTimeSeriesDictInput::modified_keys)
            .def("modified_values", &PyTimeSeriesDictInput::modified_values)
            .def("modified_items", &PyTimeSeriesDictInput::modified_items)
            .def("was_modified", &PyTimeSeriesDictInput::was_modified, "key"_a)
            .def("removed_keys", &PyTimeSeriesDictInput::removed_keys)
            .def("removed_values", &PyTimeSeriesDictInput::removed_values)
            .def("removed_items", &PyTimeSeriesDictInput::removed_items)
            .def("was_removed", &PyTimeSeriesDictInput::was_removed, "key"_a)
            .def("on_key_added", &PyTimeSeriesDictInput::on_key_added, "key"_a)
            .def("on_key_removed", &PyTimeSeriesDictInput::on_key_removed, "key"_a)
            .def("key_from_value", &PyTimeSeriesDictInput::key_from_value, "value"_a)
            .def_prop_ro("has_removed", &PyTimeSeriesDictInput::has_removed)
            .def_prop_ro("key_set", &PyTimeSeriesDictInput::key_set)
            .def("__str__", &PyTimeSeriesDictInput::py_str)
            .def("__repr__", &PyTimeSeriesDictInput::py_repr);
    }
}  // namespace hgraph
