#include <hgraph/api/python/py_tsd.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/time_series/ts_dict_view.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/time_series/observer_list.h>
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
        auto items = dict_view.items();
        for (auto it = items.begin(); it != items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        auto items = dict_view.items();
        for (auto it = items.begin(); it != items.end(); ++it) {
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
        auto modified_items = dict_view.modified_items();
        for (auto it = modified_items.begin(); it != modified_items.end(); ++it) {
            TSView ts_view = dict_view.at(it.key());
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::modified_items() const {
        auto dict_view = view().as_dict();
        nb::list result;
        auto modified_items = dict_view.modified_items();
        for (auto it = modified_items.begin(); it != modified_items.end(); ++it) {
            TSView ts_view = dict_view.at(it.key());
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
        auto valid_items = dict_view.valid_items();
        for (auto it = valid_items.begin(); it != valid_items.end(); ++it) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_values() const {
        auto dict_view = view().as_dict();

        nb::list result;
        auto valid_items = dict_view.valid_items();
        for (auto it = valid_items.begin(); it != valid_items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::valid_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        auto valid_items = dict_view.valid_items();
        for (auto it = valid_items.begin(); it != valid_items.end(); ++it) {
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
        auto added_items = dict_view.added_items();
        for (auto it = added_items.begin(); it != added_items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::added_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        auto added_items = dict_view.added_items();
        for (auto it = added_items.begin(); it != added_items.end(); ++it) {
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
        auto removed_items = dict_view.removed_items();
        for (auto it = removed_items.begin(); it != removed_items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_output_view(TSOutputView(ts_view, nullptr)));
        }
        return result;
    }

    nb::object PyTimeSeriesDictOutput::removed_items() const {
        auto dict_view = view().as_dict();

        nb::list result;
        auto removed_items = dict_view.removed_items();
        for (auto it = removed_items.begin(); it != removed_items.end(); ++it) {
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
        auto items = dict_view.items();
        for (auto it = items.begin(); it != items.end(); ++it) {
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
        const auto& vd = output_view().ts_view().view_data();
        if (!vd.ops || !vd.ops->dict_prepare_ref_entry) {
            throw std::runtime_error("get_ref: not supported for this type");
        }
        // Get evaluation time from owning node's graph (view time may be stale)
        engine_time_t time = output_view().current_time();
        if (vd.path.valid() && vd.path.node()) {
            if (auto* graph = vd.path.node()->graph()) {
                time = graph->evaluation_time();
            }
        }
        auto key_val = key_from_python(key);
        ViewData elem_vd = vd.ops->dict_prepare_ref_entry(vd, key_val.const_view(), time);
        if (!elem_vd.valid()) {
            throw std::runtime_error("get_ref: key not found in alternative TSD");
        }
        return wrap_output_view(TSOutputView(TSView(elem_vd, time), nullptr));
    }

    void PyTimeSeriesDictOutput::release_ref(const nb::object &key, const nb::object &requester) {
        // No-op: alternative TSD manages element lifecycle automatically
    }

    // ===== PyTimeSeriesDictInput Implementation =====

    // Helper: get a resolved TSDictView for the input.
    // Delegates to ts_ops resolve_bound_output which handles LinkTarget traversal
    // and alternative schema resolution when input schema differs from native output.
    static TSDView resolved_dict_view(const TSInputView& iv) {
        const auto& input_vd = iv.ts_view().view_data();
        ViewData resolved_vd = input_vd.ops && input_vd.ops->resolve_bound_output
            ? input_vd.ops->resolve_bound_output(input_vd, iv.current_time())
            : input_vd;
        return TSView(resolved_vd, iv.current_time()).as_dict();
    }

    static nb::object wrap_dict_input_child_view(
            const TSView& ts_view,
            engine_time_t current_time,
            TSInput* parent_input,
            const TSMeta* effective_meta = nullptr) {
        const TSMeta* child_meta = ts_view.ts_meta();

        // Schema override for alternative-wired dict values: when the output-side child
        // exposes REF meta but the input schema expects a concrete type (e.g. TSB),
        // resolve through links using the resolve_through_link() utility and wrap with
        // the effective schema. This is a wrapping-layer schema concern (choosing which
        // meta to pass to wrap_input_view), not business logic.
        const bool prefer_effective_meta =
            effective_meta &&
            child_meta &&
            child_meta != effective_meta &&
            child_meta->kind == TSKind::REF &&
            effective_meta->kind != TSKind::REF;

        if (!prefer_effective_meta) {
            const TSMeta* initial_meta = child_meta ? nullptr : effective_meta;
            auto wrapped = initial_meta
                               ? wrap_input_view(TSInputView(ts_view, parent_input), initial_meta)
                               : wrap_input_view(TSInputView(ts_view, parent_input));
            if (!wrapped.is_none()) {
                return wrapped;
            }
        }

        ViewData resolved_vd = resolve_through_link(ts_view.view_data());
        if (!resolved_vd.valid() || !resolved_vd.ops) {
            return nb::none();
        }

        TSView resolved_view(resolved_vd, current_time);
        const TSMeta* resolved_meta = effective_meta ? effective_meta : resolved_vd.meta;
        if (resolved_meta) {
            auto wrapped = wrap_input_view(TSInputView(resolved_view, parent_input), resolved_meta);
            if (!wrapped.is_none()) {
                return wrapped;
            }
        }
        return wrap_input_view(TSInputView(resolved_view, parent_input));
    }

    // Shorthand: wrap a dict element using the standard arguments from an input view + dict view.
    static nb::object wrap_dict_child(const TSView& child, const TSInputView& iv, const TSDView& dict_view) {
        return wrap_dict_input_child_view(child, iv.current_time(),
            const_cast<TSInput*>(iv.input()),
            dict_view.meta() ? dict_view.meta()->element_ts : nullptr);
    }

    value::Value<> PyTimeSeriesDictInput::key_from_python(const nb::object &key) const {
        return key_from_python_with_meta(key, input_view().ts_meta());
    }

    nb::object PyTimeSeriesDictInput::value() const {
        auto dict_view = resolved_dict_view(input_view());
        return TSView(dict_view.view_data(), input_view().current_time()).to_python();
    }

    nb::object PyTimeSeriesDictInput::delta_value() const {
        const auto& input_vd = input_view().ts_view().view_data();
        if (!input_vd.ops) return nb::none();
        ViewData resolved_vd = input_vd.ops->resolve_bound_output
            ? input_vd.ops->resolve_bound_output(input_vd, input_view().current_time())
            : input_vd;
        if (resolved_vd.ops && resolved_vd.ops->dict_delta_to_python_safe) {
            return resolved_vd.ops->dict_delta_to_python_safe(resolved_vd);
        }
        return nb::none();
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
        return wrap_dict_child(elem_view, input_view(), dict_view);
    }

    nb::object PyTimeSeriesDictInput::get(const nb::object &item, const nb::object &default_value) const {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(item);
        if (dict_view.contains(key_val.const_view())) {
            TSView elem_view = dict_view.at(key_val.const_view());
            return wrap_dict_child(elem_view, input_view(), dict_view);
        }
        return default_value;
    }

    nb::object PyTimeSeriesDictInput::get_or_create(const nb::object &key) {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        TSView elem_view = dict_view.get_or_create(key_val.const_view());
        return wrap_dict_child(elem_view, input_view(), dict_view);
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
        auto items = dict_view.items();
        for (auto it = items.begin(); it != items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_dict_child(ts_view, input_view(), dict_view));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto items = dict_view.items();
        for (auto it = items.begin(); it != items.end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }
        return result;
    }

    // When a dict input is modified but has no per-key delta structure (delta_data == nullptr),
    // fall back to treating all current items as modified. This occurs on bind/rebind ticks
    // where the container was marked modified without individual key-level tracking.
    template <typename AppendFn>
    static void dict_modified_fallback(const TSDView& dict_view, nb::list& result, AppendFn&& append_fn) {
        if (result.size() == 0 && dict_view.modified() && dict_view.view_data().delta_data == nullptr) {
            auto items = dict_view.items();
            for (auto it = items.begin(); it != items.end(); ++it) {
                TSView ts_view = *it;
                if (!ts_view.view_data().ops) continue;
                append_fn(it, ts_view);
            }
        }
    }

    nb::object PyTimeSeriesDictInput::modified_keys() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        for (auto key : dict_view.modified_keys()) {
            result.append(key_view_to_python(key, dict_view.meta()));
        }

        dict_modified_fallback(dict_view, result, [&](auto& it, TSView&) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        });
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto modified_items = dict_view.modified_items();
        for (auto it = modified_items.begin(); it != modified_items.end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
            if (wrapped_value.is_none()) {
                continue;
            }
            result.append(wrapped_value);
        }

        dict_modified_fallback(dict_view, result, [&](auto& it, TSView& ts_view) {
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
            if (!wrapped_value.is_none()) {
                result.append(wrapped_value);
            }
        });
        return result;
    }

    nb::object PyTimeSeriesDictInput::modified_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto modified_items = dict_view.modified_items();
        for (auto it = modified_items.begin(); it != modified_items.end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
            if (wrapped_value.is_none()) {
                continue;
            }
            result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
        }

        dict_modified_fallback(dict_view, result, [&](auto& it, TSView& ts_view) {
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
            if (!wrapped_value.is_none()) {
                result.append(nb::make_tuple(key_view_to_python(it.key(), dict_view.meta()), wrapped_value));
            }
        });
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
        auto valid_items = dict_view.valid_items();
        for (auto it = valid_items.begin(); it != valid_items.end(); ++it) {
            result.append(key_view_to_python(it.key(), dict_view.meta()));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_values() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto valid_items = dict_view.valid_items();
        for (auto it = valid_items.begin(); it != valid_items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_dict_child(ts_view, input_view(), dict_view));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::valid_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto valid_items = dict_view.valid_items();
        for (auto it = valid_items.begin(); it != valid_items.end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
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
        auto added_items = dict_view.added_items();
        for (auto it = added_items.begin(); it != added_items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_dict_child(ts_view, input_view(), dict_view));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::added_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto added_items = dict_view.added_items();
        for (auto it = added_items.begin(); it != added_items.end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
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
        auto removed_items = dict_view.removed_items();
        for (auto it = removed_items.begin(); it != removed_items.end(); ++it) {
            TSView ts_view = *it;
            result.append(wrap_dict_child(ts_view, input_view(), dict_view));
        }
        return result;
    }

    nb::object PyTimeSeriesDictInput::removed_items() const {
        auto dict_view = resolved_dict_view(input_view());
        nb::list result;
        auto removed_items = dict_view.removed_items();
        for (auto it = removed_items.begin(); it != removed_items.end(); ++it) {
            TSView ts_view = *it;
            auto wrapped_value = wrap_dict_child(ts_view, input_view(), dict_view);
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
        auto items = dict_view.items();
        for (auto it = items.begin(); it != items.end(); ++it) {
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
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        if (!dict_view.contains(key_val.const_view())) {
            dict_view.create(key_val.const_view());
        }
    }

    void PyTimeSeriesDictInput::on_key_removed(const nb::object &key) {
        auto dict_view = resolved_dict_view(input_view());
        auto key_val = key_from_python(key);
        if (dict_view.contains(key_val.const_view())) {
            dict_view.remove(key_val.const_view());
        }
    }

    // ===== Collection-specific copy operations =====

    void PyTimeSeriesDictOutput::copy_from_input(const PyTimeSeriesInput &input) {
        // PyTimeSeriesType::value() is non-virtual, so dispatch explicitly for TSD inputs.
        nb::object input_value;
        const auto *input_as_tsd = dynamic_cast<const PyTimeSeriesDictInput *>(&input);
        if (input_as_tsd) {
            input_value = input_as_tsd->value();
        } else {
            input_value = input.value();
        }
        if (input_value.is_none()) {
            input_value = nb::dict();
        }

        nb::module_ tsd_mod = nb::module_::import_("hgraph._types._tsd_type");
        nb::object remove_sentinel = tsd_mod.attr("REMOVE");

        nb::dict combined;
        for (auto item : input_value.attr("items")()) {
            auto kv = nb::cast<nb::tuple>(item);
            combined[kv[0]] = kv[1];
        }

        nb::set input_keys_set;
        if (input_as_tsd) {
            for (auto item : input_as_tsd->keys()) {
                input_keys_set.add(item);
            }
        } else {
            for (auto item : input_value.attr("keys")()) {
                input_keys_set.add(item);
            }
        }

        auto self_dict_view = output_view().ts_view().as_dict();
        for (auto key : self_dict_view.keys()) {
            nb::object py_key = key_view_to_python(key, self_dict_view.meta());
            if (!input_keys_set.contains(py_key)) {
                combined[py_key] = remove_sentinel;
            }
        }

        output_view().from_python(nb::cast<nb::object>(combined));
    }

    void PyTimeSeriesDictOutput::copy_from_output(const PyTimeSeriesOutput &output) {
        // PyTimeSeriesType::value() is non-virtual, so dispatch explicitly for TSD outputs.
        nb::object src_value;
        const auto *output_as_tsd = dynamic_cast<const PyTimeSeriesDictOutput *>(&output);
        if (output_as_tsd) {
            src_value = output_as_tsd->output_view().ts_view().to_python();
        } else {
            src_value = output.value();
        }
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
        if (output_as_tsd) {
            auto src_dict_view = output_as_tsd->output_view().ts_view().as_dict();
            for (auto key : src_dict_view.keys()) {
                src_keys_set.add(key_view_to_python(key, src_dict_view.meta()));
            }
        } else {
            for (auto item : src_value.attr("keys")()) {
                src_keys_set.add(item);
            }
        }

        auto self_dict_view = output_view().ts_view().as_dict();
        for (auto key : self_dict_view.keys()) {
            nb::object py_key = key_view_to_python(key, self_dict_view.meta());
            if (!src_keys_set.contains(py_key)) {
                combined[py_key] = remove_sentinel;
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
            .def_prop_ro("value", &PyTimeSeriesDictInput::value)
            .def_prop_ro("delta_value", &PyTimeSeriesDictInput::delta_value)
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
