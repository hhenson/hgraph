#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/value/python_conversion.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/ref_type.h>
#include <hgraph/types/time_series/access_strategy.h>
#include <hgraph/api/python/ts_python_helpers.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/delta_view_python.h>
#include <fmt/format.h>
#include <optional>

namespace hgraph
{
    // Helper to check if a CollectionAccessStrategy has "unpeered" children
    // Unpeered means children are DirectAccessStrategies bound to separate outputs
    // Peered means children are ElementAccessStrategies that navigate into a common parent output
    static bool has_unpeered_children(ts::CollectionAccessStrategy* coll_strategy) {
        if (!coll_strategy) return false;
        for (size_t i = 0; i < coll_strategy->child_count(); ++i) {
            auto* child = coll_strategy->child(i);
            if (child != nullptr) {
                // ElementAccessStrategy children mean peered mode (navigating into parent)
                // DirectAccessStrategy children mean unpeered mode (separate outputs)
                if (dynamic_cast<ts::DirectAccessStrategy*>(child) != nullptr) {
                    return true;  // Found unpeered child
                }
            }
        }
        return false;  // No unpeered children (either empty or all are ElementAccessStrategy)
    }

    // PyTimeSeriesBundleOutput implementations

    void PyTimeSeriesBundleOutput::set_value(nb::object py_value) {
        if (!_view.valid() || !_meta) return;

        auto eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        if (py_value.is_none()) {
            _view.mark_invalid();
            return;
        }

        // Get the TSB metadata to access field information
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        auto* bundle_schema = static_cast<const value::BundleTypeMeta*>(tsb_meta->bundle_value_type);

        if (!bundle_schema) {
            // Fall back to base implementation if no bundle schema
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        // Iterate over the dict and set each field
        if (!nb::isinstance<nb::dict>(py_value)) {
            PyTimeSeriesOutput::set_value(std::move(py_value));
            return;
        }

        nb::dict d = nb::cast<nb::dict>(py_value);

        // _view is already a TSView - get its inner ValueView
        auto bundle_data = _view.value_view().data();

        // Get the tracker for field modification
        auto tracker = _view.tracker();

        for (const auto& field : bundle_schema->fields) {
            if (d.contains(field.name.c_str())) {
                // Get the field's storage location
                void* field_ptr = static_cast<char*>(bundle_data) + field.offset;

                // Convert Python value to C++ and store
                if (field.type && field.type->ops && field.type->ops->from_python) {
                    value::value_from_python(field_ptr, d[field.name.c_str()], field.type);
                }

                // Find the field index and mark as modified
                size_t field_index = tsb_meta->field_index(field.name);
                if (field_index != SIZE_MAX) {
                    // Mark this specific field as modified
                    auto field_tracker = tracker.field(field_index);
                    field_tracker.mark_modified(eval_time);
                }
            }
        }

        // Also mark the bundle itself as modified
        _view.mark_modified(eval_time);
    }

    // Helper to get field index from key (int or string)
    static std::optional<size_t> get_field_index(const TSBTypeMeta* tsb_meta, const nb::handle& key) {
        if (!tsb_meta) return std::nullopt;

        if (nb::isinstance<nb::int_>(key)) {
            size_t index = nb::cast<size_t>(key);
            if (index < tsb_meta->fields.size()) {
                return index;
            }
        } else if (nb::isinstance<nb::str>(key)) {
            std::string name = nb::cast<std::string>(key);
            size_t index = tsb_meta->field_index(name);
            if (index != SIZE_MAX) {
                return index;
            }
        }
        return std::nullopt;
    }

    // Helper to create output wrapper from a view (for field access)
    static nb::object create_output_wrapper_from_view(
            const node_s_ptr& node,
            value::TSView view,
            const TSMeta* meta) {
        if (!view.valid()) return nb::none();

        // For bundle fields, we pass nullptr for TSOutput* since we're wrapping a view
        if (!meta) {
            return nb::cast(PyTimeSeriesOutput(node, std::move(view), nullptr, meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(node, std::move(view), nullptr, meta));
            default:
                return nb::cast(PyTimeSeriesOutput(node, std::move(view), nullptr, meta));
        }
    }

    nb::object PyTimeSeriesBundleOutput::get_item(const nb::handle &key) const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        auto field_idx = get_field_index(tsb_meta, key);
        if (!field_idx) return nb::none();

        // Navigate to field via view (const_cast needed as field() is not const)
        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto field_view = mutable_view.field(*field_idx);
        if (field_view.valid()) {
            return create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[*field_idx].type);
        }
        return nb::none();
    }

    nb::object PyTimeSeriesBundleOutput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleOutput::contains(const nb::handle &key) const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        return nb::bool_(get_field_index(tsb_meta, key).has_value());
    }

    nb::object PyTimeSeriesBundleOutput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleOutput::keys() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        for (const auto& field : tsb_meta->fields) {
            result.append(nb::cast(field.name));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::values() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid()) {
                result.append(create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::items() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid()) {
                auto wrapped = create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                result.append(nb::make_tuple(nb::cast(tsb_meta->fields[i].name), wrapped));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::valid_keys() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid() && field_view.has_value()) {
                result.append(nb::cast(tsb_meta->fields[i].name));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::valid_values() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid() && field_view.has_value()) {
                result.append(create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::valid_items() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid() && field_view.has_value()) {
                auto wrapped = create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                result.append(nb::make_tuple(nb::cast(tsb_meta->fields[i].name), wrapped));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::modified_keys() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta || !_node) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid() && field_view.modified_at(eval_time)) {
                result.append(nb::cast(tsb_meta->fields[i].name));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::modified_values() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta || !_node) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid() && field_view.modified_at(eval_time)) {
                result.append(create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleOutput::modified_items() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta || !_node) return nb::list();

        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (field_view.valid() && field_view.modified_at(eval_time)) {
                auto wrapped = create_output_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                result.append(nb::make_tuple(nb::cast(tsb_meta->fields[i].name), wrapped));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesBundleOutput::len() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        return nb::int_(tsb_meta ? tsb_meta->fields.size() : 0);
    }

    nb::bool_ PyTimeSeriesBundleOutput::empty() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        return nb::bool_(!tsb_meta || tsb_meta->fields.empty());
    }

    nb::bool_ PyTimeSeriesBundleOutput::all_valid() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::bool_(false);

        auto& mutable_view = const_cast<value::TSView&>(_view);

        // Check all fields for validity
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = mutable_view.field(i);
            if (!field_view.valid() || !field_view.has_value()) {
                return nb::bool_(false);
            }
        }
        return nb::bool_(true);
    }

    nb::object PyTimeSeriesBundleOutput::delta_value() const {
        // TSB.delta_value returns dict[str, Any] of modified fields' deltas
        // Python: {k: ts.delta_value for k, ts in self.items() if ts.modified and ts.valid}
        if (!_node) return nb::none();

        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::none();

        // Use fresh view from _output when available
        value::TSView view_to_use = _output ? _output->view() : _view;
        if (!view_to_use.valid()) return nb::none();

        engine_time_t eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        // If the TSB itself is not modified, return None
        if (!view_to_use.modified_at(eval_time)) return nb::none();

        nb::dict result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = view_to_use.field(i);
            if (field_view.valid() && field_view.modified_at(eval_time) && field_view.has_value()) {
                // Get the field's delta value
                auto delta = field_view.delta_view(eval_time);
                nb::object py_delta = ts::delta_to_python(delta);
                result[nb::cast(tsb_meta->fields[i].name)] = py_delta;
            }
        }
        return result;
    }

    void PyTimeSeriesBundleOutput::clear() {
        // TSB.clear() calls clear() on each field
        // Python: for v in self.values(): v.clear()
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return;

        // Use fresh view from _output when available
        value::TSView view_to_use = _output ? _output->view() : _view;
        if (!view_to_use.valid()) return;

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = view_to_use.field(i);
            if (field_view.valid()) {
                field_view.mark_invalid();
            }
        }
    }

    nb::str PyTimeSeriesBundleOutput::py_str() const {
        return nb::str("TSB{...}");
    }

    nb::str PyTimeSeriesBundleOutput::py_repr() const {
        return py_str();
    }

    // PyTimeSeriesBundleInput implementations

    // Helper to check if the input strategy has children (unpeered mode)
    static ts::CollectionAccessStrategy* get_collection_strategy(const ts::TSInputView& view) {
        auto* source = view.source();
        return dynamic_cast<ts::CollectionAccessStrategy*>(source);
    }

    nb::object PyTimeSeriesBundleInput::value() const {
        // Check if we're in unpeered mode (CollectionAccessStrategy with children)
        auto* coll_strategy = get_collection_strategy(view());
        if (has_unpeered_children(coll_strategy)) {
            // Unpeered mode: iterate over children
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
            if (!tsb_meta) return nb::none();

            nb::dict result;
            for (size_t i = 0; i < coll_strategy->child_count(); ++i) {
                auto* child = coll_strategy->child(i);
                if (child && child->has_value()) {
                    // Get field name from meta
                    const std::string& field_name = tsb_meta->fields[i].name;
                    // Get value from child strategy
                    auto value_view = child->value();
                    if (value_view.valid()) {
                        auto* field_meta = tsb_meta->fields[i].type;
                        auto* value_schema = field_meta ? field_meta->value_schema() : value_view.schema();
                        result[nb::cast(field_name)] = value::value_to_python(value_view.data(), value_schema);
                    }
                }
            }
            return result;
        }

        // Peered mode: delegate to parent implementation
        return PyTimeSeriesInput::value();
    }

    nb::object PyTimeSeriesBundleInput::delta_value() const {
        if (!_node) return nb::none();
        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;

        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::none();

        // Check if we're in unpeered mode (CollectionAccessStrategy with DirectAccessStrategy children)
        auto* coll_strategy = get_collection_strategy(view());
        bool is_unpeered = has_unpeered_children(coll_strategy);


        if (is_unpeered) {
            // Unpeered mode: iterate over children, return dict of modified fields' delta_value
            nb::dict result;
            for (size_t i = 0; i < coll_strategy->child_count(); ++i) {
                auto* child = coll_strategy->child(i);
                bool child_modified = child && child->modified_at(eval_time);
                bool child_has_value = child && child->has_value();
                if (child_modified && child_has_value) {
                    // Get field name from meta
                    const std::string& field_name = tsb_meta->fields[i].name;

                    // Get the child's bound output to access its delta
                    auto* child_output = child->bound_output();
                    if (child_output) {
                        // Get delta value from child output
                        auto* field_meta = tsb_meta->fields[i].type;
                        nb::object field_delta = ts::get_python_delta(child_output, eval_time, field_meta);
                        if (!field_delta.is_none()) {
                            result[nb::cast(field_name)] = field_delta;
                        }
                    }
                }
            }
            return result;
        }

        // Peered mode: iterate over the bound output's fields
        // For TSB, we need to check each field's modification status
        auto v = view();
        if (!v.valid()) return nb::none();

        auto* source = v.source();
        if (!source) return nb::none();

        auto* bound_output = source->bound_output();
        if (!bound_output) return nb::none();

        auto output_view = bound_output->view();
        if (!output_view.valid()) return nb::none();

        nb::dict result;
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            auto field_view = output_view.field(i);
            if (!field_view.valid()) continue;

            // Check the VIEW's ts_kind, not the meta's, because:
            // - Meta describes the INPUT field type (dereferenced to TS for REF inputs)
            // - View describes the OUTPUT field type (which is REF)
            auto view_kind = field_view.ts_kind();
            const std::string& field_name = tsb_meta->fields[i].name;

            // For REF fields in the output, we need to dereference to get the target value's delta
            // This matches Python behavior where bundle input fields are dereferenced
            if (view_kind == TSKind::REF) {
                // Use the new abstraction to dereference the REF field
                auto target_view = field_view.dereference_ref();
                if (!target_view.valid() || !target_view.has_value()) continue;

                // Check if the REF field itself was modified (new binding)
                bool ref_field_modified = field_view.modified_at(eval_time);
                bool target_modified = target_view.modified_at(eval_time);

                // Include delta if:
                // 1. The REF field was modified (new binding) - return target's current value
                // 2. The target itself was modified - return target's delta
                if (!ref_field_modified && !target_modified) continue;

                // Get delta from the target (dereferenced) view
                // If REF binding changed, target's value is the "delta" even if target wasn't modified
                // If target changed, get its actual delta
                nb::object field_delta;
                if (ref_field_modified && !target_modified) {
                    // New binding - return target's full value as the delta
                    auto value_view = target_view.value_view();
                    if (value_view.valid()) {
                        auto* schema = target_view.value_schema();
                        field_delta = value::value_to_python(value_view.data(), schema);
                    }
                } else {
                    // Target modified - return actual delta
                    auto delta = target_view.delta_view(eval_time);
                    field_delta = ts::delta_to_python(delta);
                }
                if (!field_delta.is_none()) {
                    result[nb::cast(field_name)] = field_delta;
                }
            } else {
                // Non-REF field: use normal delta path
                if (!field_view.modified_at(eval_time) || !field_view.has_value()) continue;

                // Get delta from the field view
                auto delta = field_view.delta_view(eval_time);
                nb::object field_delta = ts::delta_to_python(delta);
                if (!field_delta.is_none()) {
                    result[nb::cast(field_name)] = field_delta;
                }
            }
        }
        return result;
    }

    // Helper for creating input wrappers from views
    static nb::object create_input_wrapper_from_view(
            const node_s_ptr& node,
            ts::TSInputView view,
            const TSMeta* meta) {
        if (!view.valid()) return nb::none();

        // Create a PyTimeSeriesInput wrapper (view-only, no TSInput)
        // Use the wrapper factory for proper type dispatch
        if (!meta) {
            return nb::cast(PyTimeSeriesInput(node, std::move(view), meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueInput(node, std::move(view), nullptr, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(node, std::move(view), nullptr, meta));
            default:
                return nb::cast(PyTimeSeriesInput(node, std::move(view), meta));
        }
    }

    // Helper to wrap a field input from the strategy's child
    static nb::object wrap_field_input_from_strategy(
            const node_s_ptr& node,
            ts::AccessStrategy* child_strategy,
            const TSMeta* field_meta) {
        if (!child_strategy) return nb::none();

        // Create a TSInputView from the child strategy
        ts::TSInputView field_view(child_strategy, field_meta);
        // Use the field wrapper factory
        return create_input_wrapper_from_view(node, std::move(field_view), field_meta);
    }

    nb::object PyTimeSeriesBundleInput::get_item(const nb::handle &key) const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        auto field_idx = get_field_index(tsb_meta, key);
        if (!field_idx) return nb::none();

        // Check for unpeered mode first (CollectionAccessStrategy with children)
        auto* coll_strategy = get_collection_strategy(view());
        if (has_unpeered_children(coll_strategy)) {
            // Unpeered mode: get child strategy
            auto* child_strategy = coll_strategy->child(*field_idx);
            if (child_strategy) {
                return wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[*field_idx].type);
            }
        }

        // Navigate via view (works for both peered and unpeered modes now)
        auto v = view();
        auto field_view = v.element(*field_idx);
        if (field_view.valid()) {
            return create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[*field_idx].type);
        }

        return nb::none();
    }

    nb::object PyTimeSeriesBundleInput::get_attr(const nb::handle &key) const {
        return get_item(key);
    }

    nb::bool_ PyTimeSeriesBundleInput::contains(const nb::handle &key) const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        return nb::bool_(get_field_index(tsb_meta, key).has_value());
    }

    nb::object PyTimeSeriesBundleInput::iter() const {
        return nb::iter(keys());
    }

    nb::object PyTimeSeriesBundleInput::keys() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        for (const auto& field : tsb_meta->fields) {
            result.append(nb::cast(field.name));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::values() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            nb::object wrapped;
            if (has_unpeered_children(coll_strategy)) {
                // Unpeered mode
                auto* child_strategy = coll_strategy->child(i);
                wrapped = wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[i].type);
            } else {
                // Peered mode
                auto field_view = view().element(i);
                wrapped = create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
            }
            result.append(wrapped);
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::items() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            nb::object wrapped;
            if (has_unpeered_children(coll_strategy)) {
                // Unpeered mode
                auto* child_strategy = coll_strategy->child(i);
                wrapped = wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[i].type);
            } else {
                // Peered mode
                auto field_view = view().element(i);
                wrapped = create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
            }
            result.append(nb::make_tuple(nb::cast(tsb_meta->fields[i].name), wrapped));
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::valid_keys() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool is_valid = false;
            if (has_unpeered_children(coll_strategy)) {
                auto* child_strategy = coll_strategy->child(i);
                is_valid = child_strategy && child_strategy->has_value();
            } else {
                auto field_view = view().element(i);
                is_valid = field_view.valid() && field_view.has_value();
            }
            if (is_valid) {
                result.append(nb::cast(tsb_meta->fields[i].name));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::valid_values() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool is_valid = false;
            nb::object wrapped;

            if (has_unpeered_children(coll_strategy)) {
                auto* child_strategy = coll_strategy->child(i);
                is_valid = child_strategy && child_strategy->has_value();
                if (is_valid) {
                    wrapped = wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[i].type);
                }
            } else {
                auto field_view = view().element(i);
                is_valid = field_view.valid() && field_view.has_value();
                if (is_valid) {
                    wrapped = create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                }
            }
            if (is_valid) {
                result.append(wrapped);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::valid_items() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::list();

        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool is_valid = false;
            nb::object wrapped;

            if (has_unpeered_children(coll_strategy)) {
                auto* child_strategy = coll_strategy->child(i);
                is_valid = child_strategy && child_strategy->has_value();
                if (is_valid) {
                    wrapped = wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[i].type);
                }
            } else {
                auto field_view = view().element(i);
                is_valid = field_view.valid() && field_view.has_value();
                if (is_valid) {
                    wrapped = create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                }
            }
            if (is_valid) {
                result.append(nb::make_tuple(nb::cast(tsb_meta->fields[i].name), wrapped));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::modified_keys() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta || !_node) return nb::list();

        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool is_modified = false;
            if (has_unpeered_children(coll_strategy)) {
                auto* child_strategy = coll_strategy->child(i);
                is_modified = child_strategy && child_strategy->modified_at(eval_time);
            } else {
                auto field_view = view().element(i);
                is_modified = field_view.valid() && field_view.modified_at(eval_time);
            }
            if (is_modified) {
                result.append(nb::cast(tsb_meta->fields[i].name));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::modified_values() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta || !_node) return nb::list();

        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool is_modified = false;
            nb::object wrapped;

            if (has_unpeered_children(coll_strategy)) {
                auto* child_strategy = coll_strategy->child(i);
                is_modified = child_strategy && child_strategy->modified_at(eval_time);
                if (is_modified) {
                    wrapped = wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[i].type);
                }
            } else {
                auto field_view = view().element(i);
                is_modified = field_view.valid() && field_view.modified_at(eval_time);
                if (is_modified) {
                    wrapped = create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                }
            }
            if (is_modified) {
                result.append(wrapped);
            }
        }
        return result;
    }

    nb::object PyTimeSeriesBundleInput::modified_items() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta || !_node) return nb::list();

        auto eval_time = _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto* coll_strategy = get_collection_strategy(view());

        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool is_modified = false;
            nb::object wrapped;

            if (has_unpeered_children(coll_strategy)) {
                auto* child_strategy = coll_strategy->child(i);
                is_modified = child_strategy && child_strategy->modified_at(eval_time);
                if (is_modified) {
                    wrapped = wrap_field_input_from_strategy(_node, child_strategy, tsb_meta->fields[i].type);
                }
            } else {
                auto field_view = view().element(i);
                is_modified = field_view.valid() && field_view.modified_at(eval_time);
                if (is_modified) {
                    wrapped = create_input_wrapper_from_view(_node, std::move(field_view), tsb_meta->fields[i].type);
                }
            }
            if (is_modified) {
                result.append(nb::make_tuple(nb::cast(tsb_meta->fields[i].name), wrapped));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesBundleInput::len() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        return nb::int_(tsb_meta ? tsb_meta->fields.size() : 0);
    }

    nb::bool_ PyTimeSeriesBundleInput::empty() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        return nb::bool_(!tsb_meta || tsb_meta->fields.empty());
    }

    nb::str PyTimeSeriesBundleInput::py_str() const {
        return nb::str("TSB{...}");
    }

    nb::str PyTimeSeriesBundleInput::py_repr() const {
        return py_str();
    }

    nb::bool_ PyTimeSeriesBundleInput::all_valid() const {
        auto* tsb_meta = static_cast<const TSBTypeMeta*>(_meta);
        if (!tsb_meta) return nb::bool_(false);

        auto* coll_strategy = get_collection_strategy(view());

        // Check all fields for validity
        for (size_t i = 0; i < tsb_meta->fields.size(); ++i) {
            bool field_valid = false;

            if (has_unpeered_children(coll_strategy)) {
                // Unpeered mode: check child strategy
                auto* child_strategy = coll_strategy->child(i);
                field_valid = child_strategy && child_strategy->has_value();
            } else {
                // Peered mode: navigate into bound output's view to check each field
                auto input_view = view();
                if (input_view.valid()) {
                    auto* strategy = input_view.source();
                    if (strategy) {
                        auto* bound = strategy->bound_output();
                        if (bound) {
                            auto output_view = bound->view();
                            auto field_view = output_view.field(i);
                            field_valid = field_view.valid() && field_view.has_value();
                        }
                    }
                }
            }

            if (!field_valid) {
                return nb::bool_(false);
            }
        }
        return nb::bool_(true);
    }

    nb::bool_ PyTimeSeriesBundleInput::has_peer() const {
        // has_peer is True if the TSB input is bound directly to a TSB output (peered mode)
        // Peered: CollectionAccessStrategy has bound_output set to the source TSB output
        // Unpeered: Each child has its own separate binding (bound via from_ts)

        // Check the view's source strategy
        auto v = view();
        if (!v.valid()) {
            // Fallback: check via input() if available
            return nb::bool_(input() && input()->bound());
        }

        auto* source = v.source();
        if (!source) {
            return nb::bool_(false);
        }

        // If the source strategy itself has a bound_output, it's peered
        // (the strategy delegates to the bound TSB output for value access)
        if (source->bound_output()) {
            return nb::bool_(true);  // Peered
        }

        // If it's a CollectionAccessStrategy but has no bound_output,
        // it means child strategies are individually bound (unpeered/from_ts)
        return nb::bool_(false);
    }

    nb::object PyTimeSeriesBundleInput::as_schema() const {
        // as_schema just returns self - it's for IDE autocompletion
        return nb::cast(this);
    }

    void tsb_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesBundleOutput, PyTimeSeriesOutput>(m, "TimeSeriesBundleOutput")
            .def("__getitem__", &PyTimeSeriesBundleOutput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleOutput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleOutput::iter)
            .def("__len__", &PyTimeSeriesBundleOutput::len)
            .def("__contains__", &PyTimeSeriesBundleOutput::contains)
            // Override value property to use bundle-aware set_value
            .def_prop_rw("value",
                [](const PyTimeSeriesBundleOutput& self) { return self.PyTimeSeriesOutput::value(); },
                &PyTimeSeriesBundleOutput::set_value,
                nb::arg("value").none())
            // Override delta_value to return dict of modified fields' deltas
            .def_prop_ro("delta_value", &PyTimeSeriesBundleOutput::delta_value)
            .def("keys", &PyTimeSeriesBundleOutput::keys)
            .def("values", &PyTimeSeriesBundleOutput::values)
            .def("items", &PyTimeSeriesBundleOutput::items)
            .def("valid_keys", &PyTimeSeriesBundleOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleOutput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleOutput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleOutput::modified_items)
            .def_prop_ro("empty", &PyTimeSeriesBundleOutput::empty)
            .def_prop_ro("all_valid", &PyTimeSeriesBundleOutput::all_valid)
            .def("clear", &PyTimeSeriesBundleOutput::clear)
            .def("__str__", &PyTimeSeriesBundleOutput::py_str)
            .def("__repr__", &PyTimeSeriesBundleOutput::py_repr);

        nb::class_<PyTimeSeriesBundleInput, PyTimeSeriesInput>(m, "TimeSeriesBundleInput")
            .def("__getitem__", &PyTimeSeriesBundleInput::get_item)
            .def("__getattr__", &PyTimeSeriesBundleInput::get_attr)
            .def("__iter__", &PyTimeSeriesBundleInput::iter)
            .def("__len__", &PyTimeSeriesBundleInput::len)
            .def("__contains__", &PyTimeSeriesBundleInput::contains)
            // Override value and delta_value to use bundle-specific logic
            .def_prop_ro("value", &PyTimeSeriesBundleInput::value)
            .def_prop_ro("delta_value", &PyTimeSeriesBundleInput::delta_value)
            .def("keys", &PyTimeSeriesBundleInput::keys)
            .def("values", &PyTimeSeriesBundleInput::values)
            .def("items", &PyTimeSeriesBundleInput::items)
            .def("valid_keys", &PyTimeSeriesBundleInput::valid_keys)
            .def("valid_values", &PyTimeSeriesBundleInput::valid_values)
            .def("valid_items", &PyTimeSeriesBundleInput::valid_items)
            .def("modified_keys", &PyTimeSeriesBundleInput::modified_keys)
            .def("modified_values", &PyTimeSeriesBundleInput::modified_values)
            .def("modified_items", &PyTimeSeriesBundleInput::modified_items)
            .def_prop_ro("empty", &PyTimeSeriesBundleInput::empty)
            .def_prop_ro("all_valid", &PyTimeSeriesBundleInput::all_valid)
            .def_prop_ro("has_peer", &PyTimeSeriesBundleInput::has_peer)
            .def_prop_ro("as_schema", &PyTimeSeriesBundleInput::as_schema)
            .def("__str__", &PyTimeSeriesBundleInput::py_str)
            .def("__repr__", &PyTimeSeriesBundleInput::py_repr);
    }

}  // namespace hgraph
