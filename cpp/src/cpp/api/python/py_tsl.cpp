#include <hgraph/api/python/py_tsl.h>
#include <hgraph/api/python/py_ts.h>
#include <hgraph/api/python/py_tsb.h>
#include <hgraph/api/python/py_tss.h>
#include <hgraph/api/python/py_tsd.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/access_strategy.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/value/python_conversion.h>

namespace hgraph
{
    // Helper to create output wrapper from a view (for element access)
    static nb::object create_tsl_output_wrapper_from_view(
            const node_s_ptr& node,
            value::TSView view,
            const TSMeta* meta) {
        if (!view.valid()) return nb::none();

        // For list elements, we pass nullptr for TSOutput* since we're wrapping a view
        if (!meta) {
            return nb::cast(PyTimeSeriesOutput(node, std::move(view), nullptr, meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetOutput(node, std::move(view), nullptr, meta));
            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictOutput(node, std::move(view), nullptr, meta));
            case TSKind::REF:
            default:
                // REF and other types use the base wrapper
                return nb::cast(PyTimeSeriesOutput(node, std::move(view), nullptr, meta));
        }
    }

    // Helper to create input wrapper from a TSInputView (for element access)
    static nb::object create_tsl_input_wrapper_from_view(
            const node_s_ptr& node,
            ts::TSInputView view,
            const TSMeta* meta) {
        if (!view.valid()) return nb::none();

        if (!meta) {
            return nb::cast(PyTimeSeriesInput(node, std::move(view), meta));
        }

        switch (meta->ts_kind) {
            case TSKind::TS:
                return nb::cast(PyTimeSeriesValueInput(node, std::move(view), nullptr, meta));
            case TSKind::TSB:
                return nb::cast(PyTimeSeriesBundleInput(node, std::move(view), nullptr, meta));
            case TSKind::TSL:
                return nb::cast(PyTimeSeriesListInput(node, std::move(view), nullptr, meta));
            case TSKind::TSS:
                return nb::cast(PyTimeSeriesSetInput(node, std::move(view), nullptr, meta));
            case TSKind::TSD:
                return nb::cast(PyTimeSeriesDictInput(node, std::move(view), nullptr, meta));
            case TSKind::REF:
            default:
                // REF and other types use the base wrapper
                return nb::cast(PyTimeSeriesInput(node, std::move(view), meta));
        }
    }

    // Helper to wrap an input from a child access strategy
    static nb::object wrap_tsl_input_from_strategy(
            const node_s_ptr& node,
            ts::AccessStrategy* child_strategy,
            const TSMeta* element_meta) {
        if (!child_strategy) return nb::none();

        // Create a TSInputView from the child strategy
        ts::TSInputView element_view(child_strategy, element_meta);
        return create_tsl_input_wrapper_from_view(node, std::move(element_view), element_meta);
    }

    // PyTimeSeriesListOutput implementations

    nb::object PyTimeSeriesListOutput::value() const {
        // TSL.value returns a tuple of element values, not the raw data
        // Python: tuple(ts.value if ts.valid else None for ts in self._ts_values)
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::none();

        auto v = view();
        size_t list_size = v.list_size();
        if (list_size == 0 && tsl_meta->size > 0) {
            list_size = static_cast<size_t>(tsl_meta->size);
        }

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.has_value()) {
                // Get the element's value
                auto elem_vv = elem_view.value_view();
                if (elem_vv.valid()) {
                    auto* elem_schema = elem_view.value_schema();
                    result.append(value::value_to_python(elem_vv.data(), elem_schema));
                } else {
                    result.append(nb::none());
                }
            } else {
                result.append(nb::none());
            }
        }
        return nb::tuple(result);
    }

    nb::object PyTimeSeriesListOutput::get_item(const nb::handle &key) const {
        // Get the list metadata to access element type
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::none();

        // Get the index
        size_t index = nb::cast<size_t>(key);
        auto v = view();
        size_t list_size = v.list_size();
        if (index >= list_size) {
            throw std::out_of_range("TSL index out of range");
        }

        // Navigate to element via view
        auto& mutable_view = const_cast<value::TSView&>(_view);
        auto elem_view = mutable_view.element(index);
        if (elem_view.valid()) {
            return create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
        }
        return nb::none();
    }

    nb::object PyTimeSeriesListOutput::iter() const {
        return nb::iter(values());
    }

    nb::object PyTimeSeriesListOutput::keys() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        nb::list result;
        for (size_t i = 0; i < list_size; ++i) {
            result.append(nb::int_(i));
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::values() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid()) {
                result.append(create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::items() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid()) {
                nb::object py_value = create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
                result.append(nb::make_tuple(nb::int_(i), py_value));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::valid_keys() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.has_value()) {
                result.append(nb::int_(i));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::valid_values() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.has_value()) {
                result.append(create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::valid_items() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.has_value()) {
                nb::object py_value = create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
                result.append(nb::make_tuple(nb::int_(i), py_value));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::modified_keys() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.modified_at(eval_time)) {
                result.append(nb::int_(i));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::modified_values() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.modified_at(eval_time)) {
                result.append(create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type));
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListOutput::modified_items() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = v.list_size();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto& mutable_view = const_cast<value::TSView&>(_view);
        for (size_t i = 0; i < list_size; ++i) {
            auto elem_view = mutable_view.element(i);
            if (elem_view.valid() && elem_view.modified_at(eval_time)) {
                nb::object py_value = create_tsl_output_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
                result.append(nb::make_tuple(nb::int_(i), py_value));
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesListOutput::len() const {
        return nb::int_(view().list_size());
    }

    bool PyTimeSeriesListOutput::empty() const {
        return view().list_size() == 0;
    }

    void PyTimeSeriesListOutput::clear() {
        // TODO: Implement via view
    }

    nb::str PyTimeSeriesListOutput::py_str() const {
        return nb::str("TSL[...]");
    }

    nb::str PyTimeSeriesListOutput::py_repr() const {
        return py_str();
    }

    // Helper to check if the view's source is a CollectionAccessStrategy with children
    static ts::CollectionAccessStrategy* get_tsl_collection_strategy(const ts::TSInputView& view) {
        if (!view.valid()) return nullptr;
        auto* source = view.source();
        if (!source) return nullptr;
        return dynamic_cast<ts::CollectionAccessStrategy*>(source);
    }

    static bool has_tsl_unpeered_children(ts::CollectionAccessStrategy* coll) {
        return coll && coll->child_count() > 0;
    }

    // PyTimeSeriesListInput implementations

    nb::object PyTimeSeriesListInput::value() const {
        // TSL.value returns a tuple of element values, not the raw data
        // Python: tuple(ts.value if ts.valid else None for ts in self.values())
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::none();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;

        // Check for unpeered mode first
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->has_value()) {
                    // Create a TSInputView from the child strategy to access value methods
                    ts::TSInputView child_view(child_strategy, tsl_meta->element_ts_type);
                    auto child_vv = child_view.value_view();
                    if (child_vv.valid()) {
                        auto* schema = child_view.value_schema();
                        result.append(value::value_to_python(child_vv.data(), schema));
                    } else {
                        result.append(nb::none());
                    }
                } else {
                    result.append(nb::none());
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.has_value()) {
                    auto elem_vv = elem_view.value_view();
                    if (elem_vv.valid()) {
                        auto* elem_schema = elem_view.value_schema();
                        result.append(value::value_to_python(elem_vv.data(), elem_schema));
                    } else {
                        result.append(nb::none());
                    }
                } else {
                    result.append(nb::none());
                }
            }
        }
        return nb::tuple(result);
    }

    nb::object PyTimeSeriesListInput::get_item(const nb::handle &key) const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::none();

        size_t index = nb::cast<size_t>(key);
        // Use metadata size - the TSL's fixed size from the type definition
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : 0;
        auto v = view();
        // Fall back to view's list_size if metadata size is not set
        if (list_size == 0) {
            list_size = v.list_size();
        }
        if (index >= list_size) {
            throw std::out_of_range("TSL index out of range");
        }

        // Check for unpeered mode first (CollectionAccessStrategy with children)
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            // Unpeered mode: get child strategy
            auto* child_strategy = coll_strategy->child(index);
            if (child_strategy) {
                return wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type);
            }
        }

        // Navigate via view (works for both peered and unpeered modes)
        auto elem_view = v.element(index);
        if (elem_view.valid()) {
            return create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
        }

        return nb::none();
    }

    nb::object PyTimeSeriesListInput::iter() const {
        return nb::iter(values());
    }

    nb::object PyTimeSeriesListInput::keys() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;
        for (size_t i = 0; i < list_size; ++i) {
            result.append(nb::int_(i));
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::values() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;

        // Check for unpeered mode first
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy) {
                    result.append(wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid()) {
                    result.append(create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::items() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;

        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy) {
                    nb::object py_value = wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type);
                    result.append(nb::make_tuple(nb::int_(i), py_value));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid()) {
                    nb::object py_value = create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
                    result.append(nb::make_tuple(nb::int_(i), py_value));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::valid_keys() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->has_value()) {
                    result.append(nb::int_(i));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.has_value()) {
                    result.append(nb::int_(i));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::valid_values() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->has_value()) {
                    result.append(wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.has_value()) {
                    result.append(create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::valid_items() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        nb::list result;
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->has_value()) {
                    nb::object py_value = wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type);
                    result.append(nb::make_tuple(nb::int_(i), py_value));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.has_value()) {
                    nb::object py_value = create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
                    result.append(nb::make_tuple(nb::int_(i), py_value));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::modified_keys() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->modified_at(eval_time)) {
                    result.append(nb::int_(i));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.modified_at(eval_time)) {
                    result.append(nb::int_(i));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::modified_values() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->modified_at(eval_time)) {
                    result.append(wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.modified_at(eval_time)) {
                    result.append(create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type));
                }
            }
        }
        return result;
    }

    nb::object PyTimeSeriesListInput::modified_items() const {
        auto* tsl_meta = dynamic_cast<const TSLTypeMeta*>(_meta);
        if (!tsl_meta) return nb::list();

        auto v = view();
        size_t list_size = tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : v.list_size();

        engine_time_t eval_time = _node && _node->graph() ? _node->graph()->evaluation_time() : MIN_DT;
        nb::list result;
        auto* coll_strategy = get_tsl_collection_strategy(v);
        if (has_tsl_unpeered_children(coll_strategy)) {
            for (size_t i = 0; i < list_size; ++i) {
                auto* child_strategy = coll_strategy->child(i);
                if (child_strategy && child_strategy->modified_at(eval_time)) {
                    nb::object py_value = wrap_tsl_input_from_strategy(_node, child_strategy, tsl_meta->element_ts_type);
                    result.append(nb::make_tuple(nb::int_(i), py_value));
                }
            }
        } else {
            for (size_t i = 0; i < list_size; ++i) {
                auto elem_view = v.element(i);
                if (elem_view.valid() && elem_view.modified_at(eval_time)) {
                    nb::object py_value = create_tsl_input_wrapper_from_view(_node, std::move(elem_view), tsl_meta->element_ts_type);
                    result.append(nb::make_tuple(nb::int_(i), py_value));
                }
            }
        }
        return result;
    }

    nb::int_ PyTimeSeriesListInput::len() const {
        return nb::int_(view().list_size());
    }

    bool PyTimeSeriesListInput::empty() const {
        return view().list_size() == 0;
    }

    nb::str PyTimeSeriesListInput::py_str() const {
        return nb::str("TSL[...]");
    }

    nb::str PyTimeSeriesListInput::py_repr() const {
        return py_str();
    }

    void tsl_register_with_nanobind(nb::module_ &m) {
        nb::class_<PyTimeSeriesListOutput, PyTimeSeriesOutput>(m, "TimeSeriesListOutput")
            .def("__getitem__", &PyTimeSeriesListOutput::get_item)
            .def("__iter__", &PyTimeSeriesListOutput::iter)
            .def("__len__", &PyTimeSeriesListOutput::len)
            .def_prop_ro("empty", &PyTimeSeriesListOutput::empty)
            .def("keys", &PyTimeSeriesListOutput::keys)
            .def("values", &PyTimeSeriesListOutput::values)
            .def("items", &PyTimeSeriesListOutput::items)
            .def("valid_keys", &PyTimeSeriesListOutput::valid_keys)
            .def("valid_values", &PyTimeSeriesListOutput::valid_values)
            .def("valid_items", &PyTimeSeriesListOutput::valid_items)
            .def("modified_keys", &PyTimeSeriesListOutput::modified_keys)
            .def("modified_values", &PyTimeSeriesListOutput::modified_values)
            .def("modified_items", &PyTimeSeriesListOutput::modified_items)
            .def("clear", &PyTimeSeriesListOutput::clear)
            .def("__str__", &PyTimeSeriesListOutput::py_str)
            .def("__repr__", &PyTimeSeriesListOutput::py_repr);

        nb::class_<PyTimeSeriesListInput, PyTimeSeriesInput>(m, "TimeSeriesListInput")
            .def("__getitem__", &PyTimeSeriesListInput::get_item)
            .def("__iter__", &PyTimeSeriesListInput::iter)
            .def("__len__", &PyTimeSeriesListInput::len)
            .def_prop_ro("empty", &PyTimeSeriesListInput::empty)
            .def("keys", &PyTimeSeriesListInput::keys)
            .def("values", &PyTimeSeriesListInput::values)
            .def("items", &PyTimeSeriesListInput::items)
            .def("valid_keys", &PyTimeSeriesListInput::valid_keys)
            .def("valid_values", &PyTimeSeriesListInput::valid_values)
            .def("valid_items", &PyTimeSeriesListInput::valid_items)
            .def("modified_keys", &PyTimeSeriesListInput::modified_keys)
            .def("modified_values", &PyTimeSeriesListInput::modified_values)
            .def("modified_items", &PyTimeSeriesListInput::modified_items)
            .def("__str__", &PyTimeSeriesListInput::py_str)
            .def("__repr__", &PyTimeSeriesListInput::py_repr);
    }

}  // namespace hgraph
