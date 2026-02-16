#include <hgraph/types/time_series/ts_input.h>

#include <hgraph/types/node.h>
#include <hgraph/types/time_series/ts_meta_schema_cache.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_output.h>

#include <algorithm>
#include <optional>

namespace hgraph {

namespace {

value::View to_const_view_or_empty(const value::Value& value) {
    if (value.schema() == nullptr || !value.has_value()) {
        return {};
    }
    return value.view();
}

value::ValueView to_mut_view_or_empty(value::Value& value) {
    if (value.schema() == nullptr || !value.has_value()) {
        return {};
    }
    return value.view();
}

std::optional<value::View> navigate_active_view(const value::View& root, const TSMeta* meta,
                                                const std::vector<size_t>& indices) {
    value::View current = root;
    const TSMeta* current_meta = meta;

    for (size_t index : indices) {
        if (!current.valid() || current_meta == nullptr || !current.is_tuple()) {
            return std::nullopt;
        }

        value::TupleView tuple = current.as_tuple();
        switch (current_meta->kind) {
            case TSKind::TSB:
                if (current_meta->fields() == nullptr || index >= current_meta->field_count() || index + 1 >= tuple.size()) {
                    return std::nullopt;
                }
                current = tuple.at(index + 1);
                current_meta = current_meta->fields()[index].ts_type;
                break;

            case TSKind::TSL:
            case TSKind::TSD:
                if (tuple.size() < 2) {
                    return std::nullopt;
                }
                {
                    value::View children = tuple.at(1);
                    if (!children.valid() || !children.is_list()) {
                        return std::nullopt;
                    }
                    value::ListView list = children.as_list();
                    if (index >= list.size()) {
                        return std::nullopt;
                    }
                    current = list.at(index);
                    current_meta = current_meta->element_ts();
                }
                break;

            default:
                return std::nullopt;
        }
    }

    return current;
}

std::optional<value::ValueView> navigate_active_view_mut(value::ValueView root, const TSMeta* meta,
                                                         const std::vector<size_t>& indices) {
    value::ValueView current = root;
    const TSMeta* current_meta = meta;

    for (size_t index : indices) {
        if (!current.valid() || current_meta == nullptr || !current.is_tuple()) {
            return std::nullopt;
        }

        value::TupleView tuple = current.as_tuple();
        switch (current_meta->kind) {
            case TSKind::TSB:
                if (current_meta->fields() == nullptr || index >= current_meta->field_count() || index + 1 >= tuple.size()) {
                    return std::nullopt;
                }
                current = tuple.at(index + 1);
                current_meta = current_meta->fields()[index].ts_type;
                break;

            case TSKind::TSL:
            case TSKind::TSD:
                if (tuple.size() < 2) {
                    return std::nullopt;
                }
                {
                    value::ValueView children = tuple.at(1);
                    if (!children.valid() || !children.is_list()) {
                        return std::nullopt;
                    }
                    value::ListView list = children.as_list();
                    if (index >= list.size()) {
                        return std::nullopt;
                    }
                    current = list.at(index);
                    current_meta = current_meta->element_ts();
                }
                break;

            default:
                return std::nullopt;
        }
    }

    return current;
}

bool active_flag(const value::View& active_view) {
    if (!active_view.valid()) {
        return false;
    }
    if (active_view.is_scalar_type<bool>()) {
        return active_view.as<bool>();
    }
    if (active_view.is_tuple()) {
        value::TupleView tuple = active_view.as_tuple();
        if (tuple.size() > 0) {
            value::View head = tuple.at(0);
            if (head.valid() && head.is_scalar_type<bool>()) {
                return head.as<bool>();
            }
        }
    }
    return false;
}

bool any_active(const value::View& active_view) {
    if (!active_view.valid()) {
        return false;
    }
    if (active_view.is_scalar_type<bool>()) {
        return active_view.as<bool>();
    }
    if (active_view.is_tuple()) {
        value::TupleView tuple = active_view.as_tuple();
        const size_t n = tuple.size();
        for (size_t i = 0; i < n; ++i) {
            if (any_active(tuple.at(i))) {
                return true;
            }
        }
        return false;
    }
    if (active_view.is_list()) {
        value::ListView list = active_view.as_list();
        const size_t n = list.size();
        for (size_t i = 0; i < n; ++i) {
            if (any_active(list.at(i))) {
                return true;
            }
        }
    }
    return false;
}

void bind_static_container_recursive(const TSMeta* meta, TSView input_view, TSView output_view) {
    if (meta == nullptr || !input_view || !output_view) {
        return;
    }

    const bool bind_parent =
        !(meta->kind == TSKind::TSL &&
          meta->fixed_size() > 0 &&
          meta->element_ts() != nullptr &&
          meta->element_ts()->kind == TSKind::REF);

    if (bind_parent) {
        input_view.bind(output_view);
    }

    switch (meta->kind) {
        case TSKind::TSB:
            if (meta->fields() == nullptr) {
                return;
            }
            for (size_t i = 0; i < meta->field_count(); ++i) {
                bind_static_container_recursive(
                    meta->fields()[i].ts_type,
                    input_view.child_at(i),
                    output_view.child_at(i));
            }
            return;

        case TSKind::TSL:
            if (meta->fixed_size() == 0) {
                return;
            }
            for (size_t i = 0; i < meta->fixed_size(); ++i) {
                bind_static_container_recursive(
                    meta->element_ts(),
                    input_view.child_at(i),
                    output_view.child_at(i));
            }
            return;

        default:
            return;
    }
}

void unbind_links_recursive(const TSMeta* meta, TSView input_view) {
    input_view.unbind();

    if (meta == nullptr) {
        return;
    }

    switch (meta->kind) {
        case TSKind::TSB:
            if (meta->fields() == nullptr) {
                return;
            }
            for (size_t i = 0; i < meta->field_count(); ++i) {
                unbind_links_recursive(meta->fields()[i].ts_type, input_view.child_at(i));
            }
            return;

        case TSKind::TSL:
            if (meta->fixed_size() == 0) {
                return;
            }
            for (size_t i = 0; i < meta->fixed_size(); ++i) {
                unbind_links_recursive(meta->element_ts(), input_view.child_at(i));
            }
            return;

        default:
            return;
    }
}

}  // namespace

TSInput::TSInput(const TSMeta* meta, node_ptr owning_node)
    : value_(meta, meta != nullptr ? TSMetaSchemaCache::instance().get(meta).input_link_schema : nullptr),
      meta_(meta),
      owning_node_(owning_node) {
    if (meta_ == nullptr) {
        return;
    }

    const auto& schemas = TSMetaSchemaCache::instance().get(meta_);
    if (schemas.active_schema != nullptr) {
        active_ = value::Value(schemas.active_schema);
        active_.emplace();
    }
}

TSView TSInput::view(engine_time_t current_time) {
    TSView out(value_, current_time, root_path());
    out.view_data().uses_link_target = true;
    return out;
}

TSView TSInput::view(engine_time_t current_time, const TSMeta* schema) {
    (void)schema;
    return view(current_time);
}

TSInputView TSInput::input_view(engine_time_t current_time) {
    return TSInputView(this, view(current_time));
}

TSInputView TSInput::input_view(engine_time_t current_time, const TSMeta* schema) {
    return TSInputView(this, view(current_time, schema));
}

void TSInput::bind(TSOutput& output, engine_time_t current_time) {
    TSView input_view = view(current_time);
    TSView native_output_view = output.view(current_time);
    const bool output_is_ref = native_output_view.ts_meta() != nullptr &&
                               native_output_view.ts_meta()->kind == TSKind::REF;
    TSView output_view = ((meta_ != nullptr && meta_->kind == TSKind::REF) || output_is_ref)
                             ? native_output_view
                             : output.view(current_time, meta_);

    if (meta_ != nullptr && !output_is_ref &&
        (meta_->kind == TSKind::TSB || (meta_->kind == TSKind::TSL && meta_->fixed_size() > 0))) {
        bind_static_container_recursive(meta_, input_view, output_view);
    } else {
        input_view.bind(output_view);
    }

    if (active_root_) {
        value::ValueView av = active_view_mut();
        if (av.valid()) {
            set_active_recursive(input_view, av, true);
        }
    }
}

void TSInput::unbind(engine_time_t current_time) {
    TSView input_view = view(current_time);

    if (active_root_) {
        value::ValueView av = active_view_mut();
        if (av.valid()) {
            set_active_recursive(input_view, av, false);
        }
    }

    unbind_links_recursive(meta_, input_view);
}

void TSInput::set_active(bool active) {
    active_root_ = active;

    value::ValueView av = active_view_mut();
    if (!av.valid()) {
        return;
    }

    TSView root = view(MIN_DT);
    set_active_recursive(root, av, active);
}

void TSInput::set_active(const TSView& ts_view, bool active) {
    if (!ts_view) {
        return;
    }

    if (ts_view.short_path().indices.empty()) {
        set_active(active);
        return;
    }

    value::ValueView av = active_view_mut();
    if (!av.valid()) {
        return;
    }

    auto node_active = navigate_active_view_mut(av, meta_, ts_view.short_path().indices);
    if (!node_active.has_value()) {
        return;
    }

    set_active_recursive(ts_view, *node_active, active);

    if (active) {
        active_root_ = true;
    } else {
        active_root_ = any_active(active_view());
    }
}

bool TSInput::active(const TSView& ts_view) const {
    if (!ts_view) {
        return false;
    }

    if (ts_view.short_path().indices.empty()) {
        return active_root_;
    }

    value::View av = active_view();
    if (!av.valid()) {
        return false;
    }

    auto node_active = navigate_active_view(av, meta_, ts_view.short_path().indices);
    if (!node_active.has_value()) {
        return false;
    }
    return active_flag(*node_active);
}

value::View TSInput::active_view() const {
    return to_const_view_or_empty(active_);
}

value::ValueView TSInput::active_view_mut() {
    return to_mut_view_or_empty(active_);
}

void TSInput::notify(engine_time_t et) {
    if (owning_node_ != nullptr) {
        owning_node_->notify(et);
    }
}

void TSInput::set_active_recursive(const TSView& ts_view, value::ValueView active_view, bool active) {
    if (!ts_view || !active_view.valid()) {
        return;
    }

    ViewData vd = ts_view.view_data();
    if (vd.ops == nullptr) {
        return;
    }

    vd.ops->set_active(vd, active_view, active, this);

    const TSMeta* meta = ts_view.ts_meta();
    if (meta == nullptr) {
        return;
    }

    const size_t child_count = ts_view.size();
    if (child_count == 0 || !active_view.is_tuple()) {
        return;
    }

    value::TupleView tuple_view = active_view.as_tuple();

    if (meta->kind == TSKind::TSB) {
        for (size_t i = 0; i < child_count; ++i) {
            if (i + 1 >= tuple_view.size()) {
                break;
            }

            TSView child = ts_view.child_at(i);
            value::ValueView child_active = tuple_view.at(i + 1);
            set_active_recursive(child, child_active, active);
        }
        return;
    }

    if (meta->kind == TSKind::TSL || meta->kind == TSKind::TSD) {
        if (tuple_view.size() < 2) {
            return;
        }

        value::ValueView child_collection = tuple_view.at(1);
        if (!child_collection.valid() || !child_collection.is_list()) {
            return;
        }

        value::ListView list_view = child_collection.as_list();
        const size_t n = std::min(child_count, list_view.size());
        for (size_t i = 0; i < n; ++i) {
            TSView child = ts_view.child_at(i);
            value::ValueView child_active = list_view.at(i);
            set_active_recursive(child, child_active, active);
        }
    }
}

}  // namespace hgraph
