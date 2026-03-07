#include <hgraph/types/time_series/ts_value.h>

#include <hgraph/types/time_series/ts_meta_schema_cache.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series/ts_view.h>

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

const TSMeta* meta_at_path(const TSMeta* root, const std::vector<size_t>& indices) {
    const TSMeta* meta = root;
    for (size_t index : indices) {
        while (meta != nullptr && meta->kind == TSKind::REF) {
            meta = meta->element_ts();
        }

        if (meta == nullptr) {
            return nullptr;
        }

        switch (meta->kind) {
            case TSKind::TSB:
                if (meta->fields() == nullptr || index >= meta->field_count()) {
                    return nullptr;
                }
                meta = meta->fields()[index].ts_type;
                break;
            case TSKind::TSL:
            case TSKind::TSD:
                meta = meta->element_ts();
                break;
            default:
                return nullptr;
        }
    }
    return meta;
}

}  // namespace

TSValue::TSValue(const TSMeta* meta) {
    const value::TypeMeta* link_schema = meta != nullptr ? meta->link_schema() : nullptr;
    if (meta != nullptr && link_schema == nullptr) {
        link_schema = TSMetaSchemaCache::instance().get(meta).link_schema;
    }
    initialize(meta, link_schema, false);
}

TSValue::TSValue(const TSMeta* meta, const value::TypeMeta* input_link_schema) {
    initialize(meta, input_link_schema, true);
}

TSValue::~TSValue() {
    if (python_value_cache_.empty()) {
        return;
    }

    if (Py_IsInitialized() != 0) {
        nb::gil_scoped_acquire gil;
        python_value_cache_.clear_subtree();
    } else {
        python_value_cache_.abandon_subtree();
    }
}

void TSValue::initialize(const TSMeta* meta, const value::TypeMeta* link_schema, bool uses_link_target) {
    meta_ = meta;
    uses_link_target_ = uses_link_target;
    python_value_cache_ = PythonValueCacheNode(meta_);

    if (meta_ == nullptr) {
        return;
    }

    const auto& schema_cache = TSMetaSchemaCache::instance().get(meta_);

    // Use schema-cache value schema as runtime storage shape. For TSW this is
    // the window container (cyclic buffer / queue), while value_type remains
    // the element scalar type in TSMeta.
    const value::TypeMeta* value_schema =
        schema_cache.value_schema != nullptr ? schema_cache.value_schema : meta_->value_schema();
    const value::TypeMeta* time_schema =
        meta_->time_schema() != nullptr ? meta_->time_schema() : schema_cache.time_schema;
    const value::TypeMeta* observer_schema =
        meta_->observer_schema() != nullptr ? meta_->observer_schema() : schema_cache.observer_schema;
    const value::TypeMeta* delta_schema =
        meta_->delta_value_schema() != nullptr ? meta_->delta_value_schema() : schema_cache.delta_schema;
    const value::TypeMeta* meta_link_schema =
        meta_->link_schema() != nullptr ? meta_->link_schema() : schema_cache.link_schema;

    if (value_schema != nullptr) {
        value_ = value::Value(value_schema);
    }
    if (time_schema != nullptr) {
        time_ = value::Value(time_schema);
    }
    if (observer_schema != nullptr) {
        observer_ = value::Value(observer_schema);
    }
    if (delta_schema != nullptr) {
        delta_value_ = value::Value(delta_schema);
    }

    const value::TypeMeta* resolved_link_schema = link_schema != nullptr ? link_schema : meta_link_schema;
    if (resolved_link_schema != nullptr) {
        link_ = value::Value(resolved_link_schema);
        link_.emplace();
    }
}

value::View TSValue::value_view() const {
    return to_const_view_or_empty(value_);
}

value::ValueView TSValue::value_view_mut() {
    return to_mut_view_or_empty(value_);
}

value::View TSValue::time_view() const {
    return to_const_view_or_empty(time_);
}

value::ValueView TSValue::time_view_mut() {
    return to_mut_view_or_empty(time_);
}

value::View TSValue::observer_view() const {
    return to_const_view_or_empty(observer_);
}

value::ValueView TSValue::observer_view_mut() {
    return to_mut_view_or_empty(observer_);
}

value::View TSValue::delta_value_view() const {
    return to_const_view_or_empty(delta_value_);
}

value::ValueView TSValue::delta_value_view_mut() {
    return to_mut_view_or_empty(delta_value_);
}

value::View TSValue::link_view() const {
    return to_const_view_or_empty(link_);
}

value::ValueView TSValue::link_view_mut() {
    return to_mut_view_or_empty(link_);
}

engine_time_t TSValue::last_modified_time() const {
    ViewData vd = make_view_data();
    if (vd.ops == nullptr) {
        return MIN_DT;
    }
    return vd.ops->last_modified_time(vd);
}

bool TSValue::modified(engine_time_t current_time) const {
    ViewData vd = make_view_data();
    if (vd.ops == nullptr) {
        return false;
    }
    return vd.ops->modified(vd, current_time);
}

bool TSValue::valid() const {
    ViewData vd = make_view_data();
    if (vd.ops == nullptr) {
        return false;
    }
    return vd.ops->valid(vd);
}

bool TSValue::all_valid(engine_time_t current_time) const {
    (void)current_time;

    ViewData vd = make_view_data();
    if (vd.ops == nullptr) {
        return false;
    }
    return vd.ops->all_valid(vd);
}

bool TSValue::has_delta() const {
    return delta_value_.schema() != nullptr;
}

ViewData TSValue::make_view_data(ShortPath path, const engine_time_t* engine_time_ptr) const {
    ViewData vd;
    vd.path = std::move(path);
    vd.engine_time_ptr = engine_time_ptr;
    vd.value_data = value_.schema() != nullptr ? const_cast<value::Value*>(&value_) : nullptr;
    vd.time_data = time_.schema() != nullptr ? const_cast<value::Value*>(&time_) : nullptr;
    vd.observer_data = observer_.schema() != nullptr ? const_cast<value::Value*>(&observer_) : nullptr;
    vd.delta_data = delta_value_.schema() != nullptr ? const_cast<value::Value*>(&delta_value_) : nullptr;
    vd.link_data = link_.schema() != nullptr ? const_cast<value::Value*>(&link_) : nullptr;
    vd.python_value_cache_data = const_cast<PythonValueCacheNode*>(&python_value_cache_);
    vd.python_value_cache_slot = nullptr;
    vd.link_observer_registry = link_observer_registry_;
    vd.sampled = false;
    vd.uses_link_target = uses_link_target_;
    vd.ops = get_ts_ops(meta_at_path(meta_, vd.path.indices));
    vd.meta = meta_;

    debug_assert_view_data_consistency(vd);
    return vd;
}

TSView TSValue::ts_view(const engine_time_t* engine_time_ptr, ShortPath path) const {
    return TSView(make_view_data(std::move(path), engine_time_ptr), engine_time_ptr);
}

}  // namespace hgraph
