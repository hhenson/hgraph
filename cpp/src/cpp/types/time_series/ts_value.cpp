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

}  // namespace

TSValue::TSValue(const TSMeta* meta) {
    const value::TypeMeta* link_schema = nullptr;
    if (meta != nullptr) {
        link_schema = TSMetaSchemaCache::instance().get(meta).link_schema;
    }
    initialize(meta, link_schema, false);
}

TSValue::TSValue(const TSMeta* meta, const value::TypeMeta* input_link_schema) {
    initialize(meta, input_link_schema, true);
}

void TSValue::initialize(const TSMeta* meta, const value::TypeMeta* link_schema, bool uses_link_target) {
    meta_ = meta;
    uses_link_target_ = uses_link_target;

    if (meta_ == nullptr) {
        return;
    }

    const auto& schemas = TSMetaSchemaCache::instance().get(meta_);

    if (schemas.value_schema != nullptr) {
        value_ = value::Value(schemas.value_schema);
    }
    if (schemas.time_schema != nullptr) {
        time_ = value::Value(schemas.time_schema);
    }
    if (schemas.observer_schema != nullptr) {
        observer_ = value::Value(schemas.observer_schema);
    }
    if (schemas.delta_schema != nullptr) {
        delta_value_ = value::Value(schemas.delta_schema);
    }

    const value::TypeMeta* resolved_link_schema = link_schema != nullptr ? link_schema : schemas.link_schema;
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

ViewData TSValue::make_view_data(ShortPath path) const {
    ViewData vd;
    vd.path = std::move(path);
    vd.value_data = value_.schema() != nullptr ? const_cast<value::Value*>(&value_) : nullptr;
    vd.time_data = time_.schema() != nullptr ? const_cast<value::Value*>(&time_) : nullptr;
    vd.observer_data = observer_.schema() != nullptr ? const_cast<value::Value*>(&observer_) : nullptr;
    vd.delta_data = delta_value_.schema() != nullptr ? const_cast<value::Value*>(&delta_value_) : nullptr;
    vd.link_data = link_.schema() != nullptr ? const_cast<value::Value*>(&link_) : nullptr;
    vd.link_observer_registry = link_observer_registry_;
    vd.sampled = false;
    vd.uses_link_target = uses_link_target_;
    vd.ops = get_ts_ops(meta_);
    vd.meta = meta_;

    debug_assert_view_data_consistency(vd);
    return vd;
}

TSView TSValue::ts_view(engine_time_t current_time, ShortPath path) const {
    return TSView(make_view_data(std::move(path)), current_time);
}

}  // namespace hgraph
