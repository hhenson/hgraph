//
// ts_view.cpp - TSView, TSMutableView, TSBView implementation
//

#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <stdexcept>

namespace hgraph {

// ============================================================================
// TSView Implementation
// ============================================================================

TSView::TSView(const void* data, const TSMeta* ts_meta) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(nullptr)
    , _tracking_view()
{}

TSView::TSView(const void* data, const TSMeta* ts_meta, const TSValue* container) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(container)
    , _tracking_view(container ? container->tracking().const_view() : value::ConstValueView())
{}

TSView::TSView(const void* data, const TSMeta* ts_meta, value::ConstValueView tracking_view) noexcept
    : _view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _ts_meta(ts_meta)
    , _container(nullptr)
    , _tracking_view(tracking_view)
{}

TSView::TSView(const TSValue& ts_value)
    : _view(ts_value.value().data(), ts_value.value_schema())
    , _ts_meta(ts_value.ts_meta())
    , _container(&ts_value)
    , _tracking_view(ts_value.tracking().const_view())
{}

bool TSView::valid() const noexcept {
    return _ts_meta != nullptr && _view.valid();
}

TSBView TSView::as_bundle() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_bundle() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSB) {
        throw std::runtime_error("TSView::as_bundle() called on non-bundle type");
    }
    return TSBView(_view.data(), static_cast<const TSBTypeMeta*>(_ts_meta));
}

TSLView TSView::as_list() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_list() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSL) {
        throw std::runtime_error("TSView::as_list() called on non-list type");
    }
    return TSLView(_view.data(), static_cast<const TSLTypeMeta*>(_ts_meta));
}

TSDView TSView::as_dict() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_dict() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSD) {
        throw std::runtime_error("TSView::as_dict() called on non-dict type");
    }
    return TSDView(_view.data(), static_cast<const TSDTypeMeta*>(_ts_meta));
}

TSSView TSView::as_set() const {
    if (!valid()) {
        throw std::runtime_error("TSView::as_set() called on invalid view");
    }
    if (_ts_meta->kind() != TSTypeKind::TSS) {
        throw std::runtime_error("TSView::as_set() called on non-set type");
    }
    return TSSView(_view.data(), static_cast<const TSSTypeMeta*>(_ts_meta));
}

nb::object TSView::to_python() const {
    if (!valid()) {
        return nb::none();
    }
    const value::TypeMeta* schema = _ts_meta->value_schema();
    // Phase 0 note: this conversion path calls `TypeMeta::ops` directly and therefore does not
    // participate in `value::Value` policy behavior (e.g. `WithPythonCache`).
    // See `ts_design_docs/Value_TSValue_MIGRATION_PLAN.md` Phase 0 checklist.
    // TODO: fix that as we should not be bypassing the type-erased behavior
    return schema->ops->to_python(_view.data(), schema);
}

bool TSView::ts_valid() const {
    // Use hierarchical tracking if available
    if (_tracking_view.valid()) {
        // For scalar types, tracking is a single engine_time_t
        // For containers, the root timestamp indicates overall validity
        engine_time_t time = _tracking_view.as<engine_time_t>();
        return time != MIN_DT;
    }
    // Fall back to container's root-level tracking
    if (_container) {
        return _container->ts_valid();
    }
    return valid();  // Fall back to structural validity
}

bool TSView::modified_at(engine_time_t time) const {
    // Use hierarchical tracking if available
    if (_tracking_view.valid()) {
        engine_time_t last_mod = _tracking_view.as<engine_time_t>();
        return last_mod == time;
    }
    // Fall back to container's root-level tracking
    if (_container) {
        return _container->modified_at(time);
    }
    return false;  // No tracking means no modification tracking
}

engine_time_t TSView::last_modified_time() const {
    // Use hierarchical tracking if available
    if (_tracking_view.valid()) {
        return _tracking_view.as<engine_time_t>();
    }
    // Fall back to container's root-level tracking
    if (_container) {
        return _container->last_modified_time();
    }
    return MIN_DT;  // No tracking means no modification tracking
}

Node* TSView::owning_node() const {
    if (!_container) {
        return nullptr;  // No container means no node ownership
    }
    return _container->owning_node();
}

// ============================================================================
// TSMutableView Implementation
// ============================================================================

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _mutable_container(nullptr)
    , _mutable_tracking_view()
{}

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta, TSValue* container) noexcept
    : TSView(data, ts_meta, container)
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _mutable_container(container)
    , _mutable_tracking_view(container ? container->tracking().view() : value::ValueView())
{}

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta, value::ValueView tracking_view) noexcept
    : TSView(data, ts_meta, value::ConstValueView(tracking_view.data(), tracking_view.schema()))
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
    , _mutable_container(nullptr)
    , _mutable_tracking_view(tracking_view)
{}

TSMutableView::TSMutableView(TSValue& ts_value)
    : TSView(ts_value)
    , _mutable_view(ts_value.value().data(), ts_value.value_schema())
    , _mutable_container(&ts_value)
    , _mutable_tracking_view(ts_value.tracking().view())
{}

void TSMutableView::copy_from(const TSView& source) {
    if (!valid() || !source.valid()) {
        throw std::runtime_error("TSMutableView::copy_from() called with invalid view");
    }
    // Use the underlying value schema to perform the copy
    const value::TypeMeta* schema = _ts_meta->value_schema();
    if (schema && schema->ops) {
        schema->ops->copy_assign(_mutable_view.data(), source.value_view().data(), schema);
    }
}

void TSMutableView::from_python(const nb::object& src) {
    if (!valid()) {
        throw std::runtime_error("TSMutableView::from_python() called on invalid view");
    }
    const value::TypeMeta* schema = _ts_meta->value_schema();
    if (schema && schema->ops) {
        // Phase 0 note: this bypasses `value::Value::from_python`, so it does not:
        // - update/invalidate any python cache policy
        // - update timestamps/validity (caller must define/perform `notify_modified(time)` semantics)
        // - trigger hierarchical observer notifications
        // See `ts_design_docs/Value_TSValue_MIGRATION_PLAN.md` Phase 0 checklist.
        // TODO: fix that as we should not be bypassing the type-erased behavior
        schema->ops->from_python(_mutable_view.data(), src, schema);
    }
}

void TSMutableView::notify_modified(engine_time_t time) {
    // Update hierarchical tracking if available
    if (_mutable_tracking_view.valid()) {
        _mutable_tracking_view.as<engine_time_t>() = time;
    }
    // Also notify container for root-level tracking (if different from hierarchical)
    if (_mutable_container) {
        _mutable_container->notify_modified(time);
    }
}

void TSMutableView::invalidate_ts() {
    // Update hierarchical tracking if available
    if (_mutable_tracking_view.valid()) {
        _mutable_tracking_view.as<engine_time_t>() = MIN_DT;
    }
    // Also invalidate container for root-level tracking (if different from hierarchical)
    if (_mutable_container) {
        _mutable_container->invalidate_ts();
    }
}

// ============================================================================
// TSBView Implementation
// ============================================================================

TSBView::TSBView(const void* data, const TSBTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

TSView TSBView::field(const std::string& name) const {
    if (!valid()) {
        throw std::runtime_error("TSBView::field() called on invalid view");
    }

    const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
    const TSBFieldInfo* field_info = bundle_meta->field(name);

    if (!field_info) {
        throw std::runtime_error("TSBView::field(): field '" + name + "' not found");
    }

    // Navigate to the field data using the bundle view
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(name);

    // Navigate tracking in parallel if available
    value::ConstValueView field_tracking;
    if (_tracking_view.valid()) {
        value::ConstBundleView tracking_bundle = _tracking_view.as_bundle();
        field_tracking = tracking_bundle.at(name);
    }

    return TSView(field_value.data(), field_info->type, field_tracking);
}

TSView TSBView::field(size_t index) const {
    if (!valid()) {
        throw std::runtime_error("TSBView::field() called on invalid view");
    }

    const TSBTypeMeta* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);

    if (index >= bundle_meta->field_count()) {
        throw std::out_of_range("TSBView::field(): index " + std::to_string(index) +
                                " out of range (size=" + std::to_string(bundle_meta->field_count()) + ")");
    }

    const TSBFieldInfo& field_info = bundle_meta->field(index);

    // Navigate to the field data using the bundle view by name
    value::ConstBundleView bundle_view = _view.as_bundle();
    value::ConstValueView field_value = bundle_view.at(field_info.name);

    // Navigate tracking in parallel if available
    value::ConstValueView field_tracking;
    if (_tracking_view.valid()) {
        value::ConstBundleView tracking_bundle = _tracking_view.as_bundle();
        field_tracking = tracking_bundle.at(field_info.name);
    }

    return TSView(field_value.data(), field_info.type, field_tracking);
}

size_t TSBView::field_count() const noexcept {
    if (!valid()) return 0;
    return static_cast<const TSBTypeMeta*>(_ts_meta)->field_count();
}

bool TSBView::has_field(const std::string& name) const noexcept {
    if (!valid()) return false;
    return static_cast<const TSBTypeMeta*>(_ts_meta)->field(name) != nullptr;
}

// ============================================================================
// TSLView Implementation
// ============================================================================

TSLView::TSLView(const void* data, const TSLTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

TSView TSLView::element(size_t index) const {
    if (!valid()) {
        throw std::runtime_error("TSLView::element() called on invalid view");
    }

    const TSLTypeMeta* list_meta = static_cast<const TSLTypeMeta*>(_ts_meta);
    const TSMeta* element_type = list_meta->element_type();

    // Navigate to the element data using the list view
    value::ConstListView list_view = _view.as_list();

    if (index >= list_view.size()) {
        throw std::out_of_range("TSLView::element(): index " + std::to_string(index) +
                                " out of range (size=" + std::to_string(list_view.size()) + ")");
    }

    value::ConstValueView element_value = list_view.at(index);

    // Navigate tracking in parallel if available
    value::ConstValueView element_tracking;
    if (_tracking_view.valid()) {
        value::ConstListView tracking_list = _tracking_view.as_list();
        if (index < tracking_list.size()) {
            element_tracking = tracking_list.at(index);
        }
    }

    return TSView(element_value.data(), element_type, element_tracking);
}

size_t TSLView::size() const noexcept {
    if (!valid()) return 0;
    return _view.as_list().size();
}

bool TSLView::is_fixed_size() const noexcept {
    if (!valid()) return false;
    return static_cast<const TSLTypeMeta*>(_ts_meta)->is_fixed_size();
}

size_t TSLView::fixed_size() const noexcept {
    if (!valid()) return 0;
    return static_cast<const TSLTypeMeta*>(_ts_meta)->fixed_size();
}

// ============================================================================
// TSDView Implementation
// ============================================================================

TSDView::TSDView(const void* data, const TSDTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

size_t TSDView::size() const noexcept {
    if (!valid()) return 0;
    return _view.as_map().size();
}

// ============================================================================
// TSSView Implementation
// ============================================================================

TSSView::TSSView(const void* data, const TSSTypeMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
{}

size_t TSSView::size() const noexcept {
    if (!valid()) return 0;
    return _view.as_set().size();
}

}  // namespace hgraph
