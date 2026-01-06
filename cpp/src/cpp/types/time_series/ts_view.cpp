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
{}

TSView::TSView(const TSValue& ts_value)
    : _view(ts_value.value().data(), ts_value.value_schema())
    , _ts_meta(ts_value.ts_meta())
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
    return schema->ops->to_python(_view.data(), schema);
}

// ============================================================================
// TSMutableView Implementation
// ============================================================================

TSMutableView::TSMutableView(void* data, const TSMeta* ts_meta) noexcept
    : TSView(data, ts_meta)
    , _mutable_view(data, ts_meta ? ts_meta->value_schema() : nullptr)
{}

TSMutableView::TSMutableView(TSValue& ts_value)
    : TSView(ts_value)
    , _mutable_view(ts_value.value().data(), ts_value.value_schema())
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
        schema->ops->from_python(_mutable_view.data(), src, schema);
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

    return TSView(field_value.data(), field_info->type);
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

    return TSView(field_value.data(), field_info.type);
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
    return TSView(element_value.data(), element_type);
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
