//
// ts_value.cpp - TSValue implementation
//

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <stdexcept>

namespace hgraph {

// ============================================================================
// TSValue Implementation
// ============================================================================

TSValue::TSValue(const TSMeta* ts_schema, Node* owner, int output_id)
    : _ts_meta(ts_schema)
    , _owning_node(owner)
    , _output_id(output_id)
{
    if (ts_schema) {
        // Create Value storage using the value schema from TSMeta
        const value::TypeMeta* value_schema = ts_schema->value_schema();
        if (value_schema) {
            _value = base_value_type(value_schema);
        }
    }
}

TSValue::TSValue(TSValue&& other) noexcept
    : _value(std::move(other._value))
    , _ts_meta(other._ts_meta)
    , _owning_node(other._owning_node)
    , _output_id(other._output_id)
{
    other._ts_meta = nullptr;
    other._owning_node = nullptr;
    other._output_id = OUTPUT_MAIN;
}

TSValue& TSValue::operator=(TSValue&& other) noexcept {
    if (this != &other) {
        _value = std::move(other._value);
        _ts_meta = other._ts_meta;
        _owning_node = other._owning_node;
        _output_id = other._output_id;

        other._ts_meta = nullptr;
        other._owning_node = nullptr;
        other._output_id = OUTPUT_MAIN;
    }
    return *this;
}

TSValue TSValue::copy(const TSValue& other) {
    TSValue result(other._ts_meta, other._owning_node, other._output_id);
    if (other._value.valid()) {
        result._value = base_value_type::copy(other._value);
    }
    return result;
}

bool TSValue::valid() const noexcept {
    return _ts_meta != nullptr && _value.valid();
}

const value::TypeMeta* TSValue::value_schema() const noexcept {
    return _ts_meta ? _ts_meta->value_schema() : nullptr;
}

TSView TSValue::view() const {
    if (!valid()) {
        return TSView();
    }
    return TSView(_value.data(), _ts_meta);
}

TSMutableView TSValue::mutable_view() {
    if (!valid()) {
        return TSMutableView();
    }
    return TSMutableView(_value.data(), _ts_meta);
}

TSBView TSValue::bundle_view() const {
    if (!valid()) {
        throw std::runtime_error("TSValue::bundle_view() called on invalid TSValue");
    }
    if (_ts_meta->kind() != TSTypeKind::TSB) {
        throw std::runtime_error("TSValue::bundle_view() called on non-bundle type");
    }
    return TSBView(_value.data(), static_cast<const TSBTypeMeta*>(_ts_meta));
}

nb::object TSValue::to_python() const {
    return _value.to_python();
}

void TSValue::from_python(const nb::object& src) {
    _value.from_python(src);
}

}  // namespace hgraph
