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
        // Create overlay storage for hierarchical modification tracking
        _overlay = make_ts_overlay(ts_schema);
    }
}

TSValue::TSValue(TSValue&& other) noexcept
    : _value(std::move(other._value))
    , _overlay(std::move(other._overlay))
    , _ts_meta(other._ts_meta)
    , _owning_node(other._owning_node)
    , _output_id(other._output_id)
    , _link_support(std::move(other._link_support))
{
    other._ts_meta = nullptr;
    other._owning_node = nullptr;
    other._output_id = OUTPUT_MAIN;
}

TSValue& TSValue::operator=(TSValue&& other) noexcept {
    if (this != &other) {
        _value = std::move(other._value);
        _overlay = std::move(other._overlay);
        _ts_meta = other._ts_meta;
        _owning_node = other._owning_node;
        _output_id = other._output_id;
        _link_support = std::move(other._link_support);

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
    // Note: overlay is recreated fresh in the constructor, not copied
    // This is intentional - modification timestamps are not preserved across copies
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
    return TSView(_value.data(), _ts_meta, this);
}

TSMutableView TSValue::mutable_view() {
    if (!valid()) {
        return TSMutableView();
    }
    return TSMutableView(_value.data(), _ts_meta, this);
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

void TSValue::notify_modified(engine_time_t time) {
    _value.notify_modified(time);
}

engine_time_t TSValue::last_modified_time() const {
    return _overlay ? _overlay->last_modified_time() : MIN_DT;
}

bool TSValue::modified_at(engine_time_t time) const {
    return _overlay ? _overlay->modified_at(time) : false;
}

bool TSValue::ts_valid() const {
    return _overlay ? _overlay->valid() : false;
}

void TSValue::invalidate_ts() {
    if (_overlay) {
        _overlay->mark_invalid();
    }
}

// ============================================================================
// Link Support Implementation
// ============================================================================

void TSValue::enable_link_support() {
    if (!_ts_meta) {
        return;  // Invalid TSValue
    }

    size_t num_children = 0;
    std::vector<const TSMeta*> child_metas;

    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB: {
            auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
            num_children = bundle_meta->field_count();
            // Collect child schemas
            for (size_t i = 0; i < num_children; ++i) {
                child_metas.push_back(bundle_meta->field_meta(i));
            }
            break;
        }
        case TSTypeKind::TSL: {
            auto* list_meta = static_cast<const TSLTypeMeta*>(_ts_meta);
            num_children = list_meta->fixed_size();
            const TSMeta* elem_type = list_meta->element_type();
            // All elements have the same type
            for (size_t i = 0; i < num_children; ++i) {
                child_metas.push_back(elem_type);
            }
            break;
        }
        default:
            // Scalars, sets, windows, etc. don't have child links
            return;
    }

    // Allocate the link support structure
    _link_support = std::make_unique<LinkSupport>();

    // Allocate link slots (all nullptr initially = non-peered)
    _link_support->child_links.resize(num_children);

    // Allocate child value slots
    _link_support->child_values.resize(num_children);

    // RECURSIVE: Create child_values for all composite children
    // This ensures the entire nested structure is ready for navigation
    for (size_t i = 0; i < num_children; ++i) {
        const TSMeta* child_meta = child_metas[i];
        if (child_meta &&
            (child_meta->kind() == TSTypeKind::TSB ||
             child_meta->kind() == TSTypeKind::TSL)) {
            // This child is a composite type - create its TSValue with link support
            _link_support->child_values[i] = std::make_unique<TSValue>(child_meta, _owning_node);
            _link_support->child_values[i]->enable_link_support();  // Recursive call
        }
        // For leaf types (TS, TSS, TSW, etc.), child_values[i] stays nullptr
    }
}

void TSValue::create_link(size_t index, const TSValue* output) {
    if (!_link_support || index >= _link_support->child_links.size()) {
        throw std::out_of_range("TSValue::create_link: index out of bounds or link support not enabled");
    }

    // Get expected schema for this child position
    const TSMeta* expected_schema = nullptr;
    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB: {
            auto* bundle_meta = static_cast<const TSBTypeMeta*>(_ts_meta);
            if (index < bundle_meta->field_count()) {
                expected_schema = bundle_meta->field_meta(index);
            }
            break;
        }
        case TSTypeKind::TSL: {
            auto* list_meta = static_cast<const TSLTypeMeta*>(_ts_meta);
            expected_schema = list_meta->element_type();
            break;
        }
        default:
            throw std::runtime_error("TSValue::create_link: not a composite type");
    }

    // Validate output schema matches expected
    if (output && expected_schema) {
        const TSMeta* output_schema = output->ts_meta();
        if (!output_schema) {
            throw std::runtime_error("TSValue::create_link: output has no schema");
        }

        // Schema validation: check kind matches
        if (output_schema->kind() != expected_schema->kind()) {
            throw std::runtime_error(
                "TSValue::create_link: schema mismatch at index " + std::to_string(index) +
                " - expected kind " + std::to_string(static_cast<int>(expected_schema->kind())) +
                " but got " + std::to_string(static_cast<int>(output_schema->kind())));
        }

        // For deeper validation, check value schemas match
        const value::TypeMeta* expected_value = expected_schema->value_schema();
        const value::TypeMeta* output_value = output_schema->value_schema();
        if (expected_value != output_value) {
            // Allow both being nullptr (e.g., SIGNAL)
            if (expected_value && output_value) {
                // Check kind and size match as basic validation
                if (expected_value->kind != output_value->kind ||
                    expected_value->size != output_value->size) {
                    throw std::runtime_error(
                        "TSValue::create_link: value schema mismatch at index " + std::to_string(index) +
                        " - value type kind or size don't match");
                }
            } else if (expected_value || output_value) {
                throw std::runtime_error(
                    "TSValue::create_link: value schema mismatch at index " + std::to_string(index) +
                    " - one has value schema, other doesn't");
            }
        }
    }

    // Create link if it doesn't exist
    if (!_link_support->child_links[index]) {
        _link_support->child_links[index] = std::make_unique<TSLink>(_owning_node);
    }

    // Bind to the output
    _link_support->child_links[index]->bind(output);

    // Clear any child value at this position (it's now peered)
    _link_support->child_values[index].reset();
}

void TSValue::remove_link(size_t index) {
    if (_link_support && index < _link_support->child_links.size() && _link_support->child_links[index]) {
        _link_support->child_links[index]->unbind();
    }
}

TSValue* TSValue::get_or_create_child_value(size_t index) {
    if (!_link_support || index >= _link_support->child_values.size()) {
        throw std::out_of_range("TSValue::get_or_create_child_value: index out of bounds or link support not enabled");
    }

    // If already linked, return nullptr (should use link instead)
    if (is_linked(index)) {
        return nullptr;
    }

    // If child value already exists, return it
    if (_link_support->child_values[index]) {
        return _link_support->child_values[index].get();
    }

    // Determine child schema
    const TSMeta* child_meta = nullptr;
    switch (_ts_meta->kind()) {
        case TSTypeKind::TSB:
            child_meta = static_cast<const TSBTypeMeta*>(_ts_meta)->field_meta(index);
            break;
        case TSTypeKind::TSL:
            child_meta = static_cast<const TSLTypeMeta*>(_ts_meta)->element_type();
            break;
        default:
            return nullptr;  // No child values for non-composites
    }

    // Only create child values for composite types that can have links
    if (!child_meta || (child_meta->kind() != TSTypeKind::TSB &&
                        child_meta->kind() != TSTypeKind::TSL)) {
        return nullptr;  // Leaf types don't need nested TSValue
    }

    // Create the child TSValue
    _link_support->child_values[index] = std::make_unique<TSValue>(child_meta, _owning_node);
    _link_support->child_values[index]->enable_link_support();

    return _link_support->child_values[index].get();
}

void TSValue::make_links_active() {
    if (!_link_support) return;

    // Activate all direct links
    for (auto& link : _link_support->child_links) {
        if (link) {
            link->make_active();
        }
    }

    // Recursively activate non-linked children that have links
    for (auto& child : _link_support->child_values) {
        if (child && child->has_link_support()) {
            child->make_links_active();
        }
    }
}

void TSValue::make_links_passive() {
    if (!_link_support) return;

    // Deactivate all direct links
    for (auto& link : _link_support->child_links) {
        if (link) {
            link->make_passive();
        }
    }

    // Recursively deactivate non-linked children
    for (auto& child : _link_support->child_values) {
        if (child && child->has_link_support()) {
            child->make_links_passive();
        }
    }
}

}  // namespace hgraph
