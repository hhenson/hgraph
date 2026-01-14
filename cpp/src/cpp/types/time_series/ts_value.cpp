//
// ts_value.cpp - TSValue implementation
//

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>
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

    // Element view case: _cast_source points to source list, _cast_index is element index.
    // Return a view into the source list's element rather than our own _value.
    if (_cast_source && _cast_index >= 0) {
        auto* source_list_meta = static_cast<const TSLTypeMeta*>(_cast_source->ts_meta());
        const void* list_data = _cast_source->value().data();
        const auto* list_value_schema = source_list_meta->value_schema();

        // Get element data pointer using ListOps
        const void* elem_ptr = value::ListOps::get_element_ptr_const(
            list_data, static_cast<size_t>(_cast_index), list_value_schema);

        return TSView(elem_ptr, _ts_meta, this);
    }

    // For scalar inputs with TSRefTargetLink: delegate to the target's view.
    // This handles the case where a non-REF input (e.g., TS[int]) is bound to a REF output
    // (e.g., REF[TS[int]]). After rebind, the input should see the TARGET directly.
    if (_link_support && _ts_meta->kind() == TSTypeKind::TS) {
        // Check if link at index 0 is a TSRefTargetLink with a bound target
        TSRefTargetLink* ref_link = const_cast<TSValue*>(this)->ref_link_at(0);
        if (ref_link) {
            const TSValue* target = ref_link->target_output();
            if (target) {
                // Delegate to target's view - this gives the actual data
                return target->view();
            }
        }
    }

    TSView result(_value.data(), _ts_meta, this);

    // For INPUT TSValues with link support, set the view's link_source
    // This allows the view to follow links when accessing elements
    if (_link_support) {
        result.set_link_source(this);
    }

    return result;
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
    // Use the TSValue constructor to properly set overlay, container, and root
    return TSBView(*this);
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
        if (!child_meta) continue;

        // Check if this child needs link support (composite type or REF to composite)
        TSTypeKind kind = child_meta->kind();
        bool is_composite = (kind == TSTypeKind::TSB || kind == TSTypeKind::TSL);

        // For REF types, check if the referenced type is composite
        const TSMeta* referenced_meta = nullptr;
        if (kind == TSTypeKind::REF) {
            auto* ref_meta = static_cast<const REFTypeMeta*>(child_meta);
            referenced_meta = ref_meta->referenced_type();
            if (referenced_meta) {
                TSTypeKind ref_kind = referenced_meta->kind();
                is_composite = (ref_kind == TSTypeKind::TSB || ref_kind == TSTypeKind::TSL);
            }
        }

        if (is_composite) {
            // For REF[TSB] or REF[TSL], use the referenced type's structure for link support
            // This allows edges to navigate into the referenced type's fields
            const TSMeta* link_meta = referenced_meta ? referenced_meta : child_meta;
            _link_support->child_values[i] = std::make_unique<TSValue>(link_meta, _owning_node);
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

        // For REF types, compare against the referenced type instead
        // This handles both REF input accepting TS output and TS input accepting REF output
        const TSMeta* expected_to_compare = expected_schema;
        if (expected_schema->is_reference()) {
            expected_to_compare = static_cast<const REFTypeMeta*>(expected_schema)->referenced_type();
        }

        const TSMeta* output_to_compare = output_schema;
        if (output_schema->is_reference()) {
            output_to_compare = static_cast<const REFTypeMeta*>(output_schema)->referenced_type();
        }

        // Schema validation: check kind matches (after unwrapping REF)
        // Special case: SIGNAL accepts any time series type (it only cares about ticks, not values)
        // This matches Python's HgSignalMetaData.matches() which returns True for any HgTimeSeriesTypeMetaData
        //
        // Special case: TSL output binding to TS input is allowed when the TSL couldn't be navigated
        // to its element (because TSL outputs don't have child TSValues like Python does).
        // In this case, the binding represents element-level access that the view layer handles.
        bool kinds_compatible = (output_to_compare->kind() == expected_to_compare->kind()) ||
                                (expected_to_compare->kind() == TSTypeKind::SIGNAL);

        // Allow TSL->TS binding for element access (TSL elements are implicitly TS)
        // Track if we're doing element binding to skip deeper validation
        bool is_tsl_element_binding = false;
        if (!kinds_compatible &&
            output_to_compare->kind() == TSTypeKind::TSL &&
            expected_to_compare->kind() == TSTypeKind::TS) {
            // Check if the TSL element type matches expected TS type
            auto* list_meta = static_cast<const TSLTypeMeta*>(output_to_compare);
            if (list_meta->element_type() &&
                list_meta->element_type()->kind() == expected_to_compare->kind()) {
                // Element type matches - allow binding
                kinds_compatible = true;
                is_tsl_element_binding = true;
            }
        }

        // Allow TSB->field binding for field access (when navigation failed)
        // This happens when the output is a root TSB that couldn't be navigated
        // The field_index will be set by the graph builder to indicate which field to use
        bool is_tsb_field_binding = false;
        if (!kinds_compatible &&
            output_to_compare->kind() == TSTypeKind::TSB) {
            // Output is a TSB - this is likely a field binding where navigation failed
            // Allow it; the graph builder will set field_index on the link
            kinds_compatible = true;
            is_tsb_field_binding = true;
        }

        if (expected_to_compare && output_to_compare && !kinds_compatible) {
            throw std::runtime_error(
                "TSValue::create_link: schema mismatch at index " + std::to_string(index) +
                " - expected kind " + std::to_string(static_cast<int>(expected_to_compare->kind())) +
                " but got " + std::to_string(static_cast<int>(output_to_compare->kind())));
        }

        // For deeper validation, check value schemas match
        // Skip this check if expected is SIGNAL (SIGNAL accepts any time series regardless of value type)
        // Also skip for TSL->TS element binding and TSB->field binding (will be resolved at runtime)
        if (expected_to_compare->kind() != TSTypeKind::SIGNAL && !is_tsl_element_binding && !is_tsb_field_binding) {
            const value::TypeMeta* expected_value = expected_to_compare ? expected_to_compare->value_schema() : nullptr;
            const value::TypeMeta* output_value = output_to_compare ? output_to_compare->value_schema() : nullptr;
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
    }

    // Check if output is a REF type (requires TSRefTargetLink)
    bool is_ref_output = output && output->ts_meta() && output->ts_meta()->is_reference();
    // Note: is_ref_output used below in binding decisions

    // Check if input expects REF but output is non-REF (TS→REF conversion)
    bool is_ref_input = expected_schema && expected_schema->is_reference();
    bool is_ts_to_ref = is_ref_input && !is_ref_output;

    // Check for composite element TS→REF conversion
    // This handles cases like TSL[REF[T]] binding to TSL[T] where we need element-by-element binding
    bool needs_element_wise_ref_binding = false;
    if (!is_ts_to_ref && expected_schema && output && output->ts_meta()) {
        TSTypeKind expected_kind = expected_schema->kind();
        TSTypeKind output_kind = output->ts_meta()->kind();

        // Both TSL with different element types (REF vs non-REF)
        if (expected_kind == TSTypeKind::TSL && output_kind == TSTypeKind::TSL) {
            auto* expected_list = static_cast<const TSLTypeMeta*>(expected_schema);
            auto* output_list = static_cast<const TSLTypeMeta*>(output->ts_meta());
            bool elem_expects_ref = expected_list->element_type() && expected_list->element_type()->is_reference();
            bool output_elem_is_ref = output_list->element_type() && output_list->element_type()->is_reference();
            if (elem_expects_ref && !output_elem_is_ref) {
                needs_element_wise_ref_binding = true;
            }
        }
        // Could extend to TSB field-by-field comparison if needed
    }

    if (needs_element_wise_ref_binding) {
        // TSL[REF[T]] input binding to TSL[T] output
        // We need to create element-by-element links with TS→REF conversion

        // Get or create child value for the TSL input
        TSValue* input_child = get_or_create_child_value(index);
        if (!input_child) {
            // No child value - just bind normally (fall through)
        } else {
            // Enable link support on the input TSL child
            input_child->enable_link_support();

            // Get the TSL sizes
            auto* expected_list = static_cast<const TSLTypeMeta*>(expected_schema);
            auto* output_list = static_cast<const TSLTypeMeta*>(output->ts_meta());
            size_t size = expected_list->fixed_size();
            if (output_list->fixed_size() < size) {
                size = output_list->fixed_size();
            }

            // For each element, create a link from input_child[i] to output[i]
            for (size_t i = 0; i < size; ++i) {
                // The output element can be extracted from the output TSValue
                // We need the output's TSValue for element i
                const TSValue* output_elem = output->child_value(i);
                if (!output_elem) {
                    // Output might not have child values - use plain TSLink for TS→REF element binding
                    // Note: DON'T use notify_once here because the TSL itself (not just the REF)
                    // should trigger node notification when underlying values change
                    if (!input_child->has_link_support()) {
                        continue;  // Skip if we can't create links
                    }
                    auto* existing_link = input_child->link_at(i);
                    if (!existing_link) {
                        auto link = std::make_unique<TSLink>(_owning_node);
                        // DON'T set notify_once - TSL elements need continuous notification
                        link->set_element_index(static_cast<int>(i));  // Set element index for TSL navigation
                        input_child->_link_support->child_links[i] = std::move(link);
                    }
                    TSLink* link_ptr = input_child->link_at(i);
                    if (link_ptr) {
                        // Bind to the whole output TSL - element access uses element_index
                        link_ptr->bind(output);
                        link_ptr->make_active();
                    }
                } else {
                    // Have specific output element - create link to it
                    input_child->create_link(i, output_elem);
                }
            }
            // Don't clear child_values[index] - we're using child value for nested structure
            return;
        }
    }

    if (is_ref_output && !is_ref_input) {
        // REF→TS binding: use TSRefTargetLink (REF output, non-REF input)
        // This is the ONLY case for TSRefTargetLink - it handles dynamic rebinding
        auto* existing_ref = ref_link_at(index);
        if (!existing_ref) {
            // Create new TSRefTargetLink
            auto ref_link = std::make_unique<TSRefTargetLink>(_owning_node);
            _link_support->child_links[index] = std::move(ref_link);
        }
        // Bind the TSRefTargetLink's ref_link channel to the REF output so we receive notifications
        // This is a simplified binding - full REF semantics require bind_ref() with TimeSeriesReferenceOutput
        TSRefTargetLink* ref_link_ptr = ref_link_at(index);
        if (ref_link_ptr) {
            // Use the ref_link channel for notifications
            ref_link_ptr->ref_link().bind(output);
            // CRITICAL: Make the ref_link active so it subscribes to the output's overlay
            ref_link_ptr->ref_link().make_active();
        }

        // Register as observer of the REF output so we get rebound when its value changes
        // This implements Python's observer pattern: when REF output sets its value,
        // all observing inputs are rebound to the target that the REF points to.
        const_cast<TSValue*>(output)->observe_ref(this, index);
    } else if (is_ref_output && is_ref_input) {
        // REF→REF binding: use plain TSLink (both sides are REF)
        // The REF input peers to the REF output - when the output sets a new reference,
        // the input sees the same reference value.
        auto* existing_link = link_at(index);
        if (!existing_link) {
            _link_support->child_links[index] = std::make_unique<TSLink>(_owning_node);
        }
        TSLink* link_ptr = link_at(index);
        if (link_ptr) {
            link_ptr->bind(output);
            link_ptr->make_active();
        }
        // child_value will be cleared by fall-through to end of function (peered)
    } else if (is_ts_to_ref) {
        // TS→REF binding: input expects REF but output is non-REF
        // Use plain TSLink (TSRefTargetLink is only for REF→TS, where REF output binds to non-REF input).
        // The REF input wraps the non-REF output in a TimeSeriesReference.
        // The reference itself doesn't change when the underlying output's value changes.
        // Use notify_once=true so the node is only notified when the binding is first established,
        // not on every target modification. This matches Python's behavior where REF.modified()
        // is only True when the binding (_sampled) changes, not when the target changes.
        auto* existing_link = link_at(index);
        if (!existing_link) {
            auto link = std::make_unique<TSLink>(_owning_node);
            // Set notify_once=true for TS→REF bindings - the REF input's "value" is a reference
            // to the output, and that reference doesn't change when the output's value changes.
            link->set_notify_once(true);
            _link_support->child_links[index] = std::move(link);
        }
        TSLink* link_ptr = link_at(index);
        if (link_ptr) {
            link_ptr->bind(output);
            link_ptr->make_active();
        }
        // DON'T clear child_value - REF inputs don't peer, they reference
        return;
    } else {
        // Standard non-REF binding: use TSLink
        auto* existing_link = link_at(index);
        if (!existing_link) {
            // Create new TSLink
            _link_support->child_links[index] = std::make_unique<TSLink>(_owning_node);
        }
        // Bind to the output
        link_at(index)->bind(output);
    }

    // Clear any child value at this position (it's now peered)
    _link_support->child_values[index].reset();
}

void TSValue::remove_link(size_t index) {
    if (!_link_support || index >= _link_support->child_links.size()) return;

    // Unbind via visitor pattern
    std::visit([](auto& link) {
        using T = std::decay_t<decltype(link)>;
        if constexpr (!std::is_same_v<T, std::monostate>) {
            if (link) link->unbind();
        }
    }, _link_support->child_links[index]);
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

    // Only create child values for composite types that can have links,
    // or for REF types (which need child TSValue as a cache key)
    if (!child_meta || (child_meta->kind() != TSTypeKind::TSB &&
                        child_meta->kind() != TSTypeKind::TSL &&
                        child_meta->kind() != TSTypeKind::REF)) {
        return nullptr;  // Leaf types (except REF) don't need nested TSValue
    }

    // Create the child TSValue
    _link_support->child_values[index] = std::make_unique<TSValue>(child_meta, _owning_node);
    // Only enable link support for composite types (not for REF)
    if (child_meta->kind() == TSTypeKind::TSB || child_meta->kind() == TSTypeKind::TSL) {
        _link_support->child_values[index]->enable_link_support();
    }

    return _link_support->child_values[index].get();
}

void TSValue::make_links_active() {
    if (!_link_support) return;

    // Activate all direct links
    for (auto& link : _link_support->child_links) {
        link_storage_make_active(link);
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
        link_storage_make_passive(link);
    }

    // Recursively deactivate non-linked children
    for (auto& child : _link_support->child_values) {
        if (child && child->has_link_support()) {
            child->make_links_passive();
        }
    }
}

// ============================================================================
// Cast Cache Implementation
// ============================================================================

TSValue* TSValue::cast_to(const TSMeta* target_schema) {
    // Same schema - return self
    if (target_schema == _ts_meta) {
        return this;
    }

    // Check cache
    if (_cast_cache) {
        auto it = _cast_cache->find(target_schema);
        if (it != _cast_cache->end()) {
            return it->second.get();
        }
    }

    // Create cache if needed
    if (!_cast_cache) {
        _cast_cache = std::make_unique<std::unordered_map<const TSMeta*, std::unique_ptr<TSValue>>>();
    }

    // Create converted TSValue (dispatches by type kind)
    auto converted = create_cast_value(target_schema);
    auto* result = converted.get();
    _cast_cache->emplace(target_schema, std::move(converted));

    return result;
}

bool TSValue::has_cast(const TSMeta* target_schema) const {
    if (!_cast_cache) return false;
    return _cast_cache->find(target_schema) != _cast_cache->end();
}

void TSValue::clear_cast_cache() {
    if (_cast_cache) {
        _cast_cache->clear();
    }
}

std::unique_ptr<TSValue> TSValue::create_cast_value(const TSMeta* target_schema) {
    if (!_ts_meta || !target_schema) {
        throw std::runtime_error("TSValue::create_cast_value: null schema");
    }

    // Create the cast TSValue with target schema
    auto cast_value = std::make_unique<TSValue>(target_schema, _owning_node, _output_id);

    // Set the cast source to point back to this source TSValue
    cast_value->_cast_source = this;

    // Dispatch by source type kind to set up the cast structure
    switch (_ts_meta->kind()) {
        case TSTypeKind::TS:
            // Leaf TS → REF cast
            if (target_schema->kind() == TSTypeKind::REF) {
                setup_ts_to_ref_cast(*cast_value);
            } else {
                throw std::runtime_error("TSValue::create_cast_value: unsupported TS cast to " +
                    std::to_string(static_cast<int>(target_schema->kind())));
            }
            break;

        case TSTypeKind::TSB:
            setup_tsb_cast(*cast_value, target_schema);
            break;

        case TSTypeKind::TSL:
            setup_tsl_cast(*cast_value, target_schema);
            break;

        case TSTypeKind::TSD:
            setup_tsd_cast(*cast_value, target_schema);
            break;

        default:
            throw std::runtime_error("TSValue::create_cast_value: unsupported source type kind " +
                std::to_string(static_cast<int>(_ts_meta->kind())));
    }

    return cast_value;
}

void TSValue::setup_ts_to_ref_cast(TSValue& cast_value) {
    // This is the leaf case: TS[V] → REF[TS[V]]
    //
    // The cast TSValue represents a REF that points to this source TSValue.
    // The _cast_source pointer (set by create_cast_value) points back to this.
    //
    // When the REF's value is accessed (e.g., through TSView::to_python() or
    // when binding inputs), a TimeSeriesReference::make_view_bound(_cast_source)
    // is created to provide the reference.
    //
    // The REF's modification time is the time it was "created" (conceptually
    // when the binding was established). It does NOT change when the source
    // data changes - the REF always points to the same source.
    //
    // The cast_value's _value storage is NOT used for storing a TimeSeriesReference.
    // Instead, the reference is synthesized on-demand from _cast_source.

    // Mark the REF overlay as modified to indicate it has been initialized.
    // Using MIN_ST makes valid() return true (valid = last_modified_time > MIN_DT)
    if (cast_value._overlay) {
        cast_value._overlay->mark_modified(MIN_ST);
    }

    // Note: cast_value._cast_source was already set by create_cast_value()
    // to point to 'this', enabling view access to synthesize TimeSeriesReference
}

void TSValue::setup_tsb_cast(TSValue& cast_value, const TSMeta* target_schema) {
    // Bundle cast: recursively cast each field to its target type.
    //
    // For TSB[a: TS[V], b: TS[W]] → TSB[a: REF[TS[V]], b: REF[TS[W]]]:
    // - Cast TSValue has _cast_source pointing to source bundle
    // - Each field gets its own cast TSValue with target field schema
    // - Field cast TSValues have _cast_source pointing to this source bundle
    //
    // When accessing field N of the cast bundle:
    // - The child_values[N] provides the cast TSValue for that field
    // - That cast TSValue's _cast_source is this source bundle
    // - View navigation uses field index to access correct source data

    if (target_schema->kind() != TSTypeKind::TSB) {
        throw std::runtime_error("TSValue::setup_tsb_cast: target is not a bundle");
    }

    auto* source_bundle = static_cast<const TSBTypeMeta*>(_ts_meta);
    auto* target_bundle = static_cast<const TSBTypeMeta*>(target_schema);

    size_t field_count = source_bundle->field_count();
    if (field_count != target_bundle->field_count()) {
        throw std::runtime_error("TSValue::setup_tsb_cast: field count mismatch");
    }

    // Enable link support on cast value to store child cast TSValues
    cast_value.enable_link_support();

    for (size_t i = 0; i < field_count; ++i) {
        const TSMeta* source_field_meta = source_bundle->field_meta(i);
        const TSMeta* target_field_meta = target_bundle->field_meta(i);

        if (source_field_meta == target_field_meta) {
            // Same schema - no cast needed for this field.
            // The cast bundle's view will navigate to source field directly.
            continue;
        }

        // Different schemas - create cast TSValue for this field.
        // The child cast will have _cast_source pointing to THIS source bundle,
        // and the view will use the field index for navigation.
        auto field_cast = std::make_unique<TSValue>(target_field_meta, _owning_node, _output_id);
        field_cast->_cast_source = this;  // Point to source bundle

        // For leaf fields (TS → REF), mark as initialized
        if (target_field_meta->kind() == TSTypeKind::REF &&
            source_field_meta->kind() == TSTypeKind::TS) {
            if (field_cast->_overlay) {
                field_cast->_overlay->mark_modified(MIN_ST);
            }
        }

        // Store in cast value's child_values
        if (cast_value._link_support && i < cast_value._link_support->child_values.size()) {
            cast_value._link_support->child_values[i] = std::move(field_cast);
        }
    }

    // Mark cast bundle overlay as initialized
    if (cast_value._overlay) {
        cast_value._overlay->mark_modified(MIN_ST);
    }
}

void TSValue::setup_tsl_cast(TSValue& cast_value, const TSMeta* target_schema) {
    // List cast: cast each element to its target type.
    //
    // For TSL[TS[V], N] → TSL[REF[TS[V]], N]:
    // - Cast TSValue has _cast_source pointing to source list
    // - Each element gets its own cast TSValue with target element schema
    // - For TS→REF conversion, we also create source element TSValues
    //
    // Source element TSValue pattern:
    // - Schema: TS[V] (the source element type)
    // - _cast_source: points to source list
    // - _cast_index: which element
    // - view() returns a view into the list element's data
    //
    // Cast REF TSValue pattern:
    // - Schema: REF[TS[V]] (the target element type)
    // - _cast_source: points to source list
    // - _source_element: points to the source element TSValue
    // - TimeSeriesReference is created from _source_element

    if (target_schema->kind() != TSTypeKind::TSL) {
        throw std::runtime_error("TSValue::setup_tsl_cast: target is not a list");
    }

    auto* source_list = static_cast<const TSLTypeMeta*>(_ts_meta);
    auto* target_list = static_cast<const TSLTypeMeta*>(target_schema);

    size_t size = source_list->fixed_size();
    if (size != target_list->fixed_size()) {
        throw std::runtime_error("TSValue::setup_tsl_cast: size mismatch");
    }

    const TSMeta* source_elem_meta = source_list->element_type();
    const TSMeta* target_elem_meta = target_list->element_type();

    if (source_elem_meta == target_elem_meta) {
        // Same element schema - no cast needed.
        // The cast list's view will navigate to source elements directly.
        // Mark as initialized and return.
        if (cast_value._overlay) {
            cast_value._overlay->mark_modified(MIN_ST);
        }
        return;
    }

    // Enable link support on cast value to store child cast TSValues
    cast_value.enable_link_support();

    // Need TS → REF conversion for each element
    bool needs_ref_conversion = (target_elem_meta->kind() == TSTypeKind::REF &&
                                 source_elem_meta->kind() != TSTypeKind::REF);

    for (size_t i = 0; i < size; ++i) {
        // Create cast TSValue for this element (REF[TS[V]] schema)
        auto elem_cast = std::make_unique<TSValue>(target_elem_meta, _owning_node, _output_id);
        elem_cast->_cast_source = this;  // Point to source list
        elem_cast->_cast_index = static_cast<int64_t>(i);

        if (needs_ref_conversion) {
            // Create source element TSValue (TS[V] schema) that views into the list
            // This provides the element-level TSValue for TimeSeriesReference creation
            auto source_elem = new TSValue(source_elem_meta, _owning_node, _output_id);
            source_elem->_cast_source = this;  // Point to source list
            source_elem->_cast_index = static_cast<int64_t>(i);  // Element index
            if (source_elem->_overlay) {
                source_elem->_overlay->mark_modified(MIN_ST);
            }

            // Link cast REF to its source element
            elem_cast->_source_element = source_elem;

            // Mark the REF as initialized
            if (elem_cast->_overlay) {
                elem_cast->_overlay->mark_modified(MIN_ST);
            }
        }

        // Store in cast value's child_values
        if (cast_value._link_support && i < cast_value._link_support->child_values.size()) {
            cast_value._link_support->child_values[i] = std::move(elem_cast);
        }
    }

    // Mark cast list overlay as initialized
    if (cast_value._overlay) {
        cast_value._overlay->mark_modified(MIN_ST);
    }
}

void TSValue::setup_tsd_cast(TSValue& cast_value, const TSMeta* target_schema) {
    // Dict cast: TSD[K, V] → TSD[K, REF[V]]
    //
    // TSD is dynamic - keys can be added/removed at runtime. The cast mechanism:
    // 1. The cast TSD shares the key_set structure with source (same keys)
    // 2. Each value access goes through _cast_source to get the source value
    // 3. The value is then viewed as the target type (REF)
    //
    // Unlike TSB/TSL which have fixed structure and pre-create child casts,
    // TSD casts are more view-based: the cast TSValue provides a lens over
    // the source that presents values as REF types.
    //
    // Key aspects:
    // - _cast_source points to source TSD
    // - Source's key_set changes propagate to cast (shared structure)
    // - Value access through cast view synthesizes REF on-demand

    if (target_schema->kind() != TSTypeKind::TSD) {
        throw std::runtime_error("TSValue::setup_tsd_cast: target is not a dict");
    }

    auto* source_tsd = static_cast<const TSDTypeMeta*>(_ts_meta);
    auto* target_tsd = static_cast<const TSDTypeMeta*>(target_schema);

    // Verify key types match
    if (source_tsd->key_type() != target_tsd->key_type()) {
        throw std::runtime_error("TSValue::setup_tsd_cast: key type mismatch");
    }

    // The cast TSD's overlay should share key modification tracking with source.
    // This means when source keys are added/removed, cast reflects this.
    //
    // For value modifications:
    // - Source value at key K is TS[V]
    // - Cast presents it as REF[TS[V]]
    // - The REF's modification time is when the binding was established
    // - The underlying value modifications are accessed through the REF
    //
    // Implementation note: The MapTSOverlay could support sharing the key_set
    // overlay. For now, we set up the basic structure.

    // Mark the cast TSD as initialized
    if (cast_value._overlay) {
        cast_value._overlay->mark_modified(MIN_ST);
    }

    // The cast mechanism for TSD values works as follows:
    // - When an input binds to cast_value[key], it gets a view of the REF
    // - The REF view's _cast_source is this source TSD
    // - Value access navigates: cast_value → _cast_source → source_value[key] → as REF
    //
    // The actual value casting happens in the view layer when accessing entries.
    // The _cast_source pointer enables this navigation.
    //
    // Note: For full dynamic key tracking (add/remove callbacks), additional
    // overlay support would be needed. The current implementation handles
    // the static binding case where keys exist at binding time.
}

// ============================================================================
// REF Observer Support
// ============================================================================

void TSValue::observe_ref(TSValue* input_ts_value, size_t link_index) {
    if (!_ref_observers) {
        _ref_observers = std::make_unique<std::vector<std::pair<TSValue*, size_t>>>();
    }
    // Check if already registered
    for (const auto& [input, idx] : *_ref_observers) {
        if (input == input_ts_value && idx == link_index) {
            return;  // Already registered
        }
    }
    _ref_observers->emplace_back(input_ts_value, link_index);
}

void TSValue::stop_observing_ref(TSValue* input_ts_value, size_t link_index) {
    if (!_ref_observers) return;

    auto it = std::remove_if(_ref_observers->begin(), _ref_observers->end(),
        [input_ts_value, link_index](const auto& pair) {
            return pair.first == input_ts_value && pair.second == link_index;
        });
    _ref_observers->erase(it, _ref_observers->end());

    // Clean up if empty
    if (_ref_observers->empty()) {
        _ref_observers.reset();
    }
}

void TSValue::notify_ref_observers(const TSValue* target) {
    if (!_ref_observers || _ref_observers->empty()) return;

    // Get current engine time from owning node if available
    engine_time_t current_time = MIN_DT;
    if (_owning_node) {
        auto g = _owning_node->graph();
        if (g) {
            current_time = g->evaluation_time();
        }
    }

    for (const auto& [input_ts_value, link_index] : *_ref_observers) {
        if (!input_ts_value) continue;

        // Get the link at this index
        TSLink* link = input_ts_value->link_at(link_index);
        TSRefTargetLink* ref_link = input_ts_value->ref_link_at(link_index);

        if (ref_link) {
            // TSRefTargetLink: use rebind_target which handles both binding and activation
            ref_link->rebind_target(target, current_time);
        } else if (link) {
            // Regular TSLink: rebind to target
            if (target) {
                bool was_active = link->active();
                if (was_active) link->make_passive();
                link->bind(target);
                if (was_active) link->make_active();
            } else {
                link->unbind();
            }
        }
    }
}

void TSValue::notify_ref_observers_element(const TSValue* container, size_t elem_index) {
    if (!_ref_observers || _ref_observers->empty()) return;

    // Get current engine time from owning node if available
    engine_time_t current_time = MIN_DT;
    if (_owning_node) {
        auto g = _owning_node->graph();
        if (g) {
            current_time = g->evaluation_time();
        }
    }

    for (const auto& [input_ts_value, link_index] : *_ref_observers) {
        if (!input_ts_value) continue;

        // Get the link at this index - for element-based refs, we need TSRefTargetLink
        TSRefTargetLink* ref_link = input_ts_value->ref_link_at(link_index);

        if (ref_link) {
            // TSRefTargetLink: use rebind_target_element for container element references
            ref_link->rebind_target_element(container, elem_index, current_time);
        } else {
            // Regular TSLink cannot handle element-based binding (needs direct TSValue*)
        }
    }
}

}  // namespace hgraph
