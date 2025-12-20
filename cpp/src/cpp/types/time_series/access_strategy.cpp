//
// Created by Claude on 17/12/2025.
//
// AccessStrategy implementation - Hierarchical access strategies for TSInput
//

#include <hgraph/types/time_series/access_strategy.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/node.h>
#include <hgraph/types/graph.h>

namespace hgraph::ts {

// ============================================================================
// AccessStrategy base class implementation
// ============================================================================

engine_time_t AccessStrategy::get_evaluation_time() const {
    if (_owner) {
        auto* node = _owner->owning_node();
        if (node) {
            auto* graph = node->graph();
            if (graph) {
                return graph->evaluation_time();
            }
        }
    }
    return MIN_DT;
}

// ============================================================================
// DirectAccessStrategy implementation
// ============================================================================

void DirectAccessStrategy::bind(TSOutput* output) {
    _output = output;
}

void DirectAccessStrategy::rebind(TSOutput* output) {
    // For direct access, rebind is same as bind
    _output = output;
}

void DirectAccessStrategy::unbind() {
    // Unsubscribe if we were active
    if (_output && _owner && _owner->active()) {
        _output->unsubscribe(_owner);
    }
    _output = nullptr;
}

void DirectAccessStrategy::make_active() {
    if (_output && _owner) {
        _output->subscribe(_owner);
    }
}

void DirectAccessStrategy::make_passive() {
    if (_output && _owner) {
        _output->unsubscribe(_owner);
    }
}

value::ConstValueView DirectAccessStrategy::value() const {
    return _output ? _output->value() : value::ConstValueView{};
}

value::ModificationTracker DirectAccessStrategy::tracker() const {
    return _output
        ? _output->underlying().underlying_tracker().tracker()
        : value::ModificationTracker{};
}

bool DirectAccessStrategy::has_value() const {
    return _output && _output->has_value();
}

bool DirectAccessStrategy::modified_at(engine_time_t time) const {
    return _output && _output->modified_at(time);
}

engine_time_t DirectAccessStrategy::last_modified_time() const {
    return _output ? _output->last_modified_time() : MIN_DT;
}

// ============================================================================
// CollectionAccessStrategy implementation
// ============================================================================

CollectionAccessStrategy::CollectionAccessStrategy(TSInput* owner, size_t element_count)
    : AccessStrategy(owner)
    , _children(element_count) {}

void CollectionAccessStrategy::bind(TSOutput* output) {
    _output = output;

    // Bind child strategies to corresponding output elements
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && output) {
            _children[i]->bind(output);
        }
    }
}

void CollectionAccessStrategy::rebind(TSOutput* output) {
    _output = output;

    // Rebind all child strategies
    for (size_t i = 0; i < _children.size(); ++i) {
        if (_children[i] && output) {
            _children[i]->rebind(output);
        }
    }
}

void CollectionAccessStrategy::unbind() {
    // Unbind all children first
    for (auto& child : _children) {
        if (child) {
            child->unbind();
        }
    }
    _output = nullptr;
}

void CollectionAccessStrategy::make_active() {
    // For dynamic collections (like TSD) that have no fixed children,
    // subscribe directly to the output to receive notifications
    if (_output && _owner && _children.empty()) {
        _output->subscribe(_owner);
    }

    // Propagate to all child strategies
    for (auto& child : _children) {
        if (child) {
            child->make_active();
        }
    }
}

void CollectionAccessStrategy::make_passive() {
    // Unsubscribe from direct output subscription (if we subscribed in make_active)
    if (_output && _owner && _children.empty()) {
        _output->unsubscribe(_owner);
    }

    // Propagate to all child strategies
    for (auto& child : _children) {
        if (child) {
            child->make_passive();
        }
    }
}

void CollectionAccessStrategy::on_notify(engine_time_t time) {
    // Propagate to all child strategies
    for (auto& child : _children) {
        if (child) {
            child->on_notify(time);
        }
    }
}

value::ConstValueView CollectionAccessStrategy::value() const {
    // If we have storage (transformation needed), return from storage
    if (_storage) {
        return _storage->value();
    }
    // Otherwise delegate to output
    return _output ? _output->value() : value::ConstValueView{};
}

value::ModificationTracker CollectionAccessStrategy::tracker() const {
    if (_storage) {
        return _storage->underlying_tracker().tracker();
    }
    return _output
        ? _output->underlying().underlying_tracker().tracker()
        : value::ModificationTracker{};
}

bool CollectionAccessStrategy::has_value() const {
    // For Python-managed collections (no C++ storage), check the bound output directly
    // This handles TSL, TSB, TSD where values are stored in Python
    if (_output) {
        if (_output->has_value()) return true;

        // Fallback: Check if output was ever modified (Python-managed case)
        if (_output->last_modified_time() != MIN_DT) {
            return true;  // Was modified at some point, so has value
        }
    }

    // Otherwise check if any child has value (for C++ storage)
    for (const auto& child : _children) {
        if (child && child->has_value()) {
            return true;
        }
    }
    return false;
}

bool CollectionAccessStrategy::modified_at(engine_time_t time) const {
    // For Python-managed collections, check the bound output directly
    if (_output && _output->modified_at(time)) {
        return true;
    }

    // Otherwise check if any child is modified (for C++ storage)
    for (const auto& child : _children) {
        if (child && child->modified_at(time)) {
            return true;
        }
    }
    return false;
}

engine_time_t CollectionAccessStrategy::last_modified_time() const {
    engine_time_t max_time = MIN_DT;

    // For Python-managed collections, check the bound output
    if (_output) {
        max_time = std::max(max_time, _output->last_modified_time());
    }

    // Also check children's last_modified_time (for C++ storage)
    for (const auto& child : _children) {
        if (child) {
            max_time = std::max(max_time, child->last_modified_time());
        }
    }
    return max_time;
}

void CollectionAccessStrategy::set_child(size_t index, std::unique_ptr<AccessStrategy> child) {
    if (index < _children.size()) {
        _children[index] = std::move(child);
    }
}

void CollectionAccessStrategy::create_storage(const value::TypeMeta* schema) {
    if (schema) {
        _storage.emplace(schema);
    }
}

// ============================================================================
// RefObserverAccessStrategy implementation
// ============================================================================

RefObserverAccessStrategy::RefObserverAccessStrategy(TSInput* owner, std::unique_ptr<AccessStrategy> child)
    : AccessStrategy(owner)
    , _child(std::move(child)) {}

void RefObserverAccessStrategy::bind(TSOutput* output) {
    _ref_output = output;

    // Always subscribe to REF output for reference changes (regardless of active state)
    if (_ref_output && _owner) {
        _ref_output->subscribe(_owner);
    }

    // Resolve initial target and bind child
    if (_ref_output) {
        auto* initial_target = resolve_ref_target(_ref_output);
        update_target(initial_target, MIN_DT);
    }
}

void RefObserverAccessStrategy::rebind(TSOutput* output) {
    // Full unbind then bind for REF observer
    unbind();
    bind(output);
}

void RefObserverAccessStrategy::unbind() {
    // Unbind child first
    if (_child) {
        _child->unbind();
    }

    // Unsubscribe from REF output (always subscribed)
    if (_ref_output && _owner) {
        _ref_output->unsubscribe(_owner);
    }

    _ref_output = nullptr;
    _target_output = nullptr;
    _sample_time = MIN_DT;
}

void RefObserverAccessStrategy::make_active() {
    // Already subscribed to _ref_output (done at bind time)
    // Activate child strategy (subscribes to target)
    if (_child) {
        _child->make_active();
    }
}

void RefObserverAccessStrategy::make_passive() {
    // Stay subscribed to _ref_output
    // Deactivate child strategy (unsubscribes from target)
    if (_child) {
        _child->make_passive();
    }
}

void RefObserverAccessStrategy::on_notify(engine_time_t time) {
    // Track the notification time for Python-managed types
    _last_notify_time = time;

    // Check if the reference target has changed
    auto* new_target = resolve_ref_target(_ref_output);
    if (new_target != _target_output) {
        on_reference_changed(new_target, time);
    }
}

value::ConstValueView RefObserverAccessStrategy::value() const {
    if (!_child) {
        return {};
    }
    return _child->value();
}

value::ModificationTracker RefObserverAccessStrategy::tracker() const {
    return _child ? _child->tracker() : value::ModificationTracker{};
}

bool RefObserverAccessStrategy::has_value() const {
    // First check child strategy
    if (_child && _child->has_value()) {
        return true;
    }

    // For Python-managed collections, child may not report has_value correctly
    // If we have a target output that was sampled, consider it as having a value
    if (_target_output && _sample_time != MIN_DT) {
        return true;
    }

    return false;
}

bool RefObserverAccessStrategy::modified_at(engine_time_t time) const {
    // Modified if just rebound
    if (_sample_time == time) {
        return true;
    }

    // Check child strategy
    if (_child && _child->modified_at(time)) {
        return true;
    }

    // For Python-managed collections, C++ can't track modifications via TSOutput.
    // Use the notification time we tracked in on_notify()
    if (_last_notify_time == time) {
        return true;
    }

    // Also check target output directly
    if (_target_output && _target_output->modified_at(time)) {
        return true;
    }

    return false;
}

engine_time_t RefObserverAccessStrategy::last_modified_time() const {
    if (_child) {
        return std::max(_sample_time, _child->last_modified_time());
    }
    return _sample_time;
}

void RefObserverAccessStrategy::on_reference_changed(TSOutput* new_target, engine_time_t time) {
    // Deactivate child if owner is active
    if (_owner && _owner->active() && _child) {
        _child->make_passive();
    }

    // Update target and rebind child
    update_target(new_target, time);

    // Reactivate child if owner is active
    if (_owner && _owner->active() && _child) {
        _child->make_active();
    }

    // Notify owner (delta synthesis)
    if (_owner) {
        _owner->notify(time);
    }
}

TSOutput* RefObserverAccessStrategy::resolve_ref_target(TSOutput* ref_output) const {
    if (!ref_output) return nullptr;

    // Get the REF value from the output
    auto ref_value = ref_output->value();
    if (!ref_value.valid() || !ref_value.ref_is_bound()) {
        return nullptr;
    }

    // Extract the target TSOutput from the ValueRef's owner field
    auto* target_ref = ref_value.ref_target();
    if (!target_ref || !target_ref->has_owner()) {
        return nullptr;
    }

    // The owner field was set when ref_bind() was called with the target TSOutput*
    return static_cast<TSOutput*>(target_ref->owner);
}

void RefObserverAccessStrategy::update_target(TSOutput* new_target, engine_time_t time) {
    _target_output = new_target;
    _sample_time = time;

    if (_child && _target_output) {
        _child->rebind(_target_output);
    }
}

// ============================================================================
// RefWrapperAccessStrategy implementation
// ============================================================================

RefWrapperAccessStrategy::RefWrapperAccessStrategy(TSInput* owner, const value::TypeMeta* ref_schema)
    : AccessStrategy(owner)
    , _storage(ref_schema) {}

void RefWrapperAccessStrategy::bind(TSOutput* output) {
    _wrapped_output = output;
    _bind_time = MIN_DT;

    // Initialize storage with REF pointing to output
    if (output) {
        auto storage_view = _storage.view();

        // Create ValueRef with output's value data, tracker, schema, and output as owner
        auto& ts_value = output->underlying();
        value::ValueRef ref{
            const_cast<void*>(ts_value.value().data()),
            ts_value.underlying_tracker().storage(),
            ts_value.schema(),
            output  // owner - the TSOutput* for REF resolution
        };
        storage_view.ref_bind(ref, _bind_time);
    }
}

void RefWrapperAccessStrategy::rebind(TSOutput* output) {
    _wrapped_output = output;
    _bind_time = MIN_DT;

    if (output) {
        auto storage_view = _storage.view();
        // Create ValueRef with output's value data, tracker, schema, and output as owner
        auto& ts_value = output->underlying();
        value::ValueRef ref{
            const_cast<void*>(ts_value.value().data()),
            ts_value.underlying_tracker().storage(),
            ts_value.schema(),
            output  // owner - the TSOutput* for REF resolution
        };
        storage_view.ref_bind(ref, _bind_time);
    }
}

void RefWrapperAccessStrategy::unbind() {
    _wrapped_output = nullptr;
    auto storage_view = _storage.view();
    storage_view.ref_clear(MIN_DT);
    _bind_time = MIN_DT;
}

value::ConstValueView RefWrapperAccessStrategy::value() const {
    return _storage.value();
}

value::ModificationTracker RefWrapperAccessStrategy::tracker() const {
    return _storage.underlying_tracker().tracker();
}

bool RefWrapperAccessStrategy::modified_at(engine_time_t time) const {
    // REF is modified if:
    // 1. We just bound to the output at this time, OR
    // 2. The underlying wrapped output is modified at this time
    if (_bind_time == time) return true;
    return _wrapped_output && _wrapped_output->modified_at(time);
}

engine_time_t RefWrapperAccessStrategy::last_modified_time() const {
    // Return the max of bind_time and wrapped output's last modified time
    if (_wrapped_output) {
        return std::max(_bind_time, _wrapped_output->last_modified_time());
    }
    return _bind_time;
}

void RefWrapperAccessStrategy::make_active() {
    // Set bind time to current evaluation time - this marks the REF as "modified"
    // at the first tick after activation, which is when the REF becomes available
    auto eval_time = get_evaluation_time();
    if (eval_time != MIN_DT && _bind_time == MIN_DT) {
        _bind_time = eval_time;
    }

    // REF wrapper subscribes to the wrapped output so that when the output changes,
    // the notification propagates to the owner (even though the REF value itself doesn't change,
    // dereferencing it yields a different value)
    if (_wrapped_output && _owner) {
        _wrapped_output->subscribe(_owner);
    }
}

void RefWrapperAccessStrategy::make_passive() {
    if (_wrapped_output && _owner) {
        _wrapped_output->unsubscribe(_owner);
    }
}

// ============================================================================
// ElementAccessStrategy implementation
// ============================================================================

void ElementAccessStrategy::bind(TSOutput* output) {
    _parent_output = output;
}

void ElementAccessStrategy::rebind(TSOutput* output) {
    _parent_output = output;
}

void ElementAccessStrategy::unbind() {
    // Unsubscribe if we were active
    if (_parent_output && _owner && _owner->active()) {
        _parent_output->unsubscribe(_owner);
    }
    _parent_output = nullptr;
}

void ElementAccessStrategy::make_active() {
    if (_parent_output && _owner) {
        _parent_output->subscribe(_owner);
    }
}

void ElementAccessStrategy::make_passive() {
    if (_parent_output && _owner) {
        _parent_output->unsubscribe(_owner);
    }
}

value::TSView ElementAccessStrategy::get_element_view() const {
    if (!_parent_output) return {};

    auto view = _parent_output->view();
    if (_kind == NavigationKind::ListElement) {
        return view.element(_index);
    } else {
        return view.field(_index);
    }
}

value::ConstValueView ElementAccessStrategy::value() const {
    auto elem_view = get_element_view();
    if (!elem_view.valid()) return {};
    return elem_view.value_view();
}

value::ModificationTracker ElementAccessStrategy::tracker() const {
    auto elem_view = get_element_view();
    if (!elem_view.valid()) return {};
    return elem_view.tracker();
}

bool ElementAccessStrategy::has_value() const {
    auto elem_view = get_element_view();
    return elem_view.valid() && elem_view.has_value();
}

bool ElementAccessStrategy::modified_at(engine_time_t time) const {
    auto elem_view = get_element_view();
    return elem_view.valid() && elem_view.modified_at(time);
}

engine_time_t ElementAccessStrategy::last_modified_time() const {
    auto elem_view = get_element_view();
    return elem_view.valid() ? elem_view.last_modified_time() : MIN_DT;
}

// ============================================================================
// Strategy builder
// ============================================================================

namespace {

/**
 * Check if a type is a collection (TSL, TSB, TSD)
 */
bool is_collection(const TSMeta* meta) {
    if (!meta) return false;
    switch (meta->ts_kind) {
        case TSKind::TSL:
        case TSKind::TSB:
        case TSKind::TSD:
            return true;
        default:
            return false;
    }
}

/**
 * Get the inner type of a REF type
 */
const TSMeta* get_ref_inner_type(const TSMeta* meta) {
    if (!meta || !meta->is_reference()) return nullptr;
    auto* ref_meta = static_cast<const REFTypeMeta*>(meta);
    return ref_meta->value_ts_type;
}

/**
 * Get element count for collections
 */
size_t get_element_count(const TSMeta* meta) {
    if (!meta) return 0;

    switch (meta->ts_kind) {
        case TSKind::TSL: {
            auto* tsl_meta = static_cast<const TSLTypeMeta*>(meta);
            return tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : 0;
        }
        case TSKind::TSB: {
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(meta);
            return tsb_meta->fields.size();
        }
        default:
            return 0;
    }
}

/**
 * Get element type metadata for collections
 */
const TSMeta* get_element_meta(const TSMeta* meta, size_t index) {
    if (!meta) return nullptr;

    switch (meta->ts_kind) {
        case TSKind::TSL: {
            auto* tsl_meta = static_cast<const TSLTypeMeta*>(meta);
            return tsl_meta->element_ts_type;
        }
        case TSKind::TSB: {
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(meta);
            return index < tsb_meta->fields.size() ? tsb_meta->fields[index].type : nullptr;
        }
        case TSKind::TSD: {
            auto* tsd_meta = static_cast<const TSDTypeMeta*>(meta);
            return tsd_meta->value_ts_type;
        }
        default:
            return nullptr;
    }
}

} // anonymous namespace

std::unique_ptr<AccessStrategy> build_access_strategy(
    const TSMeta* input_meta,
    const TSMeta* output_meta,
    TSInput* owner)
{
    if (!input_meta || !output_meta) {
        return std::make_unique<DirectAccessStrategy>(owner);
    }

    // Case 1: REF output, non-REF input -> RefObserver
    if (output_meta->is_reference() && !input_meta->is_reference()) {
        // Build child strategy for dereferenced output type vs input type
        auto* deref_output_meta = get_ref_inner_type(output_meta);
        auto child = build_access_strategy(input_meta, deref_output_meta, owner);
        return std::make_unique<RefObserverAccessStrategy>(owner, std::move(child));
    }

    // Case 2: REF input, non-REF output -> RefWrapper
    if (input_meta->is_reference() && !output_meta->is_reference()) {
        // Get the schema for the REF value
        auto* ref_schema = input_meta->value_schema();
        return std::make_unique<RefWrapperAccessStrategy>(owner, ref_schema);
    }

    // Case 3: Both collections -> recurse for children
    if (is_collection(input_meta) && is_collection(output_meta)) {
        size_t count = get_element_count(input_meta);
        auto strategy = std::make_unique<CollectionAccessStrategy>(owner, count);

        // Determine navigation kind based on collection type
        auto nav_kind = (input_meta->ts_kind == TSKind::TSL)
            ? ElementAccessStrategy::NavigationKind::ListElement
            : ElementAccessStrategy::NavigationKind::BundleField;

        // Check if children need transformation
        bool needs_storage = false;
        for (size_t i = 0; i < count; ++i) {
            auto* child_input_meta = get_element_meta(input_meta, i);
            auto* child_output_meta = get_element_meta(output_meta, i);
            std::unique_ptr<AccessStrategy> child_strategy;

            // Check if child needs transformation
            bool child_needs_transform =
                (child_input_meta && child_output_meta) &&
                (child_input_meta->is_reference() != child_output_meta->is_reference() ||
                 (is_collection(child_input_meta) && is_collection(child_output_meta)));

            if (child_needs_transform) {
                // Child needs REF or nested collection handling - recursively build
                child_strategy = build_access_strategy(child_input_meta, child_output_meta, owner);
                if (!is_direct_access(child_strategy.get())) {
                    needs_storage = true;
                }
            } else {
                // Simple element access - use ElementAccessStrategy for view navigation
                child_strategy = std::make_unique<ElementAccessStrategy>(owner, i, nav_kind);
            }

            strategy->set_child(i, std::move(child_strategy));
        }

        if (needs_storage && input_meta->value_schema()) {
            strategy->create_storage(input_meta->value_schema());
        }

        return strategy;
    }

    // Case 4: Direct match -> DirectAccess
    return std::make_unique<DirectAccessStrategy>(owner);
}

bool is_direct_access(const AccessStrategy* strategy) {
    return dynamic_cast<const DirectAccessStrategy*>(strategy) != nullptr ||
           dynamic_cast<const ElementAccessStrategy*>(strategy) != nullptr;
}

} // namespace hgraph::ts
