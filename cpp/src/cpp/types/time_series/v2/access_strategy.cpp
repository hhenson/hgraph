//
// Created by Claude on 17/12/2025.
//
// AccessStrategy implementation - Hierarchical access strategies for TSInput
//

#include <hgraph/types/time_series/v2/access_strategy.h>
#include <hgraph/types/time_series/v2/ts_output.h>
#include <hgraph/types/time_series/v2/ts_input.h>
#include <hgraph/types/time_series/ts_type_meta.h>

namespace hgraph::ts {

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
            // Get the element output from the collection output
            // For now, use the view navigation - actual implementation
            // may need direct element access
            // TODO: TSOutput needs element_output(size_t) method
            // For now, children will receive the parent output and
            // navigate to their element
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
    // Propagate to all child strategies
    for (auto& child : _children) {
        if (child) {
            child->make_active();
        }
    }
}

void CollectionAccessStrategy::make_passive() {
    // Propagate to all child strategies
    for (auto& child : _children) {
        if (child) {
            child->make_passive();
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
    // Has value if any child has value
    for (const auto& child : _children) {
        if (child && child->has_value()) {
            return true;
        }
    }
    return false;
}

bool CollectionAccessStrategy::modified_at(engine_time_t time) const {
    // Modified if any child is modified
    for (const auto& child : _children) {
        if (child && child->modified_at(time)) {
            return true;
        }
    }
    return false;
}

engine_time_t CollectionAccessStrategy::last_modified_time() const {
    // Maximum of children's last_modified_time
    engine_time_t max_time = MIN_DT;
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

value::ConstValueView RefObserverAccessStrategy::value() const {
    return _child ? _child->value() : value::ConstValueView{};
}

value::ModificationTracker RefObserverAccessStrategy::tracker() const {
    return _child ? _child->tracker() : value::ModificationTracker{};
}

bool RefObserverAccessStrategy::has_value() const {
    return _child && _child->has_value();
}

bool RefObserverAccessStrategy::modified_at(engine_time_t time) const {
    // Modified if just rebound OR if child is modified
    if (_sample_time == time) return true;
    return _child ? _child->modified_at(time) : false;
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

    // The REF value contains a ValueRef pointing to the target
    // For now, we need to extract the target output from the ref
    // This requires the REF storage to track the output pointer
    // TODO: Implement ref target resolution via TimeSeriesReference
    // For now, return nullptr - actual implementation needs REF value access
    return nullptr;
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
    // TODO: Get current time from evaluation context
    _bind_time = MIN_DT;

    // Initialize storage with REF pointing to output
    if (output) {
        auto storage_view = _storage.view();
        // TODO: Create ValueRef from output and bind
        // storage_view.ref_bind(make_value_ref(output), _bind_time);
    }
}

void RefWrapperAccessStrategy::rebind(TSOutput* output) {
    _wrapped_output = output;
    // TODO: Get current time from evaluation context
    _bind_time = MIN_DT;

    if (output) {
        auto storage_view = _storage.view();
        // TODO: Update REF to point to new output
        // storage_view.ref_bind(make_value_ref(output), _bind_time);
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
    return _bind_time == time;
}

// ============================================================================
// Strategy builder
// ============================================================================

namespace {

/**
 * Check if a type is a collection (TSL, TSB, TSD)
 */
bool is_collection(const TimeSeriesTypeMeta* meta) {
    if (!meta) return false;
    switch (meta->ts_kind) {
        case TimeSeriesKind::TSL:
        case TimeSeriesKind::TSB:
        case TimeSeriesKind::TSD:
            return true;
        default:
            return false;
    }
}

/**
 * Get the inner type of a REF type
 */
const TimeSeriesTypeMeta* get_ref_inner_type(const TimeSeriesTypeMeta* meta) {
    if (!meta || !meta->is_reference()) return nullptr;
    auto* ref_meta = static_cast<const REFTypeMeta*>(meta);
    return ref_meta->value_ts_type;
}

/**
 * Get element count for collections
 */
size_t get_element_count(const TimeSeriesTypeMeta* meta) {
    if (!meta) return 0;

    switch (meta->ts_kind) {
        case TimeSeriesKind::TSL: {
            auto* tsl_meta = static_cast<const TSLTypeMeta*>(meta);
            return tsl_meta->size > 0 ? static_cast<size_t>(tsl_meta->size) : 0;
        }
        case TimeSeriesKind::TSB: {
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
const TimeSeriesTypeMeta* get_element_meta(const TimeSeriesTypeMeta* meta, size_t index) {
    if (!meta) return nullptr;

    switch (meta->ts_kind) {
        case TimeSeriesKind::TSL: {
            auto* tsl_meta = static_cast<const TSLTypeMeta*>(meta);
            return tsl_meta->element_ts_type;
        }
        case TimeSeriesKind::TSB: {
            auto* tsb_meta = static_cast<const TSBTypeMeta*>(meta);
            return index < tsb_meta->fields.size() ? tsb_meta->fields[index].type : nullptr;
        }
        case TimeSeriesKind::TSD: {
            auto* tsd_meta = static_cast<const TSDTypeMeta*>(meta);
            return tsd_meta->value_ts_type;
        }
        default:
            return nullptr;
    }
}

} // anonymous namespace

std::unique_ptr<AccessStrategy> build_access_strategy(
    const TimeSeriesTypeMeta* input_meta,
    const TimeSeriesTypeMeta* output_meta,
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

        // Check if children need transformation
        bool needs_storage = false;
        for (size_t i = 0; i < count; ++i) {
            auto* child_input_meta = get_element_meta(input_meta, i);
            auto* child_output_meta = get_element_meta(output_meta, i);
            auto child_strategy = build_access_strategy(child_input_meta, child_output_meta, owner);

            if (!is_direct_access(child_strategy.get())) {
                needs_storage = true;
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
    return dynamic_cast<const DirectAccessStrategy*>(strategy) != nullptr;
}

} // namespace hgraph::ts
