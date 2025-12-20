//
// Created by Claude on 17/12/2025.
//
// AccessStrategy - Hierarchical access strategies for TSInput
//
// Design principles:
// - Strategies form a tree mirroring the type structure
// - Each strategy handles transformation at its level
// - Subscriptions managed per-strategy (not centralized)
// - Storage allocated only where transformation occurs
//
// Strategy types:
// - DirectAccess: Simple delegation, no transformation
// - CollectionAccess: Has child strategies for elements
// - RefObserverAccess: Observes REF output, rebinds on change
// - RefWrapperAccess: Wraps non-REF output as REF value
//

#ifndef HGRAPH_ACCESS_STRATEGY_H
#define HGRAPH_ACCESS_STRATEGY_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/time_series_value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <vector>
#include <optional>

namespace hgraph::ts {

// Forward declarations
class TSOutput;
class TSInput;

// ============================================================================
// AccessStrategy - Base class for hierarchical access strategies
// ============================================================================

/**
 * AccessStrategy - Abstract interface for hierarchical value access
 *
 * Each strategy handles transformation at one level of the type hierarchy.
 * Strategies can have children for nested types (collections, etc).
 *
 * Lifecycle:
 * 1. Strategy created with owner reference
 * 2. bind() called with output when TSInput binds
 * 3. make_active() called when input becomes active
 * 4. rebind() may be called by parent RefObserver when reference changes
 * 5. make_passive() called when input becomes passive
 * 6. unbind() called when TSInput unbinds
 */
class AccessStrategy {
public:
    explicit AccessStrategy(TSInput* owner) : _owner(owner) {}
    virtual ~AccessStrategy() = default;

    // Non-copyable, movable
    AccessStrategy(const AccessStrategy&) = delete;
    AccessStrategy& operator=(const AccessStrategy&) = delete;
    AccessStrategy(AccessStrategy&&) noexcept = default;
    AccessStrategy& operator=(AccessStrategy&&) noexcept = default;

    // === Binding lifecycle ===

    /**
     * Bind to an output
     * Called when the TSInput binds to an output
     */
    virtual void bind(TSOutput* output) = 0;

    /**
     * Rebind to a different output
     * Called by parent RefObserver when reference changes
     */
    virtual void rebind(TSOutput* output) = 0;

    /**
     * Unbind from current output
     * Called when TSInput unbinds
     */
    virtual void unbind() = 0;

    // === Activation (subscription management) ===

    /**
     * Make this strategy active (subscribe to outputs)
     */
    virtual void make_active() = 0;

    /**
     * Make this strategy passive (unsubscribe from outputs)
     */
    virtual void make_passive() = 0;

    /**
     * Handle notification from subscribed output
     * Called by TSInput::notify() before notifying the owning node.
     * Strategies can override to detect changes (e.g., reference changes).
     */
    virtual void on_notify(engine_time_t time) {}

    // === Value access ===

    /**
     * Get the current value
     * Returns invalid view if not bound or no value available
     */
    [[nodiscard]] virtual value::ConstValueView value() const = 0;

    /**
     * Get modification tracker for this level
     */
    [[nodiscard]] virtual value::ModificationTracker tracker() const = 0;

    // === Query methods ===

    /**
     * Check if this level has a value
     */
    [[nodiscard]] virtual bool has_value() const = 0;

    /**
     * Check if value was modified at the given time
     */
    [[nodiscard]] virtual bool modified_at(engine_time_t time) const = 0;

    /**
     * Get the last modification time
     */
    [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

    // === Owner access ===
    [[nodiscard]] TSInput* owner() const { return _owner; }

    /**
     * Get the current evaluation time from the owner's graph context
     * Returns MIN_DT if the owner or graph is not available
     */
    [[nodiscard]] engine_time_t get_evaluation_time() const;

    // === Bound output access ===
    /**
     * Get the output this strategy is bound to for value purposes
     * For RefObserver, returns target_output (what REF points to)
     * For others, returns the directly bound output
     */
    [[nodiscard]] virtual TSOutput* bound_output() const = 0;

protected:
    TSInput* _owner;
};

// ============================================================================
// DirectAccessStrategy - Simple delegation with no transformation
// ============================================================================

/**
 * DirectAccessStrategy - Delegates directly to bound output
 *
 * Used when input and output types match exactly.
 * No storage needed - value comes directly from output.
 *
 * Subscription:
 * - Subscribes to output when active
 * - Unsubscribes when passive
 */
class DirectAccessStrategy : public AccessStrategy {
public:
    explicit DirectAccessStrategy(TSInput* owner)
        : AccessStrategy(owner) {}

    void bind(TSOutput* output) override;
    void rebind(TSOutput* output) override;
    void unbind() override;

    void make_active() override;
    void make_passive() override;

    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    [[nodiscard]] bool has_value() const override;
    [[nodiscard]] bool modified_at(engine_time_t time) const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;

    [[nodiscard]] TSOutput* output() const { return _output; }
    [[nodiscard]] TSOutput* bound_output() const override { return _output; }

private:
    TSOutput* _output{nullptr};
};

// ============================================================================
// CollectionAccessStrategy - Has child strategies for elements
// ============================================================================

/**
 * CollectionAccessStrategy - Handles collection types with child strategies
 *
 * Used for TSL, TSB when children need different access strategies.
 * May have storage if children perform transformations.
 *
 * Subscription:
 * - Propagates make_active/make_passive to child strategies
 * - May subscribe to peered output directly if no children transform
 */
class CollectionAccessStrategy : public AccessStrategy {
public:
    CollectionAccessStrategy(TSInput* owner, size_t element_count);

    void bind(TSOutput* output) override;
    void rebind(TSOutput* output) override;
    void unbind() override;

    void make_active() override;
    void make_passive() override;

    /**
     * Propagate on_notify to child strategies
     */
    void on_notify(engine_time_t time) override;

    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    [[nodiscard]] bool has_value() const override;
    [[nodiscard]] bool modified_at(engine_time_t time) const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;

    // === Child management ===

    /**
     * Add a child strategy for an element
     */
    void set_child(size_t index, std::unique_ptr<AccessStrategy> child);

    /**
     * Get child strategy for element
     */
    [[nodiscard]] AccessStrategy* child(size_t index) const {
        return index < _children.size() ? _children[index].get() : nullptr;
    }

    [[nodiscard]] size_t child_count() const { return _children.size(); }

    // === Storage management ===

    /**
     * Create storage for transformed values
     * Called when children perform transformations
     */
    void create_storage(const value::TypeMeta* schema);

    [[nodiscard]] bool has_storage() const { return _storage.has_value(); }

    [[nodiscard]] TSOutput* output() const { return _output; }
    [[nodiscard]] TSOutput* bound_output() const override { return _output; }

private:
    TSOutput* _output{nullptr};
    std::vector<std::unique_ptr<AccessStrategy>> _children;
    std::optional<value::TimeSeriesValue> _storage;
};

// ============================================================================
// RefObserverAccessStrategy - Observes REF output, rebinds on change
// ============================================================================

/**
 * RefObserverAccessStrategy - Non-REF input bound to REF output
 *
 * Observes the reference output and rebinds child strategy when reference changes.
 * Reports modified when reference changes (delta synthesis).
 *
 * Subscription rules:
 * - ALWAYS subscribed to _ref_output (at bind time, regardless of active state)
 * - Child strategy activated/deactivated based on active state
 */
class RefObserverAccessStrategy : public AccessStrategy {
public:
    RefObserverAccessStrategy(TSInput* owner, std::unique_ptr<AccessStrategy> child);

    void bind(TSOutput* output) override;
    void rebind(TSOutput* output) override;
    void unbind() override;

    void make_active() override;
    void make_passive() override;

    /**
     * Handle notification - detect reference changes
     */
    void on_notify(engine_time_t time) override;

    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    [[nodiscard]] bool has_value() const override;
    [[nodiscard]] bool modified_at(engine_time_t time) const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;

    /**
     * Called when the reference value changes
     * Updates target and rebinds child strategy
     */
    void on_reference_changed(TSOutput* new_target, engine_time_t time);

    [[nodiscard]] TSOutput* ref_output() const { return _ref_output; }
    [[nodiscard]] TSOutput* target_output() const { return _target_output; }
    [[nodiscard]] TSOutput* bound_output() const override { return _target_output; }
    [[nodiscard]] AccessStrategy* child_strategy() const { return _child.get(); }

private:
    /**
     * Resolve the target output from the REF output's value
     */
    TSOutput* resolve_ref_target(TSOutput* ref_output) const;

    /**
     * Update target and rebind child
     */
    void update_target(TSOutput* new_target, engine_time_t time);

    TSOutput* _ref_output{nullptr};      // The REF output (always subscribed)
    TSOutput* _target_output{nullptr};   // Current target (what REF points to)
    std::unique_ptr<AccessStrategy> _child;  // Strategy for accessing target's value
    engine_time_t _sample_time{MIN_DT};  // When we last rebound
    mutable engine_time_t _last_notify_time{MIN_DT};  // Last notification time (for Python-managed types)
};

// ============================================================================
// RefWrapperAccessStrategy - Wraps non-REF output as REF value
// ============================================================================

/**
 * RefWrapperAccessStrategy - REF input bound to non-REF output
 *
 * Creates a TimeSeriesReference value wrapping the output.
 * Needs storage for the REF value.
 *
 * Subscription:
 * - Does NOT subscribe to the wrapped output
 * - Only tracks binding changes, not value changes
 */
class RefWrapperAccessStrategy : public AccessStrategy {
public:
    RefWrapperAccessStrategy(TSInput* owner, const value::TypeMeta* ref_schema);

    void bind(TSOutput* output) override;
    void rebind(TSOutput* output) override;
    void unbind() override;

    void make_active() override;
    void make_passive() override;

    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    [[nodiscard]] bool has_value() const override { return _wrapped_output != nullptr; }
    [[nodiscard]] bool modified_at(engine_time_t time) const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;

    [[nodiscard]] TSOutput* wrapped_output() const { return _wrapped_output; }
    [[nodiscard]] TSOutput* bound_output() const override { return _wrapped_output; }

private:
    TSOutput* _wrapped_output{nullptr};
    value::TimeSeriesValue _storage;  // Holds the REF value
    engine_time_t _bind_time{MIN_DT};
};

// ============================================================================
// ElementAccessStrategy - Accesses a specific element of a collection
// ============================================================================

/**
 * ElementAccessStrategy - Accesses an element of a collection output via navigation
 *
 * Used for TSL elements and TSB fields when the collection has no child outputs.
 * Instead of binding to individual element outputs (which don't exist), this
 * strategy navigates via views to access element-specific values.
 *
 * Subscription:
 * - Subscribes to the parent collection output
 * - Element modification detected via view navigation
 */
class ElementAccessStrategy : public AccessStrategy {
public:
    enum class NavigationKind : uint8_t {
        ListElement,   // TSL - use view.element(index)
        BundleField    // TSB - use view.field(index)
    };

    ElementAccessStrategy(TSInput* owner, size_t index, NavigationKind kind)
        : AccessStrategy(owner), _index(index), _kind(kind) {}

    void bind(TSOutput* output) override;
    void rebind(TSOutput* output) override;
    void unbind() override;

    void make_active() override;
    void make_passive() override;

    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    [[nodiscard]] bool has_value() const override;
    [[nodiscard]] bool modified_at(engine_time_t time) const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;

    [[nodiscard]] TSOutput* parent_output() const { return _parent_output; }
    [[nodiscard]] TSOutput* bound_output() const override { return _parent_output; }
    [[nodiscard]] size_t index() const { return _index; }

private:
    /**
     * Navigate to the element's output view
     */
    [[nodiscard]] value::TimeSeriesValueView get_element_view() const;

    TSOutput* _parent_output{nullptr};
    size_t _index;
    NavigationKind _kind;
};

// ============================================================================
// Strategy builder
// ============================================================================

/**
 * Build an access strategy tree for the given input/output schema combination
 *
 * Walks both schemas to determine what transformations are needed at each level.
 * Returns the root strategy for the tree.
 *
 * @param input_meta The input's type metadata
 * @param output_meta The output's type metadata
 * @param owner The TSInput that will own this strategy tree
 * @return Root access strategy (may have children)
 */
std::unique_ptr<AccessStrategy> build_access_strategy(
    const TimeSeriesTypeMeta* input_meta,
    const TimeSeriesTypeMeta* output_meta,
    TSInput* owner);

/**
 * Check if a strategy is a DirectAccessStrategy (no transformation)
 * Used to determine if parent needs storage
 */
bool is_direct_access(const AccessStrategy* strategy);

} // namespace hgraph::ts

#endif // HGRAPH_ACCESS_STRATEGY_H
