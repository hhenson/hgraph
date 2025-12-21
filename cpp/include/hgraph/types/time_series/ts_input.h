//
// Created by Claude on 16/12/2025.
//
// TSInput and TSInputView - Value-based time-series input implementation
//
// Design principles:
// - Input binds to an output (does not own data)
// - Provides read-only access to the bound output's value
// - Uses hierarchical AccessStrategy for binding:
//   * DirectAccess: Direct delegation to bound output
//   * CollectionAccess: Child strategies for collection elements
//   * RefObserverAccess: Non-REF input bound to REF output
//   * RefWrapperAccess: REF input bound to non-REF output
// - Views are path + pointer to source - never materialized
// - Chainable navigation API: input.view().field("price").element(0)
//

#ifndef HGRAPH_TS_INPUT_H
#define HGRAPH_TS_INPUT_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/access_strategy.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/time_series_value.h>
#include <string>
#include <vector>
#include <optional>
#include <memory>

namespace hgraph::ts {

// Forward declarations
class TSInput;
class TSInputView;
class TSInputBindableView;

// ============================================================================
// TSInputView - Read-only view into TSInput with path tracking
// ============================================================================

/**
 * TSInputView - Read-only view into a bound time-series input
 *
 * Features:
 * - Read-only access to the bound output's value
 * - Chainable navigation: view.field("price").element(0)
 * - Path tracking for debugging (uses ValuePath like TSView)
 * - NEVER materialized - always goes to source for fresh data
 *
 * Views hold a pointer to the AccessStrategy and navigate on demand.
 * This ensures REF observer scenarios work correctly when the target changes.
 */
class TSInputView {
public:
    TSInputView() = default;

    /**
     * Create a view rooted at an AccessStrategy
     */
    explicit TSInputView(AccessStrategy* source, const TSMeta* meta)
        : _source(source), _meta(meta), _path() {}

    /**
     * Create a view with a navigation path (for debugging and consistency with TSView)
     */
    TSInputView(AccessStrategy* source, const TSMeta* meta, value::ValuePath path)
        : _source(source), _meta(meta), _path(std::move(path)) {}

    // === Validity and type queries ===
    [[nodiscard]] bool valid() const { return _source != nullptr; }
    [[nodiscard]] const TSMeta* meta() const { return _meta; }
    [[nodiscard]] const TSMeta* ts_meta() const { return _meta; }
    [[nodiscard]] const value::TypeMeta* value_schema() const {
        return _meta ? _meta->value_schema() : nullptr;
    }
    [[nodiscard]] value::TypeKind kind() const {
        auto v = value_view();
        return v.valid() ? v.kind() : value::TypeKind::Scalar;
    }
    [[nodiscard]] TSKind ts_kind() const { return _meta ? _meta->ts_kind : TSKind::TS; }

    // === Path tracking ===
    [[nodiscard]] const value::ValuePath& path() const { return _path; }
    [[nodiscard]] std::string path_string() const { return _path.to_string(); }

    // === Underlying value view access (fresh every call) ===
    // When path is non-empty, navigates through bound output to get field value
    [[nodiscard]] value::ConstValueView value_view() const;

    // === Scalar value access (read-only) ===
    template<typename T>
    [[nodiscard]] const T& as() const {
        return value_view().as<T>();
    }

    // === Query methods (fresh every call) ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _source && _source->modified_at(time);
    }

    // When path is non-empty, checks if the specific field has a value
    [[nodiscard]] bool has_value() const;

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _source ? _source->last_modified_time() : MIN_DT;
    }

    // === Index-based navigation (chainable, read-only) ===

    /**
     * Navigate to an element by index (for TSL or TSB types)
     *
     * Works for:
     * - TSL (list types): navigates to element at index, uses element_meta()
     * - TSB (bundle types): navigates to field at index, uses field_meta()
     *
     * Returns a new view pointing to the child strategy if available.
     */
    [[nodiscard]] TSInputView element(size_t index) const;

    /**
     * Navigate to a bundle field by name (TSB only).
     */
    [[nodiscard]] TSInputView field(const std::string& name) const;

    [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
        auto child = element(index);
        return child.valid() && child.modified_at(time);
    }

    [[nodiscard]] size_t field_count() const {
        return value_view().field_count();
    }

    [[nodiscard]] size_t list_size() const {
        return value_view().list_size();
    }

    // === Set operations (read-only) ===
    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        return value_view().set_contains(elem);
    }

    [[nodiscard]] size_t set_size() const {
        return value_view().set_size();
    }

    // === Dict operations (read-only) ===
    template<typename K>
    [[nodiscard]] bool dict_contains(const K& key) const {
        return value_view().dict_contains(key);
    }

    template<typename K>
    [[nodiscard]] value::ConstValueView dict_get(const K& key) const {
        return value_view().dict_get(key);
    }

    // Dict entry navigation (chainable, read-only)
    template<typename K>
    [[nodiscard]] TSInputView entry(const K& key) const {
        // Dict entries don't have child strategies, navigate value directly
        if (!valid() || kind() != value::TypeKind::Dict) {
            return {};
        }
        auto entry_view = value_view().dict_get(key);
        if (!entry_view.valid()) {
            return {};
        }
        // For dict entries, we still use parent source but path indicates the entry
        auto value_meta = _meta ? _meta->value_meta() : nullptr;
        return {_source, value_meta, _path.with(0)};
    }

    [[nodiscard]] size_t dict_size() const {
        return value_view().dict_size();
    }

    // === Window operations (read-only) ===
    [[nodiscard]] value::ConstValueView window_get(size_t index) const {
        return value_view().window_get(index);
    }

    [[nodiscard]] size_t window_size() const {
        return value_view().window_size();
    }

    [[nodiscard]] bool window_empty() const {
        return value_view().window_empty();
    }

    [[nodiscard]] bool window_full() const {
        return value_view().window_full();
    }

    [[nodiscard]] engine_time_t window_timestamp(size_t index) const {
        return value_view().window_timestamp(index);
    }

    // === Ref operations (read-only) ===
    [[nodiscard]] bool ref_is_empty() const { return value_view().ref_is_empty(); }
    [[nodiscard]] bool ref_is_bound() const { return value_view().ref_is_bound(); }
    [[nodiscard]] bool ref_is_valid() const { return value_view().ref_is_valid(); }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return value_view().to_string();
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        std::string result = "TSInputView{path=";
        result += _path.to_string();
        result += ", value=\"";
        result += to_string();
        result += "\", modified=";
        result += modified_at(time) ? "true" : "false";
        result += "}";
        return result;
    }

    // === Delta view access ===

    /**
     * Get a delta view for accessing modification information.
     *
     * @param time The engine time to check for modifications
     * @return DeltaView for accessing delta values, or invalid view if not modified
     */
    [[nodiscard]] DeltaView delta_view(engine_time_t time) const {
        if (!valid() || !modified_at(time)) {
            return {};
        }
        auto tracker = _source->tracker();
        return {value_view(), tracker, _meta, time};
    }

    // === Access to source strategy (for wrapper construction) ===
    [[nodiscard]] AccessStrategy* source() const { return _source; }

    // === Access to bound output (for delta cache lookup) ===
    [[nodiscard]] TSOutput* bound_output() const {
        return _source ? _source->bound_output() : nullptr;
    }

private:
    AccessStrategy* _source{nullptr};
    const TSMeta* _meta{nullptr};
    value::ValuePath _path;
};

// ============================================================================
// TSInputBindableView - View for navigating schema and binding outputs
// ============================================================================

/**
 * TSInputBindableView - Schema navigation view for binding outputs
 *
 * Unlike TSInputView (which reads bound values), TSInputBindableView is used
 * during graph construction to navigate the type schema and bind outputs
 * at specific locations within the input's structure.
 *
 * Features:
 * - Navigate to nested fields using field(index) or field(name)
 * - Bind an output at the current position using bind(output)
 * - Tracks path from root for proper strategy tree construction
 * - Handles arbitrary nesting depth
 *
 * Usage:
 *   input->bindable_field(0).field(1).bind(output);  // Bind to input[0][1]
 *   input->bindable_field("price").bind(output);     // Bind to input.price
 */
class TSInputBindableView {
public:
    TSInputBindableView() = default;

    /**
     * Create a bindable view at the root of an input
     */
    explicit TSInputBindableView(TSInput* root);

    /**
     * Create a bindable view at a specific path within an input
     */
    TSInputBindableView(TSInput* root, std::vector<size_t> path, const TSMeta* meta);

    // === Validity and type information ===

    [[nodiscard]] bool valid() const { return _root != nullptr && _meta != nullptr; }
    [[nodiscard]] const TSMeta* meta() const { return _meta; }
    [[nodiscard]] const std::vector<size_t>& path() const { return _path; }
    [[nodiscard]] TSInput* root() const { return _root; }

    // === Navigation (chainable) ===

    /**
     * Navigate to an element by index (for TSL or TSB types)
     *
     * This is the standard method for index-based navigation during binding.
     * Works for:
     * - TSL (list types): navigates to element at index
     * - TSB (bundle types): navigates to field at index
     *
     * Returns an invalid view if:
     * - This view is invalid
     * - The metadata is not a TSL or TSB type
     * - The index is out of range
     */
    [[nodiscard]] TSInputBindableView element(size_t index) const;

    /**
     * Navigate to a bundle field by name (TSB only)
     *
     * Returns an invalid view if:
     * - This view is invalid
     * - The metadata is not a bundle type
     * - The field name is not found
     */
    [[nodiscard]] TSInputBindableView field(const std::string& name) const;

    // === Binding ===

    /**
     * Bind an output view at this location in the input's structure
     *
     * This creates/navigates the CollectionAccessStrategy tree as needed,
     * then creates an appropriate child strategy for binding to the view.
     *
     * The view can point to any level of nesting within an output, enabling
     * binding to specific fields or elements of a larger output structure.
     *
     * @param output_view The output view to bind at this location
     * @throws std::runtime_error if the view is invalid
     */
    void bind(value::TSView output_view);

private:
    TSInput* _root{nullptr};           // The root input
    std::vector<size_t> _path;         // Path from root to this position (field indices)
    const TSMeta* _meta{nullptr};  // Metadata at this position
};

// ============================================================================
// TSInput - Time-series input with hierarchical access strategies
// ============================================================================

/**
 * TSInput - Value-based time-series input implementation
 *
 * Behavior:
 * - Binds to a TSOutput to receive data
 * - Provides read-only access to the bound output's value
 * - Does NOT own data (the output owns it, or strategies have storage for transformations)
 *
 * Binding uses hierarchical AccessStrategy:
 * - DirectAccess: Direct delegation to bound output (most common)
 * - CollectionAccess: Child strategies for TSL/TSB elements
 * - RefObserverAccess: Non-REF input bound to REF output
 * - RefWrapperAccess: REF input bound to non-REF output
 *
 * The strategy tree is built at bind time by comparing input/output schemas.
 *
 * Construction:
 * - Use TSMeta::make_input() to create instances
 */
class TSInput : public Notifiable {
public:
    using ptr = TSInput*;
    using s_ptr = std::shared_ptr<TSInput>;

    TSInput() = default;

    /**
     * Construct from type metadata and owning node
     */
    TSInput(const TSMeta* meta, node_ptr owning_node)
        : _meta(meta)
        , _owning_node(owning_node) {}

    // Move only
    TSInput(TSInput&&) noexcept = default;
    TSInput& operator=(TSInput&&) noexcept = default;
    TSInput(const TSInput&) = delete;
    TSInput& operator=(const TSInput&) = delete;

    // === Validity and type information ===
    [[nodiscard]] bool valid() const { return _meta != nullptr; }
    [[nodiscard]] const TSMeta* meta() const { return _meta; }
    [[nodiscard]] TSKind ts_kind() const { return _meta ? _meta->ts_kind : TSKind::TS; }

    // === Node parentage ===
    [[nodiscard]] node_ptr owning_node() const { return _owning_node; }

    // === Binding State ===

    /**
     * Check if this input is bound
     */
    [[nodiscard]] bool bound() const { return _strategy != nullptr; }

    /**
     * Get the access strategy
     */
    [[nodiscard]] AccessStrategy* strategy() const { return _strategy.get(); }

    // === Binding Operations ===

    /**
     * Bind this input to an output view
     *
     * Builds a strategy tree based on schema comparison:
     * - Matching types: DirectAccess
     * - Non-REF input to REF output: RefObserverAccess
     * - REF input to non-REF output: RefWrapperAccess
     * - Collections with nested differences: CollectionAccess with children
     *
     * The view can point to any level of nesting within an output, enabling
     * binding to specific fields or elements of a larger output structure.
     *
     * @param output_view The output view to bind to
     */
    void bind_output(value::TSView output_view);

    /**
     * Unbind this input from its current output(s)
     */
    void unbind_output();

    // === Activation ===

    /**
     * Make this input active (subscribes to bound outputs)
     */
    void make_active();

    /**
     * Make this input passive (unsubscribes from outputs)
     */
    void make_passive();

    [[nodiscard]] bool active() const { return _active; }

    // === Notification (from Notifiable interface) ===

    /**
     * Called by bound output(s) when they are modified
     *
     * Propagates notification to the owning node.
     */
    void notify(engine_time_t time) override;

    // === View creation with path tracking ===

    /**
     * Create a read-only view into the bound output's value
     *
     * The view points to the strategy and fetches fresh data on each access.
     * Returns an invalid view if not bound.
     */
    [[nodiscard]] TSInputView view() const;

    // === Direct value access (convenience) ===

    template<typename T>
    [[nodiscard]] const T& as() const {
        auto v = value();
        if (!v.valid()) {
            throw std::runtime_error("TSInput::as() called on unbound or invalid input");
        }
        return v.as<T>();
    }

    [[nodiscard]] value::ConstValueView value() const {
        return _strategy ? _strategy->value() : value::ConstValueView{};
    }

    // === Element/Field navigation (returns bindable view for binding during construction) ===

    /**
     * Get a bindable view at a specific index (for TSL or TSB types)
     *
     * Use this to bind different elements of a collection or bundle input:
     *   input->element(0).bind(output1);
     *   input->element(1).element(0).bind(output2);  // Nested binding
     *
     * Works for:
     * - TSL (list types): navigates to element at index
     * - TSB (bundle types): navigates to field at index
     *
     * For runtime value access, use value().element() or view().element() instead.
     *
     * @param index Index within the collection/bundle
     * @return Bindable view at the specified index (invalid if out of range)
     */
    [[nodiscard]] TSInputBindableView element(size_t index) const;

    /**
     * Get a bindable view by field name (TSB only)
     *
     * @param name Field name within the bundle
     * @return Bindable view at the specified field (invalid if not a bundle or name not found)
     */
    [[nodiscard]] TSInputBindableView field(const std::string& name) const;

    // === Modification queries ===

    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _strategy && _strategy->modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _strategy ? _strategy->last_modified_time() : MIN_DT;
    }

    [[nodiscard]] bool has_value() const {
        return _strategy && _strategy->has_value();
    }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        if (!bound()) {
            return "<unbound>";
        }
        return value().to_string();
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        std::string result = "TSInput{";
        if (_meta && _meta->name) {
            result += "type=";
            result += _meta->name;
            result += ", ";
        }
        result += "bound=";
        result += bound() ? "true" : "false";
        if (bound()) {
            result += ", value=\"";
            result += to_string();
            result += "\", modified=";
            result += modified_at(time) ? "true" : "false";
        }
        result += "}";
        return result;
    }

    /**
     * Check if this input is a bundle type
     */
    [[nodiscard]] bool is_bundle() const {
        return _meta && _meta->ts_kind == TSKind::TSB;
    }

    /**
     * Get the number of fields (for bundle types)
     * Returns 0 for non-bundle types.
     */
    [[nodiscard]] size_t field_count_from_meta() const;

    // === Strategy tree management (internal use) ===

    /**
     * Get or create a CollectionAccessStrategy at the root level.
     * Used internally by TSInputBindableView::bind().
     */
    CollectionAccessStrategy* ensure_collection_strategy();

    /**
     * Get or create a CollectionAccessStrategy at a given path.
     * Walks the strategy tree, creating CollectionAccessStrategies as needed.
     *
     * @param path Path of field indices from root
     * @return The CollectionAccessStrategy at the specified path (nullptr if path is empty)
     */
    CollectionAccessStrategy* ensure_collection_strategy_at_path(const std::vector<size_t>& path);

private:
    const TSMeta* _meta{nullptr};
    node_ptr _owning_node{nullptr};
    std::unique_ptr<AccessStrategy> _strategy;
    bool _active{false};
};

} // namespace hgraph::ts

#endif // HGRAPH_TS_INPUT_H
