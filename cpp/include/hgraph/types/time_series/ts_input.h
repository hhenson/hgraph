//
// Created by Claude on 16/12/2025.
//
// TSInput and TSInputView - V2 time-series input implementation
//
// Design principles:
// - Input binds to an output (does not own data)
// - Provides read-only access to the bound output's value
// - Uses hierarchical AccessStrategy for binding:
//   * DirectAccess: Direct delegation to bound output
//   * CollectionAccess: Child strategies for collection elements
//   * RefObserverAccess: Non-REF input bound to REF output
//   * RefWrapperAccess: REF input bound to non-REF output
// - Views track navigation path for debugging
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

// ============================================================================
// TSInputView - Read-only view into TSInput with path tracking
// ============================================================================

/**
 * TSInputView - Read-only view into a bound time-series input
 *
 * Features:
 * - Read-only access to the bound output's value
 * - Chainable navigation: view.field("price").element(0)
 * - Path tracking for debugging (knows where it came from)
 * - Efficient rvalue overloads that move the path instead of copying
 *
 * Views do NOT own data - they reference the bound output's storage.
 */
class TSInputView {
public:
    TSInputView() = default;

    TSInputView(value::ConstValueView value_view,
                value::ModificationTracker tracker,
                const TimeSeriesTypeMeta* meta,
                NavigationPath path)
        : _value_view(value_view)
        , _tracker(tracker)
        , _meta(meta)
        , _path(std::move(path)) {}

    // === Validity and type queries ===
    [[nodiscard]] bool valid() const { return _value_view.valid(); }
    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _meta; }
    [[nodiscard]] const value::TypeMeta* value_schema() const { return _value_view.schema(); }
    [[nodiscard]] value::TypeKind kind() const { return _value_view.kind(); }
    [[nodiscard]] TimeSeriesKind ts_kind() const { return _meta ? _meta->ts_kind : TimeSeriesKind::TS; }

    // === Path tracking ===
    [[nodiscard]] const NavigationPath& path() const { return _path; }
    [[nodiscard]] std::string path_string() const { return _path.to_string(); }

    // === Underlying value view access (read-only) ===
    [[nodiscard]] const value::ConstValueView& value_view() const { return _value_view; }

    // === Scalar value access (read-only) ===
    template<typename T>
    [[nodiscard]] const T& as() const {
        return _value_view.as<T>();
    }

    // === Query methods ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _tracker.modified_at(time);
    }

    [[nodiscard]] bool has_value() const {
        return _tracker.valid_value();
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _tracker.last_modified_time();
    }

    // === Bundle field navigation (chainable, read-only) ===

    // Lvalue path - copies path
    [[nodiscard]] TSInputView field(size_t index) & {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(index);
        auto field_tracker = _tracker.field(index);
        auto field_meta = _meta ? _meta->field_meta(index) : nullptr;
        return {field_view, field_tracker, field_meta,
                NavigationPath(_path, PathSegment::field(index, field_meta))};
    }

    // Rvalue path - moves path for efficiency
    [[nodiscard]] TSInputView field(size_t index) && {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(index);
        auto field_tracker = _tracker.field(index);
        auto field_meta = _meta ? _meta->field_meta(index) : nullptr;
        return {field_view, field_tracker, field_meta,
                NavigationPath(std::move(_path), PathSegment::field(index, field_meta))};
    }

    // By name - lvalue path
    [[nodiscard]] TSInputView field(const std::string& name) & {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(name);
        auto field_tracker = _tracker.field(name);
        auto field_meta = _meta ? _meta->field_meta(name) : nullptr;
        return {field_view, field_tracker, field_meta,
                NavigationPath(_path, PathSegment::field(name, field_meta))};
    }

    // By name - rvalue path
    [[nodiscard]] TSInputView field(const std::string& name) && {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(name);
        auto field_tracker = _tracker.field(name);
        auto field_meta = _meta ? _meta->field_meta(name) : nullptr;
        return {field_view, field_tracker, field_meta,
                NavigationPath(std::move(_path), PathSegment::field(name, field_meta))};
    }

    [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
        return _tracker.field_modified_at(index, time);
    }

    [[nodiscard]] size_t field_count() const {
        return _value_view.field_count();
    }

    // === List element navigation (chainable, read-only) ===

    // Lvalue path
    [[nodiscard]] TSInputView element(size_t index) & {
        if (!valid() || kind() != value::TypeKind::List) {
            return {};
        }
        auto elem_view = _value_view.element(index);
        auto elem_tracker = _tracker.element(index);
        auto elem_meta = _meta ? _meta->element_meta() : nullptr;
        return {elem_view, elem_tracker, elem_meta,
                NavigationPath(_path, PathSegment::at_index(index, elem_meta))};
    }

    // Rvalue path
    [[nodiscard]] TSInputView element(size_t index) && {
        if (!valid() || kind() != value::TypeKind::List) {
            return {};
        }
        auto elem_view = _value_view.element(index);
        auto elem_tracker = _tracker.element(index);
        auto elem_meta = _meta ? _meta->element_meta() : nullptr;
        return {elem_view, elem_tracker, elem_meta,
                NavigationPath(std::move(_path), PathSegment::at_index(index, elem_meta))};
    }

    [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
        return _tracker.element_modified_at(index, time);
    }

    [[nodiscard]] size_t list_size() const {
        return _value_view.list_size();
    }

    // === Set operations (read-only) ===
    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        return _value_view.set_contains(elem);
    }

    [[nodiscard]] size_t set_size() const {
        return _value_view.set_size();
    }

    // === Dict operations (read-only) ===
    template<typename K>
    [[nodiscard]] bool dict_contains(const K& key) const {
        return _value_view.dict_contains(key);
    }

    template<typename K>
    [[nodiscard]] value::ConstValueView dict_get(const K& key) const {
        return _value_view.dict_get(key);
    }

    // Dict entry navigation (chainable, read-only) - lvalue path
    template<typename K>
    [[nodiscard]] TSInputView entry(const K& key) & {
        if (!valid() || kind() != value::TypeKind::Dict) {
            return {};
        }
        auto entry_view = _value_view.dict_get(key);
        if (!entry_view.valid()) {
            return {};
        }
        auto value_meta = _meta ? _meta->value_meta() : nullptr;
        return {entry_view, _tracker, value_meta,
                NavigationPath(_path, PathSegment::at_index(0, value_meta))};
    }

    // Rvalue path
    template<typename K>
    [[nodiscard]] TSInputView entry(const K& key) && {
        if (!valid() || kind() != value::TypeKind::Dict) {
            return {};
        }
        auto entry_view = _value_view.dict_get(key);
        if (!entry_view.valid()) {
            return {};
        }
        auto value_meta = _meta ? _meta->value_meta() : nullptr;
        return {entry_view, _tracker, value_meta,
                NavigationPath(std::move(_path), PathSegment::at_index(0, value_meta))};
    }

    [[nodiscard]] size_t dict_size() const {
        return _value_view.dict_size();
    }

    // === Window operations (read-only) ===
    [[nodiscard]] value::ConstValueView window_get(size_t index) const {
        return _value_view.window_get(index);
    }

    [[nodiscard]] size_t window_size() const {
        return _value_view.window_size();
    }

    [[nodiscard]] bool window_empty() const {
        return _value_view.window_empty();
    }

    [[nodiscard]] bool window_full() const {
        return _value_view.window_full();
    }

    [[nodiscard]] engine_time_t window_timestamp(size_t index) const {
        return _value_view.window_timestamp(index);
    }

    // === Ref operations (read-only) ===
    [[nodiscard]] bool ref_is_empty() const { return _value_view.ref_is_empty(); }
    [[nodiscard]] bool ref_is_bound() const { return _value_view.ref_is_bound(); }
    [[nodiscard]] bool ref_is_valid() const { return _value_view.ref_is_valid(); }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return _value_view.to_string();
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

private:
    value::ConstValueView _value_view;
    value::ModificationTracker _tracker;
    const TimeSeriesTypeMeta* _meta{nullptr};
    NavigationPath _path;
};

// ============================================================================
// TSInput - Time-series input with hierarchical access strategies
// ============================================================================

/**
 * TSInput - V2 time-series input implementation
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
 * - Use TimeSeriesTypeMeta::make_input() to create instances
 */
class TSInput : public Notifiable {
public:
    using ptr = TSInput*;
    using s_ptr = std::shared_ptr<TSInput>;

    TSInput() = default;

    /**
     * Construct from type metadata and owning node
     */
    TSInput(const TimeSeriesTypeMeta* meta, node_ptr owning_node)
        : _meta(meta)
        , _owning_node(owning_node) {}

    // Move only
    TSInput(TSInput&&) noexcept = default;
    TSInput& operator=(TSInput&&) noexcept = default;
    TSInput(const TSInput&) = delete;
    TSInput& operator=(const TSInput&) = delete;

    // === Validity and type information ===
    [[nodiscard]] bool valid() const { return _meta != nullptr; }
    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _meta; }
    [[nodiscard]] TimeSeriesKind ts_kind() const { return _meta ? _meta->ts_kind : TimeSeriesKind::TS; }

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
     * Bind this input to an output
     *
     * Builds a strategy tree based on schema comparison:
     * - Matching types: DirectAccess
     * - Non-REF input to REF output: RefObserverAccess
     * - REF input to non-REF output: RefWrapperAccess
     * - Collections with nested differences: CollectionAccess with children
     *
     * @param output The output to bind to
     */
    void bind_output(TSOutput* output);

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

    // Element access for collections
    [[nodiscard]] value::ConstValueView element(size_t index) const {
        auto v = value();
        return v.valid() ? v.element(index) : value::ConstValueView{};
    }

    [[nodiscard]] value::ConstValueView field(size_t index) const {
        auto v = value();
        return v.valid() ? v.field(index) : value::ConstValueView{};
    }

    [[nodiscard]] value::ConstValueView field(const std::string& name) const {
        auto v = value();
        return v.valid() ? v.field(name) : value::ConstValueView{};
    }

    // === Modification queries ===

    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _strategy ? _strategy->modified_at(time) : false;
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _strategy ? _strategy->last_modified_time() : MIN_DT;
    }

    [[nodiscard]] bool has_value() const {
        return _strategy ? _strategy->has_value() : false;
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

private:
    const TimeSeriesTypeMeta* _meta{nullptr};
    node_ptr _owning_node{nullptr};
    std::unique_ptr<AccessStrategy> _strategy;
    bool _active{false};
};

} // namespace hgraph::ts

#endif // HGRAPH_TS_INPUT_H
