//
// Created by Claude on 16/12/2025.
//
// TSInput and TSInputView - V2 time-series input implementation
//
// Design principles:
// - Input binds to an output (does not own data)
// - Provides read-only access to the bound output's value
// - Uses strategy pattern for different binding modes:
//   * Unbound: Not connected to any output
//   * Peered: Bound to single matching output, shares output's value
//   * NonPeered: Collection children bound independently
//   * RefObserver: Non-REF input bound to REF output
//   * RefWrapper: REF input bound to non-REF output
// - Views track navigation path for debugging
// - Chainable navigation API: input.view().field("price").element(0)
//

#ifndef HGRAPH_TS_INPUT_V2_H
#define HGRAPH_TS_INPUT_V2_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/time_series/v2/ts_output.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/time_series_value.h>
#include <string>
#include <vector>
#include <variant>
#include <optional>

namespace hgraph::ts {

// Forward declarations
class TSInput;
class TSInputView;
struct BindingStrategy;

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
// Binding Strategies
// ============================================================================

/**
 * BindingStrategy - Abstract interface for input binding behavior
 *
 * Each strategy holds a back-pointer to its owning TSInput.
 * This allows strategies to access the input's storage and meta.
 */
struct BindingStrategy {
    TSInput* _input{nullptr};

    BindingStrategy() = default;
    explicit BindingStrategy(TSInput* input) : _input(input) {}
    virtual ~BindingStrategy() = default;

    // Move-only (contains back-pointer)
    BindingStrategy(BindingStrategy&&) noexcept = default;
    BindingStrategy& operator=(BindingStrategy&&) noexcept = default;
    BindingStrategy(const BindingStrategy&) = delete;
    BindingStrategy& operator=(const BindingStrategy&) = delete;

    // === Query methods ===
    [[nodiscard]] virtual bool has_peer() const = 0;
    [[nodiscard]] virtual bool has_value() const = 0;
    [[nodiscard]] virtual bool modified_at(engine_time_t time) const = 0;
    [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

    // === Value access ===
    [[nodiscard]] virtual value::ConstValueView value() const = 0;
    [[nodiscard]] virtual value::ModificationTracker tracker() const = 0;

    // === Lifecycle ===
    virtual void make_active() = 0;
    virtual void make_passive() = 0;
    virtual void unbind() = 0;
};

/**
 * UnboundStrategy - Default state, no binding
 *
 * Implemented as singleton since it has no per-instance state.
 */
struct UnboundStrategy : BindingStrategy {
    static UnboundStrategy& instance() {
        static UnboundStrategy s;
        return s;
    }

    UnboundStrategy() : BindingStrategy(nullptr) {}

    [[nodiscard]] bool has_peer() const override { return false; }
    [[nodiscard]] bool has_value() const override { return false; }
    [[nodiscard]] bool modified_at(engine_time_t) const override { return false; }
    [[nodiscard]] engine_time_t last_modified_time() const override { return MIN_DT; }
    [[nodiscard]] value::ConstValueView value() const override { return {}; }
    [[nodiscard]] value::ModificationTracker tracker() const override { return {}; }
    void make_active() override { /* no-op */ }
    void make_passive() override { /* no-op */ }
    void unbind() override { /* already unbound */ }
};

/**
 * PeeredStrategy - Bound to single output, delegates all queries
 */
struct PeeredStrategy : BindingStrategy {
    TSOutput* _peer_output{nullptr};

    PeeredStrategy() = default;

    PeeredStrategy(TSInput* input, TSOutput* output)
        : BindingStrategy(input), _peer_output(output) {}

    [[nodiscard]] bool has_peer() const override { return true; }

    [[nodiscard]] bool has_value() const override {
        return _peer_output && _peer_output->has_value();
    }

    [[nodiscard]] bool modified_at(engine_time_t time) const override {
        return _peer_output && _peer_output->modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _peer_output ? _peer_output->last_modified_time() : MIN_DT;
    }

    [[nodiscard]] value::ConstValueView value() const override {
        return _peer_output ? _peer_output->value() : value::ConstValueView{};
    }

    [[nodiscard]] value::ModificationTracker tracker() const override {
        return _peer_output
            ? _peer_output->underlying().underlying_tracker().tracker()
            : value::ModificationTracker{};
    }

    void make_active() override;
    void make_passive() override;
    void unbind() override;

    [[nodiscard]] TSOutput* peer_output() const { return _peer_output; }
};

/**
 * NonPeeredStrategy - Collection children bound independently
 *
 * Tracks which outputs each element is bound to for subscription management.
 * Modification tracking aggregates across all bound elements.
 */
struct NonPeeredStrategy : BindingStrategy {
    std::vector<TSOutput*> _element_outputs;

    NonPeeredStrategy() = default;

    NonPeeredStrategy(TSInput* input, size_t element_count)
        : BindingStrategy(input)
        , _element_outputs(element_count, nullptr) {}

    [[nodiscard]] bool has_peer() const override { return false; }

    [[nodiscard]] bool has_value() const override;
    [[nodiscard]] bool modified_at(engine_time_t time) const override;
    [[nodiscard]] engine_time_t last_modified_time() const override;
    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    void make_active() override;
    void make_passive() override;
    void unbind() override;

    // Element binding
    void bind_element(size_t index, TSOutput* output);
    void unbind_element(size_t index);

    [[nodiscard]] size_t element_count() const { return _element_outputs.size(); }
    [[nodiscard]] TSOutput* element_output(size_t index) const {
        return index < _element_outputs.size() ? _element_outputs[index] : nullptr;
    }
};

/**
 * RefObserverStrategy - Non-REF input bound to REF output
 *
 * Tracks reference changes and rebinds to the target output.
 * Reports modified when reference changes (delta synthesis).
 */
struct RefObserverStrategy : BindingStrategy {
    TSOutput* _ref_output{nullptr};    // The REF output we observe
    TSOutput* _target_output{nullptr}; // Current target (what REF points to)
    engine_time_t _sample_time{MIN_DT}; // When we last rebound

    RefObserverStrategy() = default;

    RefObserverStrategy(TSInput* input, TSOutput* ref_output)
        : BindingStrategy(input)
        , _ref_output(ref_output)
        , _sample_time(MIN_DT) {}

    [[nodiscard]] bool has_peer() const override { return false; }

    [[nodiscard]] bool has_value() const override {
        return _target_output && _target_output->has_value();
    }

    [[nodiscard]] bool modified_at(engine_time_t time) const override {
        // Modified if just sampled/rebound OR if target modified
        if (_sample_time == time) return true;
        return _target_output ? _target_output->modified_at(time) : false;
    }

    [[nodiscard]] engine_time_t last_modified_time() const override {
        if (_target_output) {
            return std::max(_sample_time, _target_output->last_modified_time());
        }
        return _sample_time;
    }

    [[nodiscard]] value::ConstValueView value() const override {
        return _target_output ? _target_output->value() : value::ConstValueView{};
    }

    [[nodiscard]] value::ModificationTracker tracker() const override {
        return _target_output
            ? _target_output->underlying().underlying_tracker().tracker()
            : value::ModificationTracker{};
    }

    void make_active() override;
    void make_passive() override;
    void unbind() override;

    // Called when the reference value changes
    void on_reference_changed(TSOutput* new_target, engine_time_t time);

    [[nodiscard]] TSOutput* ref_output() const { return _ref_output; }
    [[nodiscard]] TSOutput* target_output() const { return _target_output; }
};

/**
 * RefWrapperStrategy - REF input bound to non-REF output
 *
 * Wraps the output as a TimeSeriesReference value.
 * Reports modified only when first bound.
 */
struct RefWrapperStrategy : BindingStrategy {
    TSOutput* _wrapped_output{nullptr};
    engine_time_t _bind_time{MIN_DT};
    // Note: The actual TimeSeriesReference value is created in TSInput's storage

    RefWrapperStrategy() = default;

    RefWrapperStrategy(TSInput* input, TSOutput* output, engine_time_t bind_time)
        : BindingStrategy(input)
        , _wrapped_output(output)
        , _bind_time(bind_time) {}

    [[nodiscard]] bool has_peer() const override { return false; }
    [[nodiscard]] bool has_value() const override { return true; }

    [[nodiscard]] bool modified_at(engine_time_t time) const override {
        return _bind_time == time;
    }

    [[nodiscard]] engine_time_t last_modified_time() const override {
        return _bind_time;
    }

    [[nodiscard]] value::ConstValueView value() const override;
    [[nodiscard]] value::ModificationTracker tracker() const override;

    void make_active() override { /* no-op: doesn't subscribe for value changes */ }
    void make_passive() override { /* no-op */ }
    void unbind() override;

    [[nodiscard]] TSOutput* wrapped_output() const { return _wrapped_output; }
};

// ============================================================================
// TSInput - Time-series input with binding strategies
// ============================================================================

/**
 * TSInput - V2 time-series input implementation
 *
 * Behavior:
 * - Binds to a TSOutput to receive data
 * - Provides read-only access to the bound output's value
 * - Does NOT own data (the output owns it, or we have view storage for non-peered)
 *
 * Binding Modes (via strategies):
 * - Unbound: Not connected to any output
 * - Peered: Bound to single matching output, shares output's value
 * - NonPeered: Collection children bound independently
 * - RefObserver: Non-REF input bound to REF output
 * - RefWrapper: REF input bound to non-REF output
 *
 * Storage Model:
 * - Peered: _storage is view into peer output (optional, may not have own storage)
 * - NonPeered: _storage is owned with element views into bound outputs
 *
 * Construction:
 * - Use TimeSeriesTypeMeta::make_input() to create instances
 */
class TSInput {
public:
    using ptr = TSInput*;
    using s_ptr = std::shared_ptr<TSInput>;

    TSInput() = default;

    /**
     * Construct from type metadata and owning node
     */
    TSInput(const TimeSeriesTypeMeta* meta, node_ptr owning_node)
        : _meta(meta)
        , _owning_node(owning_node)
        , _strategy(&UnboundStrategy::instance()) {}

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
     *
     * - Peered: has peer output
     * - NonPeered: any element is bound (has valid view)
     */
    [[nodiscard]] bool bound() const;

    /**
     * Check if this input has a peer (single output binding)
     */
    [[nodiscard]] bool has_peer() const { return _strategy->has_peer(); }

    /**
     * Get the current binding strategy
     */
    [[nodiscard]] BindingStrategy* strategy() const { return _strategy; }

    // === Binding Operations ===

    /**
     * Bind this input to an output (peered binding)
     *
     * The binding mode is determined by type compatibility:
     * - Same types: Peered
     * - Non-REF input to REF output: RefObserver
     * - REF input to non-REF output: RefWrapper
     */
    void bind_output(TSOutput* output);

    /**
     * Bind a specific element to an output (non-peered binding)
     *
     * Only valid for collection types (TSL, TSB).
     * Automatically switches to non-peered mode if not already.
     */
    void bind_element(size_t index, TSOutput* output);

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
        return _strategy->value();
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
        return _strategy->modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _strategy->last_modified_time();
    }

    [[nodiscard]] bool has_value() const {
        return _strategy->has_value();
    }

    // === Storage access (for strategies and builders) ===

    [[nodiscard]] bool has_storage() const { return _storage.has_value(); }

    [[nodiscard]] value::TimeSeriesValue& storage() {
        if (!_storage) {
            throw std::runtime_error("TSInput::storage() called without storage");
        }
        return *_storage;
    }

    [[nodiscard]] const value::TimeSeriesValue& storage() const {
        if (!_storage) {
            throw std::runtime_error("TSInput::storage() called without storage");
        }
        return *_storage;
    }

    /**
     * Create storage for non-peered mode
     *
     * Called by builders or when transitioning to non-peered binding.
     */
    void create_storage(const value::TypeMeta* schema) {
        _storage.emplace(schema);
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
        result += ", peer=";
        result += has_peer() ? "true" : "false";
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
    friend struct PeeredStrategy;
    friend struct NonPeeredStrategy;
    friend struct RefObserverStrategy;
    friend struct RefWrapperStrategy;

    const TimeSeriesTypeMeta* _meta{nullptr};
    node_ptr _owning_node{nullptr};
    BindingStrategy* _strategy{&UnboundStrategy::instance()};
    bool _active{false};

    // Strategy storage (inline to avoid heap allocation)
    std::variant<
        std::monostate,      // Unbound uses singleton
        PeeredStrategy,
        NonPeeredStrategy,
        RefObserverStrategy,
        RefWrapperStrategy
    > _strategy_storage;

    // Unified value storage:
    // - Peered: not used (value comes from peer output)
    // - NonPeered: owned storage with element views into bound outputs
    std::optional<value::TimeSeriesValue> _storage;

    // === Strategy setup helpers ===
    void set_strategy_unbound();
    void set_strategy_peered(TSOutput* output);
    void set_strategy_non_peered(size_t element_count);
    void set_strategy_ref_observer(TSOutput* ref_output);
    void set_strategy_ref_wrapper(TSOutput* output, engine_time_t time);

    /**
     * Get element count for collection types
     */
    [[nodiscard]] size_t get_element_count() const;
};

} // namespace hgraph::ts

#endif // HGRAPH_TS_INPUT_V2_H
