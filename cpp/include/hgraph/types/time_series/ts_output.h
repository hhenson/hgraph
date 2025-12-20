//
// Created by Claude on 16/12/2025.
//
// TSOutput and TSOutputView - Value-based time-series output implementation
//
// Design principles:
// - Output owns a TimeSeriesValue (type-erased storage)
// - Time is passed as parameter to mutation methods (not stored in view)
// - Views track navigation path for debugging (root reference + path segments)
// - Chainable navigation API: output.view().field("price").element(0)
//

#ifndef HGRAPH_TS_OUTPUT_H
#define HGRAPH_TS_OUTPUT_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/time_series_value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/delta_view.h>
#include <string>
#include <variant>
#include <vector>

namespace hgraph::ts {

// Forward declarations
class TSOutput;
class TSOutputView;
class DeltaView;

// ============================================================================
// PathSegment - Describes a single navigation step
// ============================================================================

/**
 * PathSegment - A single step in a navigation path
 *
 * Supports three types of navigation:
 * - Field access by index: .field(0), .field(1)
 * - Field access by name: .field("price"), .field("quantity")
 * - Element access by index: .element(0), .element(1)
 *
 * Also tracks the type metadata at each step for debugging and validation.
 */
struct PathSegment {
    enum class Kind : uint8_t {
        Field,     // Bundle field access
        Element,   // List/dict element access
    };

    Kind kind;
    std::variant<size_t, std::string> index_value;  // Field index or name, or element index
    const TimeSeriesTypeMeta* meta{nullptr};        // Type at this point (optional, for debugging)

    // Factory methods for cleaner construction
    static PathSegment field(size_t index, const TimeSeriesTypeMeta* meta = nullptr) {
        return {Kind::Field, index, meta};
    }

    static PathSegment field(std::string name, const TimeSeriesTypeMeta* meta = nullptr) {
        return {Kind::Field, std::move(name), meta};
    }

    static PathSegment at_index(size_t index, const TimeSeriesTypeMeta* meta = nullptr) {
        return {Kind::Element, index, meta};
    }

    // Accessors
    [[nodiscard]] bool is_field() const { return kind == Kind::Field; }
    [[nodiscard]] bool is_element() const { return kind == Kind::Element; }

    [[nodiscard]] bool has_index() const {
        return std::holds_alternative<size_t>(index_value);
    }

    [[nodiscard]] bool has_name() const {
        return std::holds_alternative<std::string>(index_value);
    }

    [[nodiscard]] size_t as_index() const {
        return std::get<size_t>(index_value);
    }

    [[nodiscard]] const std::string& as_name() const {
        return std::get<std::string>(index_value);
    }

    // String representation for debugging
    [[nodiscard]] std::string to_string() const {
        std::string result;
        switch (kind) {
            case Kind::Field:
                result = ".field(";
                if (has_name()) {
                    result += "\"" + as_name() + "\"";
                } else {
                    result += std::to_string(as_index());
                }
                result += ")";
                break;
            case Kind::Element:
                result = ".element(" + std::to_string(as_index()) + ")";
                break;
        }
        return result;
    }
};

// ============================================================================
// NavigationPath - Complete path from root to current view
// ============================================================================

/**
 * NavigationPath - Tracks the complete navigation path from a root TSOutput
 *
 * Contains:
 * - Pointer to the root TSOutput (origin of navigation)
 * - Sequence of PathSegments describing the navigation steps
 *
 * This allows debugging tools to understand where a view came from
 * and reconstruct the navigation chain.
 */
class NavigationPath {
public:
    NavigationPath() = default;

    explicit NavigationPath(const TSOutput* root) : _root(root) {}

    NavigationPath(const TSOutput* root, std::vector<PathSegment> segments)
        : _root(root), _segments(std::move(segments)) {}

    // Move-efficient path extension
    NavigationPath(NavigationPath&& other, PathSegment segment)
        : _root(other._root), _segments(std::move(other._segments)) {
        _segments.push_back(std::move(segment));
    }

    // Copy-based path extension (for lvalue paths)
    NavigationPath(const NavigationPath& other, PathSegment segment)
        : _root(other._root), _segments(other._segments) {
        _segments.push_back(std::move(segment));
    }

    [[nodiscard]] const TSOutput* root() const { return _root; }
    [[nodiscard]] const std::vector<PathSegment>& segments() const { return _segments; }
    [[nodiscard]] bool empty() const { return _segments.empty(); }
    [[nodiscard]] size_t depth() const { return _segments.size(); }

    // String representation for debugging
    [[nodiscard]] std::string to_string() const {
        std::string result = "root";
        for (const auto& seg : _segments) {
            result += seg.to_string();
        }
        return result;
    }

private:
    const TSOutput* _root{nullptr};
    std::vector<PathSegment> _segments;
};

// ============================================================================
// TSOutputView - Mutable view into TSOutput with path tracking
// ============================================================================

/**
 * TSOutputView - Lightweight view into a TSOutput with navigation tracking
 *
 * Features:
 * - All mutations take time as explicit parameter (no stale time issues)
 * - Chainable navigation: view.field("price").element(0)
 * - Path tracking for debugging (knows where it came from)
 * - Efficient rvalue overloads that move the path instead of copying
 *
 * Views do NOT own data - they reference the underlying TSOutput's storage.
 */
class TSOutputView {
public:
    TSOutputView() = default;

    TSOutputView(value::TimeSeriesValueView value_view,
                 const TimeSeriesTypeMeta* meta,
                 NavigationPath path)
        : _value_view(value_view), _meta(meta), _path(std::move(path)) {}

    // === Validity and type queries ===
    [[nodiscard]] bool valid() const { return _value_view.valid(); }
    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _meta; }
    [[nodiscard]] const value::TypeMeta* value_schema() const { return _value_view.schema(); }
    [[nodiscard]] value::TypeKind kind() const { return _value_view.kind(); }
    [[nodiscard]] TimeSeriesKind ts_kind() const { return _meta ? _meta->ts_kind : TimeSeriesKind::TS; }

    // === Path tracking ===
    [[nodiscard]] const NavigationPath& path() const { return _path; }
    [[nodiscard]] std::string path_string() const { return _path.to_string(); }

    // === Underlying value view access ===
    [[nodiscard]] value::TimeSeriesValueView& value_view() { return _value_view; }
    [[nodiscard]] const value::TimeSeriesValueView& value_view() const { return _value_view; }

    // === Observer access ===
    [[nodiscard]] value::ObserverStorage* observer() { return _value_view.observer(); }
    [[nodiscard]] const value::ObserverStorage* observer() const { return _value_view.observer(); }

    // === Scalar value access ===
    template<typename T>
    [[nodiscard]] T& as() {
        return _value_view.as<T>();
    }

    template<typename T>
    [[nodiscard]] const T& as() const {
        return _value_view.as<T>();
    }

    // === Mutation methods (time as explicit parameter) ===

    template<typename T>
    void set(const T& val, engine_time_t time) {
        _value_view.set(val, time);
    }

    void mark_modified(engine_time_t time) {
        _value_view.mark_modified(time);
    }

    void mark_invalid() {
        _value_view.mark_invalid();
    }

    // === Query methods ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _value_view.modified_at(time);
    }

    [[nodiscard]] bool has_value() const {
        return _value_view.has_value();
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _value_view.last_modified_time();
    }

    // === Bundle field navigation (chainable) ===

    // Lvalue path - copies path
    [[nodiscard]] TSOutputView field(size_t index) & {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(index);
        auto field_meta = _meta ? _meta->field_meta(index) : nullptr;
        return {field_view, field_meta, NavigationPath(_path, PathSegment::field(index, field_meta))};
    }

    // Rvalue path - moves path for efficiency
    [[nodiscard]] TSOutputView field(size_t index) && {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(index);
        auto field_meta = _meta ? _meta->field_meta(index) : nullptr;
        return {field_view, field_meta, NavigationPath(std::move(_path), PathSegment::field(index, field_meta))};
    }

    // By name - lvalue path
    [[nodiscard]] TSOutputView field(const std::string& name) & {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(name);
        auto field_meta = _meta ? _meta->field_meta(name) : nullptr;
        return {field_view, field_meta, NavigationPath(_path, PathSegment::field(name, field_meta))};
    }

    // By name - rvalue path
    [[nodiscard]] TSOutputView field(const std::string& name) && {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field(name);
        auto field_meta = _meta ? _meta->field_meta(name) : nullptr;
        return {field_view, field_meta, NavigationPath(std::move(_path), PathSegment::field(name, field_meta))};
    }

    [[nodiscard]] bool field_modified_at(size_t index, engine_time_t time) const {
        return _value_view.field_modified_at(index, time);
    }

    [[nodiscard]] size_t field_count() const {
        return _value_view.field_count();
    }

    // Observer-enabled field navigation (for subscription at nested levels)
    [[nodiscard]] TSOutputView field_with_observer(size_t index) {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field_with_observer(index);
        auto field_meta = _meta ? _meta->field_meta(index) : nullptr;
        return {field_view, field_meta, NavigationPath(_path, PathSegment::field(index, field_meta))};
    }

    [[nodiscard]] TSOutputView field_with_observer(const std::string& name) {
        if (!valid() || kind() != value::TypeKind::Bundle) {
            return {};
        }
        auto field_view = _value_view.field_with_observer(name);
        if (!field_view.valid()) {
            return {};
        }
        auto field_meta = _meta ? _meta->field_meta(name) : nullptr;
        return {field_view, field_meta, NavigationPath(_path, PathSegment::field(name, field_meta))};
    }

    // === List element navigation (chainable) ===

    // Lvalue path
    [[nodiscard]] TSOutputView element(size_t index) & {
        if (!valid() || kind() != value::TypeKind::List) {
            return {};
        }
        auto elem_view = _value_view.element(index);
        auto elem_meta = _meta ? _meta->element_meta() : nullptr;
        return {elem_view, elem_meta, NavigationPath(_path, PathSegment::at_index(index, elem_meta))};
    }

    // Rvalue path
    [[nodiscard]] TSOutputView element(size_t index) && {
        if (!valid() || kind() != value::TypeKind::List) {
            return {};
        }
        auto elem_view = _value_view.element(index);
        auto elem_meta = _meta ? _meta->element_meta() : nullptr;
        return {elem_view, elem_meta, NavigationPath(std::move(_path), PathSegment::at_index(index, elem_meta))};
    }

    [[nodiscard]] bool element_modified_at(size_t index, engine_time_t time) const {
        return _value_view.element_modified_at(index, time);
    }

    [[nodiscard]] size_t list_size() const {
        return _value_view.list_size();
    }

    // Observer-enabled element navigation (for subscription at nested levels)
    [[nodiscard]] TSOutputView element_with_observer(size_t index) {
        if (!valid() || kind() != value::TypeKind::List) {
            return {};
        }
        auto elem_view = _value_view.element_with_observer(index);
        auto elem_meta = _meta ? _meta->element_meta() : nullptr;
        return {elem_view, elem_meta, NavigationPath(_path, PathSegment::at_index(index, elem_meta))};
    }

    // === Set operations ===
    template<typename T>
    bool add(const T& elem, engine_time_t time) {
        return _value_view.add(elem, time);
    }

    template<typename T>
    bool remove(const T& elem, engine_time_t time) {
        return _value_view.remove(elem, time);
    }

    template<typename T>
    [[nodiscard]] bool contains(const T& elem) const {
        return _value_view.contains(elem);
    }

    [[nodiscard]] size_t set_size() const {
        return _value_view.set_size();
    }

    // === Dict operations ===
    template<typename K, typename V>
    void insert(const K& key, const V& value, engine_time_t time) {
        _value_view.insert(key, value, time);
    }

    template<typename K>
    [[nodiscard]] bool dict_contains(const K& key) const {
        return _value_view.dict_contains(key);
    }

    template<typename K>
    [[nodiscard]] value::ConstValueView dict_get(const K& key) const {
        return _value_view.dict_get(key);
    }

    // Dict entry navigation (chainable) - lvalue path
    template<typename K>
    [[nodiscard]] TSOutputView entry(const K& key) & {
        if (!valid() || kind() != value::TypeKind::Dict) {
            return {};
        }
        auto entry_view = _value_view.entry(key);
        if (!entry_view.valid()) {
            return {};
        }
        auto value_meta = _meta ? _meta->value_meta() : nullptr;
        // For dict entries, we track by key converted to index if possible
        // This is a simplification - full implementation might store the key
        return {entry_view, value_meta, NavigationPath(_path, PathSegment::at_index(0, value_meta))};
    }

    // Rvalue path
    template<typename K>
    [[nodiscard]] TSOutputView entry(const K& key) && {
        if (!valid() || kind() != value::TypeKind::Dict) {
            return {};
        }
        auto entry_view = _value_view.entry(key);
        if (!entry_view.valid()) {
            return {};
        }
        auto value_meta = _meta ? _meta->value_meta() : nullptr;
        return {entry_view, value_meta, NavigationPath(std::move(_path), PathSegment::at_index(0, value_meta))};
    }

    template<typename K>
    bool dict_remove(const K& key, engine_time_t time) {
        return _value_view.dict_remove(key, time);
    }

    [[nodiscard]] size_t dict_size() const {
        return _value_view.dict_size();
    }

    // === Window operations ===
    template<typename T>
    void window_push(const T& value, engine_time_t timestamp, engine_time_t eval_time) {
        _value_view.window_push(value, timestamp, eval_time);
    }

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

    void window_clear(engine_time_t time) {
        _value_view.window_clear(time);
    }

    // === Ref operations ===
    [[nodiscard]] bool ref_is_empty() const { return _value_view.ref_is_empty(); }
    [[nodiscard]] bool ref_is_bound() const { return _value_view.ref_is_bound(); }
    [[nodiscard]] bool ref_is_valid() const { return _value_view.ref_is_valid(); }

    void ref_bind(value::ValueRef target, engine_time_t time) {
        _value_view.ref_bind(target, time);
    }

    void ref_clear(engine_time_t time) {
        _value_view.ref_clear(time);
    }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return _value_view.to_string();
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        std::string result = "TSOutputView{path=";
        result += _path.to_string();
        result += ", ";
        result += _value_view.to_debug_string(time);
        result += "}";
        return result;
    }

    // === Delta value access ===

    /**
     * Get a DeltaView for accessing delta values at the given time.
     *
     * Returns an invalid view if not modified at this time.
     *
     * @param time The engine time to check for modification
     * @return DeltaView for accessing delta information
     */
    [[nodiscard]] DeltaView delta_view(engine_time_t time) const {
        if (!valid() || !modified_at(time)) {
            return {};
        }
        // Get const value view and tracker from the underlying TimeSeriesValueView
        auto const_value = value::ConstValueView(_value_view.value_view().data(),
                                                  _value_view.value_view().schema());
        return {const_value, _value_view.tracker(), _meta, time};
    }

private:
    value::TimeSeriesValueView _value_view;
    const TimeSeriesTypeMeta* _meta{nullptr};
    NavigationPath _path;
};

// ============================================================================
// TSOutput - Owning time-series output with node parentage
// ============================================================================

/**
 * TSOutput - Value-based time-series output implementation
 *
 * Owns:
 * - TimeSeriesValue (type-erased value storage + modification tracking)
 * - Back-reference to owning node
 * - Type metadata for navigation and construction
 *
 * Provides:
 * - View creation with path tracking
 * - Direct value access for simple cases
 * - Observer/subscription support (delegated to TimeSeriesValue)
 *
 * Construction:
 * - Use TimeSeriesTypeMeta::make_output() to create instances
 * - The meta provides the value schema for TimeSeriesValue construction
 */
class TSOutput {
public:
    using ptr = TSOutput*;
    using s_ptr = std::shared_ptr<TSOutput>;

    TSOutput() = default;

    /**
     * Construct from type metadata and owning node
     *
     * The meta provides the value schema used to construct the
     * underlying TimeSeriesValue storage.
     */
    TSOutput(const TimeSeriesTypeMeta* meta, node_ptr owning_node)
        : _meta(meta)
        , _owning_node(owning_node)
        , _value(meta ? meta->value_schema() : nullptr) {}

    // Move only (value owns storage)
    TSOutput(TSOutput&&) noexcept = default;
    TSOutput& operator=(TSOutput&&) noexcept = default;
    TSOutput(const TSOutput&) = delete;
    TSOutput& operator=(const TSOutput&) = delete;

    // === Validity and type information ===
    [[nodiscard]] bool valid() const { return _value.valid(); }
    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _meta; }
    [[nodiscard]] const value::TypeMeta* value_schema() const { return _value.schema(); }
    [[nodiscard]] value::TypeKind kind() const { return _value.kind(); }
    [[nodiscard]] TimeSeriesKind ts_kind() const { return _meta ? _meta->ts_kind : TimeSeriesKind::TS; }

    // === Node parentage ===
    [[nodiscard]] node_ptr owning_node() const { return _owning_node; }

    // === View creation with path tracking ===

    /**
     * Create a view into this output
     *
     * The view is initialized with this TSOutput as the path root.
     * Navigation from the view creates new views with extended paths.
     *
     * Example:
     *   auto price_view = output.view().field("price");
     *   // price_view.path() == "root.field(\"price\")"
     */
    [[nodiscard]] TSOutputView view() {
        return {_value.view(), _meta, NavigationPath(this)};
    }

    // === Direct value access (convenience for simple TS values) ===

    template<typename T>
    void set_value(const T& val, engine_time_t time) {
        _value.set_value(val, time);
    }

    template<typename T>
    [[nodiscard]] const T& as() const {
        return _value.as<T>();
    }

    [[nodiscard]] value::ConstValueView value() const {
        return _value.value();
    }

    // === Modification queries ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _value.modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _value.last_modified_time();
    }

    [[nodiscard]] bool has_value() const {
        return _value.has_value();
    }

    void mark_invalid() {
        _value.mark_invalid();
    }

    // === Observer/subscription support ===
    void subscribe(Notifiable* notifiable) {
        _value.subscribe(notifiable);
    }

    void unsubscribe(Notifiable* notifiable) {
        _value.unsubscribe(notifiable);
    }

    [[nodiscard]] bool has_observers() const {
        return _value.has_observers();
    }

    // === Underlying storage access (for advanced use) ===
    [[nodiscard]] value::TimeSeriesValue& underlying() { return _value; }
    [[nodiscard]] const value::TimeSeriesValue& underlying() const { return _value; }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return _value.to_string();
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        std::string result = "TSOutput{";
        if (_meta && _meta->name) {
            result += "type=";
            result += _meta->name;
            result += ", ";
        }
        result += _value.to_debug_string(time);
        result += "}";
        return result;
    }

    // TODO: Register with visitor system
    // VISITOR_SUPPORT()

private:
    const TimeSeriesTypeMeta* _meta{nullptr};
    node_ptr _owning_node{nullptr};
    value::TimeSeriesValue _value;
};

} // namespace hgraph::ts

#endif // HGRAPH_TS_OUTPUT_H
